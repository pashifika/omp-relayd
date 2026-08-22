//! Shared test harness: a relay on an ephemeral loopback port, and a minimal
//! framed MessagePack client that can also emit bytes a correct client never
//! would.
//!
//! Every test binary that includes this module uses a different subset of it.
#![allow(dead_code)]

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use omp_relayd::protocol::{self, ClientFrame, PROTOCOL_VERSION, RoomId, ServerFrame};
use omp_relayd::relay::{self, Deadlines, ServerState};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

/// Ceiling on any single expected read. Generous: it exists to fail a hung test
/// with a message instead of hanging the suite.
pub const READ_TIMEOUT: Duration = Duration::from_secs(5);

/// How long a "nothing arrives" assertion waits before concluding silence.
pub const QUIET: Duration = Duration::from_millis(250);

/// A relay listening on loopback, with its own state and shutdown signal.
pub struct Relay {
    /// The bound address, with the port the kernel chose.
    pub addr: SocketAddr,
    /// The registry the relay is using, for assertions that look inside.
    pub state: Arc<ServerState>,
    shutdown: watch::Sender<bool>,
    accept_loop: Option<JoinHandle<()>>,
}

impl Relay {
    /// Starts a relay with the protocol's deadlines.
    pub async fn start() -> Self {
        Self::with_deadlines(Deadlines::default()).await
    }

    /// Starts a relay with deadlines shortened for a timeout test.
    pub async fn with_deadlines(deadlines: Deadlines) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind an ephemeral loopback port");
        let addr = listener.local_addr().expect("read the bound address");
        let state = Arc::new(ServerState::with_deadlines(deadlines));
        let (shutdown, shutdown_rx) = watch::channel(false);
        let accept_loop = tokio::spawn(relay::serve(listener, Arc::clone(&state), shutdown_rx));

        Self {
            addr,
            state,
            shutdown,
            accept_loop: Some(accept_loop),
        }
    }

    /// Signals shutdown and waits for the accept loop to finish.
    pub async fn shutdown(&mut self) {
        let _ = self.shutdown.send(true);
        if let Some(accept_loop) = self.accept_loop.take() {
            timeout(READ_TIMEOUT, accept_loop)
                .await
                .expect("the accept loop stops after a shutdown signal")
                .expect("the accept loop did not panic");
        }
    }
}

impl Drop for Relay {
    fn drop(&mut self) {
        // A test that never calls `shutdown` must not leave the accept loop
        // running for the rest of the binary.
        let _ = self.shutdown.send(true);
    }
}

/// A framed MessagePack client over any byte stream.
pub struct Client<S> {
    framed: Framed<S, LengthDelimitedCodec>,
}

impl Client<TcpStream> {
    /// Connects to `relay` without handshaking.
    pub async fn connect(relay: &Relay) -> Self {
        let stream = TcpStream::connect(relay.addr)
            .await
            .expect("connect to the relay");
        Self::new(stream)
    }

    /// Connects and completes a successful handshake.
    pub async fn join(relay: &Relay, room: &RoomId, peer: &str) -> Self {
        let mut client = Self::connect(relay).await;
        client.handshake(room, peer).await;
        client
    }
}

impl<S> Client<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    /// Wraps an already-connected stream in the protocol framing.
    pub fn new(io: S) -> Self {
        Self {
            framed: protocol::framed(io),
        }
    }

    /// Sends `hello` and asserts `ready` comes back.
    pub async fn handshake(&mut self, room: &RoomId, peer: &str) {
        self.send(&ClientFrame::Hello {
            protocol: PROTOCOL_VERSION,
            room: room.clone(),
            peer: peer.to_owned(),
        })
        .await;

        assert_eq!(
            self.recv().await,
            ServerFrame::Ready {
                protocol: PROTOCOL_VERSION
            },
            "handshake for peer {peer}"
        );
    }

    /// Encodes and sends one client frame.
    pub async fn send(&mut self, frame: &ClientFrame) {
        let payload = protocol::encode(frame).expect("encode a client frame");
        self.framed.send(payload).await.expect("write a frame");
    }

    /// Sends an already-encoded payload, framed normally.
    ///
    /// For payloads a well-formed client cannot produce: an explicit nil, an
    /// unknown field, an unknown frame type, or bytes that are not a map.
    pub async fn send_payload(&mut self, payload: Vec<u8>) {
        self.framed
            .send(payload.into())
            .await
            .expect("write a raw payload");
    }

    /// Writes bytes straight to the stream, bypassing the framing codec.
    ///
    /// For length-prefix cases: an oversized declaration, or a zero length.
    pub async fn send_unframed(&mut self, bytes: &[u8]) {
        let io = self.framed.get_mut();
        io.write_all(bytes).await.expect("write raw bytes");
        io.flush().await.expect("flush raw bytes");
    }

    /// Reads the next server frame, failing the test if none arrives.
    pub async fn recv(&mut self) -> ServerFrame {
        let payload = timeout(READ_TIMEOUT, self.framed.next())
            .await
            .expect("a frame arrives within the read timeout")
            .expect("the connection is still open")
            .expect("the frame is readable");

        protocol::decode(&payload).expect("the server sent a decodable frame")
    }

    /// Reads the next server frame, or returns `None` if the connection stays
    /// quiet for `within`.
    pub async fn recv_within(&mut self, within: Duration) -> Option<ServerFrame> {
        let read = timeout(within, self.framed.next()).await.ok()?;
        let payload = read
            .expect("the connection is still open")
            .expect("the frame is readable");
        Some(protocol::decode(&payload).expect("the server sent a decodable frame"))
    }

    /// Asserts the relay closed the connection.
    pub async fn expect_closed(&mut self) {
        let read = timeout(READ_TIMEOUT, self.framed.next())
            .await
            .expect("the relay closes the connection within the read timeout");

        match read {
            // An orderly end of stream, or a reset: either way it is closed.
            None | Some(Err(_)) => {}
            Some(Ok(payload)) => {
                let frame: Result<ServerFrame, _> = protocol::decode(&payload);
                panic!("expected the connection to close, received {frame:?}");
            }
        }
    }

    /// Asserts the relay sent an `error` with `code`, then closed.
    pub async fn expect_error_then_close(&mut self, code: omp_relayd::protocol::ErrorCode) {
        match self.recv().await {
            ServerFrame::Error {
                code: received,
                message,
                ..
            } => {
                assert_eq!(
                    received, code,
                    "expected error code {code:?}, received {received:?} with message {message:?}"
                );
            }
            other => panic!("expected error code {code:?}, received {other:?}"),
        }
        self.expect_closed().await;
    }
}

/// A room to run a test in.
pub fn room(task: &str) -> RoomId {
    RoomId::new("omp-relayd", task)
}

/// A four-byte big-endian length prefix.
pub fn length_prefix(length: u32) -> [u8; 4] {
    length.to_be_bytes()
}

/// Waits until `peer` is no longer registered in `room`.
///
/// A client disconnecting becomes observable to the relay only when its
/// connection task notices, so a test that depends on deregistration must wait
/// for it rather than assume it has happened.
pub async fn wait_until_deregistered(relay: &Relay, room: &RoomId, peer: &str) {
    for _ in 0..200 {
        if !relay.state.list_peers(room).iter().any(|name| name == peer) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("{peer} was still registered in {room} after 2 seconds");
}
