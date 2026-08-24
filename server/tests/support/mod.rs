//! Shared test harness: a relay on an ephemeral loopback port, and a minimal
//! framed MessagePack client that can also emit bytes a correct client never
//! would.
//!
//! Every test binary that includes this module uses a different subset of it.
#![allow(dead_code)]

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use omp_relayd::blob;
use omp_relayd::protocol::{self, ClientFrame, PROTOCOL_VERSION, RoomId, ServerFrame};
use omp_relayd::relay::{self, Deadlines, ServerState};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
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
    store: Option<Arc<blob::Store>>,
    store_base: Option<PathBuf>,
    maintenance: Option<blob::Maintenance>,
    shutdown: watch::Sender<bool>,
    accept_loop: Option<JoinHandle<()>>,
}

impl Relay {
    /// Starts a relay with the protocol's deadlines and no payload store.
    pub async fn start() -> Self {
        Self::with_deadlines(Deadlines::default()).await
    }

    /// Starts a relay with deadlines shortened for a timeout test.
    pub async fn with_deadlines(deadlines: Deadlines) -> Self {
        Self::build(deadlines, None).await
    }

    /// Starts a relay with a payload store rooted in its own temporary
    /// directory.
    ///
    /// `name` distinguishes one test's store from another's, which matters more
    /// than usual here: a store's first act is to remove whatever a predecessor
    /// of the same name left behind, so two tests sharing a name would remove
    /// each other's payloads.
    pub async fn with_store(name: &str) -> Self {
        Self::with_store_and_deadlines(name, Deadlines::default()).await
    }

    /// A store-backed relay whose deadlines a timeout test can shorten.
    pub async fn with_store_and_deadlines(name: &str, deadlines: Deadlines) -> Self {
        let base =
            std::env::temp_dir().join(format!("omp-relayd-it-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&base);
        let (store, maintenance) = blob::Store::open(&base, name)
            .await
            .expect("the payload store opens");
        let mut relay = Self::build(deadlines, Some(Arc::clone(&store))).await;
        relay.store = Some(store);
        relay.store_base = Some(base);
        relay.maintenance = Some(maintenance);
        relay
    }

    async fn build(deadlines: Deadlines, blobs: Option<Arc<blob::Store>>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind an ephemeral loopback port");
        let addr = listener.local_addr().expect("read the bound address");
        let mut state = ServerState::with_deadlines(deadlines);
        if let Some(blobs) = blobs {
            state = state.with_blobs(blobs);
        }
        let state = Arc::new(state);
        let (shutdown, shutdown_rx) = watch::channel(false);
        let accept_loop = tokio::spawn(relay::serve(listener, Arc::clone(&state), shutdown_rx));

        Self {
            addr,
            state,
            store: None,
            store_base: None,
            maintenance: None,
            shutdown,
            accept_loop: Some(accept_loop),
        }
    }

    /// The payload store, for a test that reserves or inspects directly.
    pub fn store(&self) -> &Arc<blob::Store> {
        self.store
            .as_ref()
            .expect("this relay was started without a payload store")
    }

    /// Reserves `bytes` under `digest`, as a `reserve` frame will once one
    /// exists.
    pub fn reserve(&self, room: &RoomId, digest: &str, bytes: u64) -> blob::Grant {
        self.store()
            .reserve(room, digest, bytes)
            .expect("the reservation is granted")
    }

    /// Performs the removals the store has queued, so a test can assert the
    /// filesystem rather than the queue.
    ///
    /// Deliberately not `Maintenance::run` to completion: its shutdown path
    /// removes the whole store root, which would make every filesystem
    /// assertion after it vacuously true.
    pub async fn drain_removals(&mut self) {
        if let Some(maintenance) = self.maintenance.as_mut() {
            maintenance.drain().await;
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
        // Nor leave a store directory in the platform's temporary directory:
        // the store removes its own root only on a graceful maintenance
        // shutdown, which most tests never reach.
        if let Some(base) = self.store_base.take() {
            let _ = std::fs::remove_dir_all(base);
        }
    }
}

/// One HTTP response, as a transfer test needs to see it.
#[derive(Debug)]
pub struct HttpResponse {
    /// Status code.
    pub status: u16,
    /// Header names lowercased, values as sent.
    pub headers: Vec<(String, String)>,
    /// Body bytes, empty for a `HEAD` or a status-only answer.
    pub body: Vec<u8>,
}

impl HttpResponse {
    /// First value of `name`, which is lowercased before the lookup.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.as_str())
    }
}

/// A transfer route for `room` and `digest`, with the room's components
/// percent-encoded as a client must send them.
pub fn blob_path(room: &RoomId, digest: &str) -> String {
    format!(
        "/blob/{}/{}/{digest}",
        percent_encode(&room.project),
        percent_encode(&room.task)
    )
}

/// Percent-encodes everything outside the unreserved set, and `.` as well.
///
/// `encodeURIComponent` leaves `.` alone, which would put a literal `..` in the
/// path of a room named `..`/`..` -- admissible under every identifier rule. The
/// store hashes the room's components, so nothing can traverse either way; what
/// a literal `..` would break is reachability, because any intermediary that
/// normalizes a path would rewrite the route out from under the request.
/// Encoding the dot makes the segment opaque to normalization.
pub fn percent_encode(value: &str) -> String {
    use std::fmt::Write;

    let mut out = String::with_capacity(value.len());
    for byte in value.as_bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'~') {
            out.push(char::from(*byte));
        } else {
            let _ = write!(out, "%{byte:02X}");
        }
    }
    out
}

/// Sends one request and reads the whole answer.
///
/// Reads while it writes, which is what a real HTTP client does and what this
/// surface requires: a `PUT` refused before its body is read is answered at
/// once and the connection closes, so a client that finished writing before it
/// started reading would lose the answer to a connection reset. A write failure
/// is therefore an expected outcome here rather than a test failure.
///
/// The write half is closed after the body, so a server still waiting on a
/// declared length that will never arrive sees an end rather than a stall.
///
/// One request per connection with `Connection: close`, so the body is whatever
/// arrives before the end and no response framing is reimplemented here.
pub async fn http(
    addr: SocketAddr,
    method: &str,
    path: &str,
    headers: &[(&str, String)],
    body: &[u8],
) -> HttpResponse {
    let stream = TcpStream::connect(addr)
        .await
        .expect("connect to the relay");
    let (mut reading, mut writing) = stream.into_split();

    let mut request = format!("{method} {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n");
    for (name, value) in headers {
        let _ = std::fmt::Write::write_fmt(&mut request, format_args!("{name}: {value}\r\n"));
    }
    request.push_str("\r\n");
    let body = body.to_vec();

    let writer = tokio::spawn(async move {
        writing.write_all(request.as_bytes()).await?;
        if !body.is_empty() {
            writing.write_all(&body).await?;
        }
        writing.flush().await?;
        writing.shutdown().await
    });

    let mut raw = Vec::new();
    let read = timeout(READ_TIMEOUT, reading.read_to_end(&mut raw))
        .await
        .expect("the response arrives");
    // Ignored deliberately: see above.
    let _ = writer.await;

    assert!(
        !(read.is_err() && raw.is_empty()),
        "the connection failed before any response arrived: {read:?}"
    );
    parse_response(&raw)
}

/// A `PUT` declaring `body.len()` bytes and sending them.
pub async fn http_put(addr: SocketAddr, path: &str, body: &[u8]) -> HttpResponse {
    http(
        addr,
        "PUT",
        path,
        &[("Content-Length", body.len().to_string())],
        body,
    )
    .await
}

/// A `PUT` whose declared length is chosen independently of what it sends.
pub async fn http_put_declaring(
    addr: SocketAddr,
    path: &str,
    declared: usize,
    body: &[u8],
) -> HttpResponse {
    http(
        addr,
        "PUT",
        path,
        &[("Content-Length", declared.to_string())],
        body,
    )
    .await
}

/// Splits a raw response into status, headers, and body.
fn parse_response(raw: &[u8]) -> HttpResponse {
    let separator = raw
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .unwrap_or_else(|| panic!("no header terminator in {} bytes", raw.len()));
    let head = std::str::from_utf8(&raw[..separator]).expect("headers are UTF-8");
    let mut lines = head.split("\r\n");
    let status_line = lines.next().expect("a status line");
    let status = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse().ok())
        .unwrap_or_else(|| panic!("no status code in {status_line:?}"));

    let headers = lines
        .filter_map(|line| line.split_once(':'))
        .map(|(name, value)| (name.trim().to_ascii_lowercase(), value.trim().to_owned()))
        .collect();

    HttpResponse {
        status,
        headers,
        body: raw[separator + 4..].to_vec(),
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

    /// Half-closes the connection, so the relay reads end of stream while a
    /// declared payload is still outstanding.
    pub async fn shutdown_write(&mut self) {
        self.framed
            .get_mut()
            .shutdown()
            .await
            .expect("shut down the write half");
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

    /// Drains frames until an `error` arrives, returning it, or `None` if the
    /// relay closed or fell silent without one. Prints how many frames it
    /// skipped and why it stopped.
    ///
    /// For a close that is only observable behind a backlog: a peer that
    /// stopped reading has queued deliveries and replies ahead of its
    /// diagnostic, so [`Client::expect_error_then_close`] trips over the first
    /// of them.
    pub async fn drain_until_error(&mut self) -> Option<ServerFrame> {
        let mut skipped = 0usize;
        loop {
            match timeout(READ_TIMEOUT, self.framed.next()).await {
                Ok(Some(Ok(payload))) => {
                    let frame: ServerFrame =
                        protocol::decode(&payload).expect("the server sent a decodable frame");
                    if matches!(frame, ServerFrame::Error { .. }) {
                        println!("error frame arrived behind {skipped} queued frame(s)");
                        return Some(frame);
                    }
                    skipped += 1;
                }
                // An orderly end of stream or a reset: the close named no cause.
                Ok(None | Some(Err(_))) => {
                    println!("bare close: {skipped} queued frame(s), then EOF, no error frame");
                    return None;
                }
                Err(_) => {
                    println!("no frame within {READ_TIMEOUT:?} behind {skipped} queued frame(s)");
                    return None;
                }
            }
        }
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
