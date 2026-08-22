//! Shared registry, routing, and the per-connection task.
//!
//! # Lock discipline
//!
//! The room registry is a [`std::sync::RwLock`], deliberately not an async
//! lock. Its guard is `!Send`, so a future that holds one across an `.await`
//! cannot satisfy the `Send` bound [`tokio::spawn`] requires: the invariant
//! "never hold a room lock across `.await`, socket I/O, or serialization" is
//! rejected by the compiler rather than by review. The same choice lets
//! deregistration run in [`Drop`], which no async lock permits.

use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::ops::ControlFlow;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::Duration;

use bytes::BytesMut;
use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;
use tokio::time::{Instant, sleep_until, timeout, timeout_at};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

use crate::protocol::{
    self, ClientFrame, ErrorCode, PROTOCOL_VERSION, ReceiptStatus, RoomId, ServerFrame,
};

/// Outbound queue depth per registered peer.
///
/// A code constant, not a setting: the value is referenced by the backpressure
/// test, and exposing it would enlarge the operational surface with no
/// demonstrated need. At the 64 KiB frame cap a full queue is roughly 8 MiB for
/// one stalled peer, which is the memory ceiling this constant buys.
pub const OUTBOUND_QUEUE_CAPACITY: usize = 128;

/// Time a connection has to send `hello` before it is closed.
pub const HELLO_DEADLINE: Duration = Duration::from_secs(5);

/// Time a connection may go without sending a valid frame before it is closed.
pub const IDLE_DEADLINE: Duration = Duration::from_secs(90);

/// Ceiling on one best-effort diagnostic write to a peer that is being closed.
/// Without it a peer that has stopped reading could pin the task indefinitely.
const TERMINAL_WRITE_TIMEOUT: Duration = Duration::from_secs(1);

/// Ceiling on the diagnostic text carried in an `error` frame.
///
/// Clients branch on `code` and never parse `message`, so truncation costs
/// nothing they rely on. It buys two things: an `error` frame that always fits
/// the frame cap, and a bound on how much of a rejected payload can be quoted
/// back at its sender.
const MAX_DIAGNOSTIC_BYTES: usize = 1024;

/// How long [`serve`] lets connection tasks notice shutdown and close their
/// sockets. This waits on relay tasks, never on a client acknowledging
/// anything.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(2);

/// Pause after a failed `accept` so a persistent failure (a descriptor limit,
/// most likely) cannot spin the accept loop at full speed.
const ACCEPT_BACKOFF: Duration = Duration::from_millis(100);

/// Connection deadlines.
///
/// These are protocol constants, and [`Default`] is the only value the binary
/// ever uses; there is no flag, environment variable, or configuration file for
/// them. They are a struct so that tests can drive the timeout paths in
/// milliseconds against real sockets instead of waiting out the real deadlines
/// or depending on a virtual clock that also governs the test client.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Deadlines {
    /// Handshake deadline; see [`HELLO_DEADLINE`].
    pub hello: Duration,
    /// Idle deadline; see [`IDLE_DEADLINE`].
    pub idle: Duration,
}

impl Default for Deadlines {
    fn default() -> Self {
        Self {
            hello: HELLO_DEADLINE,
            idle: IDLE_DEADLINE,
        }
    }
}

/// One registered peer's outbound queue, tagged with the connection that owns
/// it.
#[derive(Clone, Debug)]
struct PeerHandle {
    connection_id: u64,
    outbound: mpsc::Sender<ServerFrame>,
}

/// Outcome of registering a peer name.
#[derive(Clone, Copy, Debug)]
struct Registration {
    connection_id: u64,
    superseded: Option<u64>,
}

type Rooms = HashMap<RoomId, HashMap<String, PeerHandle>>;

/// Registry of rooms and their registered peers.
///
/// The relay keeps nothing else: no message history, no queue for absent peers,
/// no persistence.
#[derive(Debug)]
pub struct ServerState {
    rooms: RwLock<Rooms>,
    next_connection_id: AtomicU64,
    deadlines: Deadlines,
}

impl Default for ServerState {
    fn default() -> Self {
        Self::new()
    }
}

impl ServerState {
    /// Empty registry with the protocol's deadlines.
    pub fn new() -> Self {
        Self::with_deadlines(Deadlines::default())
    }

    /// Empty registry with deadlines overridden, for tests.
    pub fn with_deadlines(deadlines: Deadlines) -> Self {
        Self {
            rooms: RwLock::new(HashMap::new()),
            // Ids start at 1 so that 0 is never a live connection.
            next_connection_id: AtomicU64::new(1),
            deadlines,
        }
    }

    /// The deadlines this relay enforces.
    pub fn deadlines(&self) -> Deadlines {
        self.deadlines
    }

    /// A poisoned registry is still a consistent one: every critical section
    /// here is a handful of `HashMap` operations with no intermediate invalid
    /// state, so recovering the guard is strictly better than propagating one
    /// task's panic to every other connection.
    fn read_rooms(&self) -> RwLockReadGuard<'_, Rooms> {
        self.rooms.read().unwrap_or_else(PoisonError::into_inner)
    }

    fn write_rooms(&self) -> RwLockWriteGuard<'_, Rooms> {
        self.rooms.write().unwrap_or_else(PoisonError::into_inner)
    }

    /// Registers `peer` in `room`, superseding any existing connection of the
    /// same name.
    ///
    /// Dropping the superseded [`PeerHandle`] closes its outbound channel,
    /// which is how the superseded connection task learns it was replaced. That
    /// signal cannot be lost to a full queue, which is exactly the state a
    /// stalled connection being replaced is likely to be in.
    fn register(
        &self,
        room: &RoomId,
        peer: &str,
        outbound: mpsc::Sender<ServerFrame>,
    ) -> Registration {
        let connection_id = self.next_connection_id.fetch_add(1, Ordering::Relaxed);
        let handle = PeerHandle {
            connection_id,
            outbound,
        };

        let superseded = self
            .write_rooms()
            .entry(room.clone())
            .or_default()
            .insert(peer.to_owned(), handle)
            .map(|previous| previous.connection_id);

        Registration {
            connection_id,
            superseded,
        }
    }

    /// Removes `peer` from `room`, but only while the registered connection is
    /// still `connection_id`.
    ///
    /// The generation check is what stops a slow-finishing connection from
    /// evicting the replacement that already took its name. Returns whether an
    /// entry was removed.
    fn deregister(&self, room: &RoomId, peer: &str, connection_id: u64) -> bool {
        let mut rooms = self.write_rooms();

        let Some(peers) = rooms.get_mut(room) else {
            return false;
        };
        if peers.get(peer).map(|handle| handle.connection_id) != Some(connection_id) {
            return false;
        }
        peers.remove(peer);

        // An emptied room is dropped so that a long-lived relay's memory
        // tracks live peers rather than every room name it has ever seen.
        if peers.is_empty() {
            rooms.remove(room);
        }
        true
    }

    /// Peer names registered in `room`, bytewise ascending.
    pub fn list_peers(&self, room: &RoomId) -> Vec<String> {
        let mut names: Vec<String> = {
            let rooms = self.read_rooms();
            rooms
                .get(room)
                .map(|peers| peers.keys().cloned().collect())
                .unwrap_or_default()
        };
        // Sorting happens after the guard is released.
        names.sort_unstable();
        names
    }

    /// Places `frame` in the outbound queue of `to` within `room`.
    ///
    /// The room lock is released before the enqueue attempt, and the attempt is
    /// non-blocking: a full queue becomes an observable
    /// [`ReceiptStatus::RecipientBackpressure`] rather than a stalled sender or
    /// unbounded memory growth.
    pub fn route(&self, room: &RoomId, to: &str, frame: ServerFrame) -> ReceiptStatus {
        let outbound = {
            let rooms = self.read_rooms();
            rooms
                .get(room)
                .and_then(|peers| peers.get(to))
                .map(|handle| handle.outbound.clone())
        };

        let Some(outbound) = outbound else {
            return ReceiptStatus::PeerOffline;
        };

        match outbound.try_send(frame) {
            Ok(()) => ReceiptStatus::Routed,
            Err(TrySendError::Full(_)) => ReceiptStatus::RecipientBackpressure,
            // The recipient's task has already dropped its receiver; it is
            // gone even though its registry entry has not been reaped yet.
            Err(TrySendError::Closed(_)) => ReceiptStatus::PeerOffline,
        }
    }
}

/// Owns a registry entry for as long as its connection lives.
///
/// Deregistration in [`Drop`] runs on every exit path, including an unwinding
/// panic, which no `break`-and-cleanup arrangement guarantees.
struct RegisteredPeer {
    state: Arc<ServerState>,
    room: RoomId,
    peer: String,
    connection_id: u64,
}

impl Drop for RegisteredPeer {
    fn drop(&mut self) {
        if self
            .state
            .deregister(&self.room, &self.peer, self.connection_id)
        {
            tracing::info!(
                room = %self.room,
                peer = %self.peer,
                connection_id = self.connection_id,
                "peer deregistered"
            );
        } else {
            // The name now belongs to a newer connection; leave it alone.
            tracing::debug!(
                room = %self.room,
                peer = %self.peer,
                connection_id = self.connection_id,
                "superseded connection left the registry untouched"
            );
        }
    }
}

/// Accepts connections until `shutdown` is signalled.
///
/// On shutdown the listener stops accepting, live connection tasks are told to
/// stop, and the function returns once they have finished or `SHUTDOWN_GRACE`
/// elapses.
pub async fn serve(
    listener: TcpListener,
    state: Arc<ServerState>,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut connections = JoinSet::new();
    // Handed to each connection task. Kept separate from the receiver `serve`
    // waits on, which is mutably borrowed by the select below.
    let child_shutdown = shutdown.clone();

    loop {
        tokio::select! {
            () = wait_for_shutdown(&mut shutdown) => break,

            // Reap finished tasks so the set tracks live connections rather
            // than every connection ever accepted.
            Some(_) = connections.join_next(), if !connections.is_empty() => {}

            accepted = listener.accept() => match accepted {
                Ok((stream, peer_addr)) => {
                    // Frames are small and latency-sensitive; Nagle batching
                    // would add a round trip of delay for no bandwidth gain.
                    if let Err(error) = stream.set_nodelay(true) {
                        tracing::debug!(%peer_addr, %error, "could not disable Nagle batching");
                    }
                    tracing::info!(%peer_addr, "connection accepted");
                    connections.spawn(run_connection(
                        stream,
                        Arc::clone(&state),
                        child_shutdown.clone(),
                        peer_addr,
                    ));
                }
                Err(error) => {
                    tracing::warn!(%error, "accept failed");
                    tokio::time::sleep(ACCEPT_BACKOFF).await;
                }
            },
        }
    }

    tracing::info!(
        live_connections = connections.len(),
        "graceful shutdown: listener stopped"
    );

    let drained = timeout(SHUTDOWN_GRACE, async {
        while connections.join_next().await.is_some() {}
    })
    .await;

    if drained.is_err() {
        tracing::warn!(
            live_connections = connections.len(),
            grace = ?SHUTDOWN_GRACE,
            "graceful shutdown: exiting without waiting further"
        );
    }
}

/// Drives one client connection from handshake to close.
///
/// Generic over the stream so that a later private-transport change can pass a
/// TLS stream without touching framing, routing, or protocol handling. This
/// change only ever passes a [`tokio::net::TcpStream`].
pub async fn run_connection<S>(
    io: S,
    state: Arc<ServerState>,
    mut shutdown: watch::Receiver<bool>,
    peer_addr: SocketAddr,
) where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let mut framed = protocol::framed(io);

    if let Some((room, peer)) =
        admit(&mut framed, &mut shutdown, state.deadlines(), peer_addr).await
    {
        // From here every exit path deregisters: `registered` owns the registry
        // entry and drops at the end of this block, before the close log below,
        // including if the block unwinds.
        let (registered, outbound) = register_peer(&state, room, peer, peer_addr);

        // Bounded: the handshake is not complete until `ready` is out, and a
        // peer that never reads must not park this task before it is even
        // registered.
        if timeout(
            state.deadlines().hello,
            write_frame(
                &mut framed,
                &ServerFrame::Ready {
                    protocol: PROTOCOL_VERSION,
                },
            ),
        )
        .await
        .is_ok_and(|written| written.is_ok())
        {
            pump(
                &mut framed,
                &state,
                &registered,
                outbound,
                &mut shutdown,
                peer_addr,
            )
            .await;
        }
    }

    tracing::info!(%peer_addr, "connection closed");
}

/// Claims the peer name and returns the guard that owns it, along with the
/// receiving end of its outbound queue.
fn register_peer(
    state: &Arc<ServerState>,
    room: RoomId,
    peer: String,
    peer_addr: SocketAddr,
) -> (RegisteredPeer, mpsc::Receiver<ServerFrame>) {
    let (outbound_tx, outbound_rx) = mpsc::channel(OUTBOUND_QUEUE_CAPACITY);
    let registration = state.register(&room, &peer, outbound_tx);
    let connection_id = registration.connection_id;

    if let Some(superseded_connection_id) = registration.superseded {
        tracing::info!(
            %peer_addr,
            room = %room,
            peer = %peer,
            connection_id,
            superseded_connection_id,
            "peer replaced"
        );
    } else {
        tracing::info!(%peer_addr, room = %room, peer = %peer, connection_id, "peer registered");
    }

    (
        RegisteredPeer {
            state: Arc::clone(state),
            room,
            peer,
            connection_id,
        },
        outbound_rx,
    )
}

/// Drives a registered connection until it closes.
///
/// The four events a live connection waits on are inbound frames, its outbound
/// queue, the idle deadline, and shutdown. Branch selection is deliberately
/// left unbiased: a connection that both sends and receives at full rate must
/// not starve its own writes behind its own reads.
async fn pump<S>(
    framed: &mut Framed<S, LengthDelimitedCodec>,
    state: &ServerState,
    registered: &RegisteredPeer,
    mut outbound: mpsc::Receiver<ServerFrame>,
    shutdown: &mut watch::Receiver<bool>,
    peer_addr: SocketAddr,
) where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let deadlines = state.deadlines();
    // Tracked as an instant rather than a pinned `Sleep` because every socket
    // write is also bounded by it, and an instant is a value that can be handed
    // to `timeout_at` while a timer object cannot.
    let mut idle_deadline = Instant::now() + deadlines.idle;

    loop {
        tokio::select! {
            () = wait_for_shutdown(shutdown) => {
                tracing::debug!(
                    connection_id = registered.connection_id,
                    "closing connection for shutdown"
                );
                break;
            }

            () = sleep_until(idle_deadline) => {
                log_heartbeat_timeout(registered, deadlines.idle);
                close_with(
                    framed,
                    ErrorCode::IdleTimeout,
                    format!("no frame within {:?}", deadlines.idle),
                )
                .await;
                break;
            }

            queued = outbound.recv() => {
                if let Some(frame) = queued {
                    // Bounded by the same deadline the branch above enforces. A
                    // socket write to a peer that has stopped reading pends
                    // indefinitely, and because it runs in this handler rather
                    // than in the `select!`, it would otherwise stop the idle
                    // branch from ever being polled again -- parking the task,
                    // its file descriptor, and up to 8 MiB of queued frames for
                    // as long as the peer cares to hold them.
                    match timeout_at(idle_deadline, write_frame(framed, &frame)).await {
                        Ok(Ok(())) => {}
                        Ok(Err(_)) => break,
                        Err(_elapsed) => {
                            log_heartbeat_timeout(registered, deadlines.idle);
                            close_with(
                                framed,
                                ErrorCode::IdleTimeout,
                                format!("no frame within {:?}", deadlines.idle),
                            )
                            .await;
                            break;
                        }
                    }
                } else {
                    // The registry dropped this connection's sender: a newer
                    // connection has taken the peer name.
                    tracing::info!(
                        room = %registered.room,
                        peer = %registered.peer,
                        connection_id = registered.connection_id,
                        "closing superseded connection"
                    );
                    close_with(
                        framed,
                        ErrorCode::PeerReplaced,
                        "a newer connection registered this peer name".to_owned(),
                    )
                    .await;
                    break;
                }
            }

            inbound = framed.next() => {
                if handle_inbound(
                    framed,
                    state,
                    registered,
                    inbound,
                    &mut idle_deadline,
                    peer_addr,
                )
                .await
                .is_break()
                {
                    break;
                }
            }
        }
    }
}

/// Emits the heartbeat-timeout event, from either the idle branch or a write
/// that outlived the same deadline.
fn log_heartbeat_timeout(registered: &RegisteredPeer, deadline: Duration) {
    tracing::info!(
        room = %registered.room,
        peer = %registered.peer,
        connection_id = registered.connection_id,
        deadline = ?deadline,
        "heartbeat timeout"
    );
}

/// Decodes one inbound read and dispatches it.
///
/// Resets the idle deadline for any frame that decoded, which is what "valid
/// inbound frame" means: a frame the relay could read, whatever it then does
/// with it.
async fn handle_inbound<S>(
    framed: &mut Framed<S, LengthDelimitedCodec>,
    state: &ServerState,
    registered: &RegisteredPeer,
    read: Option<io::Result<BytesMut>>,
    idle_deadline: &mut Instant,
    peer_addr: SocketAddr,
) -> ControlFlow<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    // `None` is a clean end of stream: the peer hung up.
    let Some(read) = read else {
        return ControlFlow::Break(());
    };

    let payload = match read {
        Ok(payload) => payload,
        Err(error) => {
            report_framing_error(framed, &error, peer_addr).await;
            return ControlFlow::Break(());
        }
    };

    let frame: ClientFrame = match protocol::decode(&payload) {
        Ok(frame) => frame,
        Err(error) => {
            tracing::info!(
                %peer_addr,
                connection_id = registered.connection_id,
                payload_bytes = payload.len(),
                reason = error.kind(),
                "frame decode failed"
            );
            close_with(framed, ErrorCode::MalformedFrame, error.to_string()).await;
            return ControlFlow::Break(());
        }
    };

    *idle_deadline = Instant::now() + state.deadlines().idle;

    handle_frame(framed, state, registered, frame).await
}

/// Reads and validates `hello`, returning the admitted room and peer name.
///
/// Every rejection states its cause on the wire before the caller closes the
/// connection.
async fn admit<S>(
    framed: &mut Framed<S, LengthDelimitedCodec>,
    shutdown: &mut watch::Receiver<bool>,
    deadlines: Deadlines,
    peer_addr: SocketAddr,
) -> Option<(RoomId, String)>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let read = tokio::select! {
        () = wait_for_shutdown(shutdown) => return None,
        read = timeout(deadlines.hello, framed.next()) => read,
    };

    let payload = match read {
        Err(_elapsed) => {
            tracing::info!(%peer_addr, deadline = ?deadlines.hello, "handshake deadline elapsed");
            close_with(
                framed,
                ErrorCode::HelloTimeout,
                format!("no hello within {:?}", deadlines.hello),
            )
            .await;
            return None;
        }
        // Hung up before saying anything: nothing to diagnose.
        Ok(None) => return None,
        Ok(Some(Err(error))) => {
            report_framing_error(framed, &error, peer_addr).await;
            return None;
        }
        Ok(Some(Ok(payload))) => payload,
    };

    let frame: ClientFrame = match protocol::decode(&payload) {
        Ok(frame) => frame,
        Err(error) => {
            tracing::info!(
                %peer_addr,
                payload_bytes = payload.len(),
                reason = error.kind(),
                "frame decode failed"
            );
            close_with(framed, ErrorCode::MalformedFrame, error.to_string()).await;
            return None;
        }
    };

    // A connection that has not been admitted has no room, so no frame other
    // than `hello` is processable here -- including one carrying an
    // unrecognized `type`, which after admission would be answered with
    // `unsupported_frame` and kept open.
    let ClientFrame::Hello {
        protocol: offered,
        room,
        peer,
    } = frame
    else {
        let frame_type = frame.type_name();
        tracing::info!(%peer_addr, frame_type, "first frame was not hello");
        close_with(
            framed,
            ErrorCode::InvalidHello,
            format!("first frame must be hello, received {frame_type}"),
        )
        .await;
        return None;
    };

    if offered != PROTOCOL_VERSION {
        tracing::info!(
            %peer_addr,
            offered,
            supported = PROTOCOL_VERSION,
            "protocol version rejected"
        );
        close_with(
            framed,
            ErrorCode::UnsupportedProtocol,
            format!("protocol {offered} is not supported; this relay speaks {PROTOCOL_VERSION}"),
        )
        .await;
        return None;
    }

    for (field, value) in [
        ("room.project", &room.project),
        ("room.task", &room.task),
        ("peer", &peer),
    ] {
        if let Err(error) = protocol::validate_identifier(value) {
            tracing::info!(%peer_addr, field, %error, "identifier rejected");
            close_with(
                framed,
                ErrorCode::InvalidIdentifier,
                format!("{field} {error}"),
            )
            .await;
            return None;
        }
    }

    Some((room, peer))
}

/// Handles one decoded frame from a registered connection.
///
/// [`ControlFlow::Break`] means the connection must close.
async fn handle_frame<S>(
    framed: &mut Framed<S, LengthDelimitedCodec>,
    state: &ServerState,
    registered: &RegisteredPeer,
    frame: ClientFrame,
) -> ControlFlow<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    // `handle_inbound` has just reset the idle deadline, so this is that same
    // instant: every reply to this frame must be on the wire before it.
    let deadline = Instant::now() + state.deadlines().idle;

    match frame {
        ClientFrame::Hello { .. } => {
            tracing::info!(
                room = %registered.room,
                peer = %registered.peer,
                connection_id = registered.connection_id,
                "duplicate hello"
            );
            close_with(
                framed,
                ErrorCode::DuplicateHello,
                "this connection is already registered".to_owned(),
            )
            .await;
            ControlFlow::Break(())
        }

        ClientFrame::Ping => write_or_break(framed, &ServerFrame::Pong, deadline).await,

        ClientFrame::List { request_id } => {
            if let Err(error) = protocol::validate_correlation_id(&request_id) {
                return write_or_break(
                    framed,
                    &ServerFrame::Error {
                        code: ErrorCode::InvalidIdentifier,
                        message: Some(format!("list.request_id {error}")),
                        request_id: None,
                    },
                    deadline,
                )
                .await;
            }

            let peers = state.list_peers(&registered.room);
            write_or_break(framed, &ServerFrame::Peers { request_id, peers }, deadline).await
        }

        ClientFrame::Send {
            id,
            to,
            body,
            reply_to,
        } => handle_send(framed, state, registered, id, to, body, reply_to).await,

        ClientFrame::Unsupported => {
            tracing::info!(
                room = %registered.room,
                peer = %registered.peer,
                "unsupported frame type"
            );
            write_or_break(
                framed,
                &ServerFrame::Error {
                    code: ErrorCode::UnsupportedFrame,
                    message: Some("this protocol version does not implement that frame".to_owned()),
                    request_id: None,
                },
                deadline,
            )
            .await
        }
    }
}

/// Validates and routes one `send`, then reports its receipt.
///
/// The three checks are ordered by consequence. An invalid correlation id and
/// an invalid target are recoverable and keep the connection open; an
/// over-budget body is not, so it is checked last, once the frame is known to
/// be one the relay would otherwise have delivered.
async fn handle_send<S>(
    framed: &mut Framed<S, LengthDelimitedCodec>,
    state: &ServerState,
    registered: &RegisteredPeer,
    id: String,
    to: String,
    body: String,
    reply_to: Option<String>,
) -> ControlFlow<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let deadline = Instant::now() + state.deadlines().idle;

    if let Err(error) = protocol::validate_correlation_id(&id) {
        return write_or_break(
            framed,
            &ServerFrame::Error {
                code: ErrorCode::InvalidIdentifier,
                message: Some(format!("send.id {error}")),
                request_id: None,
            },
            deadline,
        )
        .await;
    }

    if let Err(error) = protocol::validate_identifier(&to) {
        tracing::debug!(
            room = %registered.room,
            peer = %registered.peer,
            %error,
            "send target rejected"
        );
        return write_or_break(
            framed,
            &ServerFrame::Receipt {
                id,
                to: bounded_target(to),
                status: ReceiptStatus::InvalidTarget,
            },
            deadline,
        )
        .await;
    }

    // `reply_to` is the other half of the envelope the body budget reserves
    // room for. Leaving it unchecked reopened exactly the attack the budget
    // closes: an oversized `reply_to` with an empty body passes every other
    // check, and the `message` built from it exceeds the frame cap, so the
    // encode failure lands on the recipient's connection.
    if let Some(error) = reply_to
        .as_deref()
        .and_then(|value| protocol::validate_correlation_id(value).err())
    {
        return write_or_break(
            framed,
            &ServerFrame::Error {
                code: ErrorCode::InvalidIdentifier,
                message: Some(format!("send.reply_to {error}")),
                // `id` passed validation above, so this is the one rejection on
                // the `send` path that can name the frame it rejected. A client
                // pipelining sends has no other way to tell which one failed:
                // the connection stays open, so the error arrives with no
                // positional relationship to anything.
                request_id: Some(id),
            },
            deadline,
        )
        .await;
    }

    // Checked before routing, so the encode failure never lands on the
    // recipient's connection: otherwise any sender could close any peer.
    if let Some(body_bytes) = protocol::body_over_budget(&body) {
        tracing::info!(
            room = %registered.room,
            peer = %registered.peer,
            connection_id = registered.connection_id,
            body_bytes,
            budget = protocol::MAX_BODY_BYTES,
            "body exceeds the relayable budget"
        );
        close_with(
            framed,
            ErrorCode::FrameTooLarge,
            format!(
                "body is {body_bytes} bytes; at most {} can be relayed within the {}-byte \
                 frame cap",
                protocol::MAX_BODY_BYTES,
                protocol::MAX_FRAME_BYTES
            ),
        )
        .await;
        return ControlFlow::Break(());
    }

    // `body` is moved into the delivered frame and is never a log field; only
    // its size is observable in logs.
    let body_bytes = body.len();
    let status = state.route(
        &registered.room,
        &to,
        ServerFrame::Message {
            id: id.clone(),
            from: registered.peer.clone(),
            body,
            reply_to,
        },
    );

    log_route(registered, &to, &id, body_bytes, status);

    write_or_break(framed, &ServerFrame::Receipt { id, to, status }, deadline).await
}

/// Clamps a rejected `to` value to the identifier limit before it is echoed.
///
/// `receipt.to` is the one field the relay echoes that is *not* already bounded
/// by a validated inbound value: on every other path `to` passed
/// [`protocol::validate_identifier`] and is at most
/// [`protocol::MAX_IDENTIFIER_BYTES`], but the `invalid_target` receipt echoes
/// the value that just failed that check -- including because it was too long.
///
/// Unclamped, that reopens the class of bug the body budget exists to close. A
/// `send` whose `to` is 65508 bytes fits the inbound frame cap exactly, and the
/// receipt built from it is 65555 bytes: `LengthDelimitedCodec::encode` refuses
/// it, `write_or_break` breaks on the `Err`, and the sender's connection closes
/// having received neither the receipt `peer-relay` requires for every valid
/// `send` nor the `error` that the close-with-a-stated-cause rule requires.
///
/// Clamping rather than closing is what keeps the promise that a target which
/// fails validation is recoverable and keeps the connection open. The excess
/// carries no information: a value past the limit is not a peer name, and the
/// client correlates the receipt by `id`, not by `to`.
fn bounded_target(mut to: String) -> String {
    if to.len() > protocol::MAX_IDENTIFIER_BYTES {
        let mut end = protocol::MAX_IDENTIFIER_BYTES;
        while end > 0 && !to.is_char_boundary(end) {
            end -= 1;
        }
        to.truncate(end);
    }
    to
}

/// Emits the routing outcome. Carries metadata only: never `body`.
fn log_route(
    registered: &RegisteredPeer,
    to: &str,
    id: &str,
    body_bytes: usize,
    status: ReceiptStatus,
) {
    let room = tracing::field::display(&registered.room);
    let from = registered.peer.as_str();
    let outcome = status.as_str();

    match status {
        ReceiptStatus::Routed => {
            tracing::debug!(%room, from, to, id, body_bytes, status = outcome, "message routed");
        }
        ReceiptStatus::RecipientBackpressure => {
            tracing::warn!(
                %room, from, to, id, body_bytes, status = outcome,
                "recipient queue full"
            );
        }
        ReceiptStatus::PeerOffline | ReceiptStatus::InvalidTarget => {
            tracing::info!(
                %room, from, to, id, body_bytes, status = outcome,
                "message not delivered"
            );
        }
    }
}

/// Encodes and writes one frame, flushing it.
async fn write_frame<S>(
    framed: &mut Framed<S, LengthDelimitedCodec>,
    frame: &ServerFrame,
) -> io::Result<()>
where
    S: AsyncWrite + Unpin,
{
    // Unreachable for the frames this relay builds, which hold only strings,
    // `u32`s, and vectors of strings.
    let payload = protocol::encode(frame).map_err(io::Error::other)?;
    framed.send(payload).await
}

/// Writes one frame, reporting whether the connection survived.
///
/// `deadline` bounds the socket write for the same reason the outbound queue's
/// write is bounded: a peer that has stopped reading would otherwise park this
/// task indefinitely, and a parked task stops enforcing its own deadlines.
async fn write_or_break<S>(
    framed: &mut Framed<S, LengthDelimitedCodec>,
    frame: &ServerFrame,
    deadline: Instant,
) -> ControlFlow<()>
where
    S: AsyncWrite + Unpin,
{
    match timeout_at(deadline, write_frame(framed, frame)).await {
        Ok(Ok(())) => ControlFlow::Continue(()),
        Ok(Err(error)) => {
            tracing::debug!(%error, "connection write failed");
            ControlFlow::Break(())
        }
        Err(_elapsed) => {
            tracing::info!("connection write outlived the idle deadline");
            // The relay is closing this connection on its own initiative, so
            // the same rule applies here as in the idle branch of `pump`: state
            // the cause. Without this the one close that a peer reaches by
            // being slow rather than by being wrong was the one close that
            // arrived bare. Bounded and best-effort like every other terminal
            // write, so a peer that is not reading cannot be helped -- but one
            // that resumes reading learns why it was dropped.
            close_with(
                framed,
                ErrorCode::IdleTimeout,
                "a reply could not be written within the idle deadline".to_owned(),
            )
            .await;
            ControlFlow::Break(())
        }
    }
}

/// Makes one bounded, best-effort attempt to name the cause of a close.
///
/// A bare EOF is indistinguishable from a crashed relay, a wrong port, or a
/// version mismatch, so every close the relay initiates states its reason. The
/// attempt is discarded if the socket is already broken or too slow.
async fn close_with<S>(
    framed: &mut Framed<S, LengthDelimitedCodec>,
    code: ErrorCode,
    mut message: String,
) where
    S: AsyncWrite + Unpin,
{
    // Two callers pass a decoder's error text, which quotes the payload it
    // rejected -- and quoting escapes, so a payload well inside the inbound cap
    // can produce a diagnostic well outside it. An unencodable `error` frame is
    // silently dropped by the `let _` below, leaving the peer with exactly the
    // bare EOF this function exists to prevent. Capping here fixes every call
    // site at once.
    if message.len() > MAX_DIAGNOSTIC_BYTES {
        let mut end = MAX_DIAGNOSTIC_BYTES;
        while end > 0 && !message.is_char_boundary(end) {
            end -= 1;
        }
        message.truncate(end);
    }

    let frame = ServerFrame::Error {
        code,
        message: Some(message),
        request_id: None,
    };
    let _ = timeout(TERMINAL_WRITE_TIMEOUT, write_frame(framed, &frame)).await;
}

/// Reports a framing-layer failure, naming its cause when the framing layer
/// knows one.
async fn report_framing_error<S>(
    framed: &mut Framed<S, LengthDelimitedCodec>,
    error: &io::Error,
    peer_addr: SocketAddr,
) where
    S: AsyncWrite + Unpin,
{
    if let Some(code) = protocol::framing_error_code(error) {
        tracing::info!(%peer_addr, code = code.as_str(), %error, "framing violation");
        close_with(framed, code, error.to_string()).await;
    } else {
        // A transport failure: there is no socket left to explain it on.
        tracing::debug!(%peer_addr, %error, "connection read failed");
    }
}

/// Resolves once shutdown has been signalled, or the supervisor is gone.
async fn wait_for_shutdown(shutdown: &mut watch::Receiver<bool>) {
    if *shutdown.borrow_and_update() {
        return;
    }
    // The flag transitions false -> true exactly once, so any change is the
    // signal. A dropped sender means the supervisor is gone, which is also a
    // reason to stop.
    let _ = shutdown.changed().await;
}

#[cfg(test)]
mod tests {
    use tokio::sync::mpsc::error::TryRecvError;

    use super::*;

    fn room() -> RoomId {
        RoomId::new("omp-relayd", "implement-tcp-relay-server")
    }

    fn peer_queue() -> (mpsc::Sender<ServerFrame>, mpsc::Receiver<ServerFrame>) {
        mpsc::channel(OUTBOUND_QUEUE_CAPACITY)
    }

    fn message(id: &str) -> ServerFrame {
        ServerFrame::Message {
            id: id.to_owned(),
            from: "macbook-reviewer".to_owned(),
            body: "review the diff".to_owned(),
            reply_to: None,
        }
    }

    #[test]
    fn defaults_match_the_protocol_constants() {
        assert_eq!(
            Deadlines::default(),
            Deadlines {
                hello: Duration::from_secs(5),
                idle: Duration::from_secs(90),
            },
            "tests override these; the defaults are the contract"
        );
        assert_eq!(OUTBOUND_QUEUE_CAPACITY, 128, "outbound queue depth");
    }

    #[test]
    fn replacement_reports_and_closes_the_superseded_connection() {
        let state = ServerState::new();

        let (first_tx, mut first_rx) = peer_queue();
        let first = state.register(&room(), "reviewer", first_tx);

        let (second_tx, _second_rx) = peer_queue();
        let second = state.register(&room(), "reviewer", second_tx);

        assert_eq!(
            second.superseded,
            Some(first.connection_id),
            "replacement must report which connection it superseded"
        );
        assert!(
            second.connection_id > first.connection_id,
            "connection ids must increase: first {}, second {}",
            first.connection_id,
            second.connection_id
        );
        assert!(
            matches!(first_rx.try_recv(), Err(TryRecvError::Disconnected)),
            "the superseded queue must close, which is how its task learns it was replaced"
        );
    }

    #[test]
    fn late_cleanup_of_a_superseded_connection_keeps_the_replacement() {
        let state = ServerState::new();

        let (first_tx, _first_rx) = peer_queue();
        let first = state.register(&room(), "reviewer", first_tx);

        let (second_tx, mut second_rx) = peer_queue();
        let second = state.register(&room(), "reviewer", second_tx);

        assert!(
            !state.deregister(&room(), "reviewer", first.connection_id),
            "connection {} no longer owns the name and must remove nothing",
            first.connection_id
        );
        assert_eq!(
            state.list_peers(&room()),
            vec!["reviewer"],
            "the replacement must still be registered"
        );
        assert_eq!(
            state.route(&room(), "reviewer", message("msg-1")),
            ReceiptStatus::Routed,
            "traffic must reach the replacement"
        );
        assert!(
            matches!(second_rx.try_recv(), Ok(ServerFrame::Message { .. })),
            "the replacement's queue must hold the routed frame"
        );

        assert!(
            state.deregister(&room(), "reviewer", second.connection_id),
            "the owning connection must be able to remove itself"
        );
        assert!(
            state.list_peers(&room()).is_empty(),
            "registry must be empty"
        );
    }

    #[test]
    fn listing_is_bytewise_sorted_and_scoped_to_one_room() {
        let state = ServerState::new();
        let other = RoomId::new("omp-relayd", "some-other-task");

        // Receivers are kept alive: a dropped one would close its queue.
        let mut queues = Vec::new();
        for (room, peer) in [
            (room(), "windows-main"),
            (room(), "macbook-reviewer"),
            (room(), "Reviewer"),
            (room(), "reviewer"),
            (other.clone(), "elsewhere"),
        ] {
            let (tx, rx) = peer_queue();
            queues.push(rx);
            state.register(&room, peer, tx);
        }

        assert_eq!(
            state.list_peers(&room()),
            vec!["Reviewer", "macbook-reviewer", "reviewer", "windows-main"],
            "bytewise ascending: `R` (0x52) sorts before `m` (0x6d), and case is never folded"
        );
        assert_eq!(
            state.list_peers(&other),
            vec!["elsewhere"],
            "listing must not leak peers of another room"
        );
        assert!(
            state.list_peers(&RoomId::new("absent", "room")).is_empty(),
            "an unknown room lists nothing"
        );
    }

    #[test]
    fn routing_reports_offline_success_and_backpressure() {
        let state = ServerState::new();
        let (tx, mut rx) = peer_queue();
        state.register(&room(), "windows-main", tx);

        assert_eq!(
            state.route(&room(), "nobody", message("msg-1")),
            ReceiptStatus::PeerOffline,
            "an unregistered name is offline"
        );
        assert_eq!(
            state.route(
                &RoomId::new("other", "task"),
                "windows-main",
                message("msg-1")
            ),
            ReceiptStatus::PeerOffline,
            "a peer of the same name in another room is not reachable"
        );

        for slot in 1..=OUTBOUND_QUEUE_CAPACITY {
            assert_eq!(
                state.route(&room(), "windows-main", message("msg-fill")),
                ReceiptStatus::Routed,
                "queue slot {slot} of {OUTBOUND_QUEUE_CAPACITY} must accept a frame"
            );
        }
        assert_eq!(
            state.route(&room(), "windows-main", message("msg-overflow")),
            ReceiptStatus::RecipientBackpressure,
            "frame {} must not fit a {OUTBOUND_QUEUE_CAPACITY}-slot queue",
            OUTBOUND_QUEUE_CAPACITY + 1
        );

        assert!(rx.try_recv().is_ok(), "draining frees a slot");
        assert_eq!(
            state.route(&room(), "windows-main", message("msg-after-drain")),
            ReceiptStatus::Routed,
            "backpressure must be transient, not a permanent state"
        );
    }

    #[test]
    fn a_gone_receiver_reads_as_offline() {
        let state = ServerState::new();
        let (tx, rx) = peer_queue();
        state.register(&room(), "windows-main", tx);
        drop(rx);

        assert_eq!(
            state.route(&room(), "windows-main", message("msg-1")),
            ReceiptStatus::PeerOffline,
            "a registered peer whose task is gone is offline, not backpressured"
        );
    }

    #[test]
    fn an_emptied_room_is_not_retained() {
        let state = ServerState::new();
        let (tx, _rx) = peer_queue();
        let registration = state.register(&room(), "solo", tx);

        assert_eq!(state.read_rooms().len(), 1, "room count after registration");
        assert!(state.deregister(&room(), "solo", registration.connection_id));
        assert_eq!(
            state.read_rooms().len(),
            0,
            "an emptied room must be dropped, so memory tracks live peers rather \
             than every room name ever used"
        );
    }

    #[test]
    fn dropping_the_guard_deregisters_the_peer() {
        let state = Arc::new(ServerState::new());
        let (tx, _rx) = peer_queue();
        let registration = state.register(&room(), "solo", tx);

        let guard = RegisteredPeer {
            state: Arc::clone(&state),
            room: room(),
            peer: "solo".to_owned(),
            connection_id: registration.connection_id,
        };
        assert_eq!(state.list_peers(&room()), vec!["solo"]);

        drop(guard);
        assert!(
            state.list_peers(&room()).is_empty(),
            "deregistration runs in Drop, so it covers every exit path including a panic"
        );
    }

    #[tokio::test]
    async fn a_write_that_outlives_its_deadline_closes_the_connection() {
        // A duplex with a 16-byte buffer whose read half is never drained: the
        // first frame larger than that blocks forever. Keeping `_client` alive
        // matters -- dropping it would fail the write immediately, which is a
        // different path from the one under test.
        let (server_io, _client) = tokio::io::duplex(16);
        let mut framed = protocol::framed(server_io);

        let outcome = write_or_break(
            &mut framed,
            &ServerFrame::Message {
                id: "msg-1".to_owned(),
                from: "sender".to_owned(),
                body: "x".repeat(4096),
                reply_to: None,
            },
            Instant::now() + Duration::from_millis(50),
        )
        .await;

        assert!(
            outcome.is_break(),
            "a write to a peer that never reads must be abandoned at its deadline, \
             because a parked write stops the task from enforcing any deadline"
        );
    }

    #[tokio::test]
    async fn a_cancelled_write_does_not_desynchronize_the_frame_stream() {
        use futures_util::StreamExt as _;

        // The claim under test: when `timeout_at` cancels `write_frame` after a
        // partial socket write, the un-flushed remainder stays in the codec's
        // write buffer, so the diagnostic written next is appended after it
        // rather than interleaved with it. If that were wrong, a length prefix
        // and its payload would desynchronize and every later frame on the
        // connection would be garbage.
        let (server_io, client_io) = tokio::io::duplex(1024);
        let mut server = protocol::framed(server_io);

        let large = ServerFrame::Message {
            id: "msg-1".to_owned(),
            from: "sender".to_owned(),
            body: "x".repeat(8 * 1024),
            reply_to: None,
        };
        let diagnostic = ServerFrame::Error {
            code: ErrorCode::IdleTimeout,
            message: Some("no frame within 400ms".to_owned()),
            request_id: None,
        };

        // Starts reading only after the first write has been cancelled, so the
        // cancellation lands mid-flush rather than before any byte moves.
        let reader = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(150)).await;
            let mut client = protocol::framed(client_io);
            let mut received = Vec::new();
            while received.len() < 2 {
                let Some(Ok(payload)) = client.next().await else {
                    break;
                };
                received.push(protocol::decode::<ServerFrame>(&payload).expect("decodable frame"));
            }
            received
        });

        let cancelled = timeout(Duration::from_millis(50), write_frame(&mut server, &large)).await;
        assert!(
            cancelled.is_err(),
            "the fixture needs the first write to be cancelled while the peer is not reading"
        );

        timeout(
            Duration::from_secs(5),
            write_frame(&mut server, &diagnostic),
        )
        .await
        .expect("the diagnostic write completes once the peer drains")
        .expect("the diagnostic write succeeds");

        let received = timeout(Duration::from_secs(5), reader)
            .await
            .expect("the reader finishes")
            .expect("the reader did not panic");

        assert_eq!(
            received,
            vec![large, diagnostic],
            "the cancelled frame must arrive whole and be followed by the diagnostic"
        );
    }
}
