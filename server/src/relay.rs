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

use bytes::{Bytes, BytesMut};
use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;
use tokio::time::{Instant, sleep_until, timeout};

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
    outbound: mpsc::Sender<Bytes>,
    /// Set when a newer connection takes this peer name.
    ///
    /// Separate from `outbound` on purpose. Closing the queue cannot serve as
    /// the eviction signal, because a closed [`mpsc`] channel yields every
    /// buffered frame *before* it reports closure -- so the signal would arrive
    /// behind the backlog, and a peer being replaced is usually one that stopped
    /// reading, so usually one whose backlog is full. Measured before this
    /// existed: the superseded connection received no diagnostic at all and
    /// held its socket until the idle deadline.
    ///
    /// A `watch` rather than a `Notify` because its receiver is cancel-safe
    /// under `select!` and its state survives a dropped waiter.
    evict: watch::Sender<bool>,
}

/// Outcome of registering a peer name.
#[derive(Clone, Copy, Debug)]
struct Registration {
    connection_id: u64,
    superseded: Option<u64>,
}

type Rooms = HashMap<RoomId, HashMap<String, PeerHandle>>;

/// What one routing attempt did with a frame.
///
/// [`Routed::Unwritable`] is kept apart from the receipt statuses because it is
/// the *sender's* failure rather than the recipient's. Encoding used to happen
/// on the recipient's writer task, where a frame that would not fit the cap
/// closed the recipient's connection -- the third-party close the reserved
/// envelope headroom in [`protocol`] exists to make unreachable. Encoding on
/// the routing call keeps the failure with the connection that built the frame,
/// and this variant is how the caller learns to charge it there.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Routed {
    /// A verdict the sender is told as a `receipt` status.
    Status(ReceiptStatus),
    /// Nothing was enqueued, because the frame did not become a payload this
    /// relay can write. Unreachable for a `send` whose identifiers and body
    /// passed validation: the body budget is sized so that the `message` built
    /// from one always fits the frame cap.
    Unwritable,
}

/// What one fanout did, as the two counts an `accepted` frame carries.
///
/// Counts rather than a [`ReceiptStatus`], because that enum is one closed
/// value and no member of it is true of five recipients of which two were
/// backpressured. Their sum is the number of peers the fanout addressed, so a
/// peer alone in its room gets two zeroes -- an empty room is a fact about the
/// room, not a failure of the request.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Fanout {
    /// Addressed recipients whose outbound queue took the payload.
    ///
    /// A `u32` because that is the wire type, and a room cannot approach the
    /// limit: every peer in it holds a live connection and a 128-slot queue.
    pub delivered: u32,
    /// Addressed recipients whose queue refused it, whether because it was full
    /// or because the receiver was already gone.
    pub shed: u32,
}

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
    /// The superseded connection is signalled through its [`PeerHandle::evict`]
    /// flag, not by the closure of its outbound queue. The queue cannot carry
    /// the signal: a closed [`mpsc`] channel drains every buffered frame before
    /// it reports closure, so the eviction would queue behind a backlog -- and
    /// the peer being replaced is usually one that stopped reading, so usually
    /// one whose backlog is full.
    fn register(
        &self,
        room: &RoomId,
        peer: &str,
        outbound: mpsc::Sender<Bytes>,
        evict: watch::Sender<bool>,
    ) -> Registration {
        let connection_id = self.next_connection_id.fetch_add(1, Ordering::Relaxed);
        let handle = PeerHandle {
            connection_id,
            outbound,
            evict,
        };

        let superseded = self
            .write_rooms()
            .entry(room.clone())
            .or_default()
            .insert(peer.to_owned(), handle)
            .map(|previous| {
                // Ignored: a receiver that is already gone needs no telling.
                let _ = previous.evict.send(true);
                previous.connection_id
            });

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

    /// Encodes `frame` and places it in the outbound queue of `to` within
    /// `room`.
    ///
    /// The room lock is released before the enqueue attempt, and the attempt is
    /// non-blocking: a full queue becomes an observable
    /// [`ReceiptStatus::RecipientBackpressure`] rather than a stalled sender or
    /// unbounded memory growth.
    ///
    /// Encoding happens here, on the task of the connection that *built* the
    /// frame, rather than on the recipient's writer. That is what makes
    /// [`Routed::Unwritable`] reportable to the right peer at all, and it is
    /// also what lets a fanout encode once and hand the same payload to every
    /// recipient.
    ///
    /// The recipient is resolved before the frame is encoded, so an offline
    /// peer costs no serialization of a body up to
    /// [`protocol::MAX_BODY_BYTES`].
    pub fn route(&self, room: &RoomId, to: &str, frame: &ServerFrame) -> Routed {
        let outbound = {
            let rooms = self.read_rooms();
            rooms
                .get(room)
                .and_then(|peers| peers.get(to))
                .map(|handle| handle.outbound.clone())
        };

        let Some(outbound) = outbound else {
            return Routed::Status(ReceiptStatus::PeerOffline);
        };

        let Some(payload) = encode_delivery(room, frame) else {
            return Routed::Unwritable;
        };

        Routed::Status(enqueue(&outbound, payload))
    }

    /// Places an encoded `payload` in the outbound queue of every peer of
    /// `room` except `announcer`, reporting how many queues took it.
    ///
    /// Every recipient handle is collected under the read lock and the lock is
    /// released before the first enqueue, so a room-sized fanout holds the
    /// registry for one `HashMap` walk rather than for N channel operations.
    /// The one allocation per announcement is that `Vec` of handles.
    ///
    /// The payload is encoded once by the caller and handed to every recipient,
    /// so the per-recipient cost is a reference-count bump rather than a clone
    /// of a body up to [`protocol::MAX_BODY_BYTES`].
    ///
    /// The announcer is excluded, deliberately asymmetric with [`Self::route`],
    /// which delivers a self-addressed frame normally. A directed message to
    /// oneself is a legible request; an announcement that starts a turn on its
    /// own author is a loop with no reader.
    pub fn fanout(&self, room: &RoomId, announcer: &str, payload: &Bytes) -> Fanout {
        let recipients: Vec<mpsc::Sender<Bytes>> = {
            let rooms = self.read_rooms();
            rooms
                .get(room)
                .map(|peers| {
                    peers
                        .iter()
                        .filter(|(name, _)| name.as_str() != announcer)
                        .map(|(_, handle)| handle.outbound.clone())
                        .collect()
                })
                .unwrap_or_default()
        };

        let mut counts = Fanout::default();
        for outbound in &recipients {
            // `Bytes::clone` is a reference-count bump on the shared buffer.
            if enqueue(outbound, payload.clone()) == ReceiptStatus::Routed {
                counts.delivered += 1;
            } else {
                counts.shed += 1;
            }
        }
        counts
    }
}

/// Places an encoded payload in one peer's outbound queue without blocking.
fn enqueue(outbound: &mpsc::Sender<Bytes>, payload: Bytes) -> ReceiptStatus {
    match outbound.try_send(payload) {
        Ok(()) => ReceiptStatus::Routed,
        Err(TrySendError::Full(_)) => ReceiptStatus::RecipientBackpressure,
        // The recipient's task has already dropped its receiver; it is gone
        // even though its registry entry has not been reaped yet.
        Err(TrySendError::Closed(_)) => ReceiptStatus::PeerOffline,
    }
}

/// Encodes one delivery, or reports that this relay cannot write it.
///
/// Both failures are invariant breaches rather than peer behaviour. The body
/// budget is sized so that a `send` whose identifiers and body passed
/// validation always produces a `message` inside the frame cap, and the frames
/// this relay builds hold only strings, `u32`s, and vectors of strings, which
/// [`rmp_serde`] cannot fail to serialize into a `Vec`. So the detail is logged
/// here at `error` level, where an operator sees it, and the caller is left
/// with a plain "nothing was enqueued".
///
/// Neither record carries a body: the payload size and
/// [`protocol::Error::kind`] are the whole of what an operator can act on, and
/// a serde error text quotes the value it rejected.
fn encode_delivery(room: &RoomId, frame: &ServerFrame) -> Option<Bytes> {
    match protocol::encode(frame) {
        Ok(payload) if payload.len() <= protocol::MAX_FRAME_BYTES => Some(payload),
        Ok(payload) => {
            tracing::error!(
                %room,
                frame = frame.type_name(),
                payload_bytes = payload.len(),
                cap = protocol::MAX_FRAME_BYTES,
                "delivery exceeds the frame cap; nothing enqueued"
            );
            None
        }
        Err(error) => {
            tracing::error!(
                %room,
                frame = frame.type_name(),
                kind = error.kind(),
                "delivery could not be encoded; nothing enqueued"
            );
            None
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
    /// Carried here rather than threaded through every handler. It is a
    /// property of the connection, and the handlers that log it already receive
    /// this guard.
    peer_addr: SocketAddr,
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
            () = wait_for_flag(&mut shutdown) => break,

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
///
/// Once admitted, the connection runs as **two concurrent futures over
/// independently framed halves**: a reader that never writes, and a writer that
/// never reads, so a peer that stops reading cannot stop the relay from reading
/// that peer's heartbeats.
pub async fn run_connection<S>(
    io: S,
    state: Arc<ServerState>,
    mut shutdown: watch::Receiver<bool>,
    peer_addr: SocketAddr,
) where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (mut reader, mut writer) = protocol::framed_split(io);

    if let Some((room, peer)) = admit(
        &mut reader,
        &mut writer,
        &mut shutdown,
        state.deadlines(),
        peer_addr,
    )
    .await
    {
        // From here every exit path deregisters: `registered` owns the registry
        // entry and drops at the end of this block, before the close log below,
        // including if the block unwinds.
        let (registered, outbound, evicted) = register_peer(&state, room, peer, peer_addr);

        // Bounded: the handshake is not complete until `ready` is out, and a
        // peer that never reads must not park this task before it is even
        // registered. Still sequential, because there is nothing to read
        // concurrently with until the peer is registered.
        if timeout(
            state.deadlines().hello,
            write_frame(
                &mut writer,
                &ServerFrame::Ready {
                    protocol: PROTOCOL_VERSION,
                },
            ),
        )
        .await
        .is_ok_and(|written| written.is_ok())
        {
            pump(
                reader,
                writer,
                &state,
                &registered,
                outbound,
                evicted,
                &mut shutdown,
            )
            .await;
        }
    }

    tracing::info!(%peer_addr, "connection closed");
}

/// Claims the peer name and returns the guard that owns it, the receiving end
/// of its outbound queue, and its eviction signal.
fn register_peer(
    state: &Arc<ServerState>,
    room: RoomId,
    peer: String,
    peer_addr: SocketAddr,
) -> (RegisteredPeer, mpsc::Receiver<Bytes>, watch::Receiver<bool>) {
    let (outbound_tx, outbound_rx) = mpsc::channel(OUTBOUND_QUEUE_CAPACITY);
    let (evict_tx, evict_rx) = watch::channel(false);
    let registration = state.register(&room, &peer, outbound_tx, evict_tx);
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
            peer_addr,
        },
        outbound_rx,
        evict_rx,
    )
}

/// Usable depth of the reply channel between the reader and the writer.
///
/// The reader emits at most one reply per inbound frame and processes frames
/// serially, so a handful of slots is ample slack for pipelining. If it does
/// fill, the writer is not draining, which means the peer is not reading its
/// own replies -- so a lossy reply is dropped and a `send` that cannot be
/// acknowledged closes the connection.
///
/// Usable, not allocated: the channel is built [`DIAGNOSTIC_RESERVE`] slots
/// larger and both [`enqueue_reply`] and [`reserve_acknowledgement`] stop at
/// this depth.
const REPLY_QUEUE_CAPACITY: usize = 8;

/// Reply slots held past [`REPLY_QUEUE_CAPACITY`] for [`queue_diagnostic`].
///
/// Without the reserve, the one close a saturated reply path forces was the one
/// close that could not state its cause. [`reserve_acknowledgement`] fails
/// because the channel is full, and the [`queue_diagnostic`] on the next line
/// `try_send`s to that same channel with no `.await` in between -- so the
/// writer cannot have freed a slot and the diagnostic provably cannot land. The
/// peer closed for not draining its replies received the bare EOF that every
/// other close path exists to avoid.
///
/// Testing capacity and then sending is sound here because of the channel's
/// shape rather than by luck. There is exactly one sender -- [`pump`] moves
/// `reply_tx` into [`run_reader`] and nothing clones it -- and the writer only
/// ever *frees* slots, so [`mpsc::Sender::capacity`] is a lower bound on free
/// slots that nothing can invalidate between the test and the send.
///
/// One slot is enough because every [`queue_diagnostic`] call site is terminal:
/// duplicate `hello`, decode failure, framing error, over-budget body, an
/// acknowledgement reservation failure, and the idle deadline each yield
/// [`ControlFlow::Break`] or return from [`run_reader`] on the next line, and
/// [`run_reader`] runs once per connection. The reserve is consumed at most
/// once.
const DIAGNOSTIC_RESERVE: usize = 1;

/// Drives a registered connection until it closes.
///
/// Reading and writing run as **two concurrent futures over independently
/// framed halves of the socket**. That is the load-bearing structure here, and
/// it replaced a single `select!` loop for a reason worth stating.
///
/// `tokio::select!` drops its branch futures before running the chosen branch's
/// handler, so an `.await` *inside* a handler suspends the whole loop: while a
/// socket write pends, no other branch is polled. Bounding that write -- which
/// an earlier revision did -- turns "parks forever" into "parks until the
/// deadline, deaf to everything", which is better but still wrong in two
/// measured ways. A peer that kept sending heartbeats on time was disconnected
/// because nobody read them, and a superseded peer was neither told nor closed
/// promptly because the eviction signal sat behind its own backlog.
///
/// Splitting removes the shape rather than patching the symptom a third time:
/// a stalled write now stalls only the writer, so the reader keeps consuming
/// heartbeats and keeps enforcing the idle deadline, which is the same division
/// of labour a message broker settles on -- `RabbitMQ`'s per-connection reader
/// and writer processes, with liveness tracked on the reader.
///
/// The reader owns the idle deadline, the protocol state machine, and shutdown.
/// The writer owns the socket's write half and nothing else. They communicate
/// over one bounded reply channel, and the reader signals "no more" by dropping
/// its end.
async fn pump<S>(
    reader: protocol::FrameReader<S>,
    writer: protocol::FrameWriter<S>,
    state: &ServerState,
    registered: &RegisteredPeer,
    outbound: mpsc::Receiver<Bytes>,
    evicted: watch::Receiver<bool>,
    shutdown: &mut watch::Receiver<bool>,
) where
    S: AsyncRead + AsyncWrite,
{
    let (reply_tx, reply_rx) = mpsc::channel(REPLY_QUEUE_CAPACITY + DIAGNOSTIC_RESERVE);
    // Both halves watch it: the writer so eviction overtakes the backlog, the
    // reader so a superseded connection stops routing.
    let mut reader_evicted = evicted.clone();

    let write_loop = run_writer(writer, outbound, reply_rx, evicted, registered);
    let read_loop = run_reader(
        reader,
        state,
        registered,
        reply_tx,
        shutdown,
        &mut reader_evicted,
    );
    tokio::pin!(write_loop);

    tokio::select! {
        () = &mut write_loop => {
            // The writer finished first: the socket failed, or this connection
            // was superseded and has already said so. Nothing left to send.
        }
        () = read_loop => {
            // The reader has queued its closing diagnostic and dropped its end
            // of the reply channel, so the writer will drain what remains and
            // stop. Bounded, because a peer that is not reading cannot be
            // allowed to hold the task open while its diagnostic fails to land.
            let _ = timeout(TERMINAL_WRITE_TIMEOUT, write_loop).await;
        }
    }
}

/// Owns the socket's write half. Never reads, so it can block freely.
///
/// Branch order is deliberate. Eviction outranks everything: it must overtake a
/// backlog rather than queue behind it, which is the whole reason it is a
/// separate signal and not the closure of the outbound queue. Replies outrank
/// deliveries so that a peer's own `receipt` or `pong` is not stuck behind
/// frames addressed to it. Neither starves the other in practice, because the
/// reader is serial: once a reply is written, the next poll finds the reply
/// channel empty and takes a delivery.
async fn run_writer<S>(
    mut writer: protocol::FrameWriter<S>,
    mut outbound: mpsc::Receiver<Bytes>,
    mut replies: mpsc::Receiver<ServerFrame>,
    mut evicted: watch::Receiver<bool>,
    registered: &RegisteredPeer,
) where
    S: AsyncWrite,
{
    loop {
        tokio::select! {
            biased;

            () = wait_for_flag(&mut evicted) => {
                log_replacement(registered);
                close_with(
                    &mut writer,
                    ErrorCode::PeerReplaced,
                    "a newer connection registered this peer name".to_owned(),
                )
                .await;
                return;
            }

            reply = replies.recv() => match reply {
                Some(frame) => {
                    // Raced against eviction for the same reason the delivery
                    // arm is. A bare await here masked the flag: once this arm
                    // was chosen, a reply write pending on a full socket buffer
                    // never polled it, so replacing a peer that had stopped
                    // reading waited out the reply instead of overtaking it.
                    let wrote = tokio::select! {
                        biased;
                        () = wait_for_flag(&mut evicted) => None,
                        result = write_frame(&mut writer, &frame) => Some(result),
                    };
                    match wrote {
                        Some(Ok(())) => {}
                        Some(Err(_)) => return,
                        None => break,
                    }
                }
                // The reader is done. Anything still buffered has been drained
                // by this same arm, because a closed channel yields its
                // contents before it reports closure.
                None => return,
            },

            delivery = outbound.recv() => match delivery {
                Some(payload) => {
                    // Already encoded, by the routing call on the task of the
                    // connection that sent it. This arm never serializes, so a
                    // frame this relay cannot write can no longer close the
                    // connection it was addressed to.
                    //
                    // The write itself is unbounded on purpose -- the reader is
                    // the watchdog, and a per-write timeout here would give a
                    // silent peer one full idle window per queued frame -- but
                    // it is raced against eviction. Without that race, being
                    // superseded still has to wait out whichever write is
                    // already in flight, which against a peer that has stopped
                    // reading means waiting out the idle deadline: the retention
                    // this signal exists to prevent.
                    //
                    // Cancelling mid-flush is safe. `Framed` writes through its
                    // own buffer, so the unflushed remainder stays there and the
                    // diagnostic is appended after it rather than interleaved --
                    // asserted by
                    // `a_cancelled_write_does_not_desynchronize_the_frame_stream`.
                    let wrote = tokio::select! {
                        biased;
                        () = wait_for_flag(&mut evicted) => None,
                        result = writer.send(payload) => Some(result),
                    };

                    match wrote {
                        Some(Ok(())) => {}
                        Some(Err(_)) => return,
                        None => break,
                    }
                }
                // Only reachable if the registry entry went away without
                // setting the eviction flag, which registration does not do.
                None => return,
            },
        }
    }

    // Left the loop because eviction interrupted a write in flight.
    log_replacement(registered);
    close_with(
        &mut writer,
        ErrorCode::PeerReplaced,
        "a newer connection registered this peer name".to_owned(),
    )
    .await;
}

/// Emits the superseded-connection event.
fn log_replacement(registered: &RegisteredPeer) {
    tracing::info!(
        room = %registered.room,
        peer = %registered.peer,
        connection_id = registered.connection_id,
        "closing superseded connection"
    );
}

/// Owns the socket's read half, the idle deadline, and the protocol state
/// machine. Never writes, so it is never blocked by a peer that stops reading.
async fn run_reader<S>(
    mut reader: protocol::FrameReader<S>,
    state: &ServerState,
    registered: &RegisteredPeer,
    replies: mpsc::Sender<ServerFrame>,
    shutdown: &mut watch::Receiver<bool>,
    evicted: &mut watch::Receiver<bool>,
) where
    S: AsyncRead,
{
    let deadlines = state.deadlines();
    let mut idle_deadline = Instant::now() + deadlines.idle;

    loop {
        tokio::select! {
            () = wait_for_flag(shutdown) => {
                tracing::debug!(
                    connection_id = registered.connection_id,
                    "closing connection for shutdown"
                );
                return;
            }

            // Being superseded has to stop *inbound* processing too, not only
            // outbound delivery. Otherwise a replaced connection keeps routing
            // frames under a peer name it no longer owns, and recipients see
            // messages attributed to the peer the replacement now is.
            () = wait_for_flag(evicted) => {
                tracing::debug!(
                    connection_id = registered.connection_id,
                    "superseded; stopping inbound processing"
                );
                return;
            }

            () = sleep_until(idle_deadline) => {
                log_heartbeat_timeout(registered, deadlines.idle);
                queue_diagnostic(
                    &replies,
                    ErrorCode::IdleTimeout,
                    format!("no frame within {:?}", deadlines.idle),
                );
                return;
            }

            inbound = reader.next() => {
                if handle_inbound(
                    &replies,
                    state,
                    registered,
                    inbound,
                    &mut idle_deadline,
                )
                .is_break()
                {
                    return;
                }

                // Hand the writer a turn before taking the next frame.
                //
                // `FramedRead` returns every frame it has already buffered
                // without pending, and nothing in this loop awaits, so a
                // pipelined burst is decoded in a single poll and the writer is
                // never scheduled in between. The reply channel then fills from
                // frames the writer would otherwise have drained, and a healthy
                // peer that simply pipelined its requests gets closed.
                // Measured before this yield: a 24-send burst in one 998-byte
                // write produced 8 receipts and a close.
                tokio::task::yield_now().await;
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
fn handle_inbound(
    replies: &mpsc::Sender<ServerFrame>,
    state: &ServerState,
    registered: &RegisteredPeer,
    read: Option<io::Result<BytesMut>>,
    idle_deadline: &mut Instant,
) -> ControlFlow<()> {
    // `None` is a clean end of stream: the peer hung up.
    let Some(read) = read else {
        return ControlFlow::Break(());
    };

    let payload = match read {
        Ok(payload) => payload,
        Err(error) => {
            report_framing_error(replies, &error, registered.peer_addr);
            return ControlFlow::Break(());
        }
    };

    let frame: ClientFrame = match protocol::decode(&payload) {
        Ok(frame) => frame,
        Err(error) => {
            tracing::info!(
                peer_addr = %registered.peer_addr,
                connection_id = registered.connection_id,
                payload_bytes = payload.len(),
                reason = error.kind(),
                "frame decode failed"
            );
            queue_diagnostic(replies, ErrorCode::MalformedFrame, error.to_string());
            return ControlFlow::Break(());
        }
    };

    *idle_deadline = Instant::now() + state.deadlines().idle;

    handle_frame(replies, state, registered, frame)
}

/// Reads and validates `hello`, returning the admitted room and peer name.
///
/// Every rejection states its cause on the wire before the caller closes the
/// connection.
async fn admit<S>(
    reader: &mut protocol::FrameReader<S>,
    writer: &mut protocol::FrameWriter<S>,
    shutdown: &mut watch::Receiver<bool>,
    deadlines: Deadlines,
    peer_addr: SocketAddr,
) -> Option<(RoomId, String)>
where
    S: AsyncRead + AsyncWrite,
{
    // Still sequential: until the peer is registered there is nothing to
    // deliver, so nothing is gained by reading and writing concurrently.
    let read = tokio::select! {
        () = wait_for_flag(shutdown) => return None,
        read = timeout(deadlines.hello, reader.next()) => read,
    };

    let payload = match read {
        Err(_elapsed) => {
            tracing::info!(%peer_addr, deadline = ?deadlines.hello, "handshake deadline elapsed");
            close_with(
                writer,
                ErrorCode::HelloTimeout,
                format!("no hello within {:?}", deadlines.hello),
            )
            .await;
            return None;
        }
        // Hung up before saying anything: nothing to diagnose.
        Ok(None) => return None,
        Ok(Some(Err(error))) => {
            report_framing_error_now(writer, &error, peer_addr).await;
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
            close_with(writer, ErrorCode::MalformedFrame, error.to_string()).await;
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
            writer,
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
            writer,
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
                writer,
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
fn handle_frame(
    replies: &mpsc::Sender<ServerFrame>,
    state: &ServerState,
    registered: &RegisteredPeer,
    frame: ClientFrame,
) -> ControlFlow<()> {
    match frame {
        ClientFrame::Hello { .. } => {
            tracing::info!(
                room = %registered.room,
                peer = %registered.peer,
                connection_id = registered.connection_id,
                "duplicate hello"
            );
            queue_diagnostic(
                replies,
                ErrorCode::DuplicateHello,
                "this connection is already registered".to_owned(),
            );
            ControlFlow::Break(())
        }

        ClientFrame::Ping => enqueue_reply(replies, ServerFrame::Pong),

        ClientFrame::List { request_id } => {
            if let Err(error) = protocol::validate_correlation_id(&request_id) {
                return enqueue_reply(
                    replies,
                    ServerFrame::Error {
                        code: ErrorCode::InvalidIdentifier,
                        message: Some(format!("list.request_id {error}")),
                        request_id: None,
                    },
                );
            }

            let peers = state.list_peers(&registered.room);
            enqueue_reply(replies, ServerFrame::Peers { request_id, peers })
        }

        ClientFrame::Send {
            id,
            to,
            body,
            reply_to,
        } => handle_send(replies, state, registered, id, to, body, reply_to),

        ClientFrame::Announce { id, body, reply_to } => {
            handle_announce(replies, state, registered, id, body, reply_to)
        }

        ClientFrame::Unsupported => {
            tracing::info!(
                room = %registered.room,
                peer = %registered.peer,
                "unsupported frame type"
            );
            enqueue_reply(
                replies,
                ServerFrame::Error {
                    code: ErrorCode::UnsupportedFrame,
                    message: Some("this protocol version does not implement that frame".to_owned()),
                    request_id: None,
                },
            )
        }
    }
}

/// Validates and routes one `send`, then reports its receipt.
///
/// The three checks are ordered by consequence. An invalid correlation id and
/// an invalid target are recoverable and keep the connection open; an
/// over-budget body is not, so it is checked last, once the frame is known to
/// be one the relay would otherwise have delivered.
fn handle_send(
    replies: &mpsc::Sender<ServerFrame>,
    state: &ServerState,
    registered: &RegisteredPeer,
    id: String,
    to: String,
    body: String,
    reply_to: Option<String>,
) -> ControlFlow<()> {
    if let Err(error) = protocol::validate_correlation_id(&id) {
        return enqueue_reply(
            replies,
            ServerFrame::Error {
                code: ErrorCode::InvalidIdentifier,
                message: Some(format!("send.id {error}")),
                request_id: None,
            },
        );
    }

    if let Err(error) = protocol::validate_identifier(&to) {
        tracing::debug!(
            room = %registered.room,
            peer = %registered.peer,
            %error,
            "send target rejected"
        );
        return enqueue_reply(
            replies,
            ServerFrame::Receipt {
                id,
                to: bounded_target(to),
                status: ReceiptStatus::InvalidTarget,
            },
        );
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
        return enqueue_reply(
            replies,
            ServerFrame::Error {
                code: ErrorCode::InvalidIdentifier,
                message: Some(format!("send.reply_to {error}")),
                // `id` passed validation above, so this is the one rejection on
                // the `send` path that can name the frame it rejected. A client
                // pipelining sends has no other way to tell which one failed:
                // the connection stays open, so the error arrives with no
                // positional relationship to anything.
                request_id: Some(id),
            },
        );
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
        queue_diagnostic(
            replies,
            ErrorCode::FrameTooLarge,
            format!(
                "body is {body_bytes} bytes; at most {} can be relayed within the {}-byte \
                 frame cap",
                protocol::MAX_BODY_BYTES,
                protocol::MAX_FRAME_BYTES
            ),
        );
        return ControlFlow::Break(());
    }

    // The acknowledgement is claimed before the frame is routed. `peer-relay`
    // requires exactly one receipt per valid `send`, so routing first and
    // finding no room afterwards delivered the message and lost its receipt --
    // leaving the sender unable to tell whether to retry.
    let Some(receipt_slot) = reserve_acknowledgement(replies) else {
        tracing::info!(
            room = %registered.room,
            peer = %registered.peer,
            connection_id = registered.connection_id,
            capacity = REPLY_QUEUE_CAPACITY,
            "cannot acknowledge a send; closing without routing it"
        );
        queue_diagnostic(
            replies,
            ErrorCode::IdleTimeout,
            "replies are not being drained, so this send cannot be acknowledged; \
             it was not routed"
                .to_owned(),
        );
        return ControlFlow::Break(());
    };

    // `body` is moved into the delivered frame and is never a log field; only
    // its size is observable in logs.
    let body_bytes = body.len();
    let outcome = state.route(
        &registered.room,
        &to,
        &ServerFrame::Message {
            id: id.clone(),
            from: registered.peer.clone(),
            body,
            reply_to,
        },
    );

    let Routed::Status(status) = outcome else {
        drop(receipt_slot);
        return refuse_unwritable(replies, registered, "message", &id, body_bytes);
    };

    log_route(registered, &to, &id, body_bytes, status);

    receipt_slot.send(ServerFrame::Receipt { id, to, status });
    ControlFlow::Continue(())
}

/// Validates and fans out one `announce`, then reports its aggregate receipt.
///
/// The checks are ordered as on the `send` path and for the same reasons. `id`
/// first, because no rejection can name the frame it answers without it.
/// `reply_to` second, because it is the one recoverable rejection here that
/// *can* name it. The body budget last, because it is the one that closes the
/// connection, so it is reached only once the frame is known to be one the
/// relay would otherwise have delivered.
///
/// There is no target check, and no place for one: an `announce` carries no
/// target field at all.
fn handle_announce(
    replies: &mpsc::Sender<ServerFrame>,
    state: &ServerState,
    registered: &RegisteredPeer,
    id: String,
    body: String,
    reply_to: Option<String>,
) -> ControlFlow<()> {
    if let Err(error) = protocol::validate_correlation_id(&id) {
        return enqueue_reply(
            replies,
            ServerFrame::Error {
                code: ErrorCode::InvalidIdentifier,
                message: Some(format!("announce.id {error}")),
                request_id: None,
            },
        );
    }

    if let Some(error) = reply_to
        .as_deref()
        .and_then(|value| protocol::validate_correlation_id(value).err())
    {
        return enqueue_reply(
            replies,
            ServerFrame::Error {
                code: ErrorCode::InvalidIdentifier,
                message: Some(format!("announce.reply_to {error}")),
                // `id` passed validation above, so this is the one rejection on
                // the `announce` path that can name the frame it rejected.
                request_id: Some(id),
            },
        );
    }

    if let Some(body_bytes) = protocol::body_over_budget(&body) {
        tracing::info!(
            room = %registered.room,
            peer = %registered.peer,
            connection_id = registered.connection_id,
            body_bytes,
            budget = protocol::MAX_BODY_BYTES,
            "announced body exceeds the relayable budget"
        );
        queue_diagnostic(
            replies,
            ErrorCode::FrameTooLarge,
            format!(
                "body is {body_bytes} bytes; at most {} can be relayed within the {}-byte \
                 frame cap",
                protocol::MAX_BODY_BYTES,
                protocol::MAX_FRAME_BYTES
            ),
        );
        return ControlFlow::Break(());
    }

    // Secured before any recipient is enqueued, exactly as a `receipt` is. The
    // invariant it buys generalizes to a fanout without change, because it
    // constrains the order of securing against enqueueing and not the number of
    // recipients: zero acknowledgements means zero deliveries, so a resend after
    // reconnecting is never a duplicate.
    let Some(accepted_slot) = reserve_acknowledgement(replies) else {
        tracing::info!(
            room = %registered.room,
            peer = %registered.peer,
            connection_id = registered.connection_id,
            capacity = REPLY_QUEUE_CAPACITY,
            "cannot acknowledge an announcement; closing without routing it"
        );
        queue_diagnostic(
            replies,
            ErrorCode::IdleTimeout,
            "replies are not being drained, so this announcement cannot be acknowledged; \
             it was not routed"
                .to_owned(),
        );
        return ControlFlow::Break(());
    };

    // `body` is moved into the notice and is never a log field; only its size is
    // observable in logs.
    let body_bytes = body.len();
    let notice = ServerFrame::Notice {
        id: id.clone(),
        from: registered.peer.clone(),
        body,
        reply_to,
    };

    // Encoded once for the whole room. Every field of a `notice` is
    // sender-derived, so the payload is byte-identical for every recipient and
    // the fanout hands out reference-counted clones of this one buffer.
    let Some(payload) = encode_delivery(&registered.room, &notice) else {
        drop(accepted_slot);
        return refuse_unwritable(replies, registered, "notice", &id, body_bytes);
    };

    let counts = state.fanout(&registered.room, &registered.peer, &payload);
    log_fanout(registered, &id, body_bytes, counts);

    accepted_slot.send(ServerFrame::Accepted {
        id,
        delivered: counts.delivered,
        shed: counts.shed,
    });
    ControlFlow::Continue(())
}

/// Closes the connection whose frame produced a delivery this relay cannot
/// write.
///
/// Charged to the sender, which built the frame, and never to the recipient it
/// was addressed to. Unreachable while the body budget holds: `id`, `reply_to`,
/// and `body` are all checked before routing and `from` is a registered peer
/// name, which is exactly the arithmetic
/// `worst_case_message_at_the_body_budget_fits_the_frame_cap` and its `notice`
/// counterpart prove. It is handled rather than asserted so that a later change
/// breaking that arithmetic closes the connection responsible instead of a third
/// party's, which is the property `an_unwritable_delivery_closes_nobody` pins.
///
/// `produced` names the delivery frame the relay was building -- `message` or
/// `notice` -- because that is the frame whose size the sender has to act on,
/// and it is not the frame the sender wrote.
///
/// [`ServerState::route`] has already logged what went wrong at `error` level;
/// this record names the connection paying for it.
fn refuse_unwritable(
    replies: &mpsc::Sender<ServerFrame>,
    registered: &RegisteredPeer,
    produced: &str,
    id: &str,
    body_bytes: usize,
) -> ControlFlow<()> {
    tracing::info!(
        room = %registered.room,
        peer = %registered.peer,
        connection_id = registered.connection_id,
        produced,
        id,
        body_bytes,
        "the delivery this frame would produce is not writable; closing without routing it"
    );
    queue_diagnostic(
        replies,
        ErrorCode::FrameTooLarge,
        format!(
            "the {produced} this frame would produce does not fit the frame cap; \
             it was not routed"
        ),
    );
    ControlFlow::Break(())
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
/// `send` whose `to` is 65507 bytes fills the inbound frame cap exactly, and the
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

/// Emits the fanout outcome. Carries metadata only: never `body`.
///
/// One record per announcement rather than one per recipient. Per-recipient
/// records would make a single announcement's log volume scale with the room
/// while adding nothing a reader could act on: the shed count already says how
/// many peers were not reading, and *which* peer that was is already observable
/// from the backpressure and registration events.
fn log_fanout(registered: &RegisteredPeer, id: &str, body_bytes: usize, counts: Fanout) {
    let room = tracing::field::display(&registered.room);
    let from = registered.peer.as_str();
    let Fanout { delivered, shed } = counts;

    if shed == 0 {
        tracing::debug!(%room, from, id, body_bytes, delivered, shed, "announcement fanned out");
    } else {
        tracing::warn!(
            %room, from, id, body_bytes, delivered, shed,
            "announcement shed by a recipient queue"
        );
    }
}

/// Encodes and writes one frame, flushing it.
async fn write_frame<S>(
    writer: &mut protocol::FrameWriter<S>,
    frame: &ServerFrame,
) -> io::Result<()>
where
    S: AsyncWrite,
{
    // Unreachable for the frames this relay builds, which hold only strings,
    // `u32`s, and vectors of strings.
    let payload = protocol::encode(frame).map_err(io::Error::other)?;
    writer.send(payload).await
}

/// Hands one reply to the writer, dropping it rather than closing when the
/// writer is not keeping up.
///
/// Non-blocking on purpose: the reader must never wait on the socket, which is
/// the point of the split. Refusal means the writer has not drained
/// [`REPLY_QUEUE_CAPACITY`] replies, which means the peer is not reading even
/// its own answers -- so this reply could not have reached it either way.
///
/// The refusal threshold is the reserve rather than the end of the channel. The
/// last [`DIAGNOSTIC_RESERVE`] slots belong to [`queue_diagnostic`], so a reply
/// that finds only those free is dropped as though the channel were full --
/// which, for every reply that is not the closing diagnostic, it is.
///
/// Dropping rather than closing is what keeps the heartbeat guarantee. A peer
/// that keeps sending `ping` while not draining generates one `pong` per ping;
/// closing on a full channel disconnected exactly the peer `peer-relay` says
/// must be kept, and did so after nine pings. Measured before this split
/// existed: 15 pings, deregistered at the 15th.
///
/// Used for every reply whose loss costs the peer nothing it can observe:
/// `pong`, `peers`, and the recoverable `error` frames. A `send` receipt is
/// contractual, as is an `announce` acceptance, and both use
/// [`reserve_acknowledgement`] instead.
fn enqueue_reply(replies: &mpsc::Sender<ServerFrame>, frame: ServerFrame) -> ControlFlow<()> {
    let queued = if replies.capacity() <= DIAGNOSTIC_RESERVE {
        Err(TrySendError::Full(frame))
    } else {
        replies.try_send(frame)
    };

    match queued {
        Ok(()) => ControlFlow::Continue(()),
        Err(TrySendError::Full(_)) => {
            tracing::debug!(
                capacity = REPLY_QUEUE_CAPACITY,
                "reply dropped: the peer is not draining its own replies"
            );
            ControlFlow::Continue(())
        }
        // The writer is gone, so the connection is already closing.
        Err(TrySendError::Closed(_)) => ControlFlow::Break(()),
    }
}

/// Claims the slot a routed frame's acknowledgement will occupy, before that
/// frame is routed.
///
/// `peer-relay` requires exactly one `receipt` for every syntactically valid
/// `send` and exactly one `accepted` for every syntactically valid `announce`,
/// so the acknowledgement has to be secured *first*. Routing and then
/// discovering there is no room routed the message and dropped its receipt:
/// measured at 24 coalesced sends, 8 receipts, 9 delivered, then a close. The
/// sender could not tell that its ninth message had been delivered, so it would
/// resend it.
///
/// One reservation serves both classes without change, because the invariant is
/// about the *order* of securing against enqueueing and not about the number of
/// recipients: zero acknowledgements means zero deliveries, whether the frame
/// addressed one peer or the whole room.
///
/// Returning `None` therefore means "do not route this frame", and the caller
/// closes the connection instead. Nothing was delivered, so the sender's retry
/// after reconnecting is correct rather than a duplicate.
///
/// Refuses on the same threshold as [`enqueue_reply`], and for a sharper
/// reason: the caller's very next act on this path is to queue the `error`
/// frame naming the close. Reserving the last slot would buy one
/// acknowledgement the peer cannot read at the price of the one diagnostic it
/// needs.
fn reserve_acknowledgement(
    replies: &mpsc::Sender<ServerFrame>,
) -> Option<mpsc::Permit<'_, ServerFrame>> {
    if replies.capacity() <= DIAGNOSTIC_RESERVE {
        return None;
    }
    replies.try_reserve().ok()
}

/// Queues the frame that names why the relay is closing this connection.
///
/// A bare EOF is indistinguishable from a crashed relay, a wrong port, or a
/// version mismatch, so every close the relay initiates states its reason.
/// [`DIAGNOSTIC_RESERVE`] keeps a slot free for this frame, and every call site
/// is terminal, so the queue has room for it. It stays best-effort in one way:
/// the writer's flush is bounded by [`TERMINAL_WRITE_TIMEOUT`] once the reader
/// has finished.
fn queue_diagnostic(replies: &mpsc::Sender<ServerFrame>, code: ErrorCode, message: String) {
    let frame = ServerFrame::Error {
        code,
        message: Some(cap_diagnostic(message)),
        request_id: None,
    };
    if replies.try_send(frame).is_err() {
        tracing::debug!(
            code = code.as_str(),
            "could not queue the closing diagnostic; the peer gets a bare close"
        );
    }
}

/// Makes one bounded, best-effort attempt to name the cause of a close, writing
/// straight to the socket.
///
/// For the two closes that must overtake anything queued: a handshake rejection,
/// which happens before there is a queue, and an eviction, which must not sit
/// behind the backlog it is displacing.
async fn close_with<S>(writer: &mut protocol::FrameWriter<S>, code: ErrorCode, message: String)
where
    S: AsyncWrite,
{
    let frame = ServerFrame::Error {
        code,
        message: Some(cap_diagnostic(message)),
        request_id: None,
    };
    let _ = timeout(TERMINAL_WRITE_TIMEOUT, write_frame(writer, &frame)).await;
}

/// Truncates diagnostic text on a UTF-8 boundary.
///
/// Callers pass a decoder's error text, which quotes the payload it rejected --
/// and quoting escapes, so a payload well inside the inbound cap can produce a
/// diagnostic well outside it. An unencodable `error` frame is silently dropped,
/// leaving the peer with exactly the bare EOF the diagnostic exists to prevent.
/// Capping in one place fixes every call site at once.
fn cap_diagnostic(mut message: String) -> String {
    if message.len() > MAX_DIAGNOSTIC_BYTES {
        let mut end = MAX_DIAGNOSTIC_BYTES;
        while end > 0 && !message.is_char_boundary(end) {
            end -= 1;
        }
        message.truncate(end);
    }
    message
}

/// Reports a framing-layer failure on a registered connection, naming its cause
/// when the framing layer knows one.
fn report_framing_error(
    replies: &mpsc::Sender<ServerFrame>,
    error: &io::Error,
    peer_addr: SocketAddr,
) {
    if let Some(code) = protocol::framing_error_code(error) {
        tracing::info!(%peer_addr, code = code.as_str(), %error, "framing violation");
        queue_diagnostic(replies, code, error.to_string());
    } else {
        // A transport failure: there is no socket left to explain it on.
        tracing::debug!(%peer_addr, %error, "connection read failed");
    }
}

/// The same report during admission, where there is no writer task yet.
async fn report_framing_error_now<S>(
    writer: &mut protocol::FrameWriter<S>,
    error: &io::Error,
    peer_addr: SocketAddr,
) where
    S: AsyncWrite,
{
    if let Some(code) = protocol::framing_error_code(error) {
        tracing::info!(%peer_addr, code = code.as_str(), %error, "framing violation");
        close_with(writer, code, error.to_string()).await;
    } else {
        tracing::debug!(%peer_addr, %error, "connection read failed");
    }
}

/// Resolves once a `watch` flag is set, or its sender is gone.
///
/// Used for both shutdown and eviction: each transitions false -> true exactly
/// once, so any change is the signal, and a dropped sender means whoever would
/// have set it has left -- also a reason to stop.
async fn wait_for_flag(flag: &mut watch::Receiver<bool>) {
    if *flag.borrow_and_update() {
        return;
    }
    let _ = flag.changed().await;
}

#[cfg(test)]
mod tests {
    use tokio::sync::mpsc::error::TryRecvError;

    use super::*;

    fn room() -> RoomId {
        RoomId::new("omp-relayd", "implement-tcp-relay-server")
    }

    fn peer_queue() -> (mpsc::Sender<Bytes>, mpsc::Receiver<Bytes>) {
        mpsc::channel(OUTBOUND_QUEUE_CAPACITY)
    }

    /// Builds the reply channel exactly as [`pump`] does, reserve included. A
    /// test that sizes it at [`REPLY_QUEUE_CAPACITY`] instead would assert a
    /// false premise: its last fill would be silently dropped rather than
    /// queued, and every assertion below it would hold for the wrong reason.
    fn reply_queue() -> (mpsc::Sender<ServerFrame>, mpsc::Receiver<ServerFrame>) {
        mpsc::channel(REPLY_QUEUE_CAPACITY + DIAGNOSTIC_RESERVE)
    }

    /// Registers a peer the way a connection does, returning the registration
    /// and the eviction flag its writer would watch.
    fn register_in(
        state: &ServerState,
        room: &RoomId,
        peer: &str,
        outbound: mpsc::Sender<Bytes>,
    ) -> (Registration, watch::Receiver<bool>) {
        let (evict_tx, evict_rx) = watch::channel(false);
        (state.register(room, peer, outbound, evict_tx), evict_rx)
    }

    fn message(id: &str) -> ServerFrame {
        ServerFrame::Message {
            id: id.to_owned(),
            from: "macbook-reviewer".to_owned(),
            body: "review the diff".to_owned(),
            reply_to: None,
        }
    }

    /// Routes and unwraps the receipt status.
    ///
    /// Every routing test but one is about the status; the unwritable outcome
    /// has its own test, and a fixture frame reaching it here would be a bug in
    /// the fixture rather than a result.
    fn route_status(
        state: &ServerState,
        room: &RoomId,
        to: &str,
        frame: &ServerFrame,
    ) -> ReceiptStatus {
        match state.route(room, to, frame) {
            Routed::Status(status) => status,
            Routed::Unwritable => panic!("this fixture frame is well inside the frame cap"),
        }
    }

    /// Decodes what a peer's outbound queue holds, which is now bytes rather
    /// than a frame.
    fn decode_delivery(payload: &Bytes) -> ServerFrame {
        protocol::decode(payload).expect("a queued delivery is an encoded server frame")
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
        let (first, mut first_evicted) = register_in(&state, &room(), "reviewer", first_tx);

        let (second_tx, _second_rx) = peer_queue();
        let (second, _second_evicted) = register_in(&state, &room(), "reviewer", second_tx);

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
            *first_evicted.borrow_and_update(),
            "the superseded connection's eviction flag must be set: it is the signal its \
             writer watches, and it has to overtake the backlog rather than queue behind it"
        );
        assert!(
            matches!(first_rx.try_recv(), Err(TryRecvError::Disconnected)),
            "the superseded queue also closes, but only the flag is relied upon"
        );
    }

    #[test]
    fn late_cleanup_of_a_superseded_connection_keeps_the_replacement() {
        let state = ServerState::new();

        let (first_tx, _first_rx) = peer_queue();
        let (first, _first_evicted) = register_in(&state, &room(), "reviewer", first_tx);

        let (second_tx, mut second_rx) = peer_queue();
        let (second, _second_evicted) = register_in(&state, &room(), "reviewer", second_tx);

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
            route_status(&state, &room(), "reviewer", &message("msg-1")),
            ReceiptStatus::Routed,
            "traffic must reach the replacement"
        );
        let queued = second_rx
            .try_recv()
            .expect("the replacement's queue holds the delivery");
        assert!(
            matches!(decode_delivery(&queued), ServerFrame::Message { .. }),
            "the queue must hold an encoded `message`, ready for the writer to hand to the \
             socket without serializing anything"
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
            register_in(&state, &room, peer, tx);
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
        register_in(&state, &room(), "windows-main", tx);

        assert_eq!(
            route_status(&state, &room(), "nobody", &message("msg-1")),
            ReceiptStatus::PeerOffline,
            "an unregistered name is offline"
        );
        assert_eq!(
            route_status(
                &state,
                &RoomId::new("other", "task"),
                "windows-main",
                &message("msg-1")
            ),
            ReceiptStatus::PeerOffline,
            "a peer of the same name in another room is not reachable"
        );

        for slot in 1..=OUTBOUND_QUEUE_CAPACITY {
            assert_eq!(
                route_status(&state, &room(), "windows-main", &message("msg-fill")),
                ReceiptStatus::Routed,
                "queue slot {slot} of {OUTBOUND_QUEUE_CAPACITY} must accept a frame"
            );
        }
        assert_eq!(
            route_status(&state, &room(), "windows-main", &message("msg-overflow")),
            ReceiptStatus::RecipientBackpressure,
            "frame {} must not fit a {OUTBOUND_QUEUE_CAPACITY}-slot queue",
            OUTBOUND_QUEUE_CAPACITY + 1
        );

        assert!(rx.try_recv().is_ok(), "draining frees a slot");
        assert_eq!(
            route_status(&state, &room(), "windows-main", &message("msg-after-drain")),
            ReceiptStatus::Routed,
            "backpressure must be transient, not a permanent state"
        );
    }

    #[test]
    fn a_gone_receiver_reads_as_offline() {
        let state = ServerState::new();
        let (tx, rx) = peer_queue();
        register_in(&state, &room(), "windows-main", tx);
        drop(rx);

        assert_eq!(
            route_status(&state, &room(), "windows-main", &message("msg-1")),
            ReceiptStatus::PeerOffline,
            "a registered peer whose task is gone is offline, not backpressured"
        );
    }

    /// The failure the `Bytes` refactor exists to relocate.
    ///
    /// Before it, a frame past the cap was discovered by the *recipient's*
    /// writer: `LengthDelimitedCodec::encode` refused it, `run_writer` returned
    /// on the `Err`, and the recipient's connection closed for a frame it did
    /// not build. The budget in `protocol` keeps a validated `send` from
    /// reaching that state, but the point here is where the failure lands when
    /// something does, so this bypasses `handle_send` and hands `route` a frame
    /// no budget check ever saw.
    #[test]
    fn an_unwritable_delivery_closes_nobody() {
        let state = ServerState::new();
        let (tx, mut rx) = peer_queue();
        register_in(&state, &room(), "windows-main", tx);

        let oversized = ServerFrame::Message {
            id: "msg-1".to_owned(),
            from: "macbook-reviewer".to_owned(),
            body: "x".repeat(protocol::MAX_FRAME_BYTES),
            reply_to: None,
        };
        assert!(
            protocol::encode(&oversized).expect("encodes").len() > protocol::MAX_FRAME_BYTES,
            "the premise: this frame is one the framing codec would refuse to write"
        );

        assert_eq!(
            state.route(&room(), "windows-main", &oversized),
            Routed::Unwritable,
            "an unwritable frame must be reported to the caller, which is the sender's \
             handler, rather than handed to the recipient's writer to fail on"
        );
        assert_eq!(
            rx.try_recv(),
            Err(TryRecvError::Empty),
            "nothing may be enqueued: the recipient's queue is untouched and its writer \
             never sees the frame, so its connection cannot be closed by it"
        );
        assert_eq!(
            route_status(&state, &room(), "windows-main", &message("msg-2")),
            ReceiptStatus::Routed,
            "and the recipient stays routable afterwards"
        );
    }

    #[test]
    fn a_fanout_addresses_every_other_peer_of_the_room() {
        let state = ServerState::new();
        let other_room = RoomId::new("omp-relayd", "some-other-task");

        let (announcer_tx, mut announcer_rx) = peer_queue();
        register_in(&state, &room(), "macbook-reviewer", announcer_tx);
        let (first_tx, mut first_rx) = peer_queue();
        register_in(&state, &room(), "windows-main", first_tx);
        let (second_tx, mut second_rx) = peer_queue();
        register_in(&state, &room(), "linux-builder", second_tx);
        let (elsewhere_tx, mut elsewhere_rx) = peer_queue();
        register_in(&state, &other_room, "elsewhere", elsewhere_tx);

        let payload = protocol::encode(&ServerFrame::Notice {
            id: "ann-1".to_owned(),
            from: "macbook-reviewer".to_owned(),
            body: "the schema landed".to_owned(),
            reply_to: None,
        })
        .expect("encodes");

        assert_eq!(
            state.fanout(&room(), "macbook-reviewer", &payload),
            Fanout {
                delivered: 2,
                shed: 0
            },
            "both other peers of the room must be addressed, and neither the announcer \
             nor the other room's peer counted"
        );

        for (peer, queue) in [
            ("windows-main", &mut first_rx),
            ("linux-builder", &mut second_rx),
        ] {
            let queued = queue.try_recv().expect("a recipient holds the notice");
            assert_eq!(
                queued, payload,
                "{peer} must hold the very bytes the caller encoded: one encode for the \
                 whole room, and a reference-count bump per recipient"
            );
        }
        assert_eq!(
            announcer_rx.try_recv(),
            Err(TryRecvError::Empty),
            "an announcement must never reach its own author: starting a turn on the \
             announcer is a loop with no reader"
        );
        assert_eq!(
            elsewhere_rx.try_recv(),
            Err(TryRecvError::Empty),
            "and it must not cross the room boundary"
        );
    }

    #[test]
    fn a_fanout_counts_a_refusing_queue_without_failing_the_others() {
        let state = ServerState::new();
        let (announcer_tx, _announcer_rx) = peer_queue();
        register_in(&state, &room(), "macbook-reviewer", announcer_tx);
        let (draining_tx, mut draining_rx) = peer_queue();
        register_in(&state, &room(), "windows-main", draining_tx);
        let (stalled_tx, _stalled_rx) = peer_queue();
        register_in(&state, &room(), "linux-builder", stalled_tx);
        let (gone_tx, gone_rx) = peer_queue();
        register_in(&state, &room(), "departed", gone_tx);
        drop(gone_rx);

        let payload = Bytes::from_static(b"a notice");

        // Fill exactly one recipient's queue, so its refusal is a full queue
        // rather than an absent one.
        for _ in 0..OUTBOUND_QUEUE_CAPACITY {
            assert_eq!(
                route_status(&state, &room(), "linux-builder", &message("filler")),
                ReceiptStatus::Routed,
                "the premise: this recipient's queue must be filled to capacity"
            );
        }

        assert_eq!(
            state.fanout(&room(), "macbook-reviewer", &payload),
            Fanout {
                delivered: 1,
                shed: 2
            },
            "a full queue and a departed receiver are both shed, and the draining peer \
             is delivered to regardless: one recipient's state must not fail the fanout"
        );
        assert_eq!(
            draining_rx.try_recv().expect("the draining peer holds it"),
            payload,
            "and it holds the announced payload rather than a filler message"
        );
    }

    #[test]
    fn a_lone_peer_fans_out_to_nobody_without_failing() {
        let state = ServerState::new();
        let (tx, mut rx) = peer_queue();
        register_in(&state, &room(), "solo", tx);

        assert_eq!(
            state.fanout(&room(), "solo", &Bytes::from_static(b"a notice")),
            Fanout::default(),
            "an empty room is a fact about the room, not a failure of the request: two \
             zeroes rather than an error"
        );
        assert_eq!(
            rx.try_recv(),
            Err(TryRecvError::Empty),
            "and the lone peer is still not its own recipient"
        );
        assert_eq!(
            state.fanout(&RoomId::new("absent", "room"), "solo", &Bytes::new()),
            Fanout::default(),
            "an unknown room addresses nobody"
        );
    }

    #[test]
    fn an_emptied_room_is_not_retained() {
        let state = ServerState::new();
        let (tx, _rx) = peer_queue();
        let (registration, _evicted) = register_in(&state, &room(), "solo", tx);

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
        let (registration, _evicted) = register_in(&state, &room(), "solo", tx);

        let guard = RegisteredPeer {
            state: Arc::clone(&state),
            room: room(),
            peer: "solo".to_owned(),
            connection_id: registration.connection_id,
            peer_addr: "127.0.0.1:0".parse().expect("a loopback address"),
        };
        assert_eq!(state.list_peers(&room()), vec!["solo"]);

        drop(guard);
        assert!(
            state.list_peers(&room()).is_empty(),
            "deregistration runs in Drop, so it covers every exit path including a panic"
        );
    }

    #[test]
    fn a_full_reply_queue_drops_a_reply_but_never_blocks_the_reader() {
        // The reader must never wait on the socket, so a full reply channel is
        // not something to wait out. What it does instead depends on whether
        // the reply is contractual -- and, since the reserve, on whether it is
        // the frame that names the close.
        let (replies, mut writer_side) = reply_queue();

        // Occupancy, not `is_continue()`: a dropped reply also continues, so a
        // fill loop that only checks the control flow cannot tell "queued" from
        // "silently discarded" and would pass with the usable depth off by one.
        for slot in 1..=REPLY_QUEUE_CAPACITY {
            assert!(
                enqueue_reply(&replies, ServerFrame::Pong).is_continue(),
                "reply slot {slot} of {REPLY_QUEUE_CAPACITY} must accept a frame"
            );
            assert_eq!(
                writer_side.len(),
                slot,
                "slot {slot} of {REPLY_QUEUE_CAPACITY} must be queued, not dropped: \
                 {REPLY_QUEUE_CAPACITY} is the usable depth, and the reserve is held \
                 past it"
            );
        }

        assert!(
            enqueue_reply(&replies, ServerFrame::Pong).is_continue(),
            "a `pong` that does not fit must be dropped and the connection kept: the peer \
             is not draining, so it could not have received the pong either way, and \
             closing here disconnected exactly the heartbeating peer `peer-relay` says \
             must be kept -- measured at 15 pings before this changed"
        );
        assert_eq!(
            writer_side.len(),
            REPLY_QUEUE_CAPACITY,
            "and the dropped `pong` must not have taken the reserved slot"
        );

        assert!(
            reserve_acknowledgement(&replies).is_none(),
            "a `send` receipt is contractual, so a full queue must refuse the reservation \
             instead: the caller then declines to route, which is what stops a send being \
             delivered with its receipt dropped"
        );

        // The point of the reserve. `reserve_receipt` has just failed, and the
        // caller's next act is this call, with no `.await` between them -- so
        // the writer cannot have freed anything. Before the reserve, this was
        // the one close in the relay that could not state its cause.
        queue_diagnostic(&replies, ErrorCode::IdleTimeout, "not draining".to_owned());
        assert_eq!(
            writer_side.len(),
            REPLY_QUEUE_CAPACITY + DIAGNOSTIC_RESERVE,
            "the closing diagnostic must land on a saturated reply queue"
        );

        for _ in 0..REPLY_QUEUE_CAPACITY {
            writer_side.try_recv().expect("a queued reply");
        }
        assert!(
            matches!(
                writer_side.try_recv(),
                Ok(ServerFrame::Error {
                    code: ErrorCode::IdleTimeout,
                    ..
                })
            ),
            "and it must be the `error` frame itself behind the replies, not a \
             displaced one of them"
        );
    }

    #[test]
    fn a_reserved_receipt_slot_is_held_until_it_is_used() {
        let (replies, mut writer_side) = reply_queue();

        // Fill every usable slot but one, then reserve it. The reserve is not
        // one of them: it stays free for the diagnostic either way.
        for slot in 1..REPLY_QUEUE_CAPACITY {
            assert!(
                enqueue_reply(&replies, ServerFrame::Pong).is_continue(),
                "filling slot {slot} must succeed"
            );
            assert_eq!(writer_side.len(), slot, "slot {slot} must be queued");
        }
        let slot = reserve_acknowledgement(&replies).expect("the last usable slot is free");

        assert!(
            reserve_acknowledgement(&replies).is_none(),
            "the reservation must actually consume capacity, or two sends could both \
             believe they are acknowledged"
        );
        assert!(
            enqueue_reply(&replies, ServerFrame::Pong).is_continue(),
            "and a lossy reply must not reach past it into the diagnostic's slot"
        );
        assert_eq!(
            writer_side.len(),
            REPLY_QUEUE_CAPACITY - 1,
            "the held permit occupies capacity without queueing a frame, and the \
             refused `pong` queued nothing"
        );

        slot.send(ServerFrame::Receipt {
            id: "m1".to_owned(),
            to: "windows-main".to_owned(),
            status: ReceiptStatus::Routed,
        });

        let mut receipts = 0usize;
        while let Ok(frame) = writer_side.try_recv() {
            if matches!(frame, ServerFrame::Receipt { .. }) {
                receipts += 1;
            }
        }
        assert_eq!(
            receipts, 1,
            "the reserved slot must deliver exactly one receipt"
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
        let (_unused_reader, mut server) = protocol::framed_split(server_io);

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
