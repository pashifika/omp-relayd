//! Room-scoped payload store: content-addressed temporary files, three
//! ceilings, a time to live, and deliberately no durability.
//!
//! # What this module is for
//!
//! A payload above [`protocol::MAX_BODY_BYTES`] cannot travel as a message
//! body, and putting it into the frame path at all would recouple the per-peer
//! memory ceiling to payload size. So a payload travels as a reference and its
//! bytes live here until the room that carried them ends or their time to live
//! elapses, whichever comes first.
//!
//! # Three rules that are load-bearing
//!
//! * **No path component is derived from peer-supplied text.** `.` and `..`
//!   satisfy every rule [`protocol::validate_identifier`] enforces, so a room
//!   named `..`/`..` is admissible and a directory built by joining its
//!   components to a base would escape that base. A room's directory is named by
//!   a digest of its components instead, which makes traversal unrepresentable
//!   rather than filtered.
//! * **A payload becomes visible only once every byte is written**, by writing
//!   to a temporary name in the same directory and renaming. Nothing is flushed
//!   to stable storage: this store is meant not to outlive its process, so a
//!   durability guarantee would be paid for on every upload and then discarded
//!   at the next startup.
//! * **Accounting is charged in whole units of [`MIN_CHARGE_BYTES`].** Without
//!   it the ceilings bound bytes but not entries, and a room could hold
//!   [`MAX_ROOM_BYTES`] one-byte payloads -- 33.5 million index entries for
//!   32 MiB of content. See [`MIN_CHARGE_BYTES`].
//!
//! # Concurrency
//!
//! The [`Store`]'s index is a [`std::sync::Mutex`] holding no I/O and no
//! `.await`: every critical section is a handful of `HashMap` operations.
//! Filesystem work happens outside it, and removals are handed to
//! [`Maintenance`] rather than performed on the caller's task, so a
//! connection's [`Drop`] never blocks on a directory tree.

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::fmt;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use sha2::{Digest, Sha256};
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tokio::sync::{mpsc, watch};
use tokio::time::{Instant, MissedTickBehavior, interval};

use crate::protocol::{self, RoomId};

/// Largest payload the store will hold, in bytes.
///
/// Measured rather than rounded: the largest artifact this repository produces
/// is its own unstripped release binary at 2581648 bytes, and the largest text
/// artifact is a whole-repository diff at 1133090 bytes. 4 MiB clears the first
/// by 1.62x and the second by 3.70x. 2 MiB does not hold the binary at all;
/// 8 MiB is 3.25x the largest artifact observed and would double what one
/// payload can claim of the two ceilings below for a need nothing measured
/// shows. The measurement is
/// `rasen/changes/extend-long-payloads-by-reference/evidence/store-ceilings-and-lifetime.md`.
pub const MAX_PAYLOAD_BYTES: u64 = 4 * 1024 * 1024;

/// Largest total one room may hold, in bytes.
///
/// A floor and a multiple, not a preference. The floor is
/// [`MAX_PAYLOAD_BYTES`], below which the per-payload maximum is unreachable.
/// The floor alone is not enough: a room whose ceiling is one maximal payload is
/// locked by a single attachment for that payload's whole life, so the room
/// bound would be reached by one sender rather than by accumulation. Eight is
/// the smallest power-of-two multiple that admits a maximal payload beside a
/// working stream of ordinary ones -- 113 pull-request diffs at the largest size
/// this repository has produced.
pub const MAX_ROOM_BYTES: u64 = 8 * MAX_PAYLOAD_BYTES;

/// Largest total the process may hold across every room, in bytes.
///
/// Eight rooms may each reach their own ceiling before this one refuses. The
/// pool is shared rather than partitioned, so many more than eight rooms coexist
/// while none is full; eight is the worst case, not a supported room count.
///
/// The cross-check that fixes the order of magnitude is the relay's existing
/// appetite: [`crate::relay::OUTBOUND_QUEUE_CAPACITY`] times
/// [`protocol::MAX_FRAME_BYTES`] is 8 MiB of resident memory for *one* stalled
/// peer. The whole store is 32 stalled peers' worth of bytes, on disk instead of
/// in the heap. A disk ceiling above what the process already permits itself in
/// RAM would be the wrong shape whatever the artifacts measure.
pub const MAX_STORE_BYTES: u64 = 8 * MAX_ROOM_BYTES;

/// Accounting granularity: no payload or reservation is charged less than this.
///
/// This is not in the design's three ceilings, and it is here because those
/// three bound *bytes* while leaving *entries* unbounded. A room may hold
/// [`MAX_ROOM_BYTES`] of one-byte payloads, which is 33554432 index entries at
/// roughly 140 bytes each -- 4.7 GiB of resident memory to account for 32 MiB of
/// content, and the process ceiling admits eight such rooms. Charging in whole
/// units of the frame cap bounds a room at 512 entries and the process at 4096,
/// which is 573 KiB.
///
/// It costs nothing real. A payload that fits inside
/// [`protocol::MAX_BODY_BYTES`] did not need to be an attachment: it fits in a
/// message body, which is the cheaper path in every respect. So the granularity
/// is exactly the size below which the feature is the wrong tool.
pub const MIN_CHARGE_BYTES: u64 = protocol::MAX_FRAME_BYTES as u64;

/// How long a stored payload remains fetchable, from the moment it was stored.
///
/// Derived from the room ceiling and a measured artifact rate. The room bound
/// already covers a *disconnected* recipient, since a peer silent past
/// [`crate::relay::IDLE_DEADLINE`] is closed and an emptied room takes its
/// payloads with it. This bound exists for a live, long-lived room, where what
/// it must hold down is steady-state occupancy:
///
/// ```text
/// occupancy = artifact rate x artifact size x time to live
/// ```
///
/// At this repository's measured peak of 10 commits in an hour and its largest
/// merged pull-request diff of 301824 bytes, that is 3018240 bytes per hour of
/// retention. Holding ordinary traffic to a quarter of [`MAX_ROOM_BYTES`] -- so
/// that a refusal means someone sent something genuinely large rather than that
/// the room silted up -- bounds the lifetime at 2.78 hours. Two hours is that
/// rounded down, and yields 18.0% occupancy.
///
/// Fixed from the moment of upload and never extended by a fetch: an
/// access-extended lifetime is unbounded for anything that keeps polling, which
/// is the accumulation this bound exists to prevent.
pub const PAYLOAD_TIME_TO_LIVE: Duration = Duration::from_secs(2 * 60 * 60);

/// How long a granted reservation waits for the upload it authorizes.
///
/// This bounds the gap between a grant and the *start* of its upload, which is a
/// client-local sequence: the client already holds the bytes it hashed. It does
/// not bound the transfer, because [`Store::begin_upload`] takes the allowance
/// out of the reservation table and holds it for as long as the upload runs.
/// Bounding the transfer here would fail a large payload on a slow link
/// *because it was working*, which is the same defect the client's request
/// deadline has and the reason a transfer is bounded by progress instead.
///
/// Sixty seconds is therefore generous for what it bounds, and is chosen for
/// what an abandoned reservation costs: a room's ceiling is eight maximal
/// reservations, so at most a minute of a room's budget can be held by a peer
/// that went away between the grant and the upload.
pub const RESERVATION_TIME_TO_LIVE: Duration = Duration::from_secs(60);

/// How often [`Maintenance`] removes what has expired.
///
/// Sets how far past its time to live a payload can survive: 2 h + 60 s, an
/// overshoot of 0.83%.
pub const SWEEP_INTERVAL: Duration = Duration::from_secs(60);

/// Directory beneath the platform's temporary directory that holds every
/// instance's store.
///
/// Stable and documentable on purpose: an operator who wants the store on a
/// sized filesystem mounts *this* path, or redirects it wholesale by setting
/// `TMPDIR`. It is not a relay configuration value -- the server's surface stays
/// at two -- because [`std::env::temp_dir`] is a platform facility rather than
/// something this process defines.
pub const STORE_BASE_DIR: &str = "omp-relayd";

/// Unpadded base64url, whose alphabet excludes `/`, `+`, and `=`, so a digest is
/// safe as a URL path component with no escaping.
const BASE64URL: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

/// Why a reservation was refused.
///
/// A refusal is an answer to a request the relay understood and acted on, so it
/// is reported as a status rather than as a protocol error: naming which bound
/// was reached is what lets a sender act on it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Refusal {
    /// Above [`MAX_PAYLOAD_BYTES`].
    PayloadTooLarge,
    /// The room's own total is reached.
    RoomFull,
    /// The process-wide total is reached.
    StoreFull,
}

impl Refusal {
    /// Stable name for logs and for the wire status.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::PayloadTooLarge => "payload_too_large",
            Self::RoomFull => "room_full",
            Self::StoreFull => "store_full",
        }
    }
}

impl fmt::Display for Refusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Why an upload could not begin.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum UploadRefusal {
    /// No live reservation exists for this room and digest.
    #[error("no live reservation for this room and digest")]
    Unreserved,
    /// The declared byte count exceeds what was reserved.
    #[error("declared {declared} bytes against a reservation of {reserved}")]
    OverReservation {
        /// What the upload declared.
        declared: u64,
        /// What the reservation allows.
        reserved: u64,
    },
}

/// Why an upload did not become a stored payload.
#[derive(Debug, thiserror::Error)]
pub enum UploadError {
    /// The bytes received hash to something other than the address they were
    /// uploaded under, so the address would have lied about its content.
    #[error("payload hashes to {computed}, not to the address {declared} it was uploaded under")]
    DigestMismatch {
        /// The address the upload claimed.
        declared: String,
        /// What its bytes actually hash to.
        computed: String,
    },
    /// Fewer or more bytes arrived than were declared.
    #[error("received {received} bytes against a declared length of {declared}")]
    LengthMismatch {
        /// What the upload declared.
        declared: u64,
        /// What arrived.
        received: u64,
    },
    /// The room's payloads were removed while the upload was in flight, so
    /// there is nothing left for it to join.
    #[error("the room ended while the upload was in flight")]
    RoomGone,
    /// The filesystem refused the write.
    #[error("writing the payload failed")]
    Io(#[from] io::Error),
}

/// A granted reservation, as the reserving peer needs to see it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Grant {
    /// How long the payload will remain fetchable once uploaded. Stated to the
    /// reserving peer so a sender can tell a recipient how long the reference
    /// resolves, which is the whole reason the reply carries anything at all.
    pub time_to_live: Duration,
}

/// One reservation, holding a room's allowance until it is used or expires.
#[derive(Clone, Copy, Debug)]
struct Reservation {
    /// Byte count the reserving peer declared.
    declared: u64,
    /// Allowance charged against the room and the process.
    charged: u64,
    expires_at: Instant,
}

/// One stored payload.
#[derive(Clone, Copy, Debug)]
struct Payload {
    /// Byte length of the file.
    bytes: u64,
    /// Allowance charged against the room and the process.
    charged: u64,
    expires_at: Instant,
}

/// One room's accounting. The room's *directory* is named by a digest of its
/// components; this map is memory and is keyed by the room itself.
#[derive(Debug, Default)]
struct RoomState {
    reserved: HashMap<String, Reservation>,
    /// Digest to allowance held by an upload currently in flight. An entry here
    /// is not fetchable: a fetch reports absent until the rename lands.
    uploading: HashMap<String, u64>,
    stored: HashMap<String, Payload>,
    /// Sum of every allowance above. Maintained rather than recomputed so a
    /// reservation costs one addition instead of a walk.
    charged: u64,
}

impl RoomState {
    fn is_idle(&self) -> bool {
        self.charged == 0 && self.reserved.is_empty() && self.uploading.is_empty()
    }
}

/// The store's whole index.
#[derive(Debug, Default)]
struct Index {
    rooms: HashMap<RoomId, RoomState>,
    charged: u64,
}

/// A room-scoped, content-addressed store of temporary files.
#[derive(Debug)]
pub struct Store {
    root: PathBuf,
    index: Mutex<Index>,
    /// Paths handed to [`Maintenance`] for removal. Unbounded because the
    /// alternative is blocking a [`Drop`] on a filesystem tree, and because
    /// every entry is bounded by the ceilings that admitted it.
    removals: mpsc::UnboundedSender<PathBuf>,
    /// Distinguishes concurrent uploads of the same digest, so two of them
    /// cannot write the same temporary file.
    next_temp: AtomicU64,
}

impl Store {
    /// Creates the store's own directory beneath `base`, removing whatever a
    /// previous run of this instance left there.
    ///
    /// `instance` distinguishes one relay from another on a shared host: two
    /// relays on one filesystem get separate directories, and a restart of
    /// either adopts nothing from the other. `base` itself is left in place and
    /// is the path an operator mounts.
    ///
    /// Nothing here is durable, so removing a predecessor's directory is the
    /// point rather than a cleanup: a relay that was killed outright must not
    /// serve payloads whose rooms no longer exist.
    ///
    /// # Errors
    ///
    /// Returns the first filesystem error that prevents the directory from
    /// existing, being private, or being empty.
    pub async fn open(base: &Path, instance: &str) -> io::Result<(Arc<Self>, Maintenance)> {
        let root = base.join(instance_dir_name(instance));

        // A predecessor's tree, if any. `NotFound` is the ordinary case.
        match fs::remove_dir_all(&root).await {
            Ok(()) => {
                tracing::info!(root = %root.display(), "removed a previous run's payload store");
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }

        fs::create_dir_all(base).await?;
        create_private_dir(&root).await?;

        let (removals, receiver) = mpsc::unbounded_channel();
        let store = Arc::new(Self {
            root,
            index: Mutex::new(Index::default()),
            removals,
            next_temp: AtomicU64::new(0),
        });
        let maintenance = Maintenance {
            store: Arc::clone(&store),
            removals: receiver,
        };
        Ok((store, maintenance))
    }

    /// The directory this store owns.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// A poisoned index is still a consistent one: every critical section is a
    /// handful of `HashMap` operations with no intermediate invalid state, so
    /// recovering the guard beats propagating one task's panic into every
    /// connection.
    fn index(&self) -> MutexGuard<'_, Index> {
        self.index.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Charges `bytes` against the room and the process, or names the bound it
    /// would have crossed.
    ///
    /// The reservation is what makes an upload authorized at all: the transfer
    /// route carries no credential, so write authority is exactly the handshake
    /// that admitted the reserving connection to this room.
    ///
    /// # Errors
    ///
    /// Returns which of the three ceilings refused.
    pub fn reserve(&self, room: &RoomId, digest: &str, bytes: u64) -> Result<Grant, Refusal> {
        if bytes > MAX_PAYLOAD_BYTES {
            return Err(Refusal::PayloadTooLarge);
        }
        let charged = charge_for(bytes);
        let now = Instant::now();
        let mut index = self.index();

        // Read what the room already owes before anything is inserted, so a
        // refusal leaves no empty room entry behind.
        let (room_charged, released, held) = match index.rooms.get(room) {
            Some(state) => (
                state.charged,
                // A second reservation of the same digest replaces the first
                // rather than charging twice: the address names one payload, so
                // two reservations of it are one intent retried.
                state
                    .reserved
                    .get(digest)
                    .map_or(0, |reservation| reservation.charged),
                state
                    .stored
                    .get(digest)
                    .is_some_and(|payload| payload.expires_at > now),
            ),
            None => (0, 0, false),
        };

        if held {
            // Nothing to charge: the payload is already held, and an upload of
            // it will be answered "already held" without writing anything.
            return Ok(Grant {
                time_to_live: PAYLOAD_TIME_TO_LIVE,
            });
        }

        // The room's own bound is checked first, because it is the one a peer
        // can act on: `room_full` says wait or let something expire, while
        // `store_full` says the relay is loaded and names nothing the peer
        // controls. A single room can only ever hold `MAX_ROOM_BYTES` of the
        // process total, so reaching the store bound always means other rooms.
        if room_charged + charged - released > MAX_ROOM_BYTES {
            return Err(Refusal::RoomFull);
        }
        if index.charged + charged - released > MAX_STORE_BYTES {
            return Err(Refusal::StoreFull);
        }

        index.charged = index.charged + charged - released;
        let state = index.rooms.entry(room.clone()).or_default();
        state.reserved.insert(
            digest.to_owned(),
            Reservation {
                declared: bytes,
                charged,
                expires_at: now + RESERVATION_TIME_TO_LIVE,
            },
        );
        state.charged = room_charged + charged - released;

        Ok(Grant {
            time_to_live: PAYLOAD_TIME_TO_LIVE,
        })
    }

    /// Byte length of a stored, unexpired payload.
    ///
    /// An upload in flight is not a payload: this reports absent until the
    /// rename lands, which is what keeps a fetch from ever observing a partial
    /// write.
    pub fn payload_len(&self, room: &RoomId, digest: &str) -> Option<u64> {
        let now = Instant::now();
        self.index()
            .rooms
            .get(room)?
            .stored
            .get(digest)
            .filter(|payload| payload.expires_at > now)
            .map(|payload| payload.bytes)
    }

    /// Path of a stored payload's file.
    ///
    /// Both components are digests -- of the room's parts and of the payload's
    /// content -- so neither can be a relative path segment.
    pub fn payload_path(&self, room: &RoomId, digest: &str) -> PathBuf {
        self.room_dir(room).join(digest)
    }

    fn room_dir(&self, room: &RoomId) -> PathBuf {
        self.root.join(room_dir_name(room))
    }

    /// Takes a reservation's allowance and hands back the guard that owns it
    /// for the length of the upload.
    ///
    /// The allowance moves out of the reservation table here, so the transfer
    /// cannot be failed by its own reservation expiring underneath it.
    ///
    /// # Errors
    ///
    /// Returns [`UploadRefusal::Unreserved`] when no live reservation matches,
    /// and [`UploadRefusal::OverReservation`] when more bytes are declared than
    /// were reserved.
    pub fn begin_upload(
        self: &Arc<Self>,
        room: &RoomId,
        digest: &str,
        declared: u64,
    ) -> Result<Accepted, UploadRefusal> {
        let now = Instant::now();
        let mut index = self.index();

        let Some(state) = index.rooms.get_mut(room) else {
            return Err(UploadRefusal::Unreserved);
        };
        if state
            .stored
            .get(digest)
            .is_some_and(|payload| payload.expires_at > now)
        {
            return Ok(Accepted::AlreadyHeld);
        }
        let Some(reservation) = state.reserved.get(digest).copied() else {
            return Err(UploadRefusal::Unreserved);
        };
        if reservation.expires_at <= now {
            return Err(UploadRefusal::Unreserved);
        }
        if declared > reservation.declared {
            return Err(UploadRefusal::OverReservation {
                declared,
                reserved: reservation.declared,
            });
        }

        state.reserved.remove(digest);
        match state.uploading.entry(digest.to_owned()) {
            // A concurrent upload of the same digest already holds an
            // allowance; this one returns the reservation's rather than
            // stacking a second charge on one address.
            Entry::Occupied(_) => {
                let released = reservation.charged;
                state.charged = state.charged.saturating_sub(released);
                index.charged = index.charged.saturating_sub(released);
                return Err(UploadRefusal::Unreserved);
            }
            Entry::Vacant(slot) => {
                slot.insert(reservation.charged);
            }
        }

        Ok(Accepted::Upload(Upload {
            store: Arc::clone(self),
            room: room.clone(),
            digest: digest.to_owned(),
            declared,
            hasher: Sha256::new(),
            written: 0,
            temp: None,
            file: None,
            settled: false,
        }))
    }

    /// Returns an in-flight upload's allowance, having stored nothing.
    fn abandon_upload(&self, room: &RoomId, digest: &str) {
        let mut index = self.index();
        let Index { rooms, charged } = &mut *index;
        let Some(state) = rooms.get_mut(room) else {
            return;
        };
        if let Some(released) = state.uploading.remove(digest) {
            state.charged = state.charged.saturating_sub(released);
            *charged = charged.saturating_sub(released);
        }
        Self::drop_room_if_idle(&mut index, room);
    }

    /// Converts an in-flight upload into a stored payload.
    fn commit_upload(&self, room: &RoomId, digest: &str, bytes: u64) -> Result<(), UploadError> {
        let mut index = self.index();
        let Index { rooms, charged } = &mut *index;
        let Some(state) = rooms.get_mut(room) else {
            return Err(UploadError::RoomGone);
        };
        let Some(held) = state.uploading.remove(digest) else {
            return Err(UploadError::RoomGone);
        };
        // The upload may have declared fewer bytes than were reserved, and the
        // charge is per unit, so the difference goes back to the budget.
        let settled = charge_for(bytes);
        state.charged = state.charged + settled - held;
        *charged = *charged + settled - held;
        state.stored.insert(
            digest.to_owned(),
            Payload {
                bytes,
                charged: settled,
                expires_at: Instant::now() + PAYLOAD_TIME_TO_LIVE,
            },
        );
        Ok(())
    }

    /// Removes every payload of `room` and returns its whole allowance.
    ///
    /// Called when a room's last peer deregisters. The index entry goes
    /// immediately, so the payloads stop being fetchable on the spot; the
    /// directory is handed to [`Maintenance`], because the caller is a
    /// connection task's [`Drop`] and must not block on a filesystem tree.
    pub fn forget_room(&self, room: &RoomId) {
        let released = {
            let mut index = self.index();
            match index.rooms.remove(room) {
                Some(state) => {
                    index.charged = index.charged.saturating_sub(state.charged);
                    state.charged
                }
                None => return,
            }
        };
        self.queue_removal(self.room_dir(room));
        tracing::debug!(%room, released_bytes = released, "room payloads removed");
    }

    /// Removes every payload and reservation whose time has passed.
    ///
    /// Returns how many payloads and how many reservations were removed, so a
    /// test asserts observed counts rather than a bare verdict.
    pub fn sweep(&self) -> Swept {
        let now = Instant::now();
        let mut swept = Swept::default();
        let mut files = Vec::new();
        let mut idle_rooms = Vec::new();

        {
            let mut index = self.index();
            let Index { rooms, charged } = &mut *index;
            for (room, state) in rooms.iter_mut() {
                // Accumulated rather than subtracted inside each closure: a
                // `retain` closure holds `state.reserved` borrowed, so it cannot
                // also touch `state.charged`.
                let mut released = 0_u64;
                state.reserved.retain(|_, reservation| {
                    let live = reservation.expires_at > now;
                    if !live {
                        released += reservation.charged;
                        swept.reservations += 1;
                    }
                    live
                });
                let dir = self.room_dir(room);
                state.stored.retain(|digest, payload| {
                    let live = payload.expires_at > now;
                    if !live {
                        released += payload.charged;
                        files.push(dir.join(digest));
                        swept.payloads += 1;
                    }
                    live
                });
                state.charged = state.charged.saturating_sub(released);
                *charged = charged.saturating_sub(released);

                if state.is_idle() && state.stored.is_empty() {
                    idle_rooms.push(room.clone());
                }
            }
            for room in &idle_rooms {
                rooms.remove(room);
            }
        }

        for file in files {
            self.queue_removal(file);
        }
        swept
    }

    /// Drops a room's index entry once it holds and owes nothing, so a
    /// long-lived relay's index tracks rooms with payloads rather than every
    /// room that ever reserved one.
    fn drop_room_if_idle(index: &mut Index, room: &RoomId) {
        if index
            .rooms
            .get(room)
            .is_some_and(|state| state.is_idle() && state.stored.is_empty())
        {
            index.rooms.remove(room);
        }
    }

    fn queue_removal(&self, path: PathBuf) {
        // Ignored: a closed channel means maintenance has stopped, which
        // happens only on the shutdown path that removes the whole root.
        let _ = self.removals.send(path);
    }

    fn temp_path(&self, room: &RoomId, digest: &str) -> PathBuf {
        let n = self.next_temp.fetch_add(1, Ordering::Relaxed);
        // Leading dot and a per-store counter: in the same directory, so the
        // rename onto the final name is atomic, and distinct from any concurrent
        // upload of the same digest.
        self.room_dir(room).join(format!(".{digest}.{n}"))
    }
}

/// What one sweep removed.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Swept {
    /// Stored payloads whose time to live had elapsed.
    pub payloads: u32,
    /// Reservations never taken up.
    pub reservations: u32,
}

/// What [`Store::begin_upload`] admitted.
///
/// A separate variant rather than a flag on [`Upload`], because "already held"
/// owns no allowance, no temporary file, and no room -- it is the absence of an
/// upload, and modelling it as an upload that declines to do anything means
/// every method carrying a branch for a state it cannot act in.
// The large variant is the whole point of the type, and boxing it would add an
// allocation to avoid a stack move that returning `Upload` directly performs
// anyway. `AlreadyHeld` is constructed at most once per upload request.
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum Accepted {
    /// The store already holds this payload. Writing nothing is what makes a
    /// retried upload safe.
    AlreadyHeld,
    /// The upload may proceed.
    Upload(Upload),
}

/// An upload in flight, owning the room allowance it will either commit or
/// return.
///
/// Every failure path returns the allowance and removes the partial file, and
/// that is enforced by [`Drop`] rather than by every caller remembering: an
/// upload whose connection closes partway is the ordinary case, not an
/// exception.
#[derive(Debug)]
pub struct Upload {
    store: Arc<Store>,
    room: RoomId,
    digest: String,
    declared: u64,
    hasher: Sha256,
    written: u64,
    temp: Option<PathBuf>,
    file: Option<fs::File>,
    settled: bool,
}

impl Upload {
    /// Bytes written so far.
    pub fn written(&self) -> u64 {
        self.written
    }

    /// Writes one chunk, hashing it on the way through.
    ///
    /// # Errors
    ///
    /// Returns [`UploadError::LengthMismatch`] when the chunk would carry the
    /// upload past its declared length, and [`UploadError::Io`] when the write
    /// fails.
    pub async fn write(&mut self, chunk: &[u8]) -> Result<(), UploadError> {
        let len = chunk.len() as u64;
        if self.written + len > self.declared {
            return Err(UploadError::LengthMismatch {
                declared: self.declared,
                received: self.written + len,
            });
        }

        // Opened on first use rather than by `begin_upload`, so a refused or
        // abandoned upload costs no directory creation, and the room's
        // directory is created only by an upload that has bytes for it.
        // `Option::insert` hands back the handle it stored, so no path here
        // unwraps an `Option` it just filled.
        let file = if let Some(file) = self.file.as_mut() {
            file
        } else {
            create_private_dir_if_absent(&self.store.room_dir(&self.room)).await?;
            let temp = self.store.temp_path(&self.room, &self.digest);
            let handle = fs::File::create(&temp).await?;
            self.temp = Some(temp);
            self.file.insert(handle)
        };

        file.write_all(chunk).await?;
        self.hasher.update(chunk);
        self.written += len;
        Ok(())
    }

    /// Verifies the digest and length, then makes the payload visible with one
    /// rename.
    ///
    /// Nothing is flushed to stable storage: see this module's header. The
    /// rename is the only guarantee a fetch needs, because it must never
    /// observe a partial payload.
    ///
    /// # Errors
    ///
    /// Returns [`UploadError::LengthMismatch`] when fewer bytes arrived than
    /// were declared, [`UploadError::DigestMismatch`] when the bytes hash to
    /// something other than the address, [`UploadError::RoomGone`] when the
    /// room ended mid-upload, and [`UploadError::Io`] when the rename fails.
    pub async fn finish(mut self) -> Result<u64, UploadError> {
        match self.commit().await {
            Ok(bytes) => {
                self.settled = true;
                Ok(bytes)
            }
            Err(error) => {
                // Removed here rather than handed to [`Maintenance`]: this path
                // can await, so the partial file is gone before the caller is
                // told anything. Only [`Drop`], which cannot await, needs the
                // queue -- so "a refused upload leaves nothing behind" is an
                // observable fact rather than an eventual one.
                if let Some(temp) = self.temp.take() {
                    remove_path(&temp).await;
                }
                Err(error)
            }
        }
    }

    /// The whole of `finish` except the settling, so that every failure inside
    /// it reaches one cleanup path instead of repeating it.
    async fn commit(&mut self) -> Result<u64, UploadError> {
        if self.written != self.declared {
            return Err(UploadError::LengthMismatch {
                declared: self.declared,
                received: self.written,
            });
        }

        // `mem::take` rather than a clone: `Upload` implements `Drop`, so the
        // hasher cannot be moved out of `self`.
        let computed = base64url(&std::mem::take(&mut self.hasher).finalize());
        if computed != self.digest {
            return Err(UploadError::DigestMismatch {
                declared: self.digest.clone(),
                computed,
            });
        }

        // A zero-length payload never wrote a chunk, so it has no file yet.
        if self.temp.is_none() {
            create_private_dir_if_absent(&self.store.room_dir(&self.room)).await?;
            let temp = self.store.temp_path(&self.room, &self.digest);
            fs::File::create(&temp).await?;
            self.temp = Some(temp);
        }
        // Dropped before the rename so no handle is open across it.
        self.file = None;

        // Left in place until the rename succeeds, so the cleanup path in
        // `finish` still knows what to remove if it does not.
        let Some(temp) = self.temp.as_ref() else {
            return Err(UploadError::Io(io::Error::new(
                io::ErrorKind::NotFound,
                "the upload's temporary file was not created",
            )));
        };
        let final_path = self.store.payload_path(&self.room, &self.digest);
        fs::rename(temp, &final_path).await?;
        self.temp = None;

        // The index is committed *after* the rename, and that order is the
        // whole of "a fetch never observes a partial payload": the index is
        // what a fetch consults, so a file is unreachable until the entry that
        // governs its lifetime exists. Committing first would open a window in
        // which the index promises a file that is not there yet.
        if let Err(error) = self
            .store
            .commit_upload(&self.room, &self.digest, self.written)
        {
            // The room ended under the upload, so the renamed file is now
            // unreferenced. Removed on the caller's path in `finish`.
            self.temp = Some(final_path);
            return Err(error);
        }

        Ok(self.written)
    }
}

impl Drop for Upload {
    fn drop(&mut self) {
        if self.settled {
            return;
        }
        self.store.abandon_upload(&self.room, &self.digest);
        if let Some(temp) = self.temp.take() {
            self.store.queue_removal(temp);
        }
        tracing::debug!(
            room = %self.room,
            digest = %self.digest,
            written = self.written,
            declared = self.declared,
            "upload abandoned; allowance returned"
        );
    }
}

/// The store's background work: expiry and removal.
///
/// One task rather than two. Both are periodic filesystem work with no ordering
/// between them, and a single `select!` keeps the store's whole lifetime -- root
/// removal included -- on one join handle the binary can await.
#[derive(Debug)]
pub struct Maintenance {
    store: Arc<Store>,
    removals: mpsc::UnboundedReceiver<PathBuf>,
}

impl Maintenance {
    /// Sweeps expired payloads, removes what has been queued, and removes the
    /// store's own directory when `shutdown` is signalled.
    ///
    /// The final removal is why this is awaited rather than detached: a store
    /// that outlives its process is exactly what the startup sweep exists to
    /// clean up after, and a graceful shutdown should not need it.
    pub async fn run(mut self, mut shutdown: watch::Receiver<bool>) {
        let mut ticks = interval(SWEEP_INTERVAL);
        // The first tick fires immediately; skip missed ones rather than
        // catching up, since a sweep is idempotent.
        ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                _ = shutdown.changed() => break,
                _ = ticks.tick() => {
                    let swept = self.store.sweep();
                    if swept != Swept::default() {
                        tracing::info!(
                            payloads = swept.payloads,
                            reservations = swept.reservations,
                            "expired payloads removed"
                        );
                    }
                }
                Some(path) = self.removals.recv() => remove_path(&path).await,
            }
        }

        // Drain what is already queued, then take the root with it. Draining
        // first is not strictly needed -- the root removal subsumes every path
        // beneath it -- but it keeps the two orders equivalent if the root ever
        // stops being the common ancestor.
        while let Ok(path) = self.removals.try_recv() {
            remove_path(&path).await;
        }
        match fs::remove_dir_all(self.store.root()).await {
            Ok(()) => tracing::info!(
                root = %self.store.root().display(),
                "payload store removed on shutdown"
            ),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => tracing::warn!(
                root = %self.store.root().display(),
                %error,
                "could not remove the payload store on shutdown"
            ),
        }
    }

    /// Performs every removal queued so far, without shutting down.
    ///
    /// The removal path exists because [`Drop`] cannot await, so a test that
    /// asserts the filesystem has to perform the queued work first -- and a test
    /// that ran [`Self::run`] concurrently would be asserting against a race
    /// instead of against the rule, while one that ran it to completion would
    /// find the whole store root removed by its shutdown path.
    ///
    /// It is the same loop body [`Self::run`] uses, so nothing here is a
    /// separate implementation that could drift from production behaviour.
    pub async fn drain(&mut self) {
        while let Ok(path) = self.removals.try_recv() {
            remove_path(&path).await;
        }
    }
}

/// Removes a file or a directory tree, whichever the path names.
async fn remove_path(path: &Path) {
    // A payload is a file and a room is a directory, and the caller knows
    // which -- but not usefully: a queued room directory may already be gone
    // because its last payload expired. Trying both is one syscall more in the
    // directory case and no branch to keep correct.
    match fs::remove_file(path).await {
        Ok(()) => return,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return,
        Err(_) => {}
    }
    match fs::remove_dir_all(path).await {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => {
            tracing::warn!(path = %path.display(), %error, "could not remove a payload path");
        }
    }
}

/// Creates a directory only this user may enter.
///
/// The mode matters beyond a container. The deployment model contemplates a
/// trusted host, where a world-traversable temporary directory would expose to
/// every local user exactly the payload content the logging rules forbid
/// recording.
async fn create_private_dir(path: &Path) -> io::Result<()> {
    fs::create_dir(path).await?;
    set_private(path).await
}

async fn create_private_dir_if_absent(path: &Path) -> io::Result<()> {
    match fs::create_dir(path).await {
        Ok(()) => set_private(path).await,
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => Ok(()),
        Err(error) => Err(error),
    }
}

#[cfg(unix)]
async fn set_private(path: &Path) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).await
}

#[cfg(not(unix))]
async fn set_private(_path: &Path) -> io::Result<()> {
    // No portable equivalent. The supported deployment is a Linux container.
    Ok(())
}

/// Rounds a byte count up to whole units of [`MIN_CHARGE_BYTES`].
fn charge_for(bytes: u64) -> u64 {
    bytes.div_ceil(MIN_CHARGE_BYTES) * MIN_CHARGE_BYTES
}

/// Directory name for one relay instance.
fn instance_dir_name(instance: &str) -> String {
    format!("blobs-{}", &digest_of(&[instance.as_bytes()])[..16])
}

/// Directory name for one room.
///
/// A digest of the components, never the components themselves. `.` and `..`
/// satisfy every rule [`protocol::validate_identifier`] enforces, so a room
/// named `..`/`..` is admissible and joining its parts to a base directory would
/// escape that base. Hashing makes the escape unrepresentable instead of
/// filtered, and cannot be reopened by someone relaxing an identifier rule.
///
/// Each component is length-prefixed before hashing. A separator byte would not
/// do: an identifier may contain a NUL, so `("a\0b", "c")` and `("a", "b\0c")`
/// would collide onto one directory.
fn room_dir_name(room: &RoomId) -> String {
    let project = room.project.as_bytes();
    let task = room.task.as_bytes();
    let project_len = (project.len() as u64).to_le_bytes();
    let task_len = (task.len() as u64).to_le_bytes();
    digest_of(&[&project_len, project, &task_len, task])
}

/// SHA-256 of the concatenated parts, as unpadded base64url.
fn digest_of(parts: &[&[u8]]) -> String {
    let mut hasher = Sha256::new();
    for part in parts {
        hasher.update(part);
    }
    base64url(&hasher.finalize())
}

/// SHA-256 of one payload, as unpadded base64url. The address a payload is
/// stored under.
pub fn digest(bytes: &[u8]) -> String {
    base64url(&Sha256::digest(bytes))
}

/// Encodes 32 bytes as 43 unpadded base64url characters.
///
/// Hand-written rather than a dependency: this crate needs to *encode* a digest
/// and never to decode one, which is fifteen lines against a fourth crate on a
/// graph deliberately kept small.
fn base64url(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for group in bytes.chunks(3) {
        let b0 = u32::from(group[0]);
        let b1 = group.get(1).copied().map_or(0, u32::from);
        let b2 = group.get(2).copied().map_or(0, u32::from);
        let packed = (b0 << 16) | (b1 << 8) | b2;
        // One character per 6 bits, minus the characters that would encode only
        // padding: 3 bytes give 4, 2 give 3, 1 gives 2.
        for i in 0..=group.len() {
            let index = (packed >> (18 - 6 * i)) & 0x3f;
            out.push(char::from(BASE64URL[index as usize]));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A store rooted in a fresh directory of its own, removed when the test
    /// ends.
    ///
    /// Every test names its own base directory, so nothing here shares the
    /// process's temporary directory with a concurrently running test -- which
    /// matters more than usual, because a store's first act is to remove what a
    /// predecessor of the same name left behind.
    struct Fixture {
        base: PathBuf,
        store: Arc<Store>,
        maintenance: Maintenance,
    }

    impl Fixture {
        async fn open(name: &str) -> Self {
            let base =
                std::env::temp_dir().join(format!("omp-relayd-test-{name}-{}", std::process::id()));
            // A previous run of this test, if the process id was reused.
            let _ = std::fs::remove_dir_all(&base);
            let (store, maintenance) = Store::open(&base, name).await.expect("store opens");
            Self {
                base,
                store,
                maintenance,
            }
        }

        /// Performs the removals the store has queued, so a test can assert the
        /// filesystem instead of the queue.
        async fn drain(&mut self) {
            self.maintenance.drain().await;
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.base);
        }
    }

    fn room(project: &str, task: &str) -> RoomId {
        RoomId::new(project, task)
    }

    /// Reserves and uploads `bytes` under its own digest, returning the digest.
    async fn put(store: &Arc<Store>, room: &RoomId, bytes: &[u8]) -> String {
        let address = digest(bytes);
        store
            .reserve(room, &address, bytes.len() as u64)
            .expect("reservation granted");
        let Accepted::Upload(mut upload) = store
            .begin_upload(room, &address, bytes.len() as u64)
            .expect("upload accepted")
        else {
            return address;
        };
        upload.write(bytes).await.expect("chunk written");
        upload.finish().await.expect("upload finished");
        address
    }

    /// Every file under `root`, as paths relative to it.
    fn tree(root: &Path) -> Vec<PathBuf> {
        let mut found = Vec::new();
        let mut pending = vec![root.to_path_buf()];
        while let Some(dir) = pending.pop() {
            let Ok(entries) = std::fs::read_dir(&dir) else {
                continue;
            };
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    pending.push(path);
                } else if let Ok(relative) = path.strip_prefix(root) {
                    found.push(relative.to_path_buf());
                }
            }
        }
        found.sort();
        found
    }

    // 1.10: the two implementations are pinned to one encoding before any
    // transfer exists. Each right-hand side was produced by Bun 1.3.14's
    // `crypto.subtle.digest("SHA-256", ...)` rendered as base64url, which is
    // exactly what `extension/src/client.ts` will call.
    #[test]
    fn digests_match_the_typescript_web_crypto_encoding() {
        assert_eq!(
            digest(b""),
            "47DEQpj8HBSa-_TImW-5JCeuQeRkm5NMpJWZG3hSuFU",
            "empty payload"
        );
        assert_eq!(
            digest(b"abc"),
            "ungWv48Bz-pBQUDeXa4iI7ADYaOWF3qctBD_YfIAFa0",
            "three bytes"
        );
        assert_eq!(
            digest(b"omp-relay attachment probe"),
            "EcmMnNusN8grkJEJrdmXDLtLYl13I_TymKTGcRCQx4k",
            "text payload"
        );
        assert_eq!(
            digest(&vec![0_u8; 65536]),
            "3i8lYGSgr3l3R8K5dQXcC5898N5PSJ6scxwjrpypzDE",
            "one frame cap of zeros"
        );
    }

    #[test]
    fn a_digest_is_43_unpadded_base64url_characters() {
        let value = digest(b"anything");
        assert_eq!(
            value.len(),
            protocol::DIGEST_CHARS,
            "digest {value} has the wrong length"
        );
        // The wire rule and the encoder are asserted against each other rather
        // than each against a literal, so neither can drift alone.
        assert_eq!(
            protocol::validate_digest(&value),
            Ok(()),
            "the store produced an address the wire rule rejects: {value}"
        );
    }

    #[test]
    fn a_room_and_its_components_never_collide_across_a_nul() {
        // The reason each component is length-prefixed rather than separated by
        // a byte: an identifier may contain a NUL, so a separator alone would
        // map these two rooms onto one directory.
        assert_ne!(
            room_dir_name(&room("a\0b", "c")),
            room_dir_name(&room("a", "b\0c")),
        );
    }

    // 1.4: `..`/`..` is admissible under every `wire-protocol` identifier rule,
    // so the directory naming is what stops it escaping.
    #[tokio::test]
    async fn a_room_of_relative_segments_cannot_escape_the_store() {
        let fixture = Fixture::open("traversal").await;
        let escaping = room("..", "..");

        // What is outside the root before the upload, so the assertion is about
        // this upload rather than about an empty parent.
        let outside_before = tree(&fixture.base);

        let address = put(&fixture.store, &escaping, b"payload that must stay inside").await;
        let path = fixture.store.payload_path(&escaping, &address);

        assert!(
            path.starts_with(fixture.store.root()),
            "payload path {} escaped the store root {}",
            path.display(),
            fixture.store.root().display(),
        );
        assert!(
            path.is_file(),
            "payload was not written at {}",
            path.display()
        );

        // Every component of the path below the root, so the assertion covers
        // the room directory as well as the payload file.
        let inside = tree(fixture.store.root());
        assert_eq!(
            inside.len(),
            1,
            "expected exactly the one payload under the root, found {inside:?}"
        );

        let outside_after: Vec<PathBuf> = tree(&fixture.base)
            .into_iter()
            .filter(|path| !path.starts_with(fixture.store.root().file_name().unwrap_or_default()))
            .collect();
        assert_eq!(
            outside_after, outside_before,
            "the upload created or modified something outside the store root"
        );
    }

    #[tokio::test]
    async fn the_store_root_is_private_to_its_own_user() {
        let fixture = Fixture::open("private").await;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(fixture.store.root())
                .expect("root exists")
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(mode, 0o700, "store root mode is {mode:o}, not 700");
        }
        assert!(fixture.store.root().is_dir());
    }

    #[tokio::test]
    async fn a_previous_runs_directory_is_removed_at_startup() {
        let fixture = Fixture::open("predecessor").await;
        let room = room("project", "task");
        let address = put(&fixture.store, &room, b"left behind by a killed relay").await;
        let path = fixture.store.payload_path(&room, &address);
        assert!(path.is_file(), "payload was not stored");

        // A restart of the same instance: the store is reopened at the same
        // root, exactly as `main` does after a kill.
        let (successor, _maintenance) = Store::open(&fixture.base, "predecessor")
            .await
            .expect("successor opens");
        assert_eq!(
            successor.root(),
            fixture.store.root(),
            "same instance, same root"
        );
        assert!(
            !path.exists(),
            "the predecessor's payload at {} was adopted",
            path.display()
        );
        assert_eq!(
            successor.payload_len(&room, &address),
            None,
            "the predecessor's payload is still fetchable"
        );
    }

    // 1.7: a full room keeps what it holds.
    #[tokio::test]
    async fn a_full_room_refuses_a_reservation_and_keeps_its_payloads() {
        let fixture = Fixture::open("room-full").await;
        let room = room("project", "task");

        // One payload, then reservations up to the room's total.
        let held = put(&fixture.store, &room, b"the payload that must survive").await;
        let already = charge_for(1);

        let mut reserved = already;
        let mut n = 0_u32;
        while reserved + MAX_PAYLOAD_BYTES <= MAX_ROOM_BYTES {
            fixture
                .store
                .reserve(&room, &format!("filler-{n:037}xxxxx"), MAX_PAYLOAD_BYTES)
                .expect("reservation inside the room total is granted");
            reserved += MAX_PAYLOAD_BYTES;
            n += 1;
        }

        assert_eq!(
            fixture
                .store
                .reserve(&room, &digest(b"one more"), MAX_PAYLOAD_BYTES),
            Err(Refusal::RoomFull),
            "the room admitted {} bytes past its {MAX_ROOM_BYTES}-byte total",
            reserved + MAX_PAYLOAD_BYTES,
        );
        assert_eq!(
            fixture.store.payload_len(&room, &held),
            Some(29),
            "a refused reservation cost the room a payload it already held"
        );
    }

    #[tokio::test]
    async fn an_over_sized_payload_is_refused_before_any_transfer() {
        let fixture = Fixture::open("payload-too-large").await;
        let room = room("project", "task");
        assert_eq!(
            fixture
                .store
                .reserve(&room, &digest(b"x"), MAX_PAYLOAD_BYTES + 1),
            Err(Refusal::PayloadTooLarge)
        );
        assert!(
            tree(fixture.store.root()).is_empty(),
            "a refused reservation wrote something"
        );
    }

    // 1.7: one room reserving to its own total still leaves the process total
    // admitting another room.
    #[tokio::test]
    async fn one_room_cannot_exhaust_the_store() {
        let fixture = Fixture::open("store-share").await;
        let greedy = room("greedy", "task");

        let mut reserved = 0;
        let mut n = 0_u32;
        while reserved + MAX_PAYLOAD_BYTES <= MAX_ROOM_BYTES {
            fixture
                .store
                .reserve(&greedy, &format!("filler-{n:037}xxxxx"), MAX_PAYLOAD_BYTES)
                .expect("reservation inside the room total is granted");
            reserved += MAX_PAYLOAD_BYTES;
            n += 1;
        }
        assert_eq!(
            reserved, MAX_ROOM_BYTES,
            "the greedy room did not reach its own total"
        );
        assert_eq!(
            fixture
                .store
                .reserve(&greedy, &digest(b"one more"), MAX_PAYLOAD_BYTES),
            Err(Refusal::RoomFull)
        );

        let other = room("other", "task");
        assert!(
            fixture
                .store
                .reserve(
                    &other,
                    &digest(b"another room's payload"),
                    MAX_PAYLOAD_BYTES
                )
                .is_ok(),
            "one room at its own total left the process total refusing another room"
        );
    }

    // 1.8: a fetched payload expires on the same schedule as an unfetched one.
    #[tokio::test(start_paused = true)]
    async fn fetching_does_not_extend_a_payloads_life() {
        let fixture = Fixture::open("expiry").await;
        let room = room("project", "task");

        let fetched = put(&fixture.store, &room, b"polled repeatedly").await;
        let untouched = put(&fixture.store, &room, b"never read").await;

        // Repeated fetches across most of the lifetime.
        for _ in 0..4 {
            tokio::time::advance(PAYLOAD_TIME_TO_LIVE / 8).await;
            assert_eq!(
                fixture.store.payload_len(&room, &fetched),
                Some(17),
                "the fetched payload went absent early"
            );
        }

        tokio::time::advance(PAYLOAD_TIME_TO_LIVE).await;
        let swept = fixture.store.sweep();
        assert_eq!(
            swept,
            Swept {
                payloads: 2,
                reservations: 0
            },
            "the fetched and unfetched payloads did not expire together"
        );
        assert_eq!(fixture.store.payload_len(&room, &fetched), None);
        assert_eq!(fixture.store.payload_len(&room, &untouched), None);
    }

    #[tokio::test(start_paused = true)]
    async fn an_abandoned_reservation_returns_its_allowance() {
        let fixture = Fixture::open("reservation-expiry").await;
        let room = room("project", "task");

        // The room's whole total, reserved and never uploaded.
        let mut n = 0_u32;
        while n < u32::try_from(MAX_ROOM_BYTES / MAX_PAYLOAD_BYTES).unwrap_or(u32::MAX) {
            fixture
                .store
                .reserve(&room, &format!("filler-{n:037}xxxxx"), MAX_PAYLOAD_BYTES)
                .expect("reservation granted");
            n += 1;
        }
        assert_eq!(
            fixture
                .store
                .reserve(&room, &digest(b"blocked"), MAX_PAYLOAD_BYTES),
            Err(Refusal::RoomFull)
        );

        tokio::time::advance(RESERVATION_TIME_TO_LIVE + Duration::from_secs(1)).await;
        let swept = fixture.store.sweep();
        assert_eq!(swept.reservations, n, "not every reservation expired");

        assert!(
            fixture
                .store
                .reserve(&room, &digest(b"blocked"), MAX_PAYLOAD_BYTES)
                .is_ok(),
            "an expired reservation kept holding the room's budget"
        );
    }

    // 1.9: a later room of the same name finds the full ceiling available.
    #[tokio::test]
    async fn an_emptied_room_releases_its_whole_ceiling() {
        let mut fixture = Fixture::open("room-forget").await;
        let room = room("project", "task");

        // One short of the ceiling in maximal reservations, so the payload
        // below has a unit to occupy. Its own charge takes the room to exactly
        // its total.
        let fillers = u32::try_from(MAX_ROOM_BYTES / MAX_PAYLOAD_BYTES).unwrap_or(u32::MAX) - 1;
        for n in 0..fillers {
            fixture
                .store
                .reserve(&room, &format!("filler-{n:037}xxxxx"), MAX_PAYLOAD_BYTES)
                .expect("reservation granted");
        }
        let held = put(&fixture.store, &room, b"a stored payload").await;
        assert_eq!(fixture.store.payload_len(&room, &held), Some(16));
        assert_eq!(
            fixture
                .store
                .reserve(&room, &digest(b"one unit too many"), MAX_PAYLOAD_BYTES),
            Err(Refusal::RoomFull),
            "the room was not at its total before it was emptied"
        );

        fixture.store.forget_room(&room);

        assert_eq!(
            fixture.store.payload_len(&room, &held),
            None,
            "an emptied room kept a payload fetchable"
        );
        // The directory removal is queued, because the caller of `forget_room`
        // is a connection's `Drop`. This is that removal, performed.
        fixture.drain().await;
        assert!(
            tree(fixture.store.root()).is_empty(),
            "an emptied room left {:?} on disk",
            tree(fixture.store.root())
        );
        // The full ceiling, in one reservation per maximal payload.
        for i in 0..=fillers {
            fixture
                .store
                .reserve(&room, &format!("later-{i:038}xxxxx"), MAX_PAYLOAD_BYTES)
                .expect("a later room of the same name found the full ceiling");
        }
    }

    // 1.6: verification is what makes the address mean anything.
    #[tokio::test]
    async fn an_upload_whose_bytes_do_not_match_its_address_stores_nothing() {
        let fixture = Fixture::open("digest-mismatch").await;
        let room = room("project", "task");
        let claimed = digest(b"what the uploader claims");

        fixture
            .store
            .reserve(&room, &claimed, 5)
            .expect("reservation granted");
        let Accepted::Upload(mut upload) = fixture
            .store
            .begin_upload(&room, &claimed, 5)
            .expect("upload accepted")
        else {
            panic!("nothing is held yet");
        };
        upload.write(b"other").await.expect("chunk written");

        let error = upload.finish().await.expect_err("a mismatch is refused");
        assert!(
            matches!(error, UploadError::DigestMismatch { .. }),
            "expected a digest mismatch, got {error}"
        );
        assert_eq!(
            fixture.store.payload_len(&room, &claimed),
            None,
            "a mismatched upload became fetchable"
        );
        assert!(
            tree(fixture.store.root()).is_empty(),
            "a refused upload left {:?} behind",
            tree(fixture.store.root())
        );
    }

    #[tokio::test]
    async fn an_interrupted_upload_leaves_nothing_and_returns_its_allowance() {
        let mut fixture = Fixture::open("interrupted").await;
        let room = room("project", "task");
        let bytes = vec![7_u8; 4096];
        let address = digest(&bytes);

        fixture
            .store
            .reserve(&room, &address, bytes.len() as u64)
            .expect("reservation granted");
        let Accepted::Upload(mut upload) = fixture
            .store
            .begin_upload(&room, &address, bytes.len() as u64)
            .expect("upload accepted")
        else {
            panic!("nothing is held yet");
        };
        upload.write(&bytes[..1024]).await.expect("first chunk");
        assert_eq!(upload.written(), 1024);
        assert_eq!(
            tree(fixture.store.root()).len(),
            1,
            "expected one partial file mid-upload, found {:?}",
            tree(fixture.store.root())
        );

        // The connection closes: the guard is dropped without `finish`.
        drop(upload);

        assert_eq!(
            fixture.store.payload_len(&room, &address),
            None,
            "a partial upload became fetchable"
        );
        // `Drop` cannot await, so it queues the partial file. This is the
        // removal it queued, performed.
        fixture.drain().await;
        assert!(
            tree(fixture.store.root()).is_empty(),
            "the partial file survived: {:?}",
            tree(fixture.store.root())
        );
        // Reserving the room's whole total proves the allowance came back.
        for i in 0..u32::try_from(MAX_ROOM_BYTES / MAX_PAYLOAD_BYTES).unwrap_or(u32::MAX) {
            fixture
                .store
                .reserve(&room, &format!("after-{i:038}xxxxx"), MAX_PAYLOAD_BYTES)
                .expect("the abandoned upload's allowance was not returned");
        }
    }

    #[tokio::test]
    async fn an_upload_in_progress_is_not_fetchable() {
        let fixture = Fixture::open("partial").await;
        let room = room("project", "task");
        let bytes = b"a payload written in two chunks".to_vec();
        let address = digest(&bytes);

        fixture
            .store
            .reserve(&room, &address, bytes.len() as u64)
            .expect("reservation granted");
        let Accepted::Upload(mut upload) = fixture
            .store
            .begin_upload(&room, &address, bytes.len() as u64)
            .expect("upload accepted")
        else {
            panic!("nothing is held yet");
        };
        upload.write(&bytes[..10]).await.expect("first chunk");

        assert_eq!(
            fixture.store.payload_len(&room, &address),
            None,
            "a fetch observed an upload in progress"
        );

        upload.write(&bytes[10..]).await.expect("second chunk");
        upload.finish().await.expect("upload finished");
        assert_eq!(
            fixture.store.payload_len(&room, &address),
            Some(bytes.len() as u64)
        );
    }

    #[tokio::test]
    async fn an_upload_with_no_reservation_is_refused() {
        let fixture = Fixture::open("unreserved").await;
        let room = room("project", "task");
        let address = digest(b"never reserved");
        assert_eq!(
            fixture
                .store
                .begin_upload(&room, &address, 14)
                .expect_err("an unreserved upload is refused"),
            UploadRefusal::Unreserved
        );
        assert!(tree(fixture.store.root()).is_empty());
    }

    #[tokio::test]
    async fn an_upload_larger_than_its_reservation_is_refused() {
        let fixture = Fixture::open("over-reservation").await;
        let room = room("project", "task");
        let address = digest(b"reserved small, uploaded large");

        fixture
            .store
            .reserve(&room, &address, 10)
            .expect("reservation granted");
        assert_eq!(
            fixture
                .store
                .begin_upload(&room, &address, 30)
                .expect_err("an over-sized upload is refused"),
            UploadRefusal::OverReservation {
                declared: 30,
                reserved: 10,
            }
        );
    }

    #[tokio::test]
    async fn re_uploading_a_held_payload_succeeds_without_rewriting_it() {
        let fixture = Fixture::open("already-held").await;
        let room = room("project", "task");
        let bytes = b"uploaded twice".to_vec();
        let address = put(&fixture.store, &room, &bytes).await;

        let path = fixture.store.payload_path(&room, &address);
        let first = std::fs::metadata(&path).expect("payload exists");

        fixture
            .store
            .reserve(&room, &address, bytes.len() as u64)
            .expect("a reservation of a held payload is granted");
        let accepted = fixture
            .store
            .begin_upload(&room, &address, bytes.len() as u64)
            .expect("upload accepted");
        assert!(
            matches!(accepted, Accepted::AlreadyHeld),
            "a held payload was accepted for rewriting"
        );

        let second = std::fs::metadata(&path).expect("payload still exists");
        assert_eq!(
            first.modified().ok(),
            second.modified().ok(),
            "the stored bytes were replaced"
        );
        assert_eq!(
            fixture.store.payload_len(&room, &address),
            Some(bytes.len() as u64)
        );
    }

    #[tokio::test]
    async fn a_stored_payload_holds_the_bytes_it_was_given() {
        let fixture = Fixture::open("round-trip").await;
        let room = room("project", "task");
        // Larger than one write, so the streaming path is what is exercised.
        let bytes: Vec<u8> = (0..300_000_u32).map(|i| (i % 251) as u8).collect();
        let address = put(&fixture.store, &room, &bytes).await;

        assert_eq!(
            fixture.store.payload_len(&room, &address),
            Some(bytes.len() as u64)
        );
        let read = std::fs::read(fixture.store.payload_path(&room, &address))
            .expect("payload is readable");
        assert_eq!(
            read, bytes,
            "the stored bytes differ from the uploaded ones"
        );
        assert_eq!(
            digest(&read),
            address,
            "the stored bytes are not at their address"
        );
    }

    #[tokio::test]
    async fn charging_is_rounded_up_to_whole_units() {
        assert_eq!(charge_for(0), 0);
        assert_eq!(charge_for(1), MIN_CHARGE_BYTES);
        assert_eq!(charge_for(MIN_CHARGE_BYTES), MIN_CHARGE_BYTES);
        assert_eq!(charge_for(MIN_CHARGE_BYTES + 1), 2 * MIN_CHARGE_BYTES);

        // The bound the granularity buys: entries per room and per process.
        assert_eq!(MAX_ROOM_BYTES / MIN_CHARGE_BYTES, 512);
        assert_eq!(MAX_STORE_BYTES / MIN_CHARGE_BYTES, 4096);
    }

    #[tokio::test]
    async fn graceful_shutdown_removes_the_store() {
        let base =
            std::env::temp_dir().join(format!("omp-relayd-test-shutdown-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&base);
        let (store, maintenance) = Store::open(&base, "shutdown").await.expect("store opens");
        let holder = room("project", "task");
        let address = put(&store, &holder, b"removed on shutdown").await;
        assert!(store.payload_path(&holder, &address).is_file());

        let root = store.root().to_path_buf();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let task = tokio::spawn(maintenance.run(shutdown_rx));
        shutdown_tx.send(true).expect("shutdown observed");
        task.await.expect("maintenance finished");

        assert!(
            !root.exists(),
            "the store root {} survived a graceful shutdown",
            root.display()
        );
        let _ = std::fs::remove_dir_all(&base);
    }
}
