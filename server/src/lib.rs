//! OMP Relay: an in-memory TCP relay that routes MessagePack frames between
//! named peers inside a `<project>/<task>` room.
//!
//! The relay persists no *messages*. It holds a registry of live connections, a
//! bounded outbound queue per registered peer, and a bounded store of temporary
//! payload files: no message history, no queue for absent peers, no replay
//! after a reconnect, and nothing at all that survives a restart.
//!
//! * [`protocol`] owns the wire contract: framing, frame representation, and
//!   identifier validation.
//! * [`relay`] owns the registry, routing, and the per-connection task.
//! * [`blob`] owns the room-scoped payload store: content addressing, the
//!   ceilings, the time to live, and the removal rules.
//!
//! The binary is a thin wrapper: it resolves a bind address, initializes
//! logging, opens the store, and hands a listener to [`relay::serve`].
//! Everything worth testing is reachable from this library, so the integration
//! tests drive a real loopback listener instead of a child process.

pub mod blob;
pub mod protocol;
pub mod relay;
