//! OMP Relay: an in-memory TCP relay that routes MessagePack frames between
//! named peers inside a `<project>/<task>` room.
//!
//! The relay persists nothing. It holds a registry of live connections, a
//! bounded outbound queue per registered peer, and nothing else: no message
//! history, no queue for absent peers, no replay after a reconnect.
//!
//! * [`protocol`] owns the wire contract: framing, frame representation, and
//!   identifier validation.
//! * [`relay`] owns the registry, routing, and the per-connection task.
//!
//! The binary is a thin wrapper: it resolves a bind address, initializes
//! logging, and hands a listener to [`relay::serve`]. Everything worth testing
//! is reachable from this library, so the integration tests drive a real
//! loopback listener instead of a child process.

pub mod protocol;
pub mod relay;
