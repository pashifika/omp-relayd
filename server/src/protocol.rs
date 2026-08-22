//! Protocol v1 wire contract: framing, frame representation, and identifier
//! validation.
//!
//! Two rules in this module are load-bearing for cross-language
//! interoperability and are asserted by tests rather than left to convention:
//!
//! * Frames are encoded with [`rmp_serde::to_vec_named`], so every payload is a
//!   MessagePack **map** keyed by field name. `rmp_serde::to_vec` would encode
//!   structs as positional arrays and couple both language implementations to
//!   field order.
//! * No protocol type carries `#[serde(deny_unknown_fields)]`. Serde's default
//!   of ignoring unrecognized fields *is* the additive-evolution behavior the
//!   contract requires within protocol major version 1.

use std::fmt;
use std::io;

use bytes::Bytes;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

/// The only protocol major version this relay speaks.
pub const PROTOCOL_VERSION: u32 = 1;

/// Largest accepted frame payload, excluding the four-byte length prefix.
pub const MAX_FRAME_BYTES: usize = 64 * 1024;

/// Largest accepted room or peer identifier, in UTF-8 bytes.
pub const MAX_IDENTIFIER_BYTES: usize = 64;

/// Largest accepted correlation identifier (`send.id`, `list.request_id`), in
/// UTF-8 bytes.
pub const MAX_CORRELATION_BYTES: usize = 128;

/// Headroom reserved for the worst-case `message` envelope.
///
/// The worst case is a maximal `id` and `reply_to` (128 bytes each), a maximal
/// `from` (64 bytes), every key name, and the map and string markers: 365
/// bytes. 512 is that rounded up, so the budget below sits 147 bytes inside the
/// true limit rather than exactly on it.
///
/// Those maxima are a *premise*, and every one of them has to be enforced for
/// the budget to mean anything. `id` and `reply_to` are checked against
/// [`MAX_CORRELATION_BYTES`] and `from` is the connection's registered peer
/// name, already checked against [`MAX_IDENTIFIER_BYTES`]. An unchecked field
/// here is not a slack calculation — it is a way for one sender to build a
/// `message` the recipient's connection cannot encode, which closes that
/// connection. `reply_to` was missed on the first pass and found in review.
const MESSAGE_ENVELOPE_HEADROOM: usize = 512;

/// Largest accepted `send.body`, in UTF-8 bytes.
///
/// Deliberately smaller than [`MAX_FRAME_BYTES`]. The relay does not forward
/// the frame it received: it builds a `message` frame, whose envelope is larger
/// than the `send` envelope that produced it, because `message` and `from` are
/// longer key names than `send` and `to`. A body that fits an inbound `send`
/// can therefore produce an outbound `message` that does not fit the frame cap.
///
/// Enforcing the budget on the way in is what keeps that from becoming a way to
/// attack a third party: the encode failure would otherwise land on the
/// *recipient's* connection, letting any sender close any peer's connection at
/// will. `worst_case_message_at_the_body_budget_fits_the_frame_cap` proves the
/// arithmetic instead of trusting this comment.
pub const MAX_BODY_BYTES: usize = MAX_FRAME_BYTES - MESSAGE_ENVELOPE_HEADROOM;

/// The room a connection is admitted to, fixed for its lifetime by `hello`.
///
/// This is both the wire representation of `hello.room` and the registry key.
/// One type for both means no conversion and no second allocation, and it keeps
/// the combined `<project>/<task>` spelling off the wire entirely: the two
/// components are always transmitted as separate map fields.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RoomId {
    /// Project component of the room identity.
    pub project: String,
    /// Task component of the room identity.
    pub task: String,
}

impl RoomId {
    /// Builds a room identity from its two components.
    pub fn new(project: impl Into<String>, task: impl Into<String>) -> Self {
        Self {
            project: project.into(),
            task: task.into(),
        }
    }
}

impl fmt::Display for RoomId {
    /// Renders `project/task` for log fields only. The combined spelling is
    /// never parsed off the wire.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.project, self.task)
    }
}

/// A frame sent by a client to the relay.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientFrame {
    /// First frame on a connection; fixes the room and peer name.
    Hello {
        /// Protocol major version the client speaks.
        protocol: u32,
        /// Room to join.
        room: RoomId,
        /// Name to register under, within that room.
        peer: String,
    },
    /// Requests the peer roster of the sender's own room.
    List {
        /// Opaque correlation token echoed in the `peers` reply.
        request_id: String,
    },
    /// Requests delivery of `body` to another peer in the same room.
    Send {
        /// Opaque correlation token echoed in the `receipt` and delivered
        /// `message`.
        id: String,
        /// Recipient peer name.
        to: String,
        /// Payload, relayed uninterpreted and never logged.
        body: String,
        /// Identifier of the message being answered, when this is a reply.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reply_to: Option<String>,
    },
    /// Liveness probe; answered with `pong`.
    Ping,
    /// Any frame whose `type` is not one of the above.
    ///
    /// `#[serde(other)]` catches an unrecognized discriminator during decoding,
    /// which keeps "unknown frame type" (recoverable, answered with
    /// `unsupported_frame`) distinct from "undecodable payload" (unrecoverable)
    /// without inspecting a serde error message.
    #[serde(other)]
    Unsupported,
}

impl ClientFrame {
    /// The discriminator this frame decoded as, for log fields.
    pub fn type_name(&self) -> &'static str {
        match self {
            Self::Hello { .. } => "hello",
            Self::List { .. } => "list",
            Self::Send { .. } => "send",
            Self::Ping => "ping",
            Self::Unsupported => "unsupported",
        }
    }
}

/// A frame sent by the relay to a client.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerFrame {
    /// Admission confirmation, carrying the negotiated protocol version.
    Ready {
        /// Negotiated protocol major version.
        protocol: u32,
    },
    /// Roster reply to `list`.
    Peers {
        /// Correlation token copied from the request.
        request_id: String,
        /// Peer names in the sender's room, bytewise ascending.
        peers: Vec<String>,
    },
    /// A message relayed from another peer.
    Message {
        /// Correlation token chosen by the sender.
        id: String,
        /// Registered peer name of the sender, derived by the relay.
        from: String,
        /// Payload, relayed uninterpreted.
        body: String,
        /// Identifier of the message being answered, when the sender set one.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reply_to: Option<String>,
    },
    /// Relay-level outcome of one `send`.
    Receipt {
        /// Correlation token copied from the `send`.
        id: String,
        /// Recipient the `send` named.
        to: String,
        /// What the relay did with the frame.
        status: ReceiptStatus,
    },
    /// Answer to `ping`.
    Pong,
    /// A named failure. Clients branch on `code` and never parse `message`.
    Error {
        /// Machine-readable cause.
        code: ErrorCode,
        /// Human-readable diagnostic detail.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        message: Option<String>,
        /// Correlation token of the request that failed, when there was one.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        request_id: Option<String>,
    },
}

/// Relay-level outcome of a routing attempt.
///
/// Serialized as a `snake_case` string rather than an integer: the few extra
/// bytes are irrelevant at this traffic level, and both logs and packet dumps
/// stay readable.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReceiptStatus {
    /// The frame was placed in the recipient's outbound queue. This says
    /// nothing about the recipient having read, processed, or answered it.
    Routed,
    /// No peer of that name is registered in the sender's room.
    PeerOffline,
    /// The recipient is registered but its outbound queue is full.
    RecipientBackpressure,
    /// The `to` value is not a valid peer identifier.
    InvalidTarget,
}

impl ReceiptStatus {
    /// The wire spelling, for log fields.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Routed => "routed",
            Self::PeerOffline => "peer_offline",
            Self::RecipientBackpressure => "recipient_backpressure",
            Self::InvalidTarget => "invalid_target",
        }
    }
}

/// Closed set of failure causes a client may branch on.
///
/// The set is closed deliberately: codes are the client's only branching
/// surface, so a new one is a protocol change rather than an implementation
/// detail.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    /// A decodable frame carried a `type` this version does not implement.
    /// Recoverable: the connection stays open.
    UnsupportedFrame,
    /// `hello.protocol` named a version this relay does not speak.
    UnsupportedProtocol,
    /// The first frame on the connection was not `hello`.
    InvalidHello,
    /// A second `hello` arrived on an already registered connection.
    DuplicateHello,
    /// A room, peer, or correlation identifier failed validation.
    InvalidIdentifier,
    /// A payload was empty or not decodable as a protocol frame map.
    MalformedFrame,
    /// A declared frame length exceeded [`MAX_FRAME_BYTES`].
    FrameTooLarge,
    /// No `hello` arrived within the handshake deadline.
    HelloTimeout,
    /// No valid frame arrived within the idle deadline.
    IdleTimeout,
    /// A newer connection registered the same room and peer name.
    PeerReplaced,
}

impl ErrorCode {
    /// The wire spelling, for log fields.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::UnsupportedFrame => "unsupported_frame",
            Self::UnsupportedProtocol => "unsupported_protocol",
            Self::InvalidHello => "invalid_hello",
            Self::DuplicateHello => "duplicate_hello",
            Self::InvalidIdentifier => "invalid_identifier",
            Self::MalformedFrame => "malformed_frame",
            Self::FrameTooLarge => "frame_too_large",
            Self::HelloTimeout => "hello_timeout",
            Self::IdleTimeout => "idle_timeout",
            Self::PeerReplaced => "peer_replaced",
        }
    }
}

/// A payload that could not be encoded or decoded as a protocol frame.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The payload was zero bytes long. Rejected before the decoder runs so the
    /// cause is stated rather than reported as a generic decode failure.
    #[error("frame payload is empty")]
    EmptyPayload,
    /// The payload's top-level MessagePack value was not a map.
    #[error("frame payload is not a MessagePack map: leading byte 0x{marker:02x}")]
    NotAMap {
        /// The observed leading byte.
        marker: u8,
    },
    /// The payload was a map, but not one describing a known frame: a missing
    /// field, a wrong field type, or truncated bytes.
    #[error("frame payload is not a decodable protocol map: {0}")]
    Decode(#[from] rmp_serde::decode::Error),
    /// A frame could not be serialized. Unreachable for the frames this relay
    /// emits, which contain only strings, `u32`s, and vectors of strings.
    #[error("frame could not be encoded: {0}")]
    Encode(#[from] rmp_serde::encode::Error),
}

impl Error {
    /// A stable classification carrying no payload content, for log fields.
    ///
    /// The full text is safe to return to the peer that sent the frame -- it is
    /// that peer's own data coming back -- but it is not safe to log. Serde
    /// embeds the offending value in a type-mismatch message, so
    /// `{"protocol": "<64 KiB of body>"}` would put those bytes in a log record
    /// and make the record itself unbounded. An operator needs to know that a
    /// peer is sending garbage and how much of it, not what it said.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::EmptyPayload => "empty_payload",
            Self::NotAMap { .. } => "not_a_map",
            Self::Decode(_) => "undecodable",
            Self::Encode(_) => "unencodable",
        }
    }
}

/// Whether `marker` introduces a MessagePack map: `fixmap`, `map16`, `map32`.
///
/// This is the complete set of map markers, so every other top-level value --
/// array, string, integer, nil, boolean, float, binary, extension -- is not a
/// map.
fn is_map_marker(marker: u8) -> bool {
    matches!(marker, 0x80..=0x8f | 0xde | 0xdf)
}

/// Encodes a frame as a length-prefixable MessagePack map.
///
/// # Errors
///
/// Returns [`Error::Encode`] if serialization fails.
pub fn encode<T>(frame: &T) -> Result<Bytes, Error>
where
    T: Serialize + ?Sized,
{
    Ok(Bytes::from(rmp_serde::to_vec_named(frame)?))
}

/// Decodes one frame payload, which must be a MessagePack map.
///
/// The map check is not redundant with the frame types. Serde's internally
/// tagged representation also accepts a *sequence*, reading its first element
/// as the discriminator and the rest positionally, so `["send", id, to, body]`
/// would otherwise decode as a valid `send` frame. That would silently admit
/// the positional encoding this protocol forbids, and the resulting drift
/// between two implementations would surface only when a field was added or
/// reordered. One leading-byte comparison closes it.
///
/// # Errors
///
/// Returns [`Error::EmptyPayload`] for a zero-length payload,
/// [`Error::NotAMap`] when the top-level value is not a map, and
/// [`Error::Decode`] when the map does not describe a known frame.
pub fn decode<T>(payload: &[u8]) -> Result<T, Error>
where
    T: DeserializeOwned,
{
    let Some(&marker) = payload.first() else {
        return Err(Error::EmptyPayload);
    };
    if !is_map_marker(marker) {
        return Err(Error::NotAMap { marker });
    }
    Ok(rmp_serde::from_slice(payload)?)
}

/// Builds the framing codec: a four-byte big-endian length prefix counting
/// payload bytes only, capped at [`MAX_FRAME_BYTES`].
///
/// The cap is enforced against the declared length before the payload is
/// buffered, so an oversized declaration costs no allocation.
pub fn codec() -> LengthDelimitedCodec {
    LengthDelimitedCodec::builder()
        .big_endian()
        .length_field_type::<u32>()
        .max_frame_length(MAX_FRAME_BYTES)
        .new_codec()
}

/// Wraps a byte stream in the protocol framing codec.
pub fn framed<S>(io: S) -> Framed<S, LengthDelimitedCodec> {
    Framed::new(io, codec())
}

/// Classifies a framing-layer error, returning the code to report to the peer.
///
/// [`LengthDelimitedCodec`] reports an over-long declared length as
/// [`io::ErrorKind::InvalidData`], and with a fixed `u32` length field and no
/// length adjustment that is the only `InvalidData` it produces. Every other
/// error is a transport failure with no diagnostic worth attempting: the socket
/// is already gone. The mapping is asserted end-to-end by the oversized-frame
/// integration test rather than trusted from this comment.
pub fn framing_error_code(error: &io::Error) -> Option<ErrorCode> {
    (error.kind() == io::ErrorKind::InvalidData).then_some(ErrorCode::FrameTooLarge)
}

/// Why an identifier was rejected.
///
/// Rendered into `error.message` as diagnostic text; clients branch on
/// [`ErrorCode::InvalidIdentifier`] instead.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum IdentifierError {
    /// The value was the empty string.
    #[error("must not be empty")]
    Empty,
    /// The value exceeded its byte budget.
    #[error("must be at most {limit} UTF-8 bytes, found {found}")]
    TooLong {
        /// Permitted length in UTF-8 bytes.
        limit: usize,
        /// Observed length in UTF-8 bytes.
        found: usize,
    },
    /// The value contained `/` or `@`, reserved for the display spelling
    /// `<project>/<task>@<peer>`.
    #[error("must not contain the reserved separator {separator:?}")]
    ReservedSeparator {
        /// The separator that was found.
        separator: char,
    },
    /// The value began or ended with whitespace.
    #[error("must not begin or end with whitespace")]
    SurroundingWhitespace,
}

/// Validates a room component or peer name.
///
/// Non-empty, at most [`MAX_IDENTIFIER_BYTES`] UTF-8 bytes, free of `/` and
/// `@`, and free of leading and trailing whitespace. Comparison elsewhere is
/// bytewise: identifiers are never case-folded.
///
/// # Errors
///
/// Returns the first rule the value breaks.
pub fn validate_identifier(value: &str) -> Result<(), IdentifierError> {
    if value.is_empty() {
        return Err(IdentifierError::Empty);
    }
    if value.len() > MAX_IDENTIFIER_BYTES {
        return Err(IdentifierError::TooLong {
            limit: MAX_IDENTIFIER_BYTES,
            found: value.len(),
        });
    }
    if let Some(separator) = value.chars().find(|c| matches!(c, '/' | '@')) {
        return Err(IdentifierError::ReservedSeparator { separator });
    }
    if value.starts_with(char::is_whitespace) || value.ends_with(char::is_whitespace) {
        return Err(IdentifierError::SurroundingWhitespace);
    }
    Ok(())
}

/// Validates a correlation token (`send.id`, `list.request_id`).
///
/// Length only: the relay treats the value as an opaque sender-scoped token. It
/// does not require uniqueness and never deduplicates by it.
///
/// # Errors
///
/// Returns [`IdentifierError::Empty`] or [`IdentifierError::TooLong`].
pub fn validate_correlation_id(value: &str) -> Result<(), IdentifierError> {
    if value.is_empty() {
        return Err(IdentifierError::Empty);
    }
    if value.len() > MAX_CORRELATION_BYTES {
        return Err(IdentifierError::TooLong {
            limit: MAX_CORRELATION_BYTES,
            found: value.len(),
        });
    }
    Ok(())
}

/// Reports the body length when it exceeds [`MAX_BODY_BYTES`].
///
/// Checked on the way in, because the frame that must fit the cap is the
/// `message` the relay will build, not the `send` it received.
pub fn body_over_budget(body: &str) -> Option<usize> {
    (body.len() > MAX_BODY_BYTES).then_some(body.len())
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;
    use tokio_util::codec::Encoder;

    use super::*;

    /// MessagePack array markers: `fixarray`, `array16`, `array32`.
    fn is_array_marker(byte: u8) -> bool {
        matches!(byte, 0x90..=0x9f | 0xdc | 0xdd)
    }

    fn contains_key(payload: &[u8], key: &[u8]) -> bool {
        payload.windows(key.len()).any(|window| window == key)
    }

    fn client_frames() -> Vec<ClientFrame> {
        vec![
            ClientFrame::Hello {
                protocol: PROTOCOL_VERSION,
                room: RoomId::new("omp-relayd", "implement-tcp-relay-server"),
                peer: "macbook-reviewer".to_owned(),
            },
            ClientFrame::List {
                request_id: "req-1".to_owned(),
            },
            ClientFrame::Send {
                id: "msg-1".to_owned(),
                to: "windows-main".to_owned(),
                body: "review the diff".to_owned(),
                reply_to: None,
            },
            ClientFrame::Send {
                id: "msg-2".to_owned(),
                to: "windows-main".to_owned(),
                body: "on it".to_owned(),
                reply_to: Some("msg-1".to_owned()),
            },
            ClientFrame::Ping,
        ]
    }

    fn server_frames() -> Vec<ServerFrame> {
        vec![
            ServerFrame::Ready {
                protocol: PROTOCOL_VERSION,
            },
            ServerFrame::Peers {
                request_id: "req-1".to_owned(),
                peers: vec!["macbook-reviewer".to_owned(), "windows-main".to_owned()],
            },
            ServerFrame::Message {
                id: "msg-1".to_owned(),
                from: "macbook-reviewer".to_owned(),
                body: "review the diff".to_owned(),
                reply_to: None,
            },
            ServerFrame::Message {
                id: "msg-2".to_owned(),
                from: "macbook-reviewer".to_owned(),
                body: "on it".to_owned(),
                reply_to: Some("msg-1".to_owned()),
            },
            ServerFrame::Receipt {
                id: "msg-1".to_owned(),
                to: "windows-main".to_owned(),
                status: ReceiptStatus::Routed,
            },
            ServerFrame::Pong,
            ServerFrame::Error {
                code: ErrorCode::UnsupportedFrame,
                message: Some("no such frame".to_owned()),
                request_id: Some("req-1".to_owned()),
            },
            ServerFrame::Error {
                code: ErrorCode::IdleTimeout,
                message: None,
                request_id: None,
            },
        ]
    }

    #[test]
    fn protocol_constants_match_the_contract() {
        assert_eq!(PROTOCOL_VERSION, 1, "protocol version");
        assert_eq!(MAX_FRAME_BYTES, 65536, "maximum frame payload in bytes");
        assert_eq!(MAX_IDENTIFIER_BYTES, 64, "maximum identifier in bytes");
        assert_eq!(
            MAX_CORRELATION_BYTES, 128,
            "maximum correlation id in bytes"
        );
    }

    #[test]
    fn every_frame_round_trips() {
        for frame in client_frames() {
            let payload = encode(&frame).expect("client frame encodes");
            let decoded: ClientFrame = decode(&payload).expect("client frame decodes");
            assert_eq!(decoded, frame, "round trip altered {frame:?}");
        }
        for frame in server_frames() {
            let payload = encode(&frame).expect("server frame encodes");
            let decoded: ServerFrame = decode(&payload).expect("server frame decodes");
            assert_eq!(decoded, frame, "round trip altered {frame:?}");
        }
    }

    #[test]
    fn frames_encode_as_maps_never_as_arrays() {
        for frame in server_frames() {
            let payload = encode(&frame).expect("encodes");
            let marker = payload[0];
            assert!(
                is_map_marker(marker),
                "{frame:?} encoded with leading byte 0x{marker:02x}, which is not a map marker"
            );
            assert!(
                !is_array_marker(marker),
                "{frame:?} encoded with leading byte 0x{marker:02x}, which is an array marker"
            );
        }
    }

    #[test]
    fn discriminator_and_enums_are_snake_case_strings() {
        let receipt = encode(&ServerFrame::Receipt {
            id: "msg-1".to_owned(),
            to: "windows-main".to_owned(),
            status: ReceiptStatus::RecipientBackpressure,
        })
        .expect("encodes");
        assert!(
            contains_key(&receipt, b"receipt") && contains_key(&receipt, b"recipient_backpressure"),
            "receipt did not carry its type and status as strings: {receipt:02x?}"
        );

        let error = encode(&ServerFrame::Error {
            code: ErrorCode::UnsupportedProtocol,
            message: None,
            request_id: None,
        })
        .expect("encodes");
        assert!(
            contains_key(&error, b"unsupported_protocol"),
            "error did not carry its code as a string: {error:02x?}"
        );
    }

    #[test]
    fn absent_optional_field_is_omitted_and_present_one_is_not() {
        let absent = encode(&ServerFrame::Message {
            id: "msg-1".to_owned(),
            from: "macbook-reviewer".to_owned(),
            body: "review the diff".to_owned(),
            reply_to: None,
        })
        .expect("encodes");
        assert!(
            !contains_key(&absent, b"reply_to"),
            "an absent reply_to still produced the key: {absent:02x?}"
        );

        let present = encode(&ServerFrame::Message {
            id: "msg-2".to_owned(),
            from: "macbook-reviewer".to_owned(),
            body: "on it".to_owned(),
            reply_to: Some("msg-1".to_owned()),
        })
        .expect("encodes");
        assert!(
            contains_key(&present, b"reply_to"),
            "a present reply_to produced no key: {present:02x?}"
        );
    }

    /// A `send` frame built field by field, so a test can put an explicit nil
    /// or an unrecognized field on the wire the way another implementation
    /// might.
    #[derive(Serialize)]
    struct RawSend<'a> {
        #[serde(rename = "type")]
        kind: &'a str,
        id: &'a str,
        to: &'a str,
        body: &'a str,
        // No `skip_serializing_if`, so `None` is encoded as an explicit nil.
        reply_to: Option<&'a str>,
        #[serde(skip_serializing_if = "Option::is_none")]
        priority: Option<u32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        from: Option<&'a str>,
    }

    impl RawSend<'_> {
        fn new() -> Self {
            Self {
                kind: "send",
                id: "msg-1",
                to: "windows-main",
                body: "review the diff",
                reply_to: None,
                priority: None,
                from: None,
            }
        }
    }

    #[test]
    fn explicit_nil_optional_field_decodes_as_absent() {
        let payload = encode(&RawSend::new()).expect("encodes");
        assert!(
            contains_key(&payload, b"reply_to"),
            "the fixture was meant to carry an explicit nil reply_to: {payload:02x?}"
        );

        let decoded: ClientFrame = decode(&payload).expect("explicit nil decodes");
        let ClientFrame::Send { reply_to, .. } = &decoded else {
            panic!("decoded as {decoded:?}, expected a send frame");
        };
        assert_eq!(*reply_to, None, "explicit nil did not decode as absent");
    }

    #[test]
    fn unrecognized_fields_are_ignored() {
        let payload = encode(&RawSend {
            priority: Some(9),
            from: Some("impersonated"),
            ..RawSend::new()
        })
        .expect("encodes");

        let decoded: ClientFrame = decode(&payload).expect("unknown fields are ignored");
        assert_eq!(
            decoded,
            ClientFrame::Send {
                id: "msg-1".to_owned(),
                to: "windows-main".to_owned(),
                body: "review the diff".to_owned(),
                reply_to: None,
            },
            "unknown fields changed the decoded frame"
        );
    }

    #[test]
    fn unrecognized_frame_type_decodes_as_unsupported() {
        #[derive(Serialize)]
        struct Broadcast<'a> {
            #[serde(rename = "type")]
            kind: &'a str,
            body: &'a str,
        }

        let payload = encode(&Broadcast {
            kind: "broadcast",
            body: "everyone",
        })
        .expect("encodes");

        let decoded: ClientFrame = decode(&payload).expect("an unknown type is still decodable");
        assert_eq!(
            decoded,
            ClientFrame::Unsupported,
            "an unknown discriminator must decode as Unsupported, not fail"
        );
    }

    #[test]
    fn non_map_payloads_are_rejected() {
        // A positional encoding of the same four fields, which is what
        // `rmp_serde::to_vec` would produce for a struct.
        let positional =
            rmp_serde::to_vec(&("send", "msg-1", "windows-main", "review")).expect("encodes");
        assert!(
            is_array_marker(positional[0]),
            "fixture was meant to be an array: 0x{:02x}",
            positional[0]
        );

        let cases: Vec<(&str, Vec<u8>)> = vec![
            ("positional array", positional),
            ("nil", vec![0xc0]),
            ("integer", vec![0x2a]),
            ("string", rmp_serde::to_vec_named("hello").expect("encodes")),
            ("empty payload", Vec::new()),
            ("corrupt bytes", vec![0xc1, 0x00, 0x00]),
        ];

        for (label, payload) in cases {
            let decoded: Result<ClientFrame, Error> = decode(&payload);
            assert!(
                decoded.is_err(),
                "{label} decoded as {:?}, expected a malformed-frame error",
                decoded.ok()
            );
        }
    }

    #[test]
    fn identifier_length_limit_counts_utf8_bytes() {
        let at_limit = "a".repeat(MAX_IDENTIFIER_BYTES);
        assert_eq!(
            validate_identifier(&at_limit),
            Ok(()),
            "{} bytes must be accepted",
            at_limit.len()
        );

        let over_limit = "a".repeat(MAX_IDENTIFIER_BYTES + 1);
        assert_eq!(
            validate_identifier(&over_limit),
            Err(IdentifierError::TooLong {
                limit: MAX_IDENTIFIER_BYTES,
                found: 65,
            }),
            "{} bytes must be rejected",
            over_limit.len()
        );

        // Sixteen four-byte characters are 64 bytes but only 16 characters: the
        // budget is bytes, so this is at the limit rather than well under it.
        let wide = "\u{1f600}".repeat(16);
        assert_eq!(wide.len(), 64, "fixture byte length");
        assert_eq!(wide.chars().count(), 16, "fixture character length");
        assert_eq!(validate_identifier(&wide), Ok(()), "64 bytes of emoji");

        let wide_over = "\u{1f600}".repeat(17);
        assert_eq!(
            validate_identifier(&wide_over),
            Err(IdentifierError::TooLong {
                limit: MAX_IDENTIFIER_BYTES,
                found: 68,
            }),
            "68 bytes of emoji must be rejected"
        );
    }

    #[test]
    fn identifiers_reject_reserved_separators_and_surrounding_whitespace() {
        assert_eq!(validate_identifier(""), Err(IdentifierError::Empty));
        assert_eq!(
            validate_identifier("omp/relayd"),
            Err(IdentifierError::ReservedSeparator { separator: '/' })
        );
        assert_eq!(
            validate_identifier("peer@host"),
            Err(IdentifierError::ReservedSeparator { separator: '@' })
        );
        assert_eq!(
            validate_identifier(" leading"),
            Err(IdentifierError::SurroundingWhitespace)
        );
        assert_eq!(
            validate_identifier("trailing\n"),
            Err(IdentifierError::SurroundingWhitespace)
        );
        // Interior whitespace is not reserved.
        assert_eq!(validate_identifier("windows main"), Ok(()));
        // Case is never folded, so these are two different identifiers.
        assert_eq!(validate_identifier("Reviewer"), Ok(()));
        assert_eq!(validate_identifier("reviewer"), Ok(()));
        assert_ne!("Reviewer", "reviewer", "identifiers compare bytewise");
    }

    #[test]
    fn correlation_ids_are_length_checked_and_otherwise_opaque() {
        let at_limit = "b".repeat(MAX_CORRELATION_BYTES);
        assert_eq!(
            validate_correlation_id(&at_limit),
            Ok(()),
            "{} bytes must be accepted",
            at_limit.len()
        );

        let over_limit = "b".repeat(MAX_CORRELATION_BYTES + 1);
        assert_eq!(
            validate_correlation_id(&over_limit),
            Err(IdentifierError::TooLong {
                limit: MAX_CORRELATION_BYTES,
                found: 129,
            }),
            "{} bytes must be rejected",
            over_limit.len()
        );

        assert_eq!(validate_correlation_id(""), Err(IdentifierError::Empty));
        // Opaque: characters reserved in identifiers carry no meaning here.
        assert_eq!(validate_correlation_id("a/b@c d"), Ok(()));
    }

    #[test]
    fn framing_error_code_names_only_the_oversized_frame() {
        let too_big = io::Error::new(io::ErrorKind::InvalidData, "frame size too big");
        assert_eq!(
            framing_error_code(&too_big),
            Some(ErrorCode::FrameTooLarge),
            "an over-long declared length must be reportable"
        );

        for kind in [
            io::ErrorKind::ConnectionReset,
            io::ErrorKind::UnexpectedEof,
            io::ErrorKind::BrokenPipe,
        ] {
            let transport = io::Error::new(kind, "transport");
            assert_eq!(
                framing_error_code(&transport),
                None,
                "{kind:?} must not be reported as a protocol violation"
            );
        }
    }

    #[test]
    fn worst_case_message_at_the_body_budget_fits_the_frame_cap() {
        // Every variable-length field at its maximum, so no legal `send` can
        // produce a larger `message` than this one.
        let frame = ServerFrame::Message {
            id: "i".repeat(MAX_CORRELATION_BYTES),
            from: "f".repeat(MAX_IDENTIFIER_BYTES),
            body: "b".repeat(MAX_BODY_BYTES),
            reply_to: Some("r".repeat(MAX_CORRELATION_BYTES)),
        };

        let encoded = encode(&frame).expect("encodes").len();
        let envelope = encoded - MAX_BODY_BYTES;
        assert!(
            encoded <= MAX_FRAME_BYTES,
            "worst-case message is {encoded} bytes ({envelope} of envelope around a \
             {MAX_BODY_BYTES}-byte body), which exceeds the {MAX_FRAME_BYTES}-byte cap"
        );
        println!(
            "worst-case message: {encoded} bytes = {envelope} envelope + {MAX_BODY_BYTES} body, \
             {} bytes below the {MAX_FRAME_BYTES}-byte cap",
            MAX_FRAME_BYTES - encoded
        );
    }

    #[test]
    fn the_body_budget_is_enforced_at_its_boundary() {
        let at_budget = "b".repeat(MAX_BODY_BYTES);
        assert_eq!(
            body_over_budget(&at_budget),
            None,
            "{MAX_BODY_BYTES} bytes must be relayable"
        );

        let over_budget = "b".repeat(MAX_BODY_BYTES + 1);
        assert_eq!(
            body_over_budget(&over_budget),
            Some(MAX_BODY_BYTES + 1),
            "one byte over the budget must be reported with its observed size"
        );
    }

    #[test]
    fn the_length_prefix_counts_payload_bytes_only() {
        let mut codec = codec();
        let mut wire = BytesMut::new();

        codec
            .encode(Bytes::from(vec![0x5a; 42]), &mut wire)
            .expect("encodes");

        assert_eq!(
            &wire[..4],
            &[0x00, 0x00, 0x00, 0x2a],
            "a 42-byte payload must be introduced by 00 00 00 2A, big-endian and \
             excluding the prefix itself; observed {:02x?}",
            &wire[..4]
        );
        assert_eq!(
            wire.len(),
            4 + 42,
            "the frame must be the four-byte prefix plus exactly 42 payload bytes"
        );
        assert!(
            wire[4..].iter().all(|&byte| byte == 0x5a),
            "the payload must follow the prefix unaltered: {:02x?}",
            &wire[4..]
        );
    }

    #[test]
    fn a_real_frame_is_framed_with_its_own_length() {
        let payload = encode(&ServerFrame::Pong).expect("encodes");
        let mut codec = codec();
        let mut wire = BytesMut::new();
        codec.encode(payload.clone(), &mut wire).expect("encodes");

        let declared = u32::from_be_bytes(wire[..4].try_into().expect("four bytes"));
        assert_eq!(
            declared as usize,
            payload.len(),
            "the declared length must equal the payload length, not the framed length"
        );
        assert_eq!(
            wire.len() - 4,
            payload.len(),
            "the bytes after the prefix must be exactly the payload"
        );
    }
}
