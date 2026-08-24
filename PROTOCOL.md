# OMP Relay Protocol v1

This document renders the accepted planning specifications for implementers working from the code repository. The accepted specifications under `rasen/specs/` are normative. If this document conflicts with them, the specification wins and this rendering must be corrected.

## Transport and framing

Protocol v1 runs over plain TCP. It provides no authentication, authorization, or encryption.

Each frame is:

1. A four-byte unsigned big-endian integer containing the payload length.
2. Exactly that many MessagePack payload bytes.

The length excludes the prefix. Zero-length payloads and payloads larger than 65,536 bytes are fatal framing errors. There is no compression, frame magic, or padding. A decoder consumes exactly one MessagePack value from each payload; trailing bytes are malformed input.

TCP is a byte stream. Implementations must retain partial prefixes and payloads, and must extract every complete frame when multiple frames arrive in one read.

## MessagePack representation

Every payload is a MessagePack map with UTF-8 string keys and a string `type` discriminator. Struct-shaped values such as `hello.room` are maps too. Positional arrays are invalid.

Encoders:

- Encode strings as MessagePack `str`.
- Omit absent optional fields rather than writing `nil`.
- Do not emit extension values, timestamp values, or floating-point numbers.
- Keep protocol integers in the unsigned 32-bit range.

Decoders:

- Reject non-map frame and nested struct representations.
- Accept an omitted optional field or explicit `nil` as absent.
- Ignore unknown fields within protocol major version 1.
- May accept valid UTF-8 carried as MessagePack `bin` for a string field, but re-encode it as `str` if forwarded.

## Connection lifecycle

The client sends `hello` as the first frame, within five seconds of TCP acceptance. The server answers a valid handshake with `ready`. The room and peer name are fixed for that connection.

After registration, any valid inbound frame resets the server's 90-second idle deadline. Clients normally send `ping` after 30 seconds without outbound traffic; the server answers with `pong`.

A newer connection registering the same room and peer replaces the older connection. The older connection is closed with `peer_replaced` on a best-effort basis.

## Client-to-server frames

Every row includes the string `type` field shown in the first column.

| `type` | Required fields | Optional fields | Meaning |
|---|---|---|---|
| `hello` | `protocol: u32`, `room: map`, `room.project: string`, `room.task: string`, `peer: string` | none | Admit protocol version 1, one room, and one peer name |
| `list` | `request_id: string` | none | Request the sorted peers registered in the sender's room |
| `send` | `id: string`, `to: string`, `body: string` | `reply_to: string` | Queue one message for a peer in the sender's room |
| `announce` | `id: string`, `body: string` | `reply_to: string` | Queue one `notice` for every other peer in the sender's room |
| `ping` | none | none | Reset the idle deadline and request `pong` |

A client-supplied `from` field has no authority. The relay derives `message.from` and `notice.from` from the registered connection.

`announce` carries no target field. The absence of a peer component is what addresses the sender's whole room, so there is no reserved target value a peer could register under and capture. A frame typed `send` whose `to` is absent is a malformed `send`, never an announcement.

## Server-to-client frames

Every row includes the string `type` field shown in the first column.

| `type` | Required fields | Optional fields | Meaning |
|---|---|---|---|
| `ready` | `protocol: u32` | none | Confirm a successful handshake and negotiated version |
| `peers` | `request_id: string`, `peers: array<string>` | none | Return all peers in the room, including the requester, in bytewise ascending order |
| `message` | `id: string`, `from: string`, `body: string` | `reply_to: string` | Deliver one directed message from a registered peer |
| `notice` | `id: string`, `from: string`, `body: string` | `reply_to: string` | Deliver one announcement from a registered peer, addressed to the room rather than to the recipient |
| `receipt` | `id: string`, `to: string`, `status: string` | none | Report the relay-level outcome of one `send` |
| `accepted` | `id: string`, `delivered: u32`, `shed: u32` | none | Report the two counts one `announce` produced |
| `pong` | none | none | Answer `ping` |
| `error` | `code: string` | `message: string`, `request_id: string` | Report a protocol or connection failure |

`error.message` is diagnostic text. Clients branch on `error.code`, never on the message text. When a recoverable rejection answers a request whose correlation identifier was valid, `error.request_id` echoes that identifier.

## Receipt statuses

| Status | Meaning |
|---|---|
| `routed` | The message entered the recipient's bounded outbound queue. It does not mean the recipient read, processed, accepted, or answered it. |
| `peer_offline` | No peer with the requested name is currently registered in the room. Nothing is queued for later. |
| `recipient_backpressure` | The recipient's 128-frame outbound queue is full. The sender remains connected. |
| `invalid_target` | The target violates the peer identifier rules. The sender remains connected. |

For an overlong invalid target, `receipt.to` contains at most the first 64 UTF-8 bytes, truncated at a character boundary.

## Acceptance counts

`accepted` carries no status field. Its two counts are the outcome of one `announce`, and they sum to the number of peers the fanout addressed — every peer registered in the sender's room except the sender itself.

| Count | Meaning |
|---|---|
| `delivered` | Addressed recipients whose outbound queue took the `notice`. It does not mean any of them read, processed, accepted, or answered it. |
| `shed` | Addressed recipients whose 128-frame outbound queue refused it. Both classes share one queue and one capacity, so a fanout reports a refused enqueue as one increment here instead of as a status. |

`delivered` 0 with `shed` 0 means the sender was alone in its room. That is a fact about the room rather than a failure of the request: no `error` is emitted and the connection stays open. A shed count is not a delivery to retry blindly, because the peer that shed it is a peer that is not reading its connection.

## Error codes

| Code | Meaning |
|---|---|
| `unsupported_frame` | A registered peer sent an unrecognized frame type. |
| `unsupported_protocol` | `hello.protocol` is not `1`. |
| `invalid_hello` | The first frame is not a valid `hello`. |
| `duplicate_hello` | A registered connection sent a second `hello`. |
| `invalid_identifier` | A room, peer, correlation, or reply identifier violates its rules. |
| `malformed_frame` | The MessagePack value or required frame fields are malformed. |
| `frame_too_large` | The declared payload or relayable body exceeds its limit. |
| `hello_timeout` | The connection did not complete its initial handshake within five seconds. |
| `idle_timeout` | A registered connection was idle for 90 seconds, or a non-reading sender could not receive the receipt required before routing. |
| `peer_replaced` | A newer connection registered the same room and peer. |

Handshake, framing, decode, body-budget, timeout, and replacement failures close the connection. Recoverable request errors keep a healthy registered connection open.

## Resource limits

| Resource | Limit |
|---|---|
| Frame payload | 65,536 bytes, excluding the four-byte prefix |
| `room.project`, `room.task`, `peer`, and valid `send.to` | 1–64 UTF-8 bytes; no `/` or `@`; no leading or trailing whitespace |
| `send.id`, `announce.id`, `list.request_id`, `send.reply_to`, and `announce.reply_to` | 1–128 UTF-8 bytes |
| `send.body` and `announce.body` | 65,024 UTF-8 bytes |
| Per-peer outbound queue | 128 frames |
| Initial `hello` deadline | 5 seconds after TCP acceptance |
| Registered connection idle deadline | 90 seconds since the last valid inbound frame |
| Client heartbeat interval | 30 seconds without outbound traffic |
| Client request deadline | 5 seconds |
| Client reconnect delay | Exponential backoff with jitter, capped at 30 seconds |

Identifiers are case-sensitive and compared byte-for-byte. Correlation identifiers are opaque, sender-scoped, and not deduplicated.

## Delivery semantics

The relay secures the sender's receipt before placing a routable message in the recipient queue. A missing receipt therefore means the message was not delivered, so retrying after reconnect does not create a duplicate from that attempt.

Messages from one sender connection enter a recipient queue in read order. There is no global ordering across senders. The relay persists nothing, does not queue for offline peers, and does not replay history after reconnect.

## Compatibility

Receivers ignore unknown fields within protocol major version 1. A registered peer sending an unknown `type` receives `unsupported_frame`; a connection that has not completed `hello` receives `invalid_hello` instead.

An incompatible contract change increments `hello.protocol`. Cross-language fixtures under `test-fixtures/protocol-v1/` pin semantic interoperability between the Rust and TypeScript implementations.
