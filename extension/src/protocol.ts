/**
 * Protocol v1 wire contract, TypeScript side: frame shapes, the MessagePack
 * codec, byte-stream reassembly, and validation of everything that arrives.
 *
 * Three rules here are load-bearing for interoperability with the Rust relay
 * and are asserted by tests rather than left to convention:
 *
 * - Frames are encoded as MessagePack **maps** keyed by field name. Plain
 *   object literals give that by construction; nothing here builds a `Map`, a
 *   class instance, a `BigInt`, or an extension type, so the emitted bytes stay
 *   inside the compatibility profile without review.
 * - An **absent optional field is omitted**, never emitted as nil. Two
 *   independent mechanisms enforce it: `exactOptionalPropertyTypes` makes
 *   `undefined` unassignable to `reply_to?: string` at compile time, and
 *   `ignoreUndefined` makes the encoder drop such a key at run time.
 * - Decoding produces `unknown` and passes through {@link validateServerFrame}
 *   before any caller sees it. The next consumer of these frames hands their
 *   contents to OMP APIs, so the transport boundary is where a hostile payload
 *   has to stop.
 *
 * Nothing in this module imports OMP.
 */

import { Decoder, Encoder } from "@msgpack/msgpack";

/** The only protocol major version this client speaks. */
export const PROTOCOL_VERSION = 1;

/** Bytes of length prefix preceding every payload: a big-endian `u32`. */
export const LENGTH_PREFIX_BYTES = 4;

/** Largest accepted frame payload, excluding the length prefix. */
export const MAX_FRAME_BYTES = 64 * 1024;

/** Largest accepted room or peer identifier, in UTF-8 bytes. */
export const MAX_IDENTIFIER_BYTES = 64;

/** Largest accepted correlation token (`send.id`, `list.request_id`). */
export const MAX_CORRELATION_BYTES = 128;

/**
 * Largest `send.body` a client may offer, in UTF-8 bytes.
 *
 * Deliberately below {@link MAX_FRAME_BYTES}: the relay does not forward the
 * frame it received, it builds a larger `message` envelope from it. The relay
 * enforces this budget too, but it enforces it by closing the sender's
 * connection, so a client that checked nothing would turn one over-long body
 * into a dropped connection and a reconnect. `wire-protocol` requires the
 * client to apply the same budget before sending; this is that check's value.
 */
export const MAX_BODY_BYTES = MAX_FRAME_BYTES - 512;

// ---------------------------------------------------------------------------
// Frame inventory
// ---------------------------------------------------------------------------

/** The room a connection is admitted to, fixed for its lifetime by `hello`. */
export interface RoomId {
  readonly project: string;
  readonly task: string;
}

/** First frame on a connection; fixes the room and peer name. */
export interface HelloFrame {
  readonly type: "hello";
  readonly protocol: number;
  readonly room: RoomId;
  readonly peer: string;
}

/** Requests the peer roster of the sender's own room. */
export interface ListFrame {
  readonly type: "list";
  readonly request_id: string;
}

/** Requests delivery of `body` to another peer in the same room. */
export interface SendFrame {
  readonly type: "send";
  readonly id: string;
  readonly to: string;
  readonly body: string;
  readonly reply_to?: string;
}

/** Liveness probe; answered with `pong`. */
export interface PingFrame {
  readonly type: "ping";
}

/** Every frame a client may write. Protocol v1 has exactly these four. */
export type ClientFrame = HelloFrame | ListFrame | SendFrame | PingFrame;

/** Admission confirmation, carrying the negotiated protocol version. */
export interface ReadyFrame {
  readonly type: "ready";
  readonly protocol: number;
}

/** Roster reply to `list`. */
export interface PeersFrame {
  readonly type: "peers";
  readonly request_id: string;
  readonly peers: readonly string[];
}

/** A message relayed from another peer. */
export interface MessageFrame {
  readonly type: "message";
  readonly id: string;
  readonly from: string;
  readonly body: string;
  readonly reply_to?: string;
}

/** Statuses `wire-protocol` defines for protocol v1. */
export type KnownReceiptStatus =
  | "routed"
  | "peer_offline"
  | "recipient_backpressure"
  | "invalid_target";

/**
 * Open on purpose.
 *
 * `wire-protocol` closes the status set for the *relay*, so a conforming relay
 * emits nothing else. It does not follow that this client should reject an
 * unrecognized status: a relay one version ahead would then have its receipts
 * discarded rather than surfaced, turning an additive change into a silent
 * failure. Validation checks that the value is a string; the union documents
 * what a v1 relay actually sends.
 */
export type ReceiptStatus = KnownReceiptStatus | (string & {});

/** Relay-level outcome of one `send`. */
export interface ReceiptFrame {
  readonly type: "receipt";
  readonly id: string;
  readonly to: string;
  readonly status: ReceiptStatus;
}

/** Answer to `ping`. */
export interface PongFrame {
  readonly type: "pong";
}

/** Failure codes `wire-protocol` defines for protocol v1. */
export type KnownErrorCode =
  | "unsupported_frame"
  | "unsupported_protocol"
  | "invalid_hello"
  | "duplicate_hello"
  | "invalid_identifier"
  | "malformed_frame"
  | "frame_too_large"
  | "hello_timeout"
  | "idle_timeout"
  | "peer_replaced";

/** Open for the same reason as {@link ReceiptStatus}. */
export type ErrorCode = KnownErrorCode | (string & {});

/** A named failure. Callers branch on `code` and never parse `message`. */
export interface ErrorFrame {
  readonly type: "error";
  readonly code: ErrorCode;
  readonly message?: string;
  readonly request_id?: string;
}

/** Every frame the relay may write. Protocol v1 has exactly these six. */
export type ServerFrame =
  | ReadyFrame
  | PeersFrame
  | MessageFrame
  | ReceiptFrame
  | PongFrame
  | ErrorFrame;

const SERVER_FRAME_TYPES: readonly ServerFrame["type"][] = [
  "ready",
  "peers",
  "message",
  "receipt",
  "pong",
  "error",
];

/**
 * Narrows an arbitrary discriminator to one this client handles.
 *
 * A guard rather than a bare `includes` call, because the narrowing is what
 * makes the `switch` in {@link validateServerFrame} exhaustive: adding a frame
 * to {@link ServerFrame} without handling it there becomes a type error
 * instead of a frame silently classified as ignorable.
 */
function isServerFrameType(value: string): value is ServerFrame["type"] {
  return (SERVER_FRAME_TYPES as readonly string[]).includes(value);
}

// ---------------------------------------------------------------------------
// Codec
// ---------------------------------------------------------------------------

/**
 * One encoder and one decoder for the process.
 *
 * Instantiating per frame would allocate on every message for no benefit;
 * both are synchronous and are only ever driven from synchronous code, so
 * sharing them cannot interleave. `Encoder#encode` returns a copy of its
 * internal buffer as of `@msgpack/msgpack` 3.0, so the returned bytes are safe
 * to hold across an await — this deliberately does not use `encodeSharedRef`,
 * whose result the next encode invalidates.
 *
 * `ignoreUndefined` is the run-time half of the omit-not-null rule: a key
 * whose value is `undefined` is dropped rather than encoded as nil. The type
 * system forbids constructing such a frame, but a value crossing an `unknown`
 * boundary can still carry one, and the wire contract is not a place to rely
 * on a single line of defense.
 *
 * Every decoder bound below is mechanization of a rule this client is already
 * held to, not a new limit. `specs/relay-client/spec.md:126` puts binary,
 * extension-codec, and timestamp values outside the compatibility profile, and
 * the design record adds that this contract specifies `str`, that "The relay
 * never emits `bin`", and that a `bin` the relay does accept is "re-encoded as
 * `str` before it reaches any peer"
 * (`rasen/docs/omp-relay-messagepack-protocol-design.md:232-241`), so no
 * conforming relay can trip either bound. Note precisely what they buy: the
 * option compares `size > max`, so a maximum of zero is a resource bound and
 * not a type ban — a zero-length `bin` or `ext` still decodes, and is then
 * discarded by validation like any other unknown field. Rejecting the markers
 * themselves would mean hand-rolling marker checks for a value that reaches no
 * caller. A string, likewise, cannot be longer than the frame carrying it.
 *
 * Neither container count is bounded, and both omissions are decisions.
 *
 * Map entries are unbounded because capping them would break schema evolution.
 * Within one major version "receivers must ignore unknown map fields" and "new
 * fields must be optional", and the Rust side is forbidden
 * `#[serde(deny_unknown_fields)]` for that same reason (design
 * record:253-260). A cap at today's field count would fail the whole
 * connection on the one input the other implementation of this protocol may
 * not reject — the asymmetry the cross-language fixtures exist to catch. It
 * would also buy almost nothing: a map header allocates no backing store, so a
 * 65,536-byte payload of nothing but nested map headers costs 2.6 ms and about
 * 12 MB of resident memory before it is refused for running out of bytes.
 *
 * Array elements are unbounded because `peers.peers` is the one array v1
 * contains and the design record leaves it uncapped with its arithmetic shown
 * — a room of 7264 eight-byte names is a legitimate roster (design
 * record:1047-1054, "No peer-count limit is imposed... Recorded so the absence
 * reads as a decision"). Unlike a map header, an array header does allocate
 * for everything it declares, so nested array headers are genuinely expensive.
 * No cap both admits that roster and bounds that cost, so the cost is measured
 * and accepted as a known limitation rather than closed by guessing a peer
 * ceiling here.
 */
const encoder = new Encoder({ ignoreUndefined: true });
const decoder = new Decoder({
  maxStrLength: MAX_FRAME_BYTES,
  maxBinLength: 0,
  maxExtLength: 0,
});

const textEncoder = new TextEncoder();

/** UTF-8 length of `value`, which is the unit every protocol budget counts. */
export function utf8Length(value: string): number {
  return textEncoder.encode(value).length;
}

/** A payload this client refuses to produce. */
export class EncodeError extends Error {
  override readonly name = "EncodeError";
}

/**
 * Encodes one frame as a MessagePack map, without the length prefix.
 *
 * This is the form the cross-language fixtures under `test-fixtures/` hold, and
 * it accepts a server frame as well as a client one so a fixture or a test
 * double can produce the bytes a relay would.
 *
 * {@link encodeFrame}, which produces what actually goes on the wire, stays
 * client-only on purpose: widening it would make "the client wrote a server
 * frame" a type-checked impossibility into a reviewable mistake.
 *
 * @throws {EncodeError} if the payload would exceed {@link MAX_FRAME_BYTES}.
 */
export function encodePayload(frame: ClientFrame | ServerFrame): Uint8Array {
  const payload = encoder.encode(frame);
  if (payload.length > MAX_FRAME_BYTES) {
    throw new EncodeError(
      `${frame.type} frame is ${payload.length} bytes, over the ${MAX_FRAME_BYTES}-byte cap`,
    );
  }
  return payload;
}

/**
 * Encodes one frame as it goes on the wire: a big-endian `u32` payload length
 * followed by that many payload bytes.
 *
 * @throws {EncodeError} if the payload would exceed {@link MAX_FRAME_BYTES}.
 */
export function encodeFrame(frame: ClientFrame): Uint8Array {
  const payload = encodePayload(frame);
  const framed = new Uint8Array(LENGTH_PREFIX_BYTES + payload.length);
  new DataView(framed.buffer).setUint32(0, payload.length, false);
  framed.set(payload, LENGTH_PREFIX_BYTES);
  return framed;
}

/** Outcome of decoding one payload. Absent by design: a thrown exception. */
export type DecodeResult =
  | { readonly ok: true; readonly value: unknown }
  | { readonly ok: false; readonly detail: string };

/**
 * Decodes exactly one MessagePack value from `payload`.
 *
 * `@msgpack/msgpack` throws on malformed input and on trailing bytes after the
 * first complete value. Both are genuine framing failures — the length prefix
 * declared how many bytes the frame occupies, so a remainder means the two ends
 * disagree about where frames begin — but neither may surface as an exception,
 * because every caller here is a socket callback whose throw would escape into
 * the host. So the throw is converted at its source.
 */
export function decodePayload(payload: Uint8Array): DecodeResult {
  try {
    return { ok: true, value: decoder.decode(payload) };
  } catch (error) {
    return { ok: false, detail: describe(error) };
  }
}

/**
 * Diagnostic text for a caught value, which need not be an `Error`.
 *
 * Never throws, and always returns an actual string. Both halves are
 * load-bearing rather than defensive: every caller is a containment path — a
 * socket callback's `catch`, a detached rejection handler, a host-callback
 * guard — so a throw from here, or a non-string escaping into a caller's
 * template literal, would defeat the very guard that called it.
 *
 * The conversion is inside the guard because the value being converted is host
 * code. `message` is declared `string` but may be a getter that throws or that
 * returns something else entirely, and converting a null-prototype object
 * throws. Note it is the interpolation that raises on a symbol; `String` itself
 * accepts one, which is why the conversion is spelled out here rather than left
 * to the caller's `${}`.
 */
export function describe(error: unknown): string {
  try {
    return String(error instanceof Error ? error.message : error);
  } catch {
    return "an error that could not be described";
  }
}

// ---------------------------------------------------------------------------
// Byte-stream reassembly
// ---------------------------------------------------------------------------

/** Why a connection's byte stream stopped being trustworthy. */
export type FramingFailureReason =
  /** A length prefix declared zero payload bytes. */
  | "zero_length"
  /** A length prefix declared more than {@link MAX_FRAME_BYTES}. */
  | "oversized"
  /** A complete payload was not exactly one MessagePack value. */
  | "undecodable";

/** A framing failure, which is always fatal to its connection. */
export interface FramingFailure {
  readonly reason: FramingFailureReason;
  readonly detail: string;
}

/** What one chunk of received bytes yielded. */
export interface AccumulatorOutcome {
  /**
   * Values decoded from complete frames, in arrival order. Populated even
   * alongside a failure: frames that arrived ahead of the bad length were
   * validly framed, and dropping them would discard correctly received data
   * because of what followed it.
   */
  readonly values: readonly unknown[];
  /** Set once the stream is no longer trustworthy; `null` while it is. */
  readonly failure: FramingFailure | null;
}

const NO_VALUES: readonly unknown[] = [];
const EMPTY = new Uint8Array(0);

/**
 * Reassembles length-prefixed frames from a TCP byte stream.
 *
 * A socket delivers part of a frame, exactly one frame, or several frames per
 * `data` event, and this handles all three by treating the transport as an
 * unframed byte stream and re-examining the accumulated buffer after every
 * chunk.
 *
 * A malformed length fails the connection instead of triggering a search for
 * the next plausible frame boundary. Once framing is untrustworthy no later
 * byte offset is trustworthy either, and guessing would convert a clean failure
 * into silent corruption — so the failure is latched and this accumulator
 * consumes nothing further.
 *
 * Buffer ownership invariant: `#buffer` never aliases a caller-supplied chunk
 * beyond the call that delivered it; a retained tail is always copied. Node and
 * Bun both hand out a fresh buffer per `data` event, but that is their choice
 * rather than this module's contract, and an accumulator holding a borrowed
 * view would corrupt frames if either ever pooled.
 */
export class FrameAccumulator {
  #buffer: Uint8Array = EMPTY;
  #failure: FramingFailure | null = null;

  /** Bytes held back waiting for the rest of their frame. */
  get buffered(): number {
    return this.#buffer.length;
  }

  /** Appends `chunk` and extracts every frame it completed. */
  push(chunk: Uint8Array): AccumulatorOutcome {
    if (this.#failure !== null) {
      return { values: NO_VALUES, failure: this.#failure };
    }

    const buffer =
      this.#buffer.length === 0 ? chunk : concat(this.#buffer, chunk);
    const values: unknown[] = [];
    let offset = 0;

    while (buffer.length - offset >= LENGTH_PREFIX_BYTES) {
      const declared = new DataView(
        buffer.buffer,
        buffer.byteOffset + offset,
        LENGTH_PREFIX_BYTES,
      ).getUint32(0, false);

      // Both checks precede the availability test on purpose: an oversized
      // declaration must fail without waiting for, or buffering, the payload it
      // claims. Testing availability first would let one four-byte lie reserve
      // up to 4 GiB.
      if (declared === 0) {
        return this.#fail(
          "zero_length",
          "a length prefix declared 0 bytes",
          values,
        );
      }
      if (declared > MAX_FRAME_BYTES) {
        return this.#fail(
          "oversized",
          `a length prefix declared ${declared} bytes, over the ${MAX_FRAME_BYTES}-byte cap`,
          values,
        );
      }

      const end = offset + LENGTH_PREFIX_BYTES + declared;
      if (buffer.length < end) {
        break;
      }

      const decoded = decodePayload(
        buffer.subarray(offset + LENGTH_PREFIX_BYTES, end),
      );
      if (!decoded.ok) {
        return this.#fail(
          "undecodable",
          `a ${declared}-byte payload did not decode as one MessagePack value: ${decoded.detail}`,
          values,
        );
      }

      values.push(decoded.value);
      offset = end;
    }

    this.#buffer =
      offset === buffer.length ? EMPTY : buffer.subarray(offset).slice();
    return { values, failure: null };
  }

  #fail(
    reason: FramingFailureReason,
    detail: string,
    values: readonly unknown[],
  ): AccumulatorOutcome {
    this.#failure = { reason, detail };
    this.#buffer = EMPTY;
    return { values, failure: this.#failure };
  }
}

function concat(head: Uint8Array, tail: Uint8Array): Uint8Array {
  const joined = new Uint8Array(head.length + tail.length);
  joined.set(head);
  joined.set(tail, head.length);
  return joined;
}

// ---------------------------------------------------------------------------
// Inbound validation
// ---------------------------------------------------------------------------

/** What a decoded payload turned out to be. */
export type ServerFrameOutcome =
  /** A frame this client understands, with every required field present. */
  | { readonly kind: "frame"; readonly frame: ServerFrame }
  /**
   * A map carrying a `type` this client does not recognize. Distinct from
   * `invalid` because forward compatibility requires ignoring it and keeping
   * the connection open, where a malformed frame fails the connection.
   */
  | { readonly kind: "ignorable"; readonly type: string }
  /** Not a frame. Never reaches a caller. */
  | { readonly kind: "invalid"; readonly reason: string };

/**
 * Validates one decoded payload before any caller can observe it.
 *
 * The array rejection is not redundant with the frame types. A structural cast
 * would accept `["receipt", id, to, status]` as readily as the map form, which
 * is exactly the positional encoding `wire-protocol` forbids; two
 * implementations disagreeing that way interoperate until a field moves.
 */
export function validateServerFrame(value: unknown): ServerFrameOutcome {
  const map = asRecord(value);
  if (map === null) {
    return {
      kind: "invalid",
      reason: `payload is ${describeType(value)}, not a map keyed by field name`,
    };
  }

  const type = map["type"];
  if (typeof type !== "string") {
    return {
      kind: "invalid",
      reason: `frame discriminator "type" is ${describeType(type)}, not a string`,
    };
  }
  if (!isServerFrameType(type)) {
    return { kind: "ignorable", type };
  }

  switch (type) {
    case "pong":
      return { kind: "frame", frame: { type: "pong" } };

    case "ready": {
      const protocol = map["protocol"];
      if (
        typeof protocol !== "number" ||
        !Number.isInteger(protocol) ||
        protocol < 0 ||
        protocol > 0xffff_ffff
      ) {
        return fieldInvalid("ready", "protocol", protocol, "a u32");
      }
      return { kind: "frame", frame: { type: "ready", protocol } };
    }

    case "peers": {
      const requestId = map["request_id"];
      if (!isNonEmptyString(requestId)) {
        return fieldInvalid("peers", "request_id", requestId);
      }
      const peers = map["peers"];
      if (
        !Array.isArray(peers) ||
        !peers.every((peer): peer is string => typeof peer === "string")
      ) {
        return fieldInvalid("peers", "peers", peers, "an array of strings");
      }
      return {
        kind: "frame",
        frame: {
          type: "peers",
          request_id: requestId,
          peers,
        },
      };
    }

    case "message": {
      const id = map["id"];
      if (!isNonEmptyString(id)) return fieldInvalid("message", "id", id);
      const from = map["from"];
      if (!isNonEmptyString(from)) return fieldInvalid("message", "from", from);
      const body = map["body"];
      if (typeof body !== "string") {
        return fieldInvalid("message", "body", body, "a string");
      }
      const replyTo = optionalString(map, "reply_to");
      if (replyTo.kind === "invalid") {
        return fieldInvalid("message", "reply_to", map["reply_to"], "a string");
      }
      return {
        kind: "frame",
        frame:
          replyTo.value === null
            ? { type: "message", id, from, body }
            : { type: "message", id, from, body, reply_to: replyTo.value },
      };
    }

    case "receipt": {
      const id = map["id"];
      if (!isNonEmptyString(id)) return fieldInvalid("receipt", "id", id);
      const to = map["to"];
      if (typeof to !== "string") {
        return fieldInvalid("receipt", "to", to, "a string");
      }
      const status = map["status"];
      if (!isNonEmptyString(status)) {
        return fieldInvalid("receipt", "status", status);
      }
      return { kind: "frame", frame: { type: "receipt", id, to, status } };
    }

    case "error": {
      const code = map["code"];
      if (!isNonEmptyString(code)) return fieldInvalid("error", "code", code);
      const message = optionalString(map, "message");
      if (message.kind === "invalid") {
        return fieldInvalid("error", "message", map["message"], "a string");
      }
      const requestId = optionalString(map, "request_id");
      if (requestId.kind === "invalid") {
        return fieldInvalid("error", "request_id", map["request_id"], "a string");
      }
      return {
        kind: "frame",
        frame: {
          type: "error",
          code,
          ...(message.value === null ? {} : { message: message.value }),
          ...(requestId.value === null ? {} : { request_id: requestId.value }),
        },
      };
    }

    default: {
      // Unreachable, and that is the point: `isServerFrameType` narrowed `type`
      // to the discriminator union, so a frame added to `ServerFrame` and not
      // handled above fails to compile here.
      const unhandled: never = type;
      return {
        kind: "invalid",
        reason: `unhandled server frame type: ${String(unhandled)}`,
      };
    }
  }
}

/**
 * Narrows an unknown value to something whose string keys can be read.
 *
 * The single conversion boundary for untrusted maps in this package, exported
 * so `config.ts` shares it rather than repeating the checks. The assertion is
 * sound because the three tests above it — not an object, null, an array —
 * exhaust every way a decoded MessagePack value or a parsed YAML document can
 * fail to be a string-keyed mapping; TypeScript simply has no narrowing from
 * `object` to `Record<string, unknown>`.
 *
 * @returns the readable map, or `null` when `value` is not one.
 */
export function asRecord(value: unknown): Record<string, unknown> | null {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return null;
  }
  return value as Record<string, unknown>;
}

function fieldInvalid(
  frame: string,
  field: string,
  actual: unknown,
  expected = "a non-empty string",
): ServerFrameOutcome {
  return {
    kind: "invalid",
    reason: `${frame}.${field} is ${describeType(actual)}, expected ${expected}`,
  };
}

/**
 * An optional string field: omitted and explicit nil both mean absent.
 *
 * Accepting nil is required rather than lenient. `wire-protocol` obliges every
 * encoder to omit an absent optional and every decoder to accept both forms, so
 * that interoperability does not depend on the peer's encoder choice.
 */
function optionalString(
  map: Record<string, unknown>,
  field: string,
): { readonly kind: "ok"; readonly value: string | null } | { readonly kind: "invalid" } {
  const value = map[field];
  if (value === undefined || value === null) {
    return { kind: "ok", value: null };
  }
  if (typeof value !== "string") {
    return { kind: "invalid" };
  }
  return { kind: "ok", value };
}

function isNonEmptyString(value: unknown): value is string {
  return typeof value === "string" && value.length > 0;
}

function describeType(value: unknown): string {
  if (value === undefined) return "absent";
  if (value === null) return "nil";
  if (Array.isArray(value)) return "an array";
  if (value instanceof Uint8Array) return "binary";
  return `a ${typeof value}`;
}

// ---------------------------------------------------------------------------
// Identifier rules
// ---------------------------------------------------------------------------

/** Why an identifier was rejected. */
export type IdentifierProblem =
  | { readonly kind: "empty" }
  | { readonly kind: "too_long"; readonly limit: number; readonly found: number }
  | { readonly kind: "reserved_separator"; readonly separator: string }
  | { readonly kind: "surrounding_whitespace" };

/** Renders a problem as the diagnostic half of a validation message. */
export function describeIdentifierProblem(problem: IdentifierProblem): string {
  switch (problem.kind) {
    case "empty":
      return "must not be empty";
    case "too_long":
      return `must be at most ${problem.limit} UTF-8 bytes, found ${problem.found}`;
    case "reserved_separator":
      return `must not contain the reserved separator "${problem.separator}"`;
    case "surrounding_whitespace":
      return "must not begin or end with whitespace";
  }
}

/**
 * Validates a room component or peer name against the `wire-protocol` rules:
 * non-empty, at most {@link MAX_IDENTIFIER_BYTES} UTF-8 bytes, free of `/` and
 * `@`, and free of surrounding whitespace.
 *
 * Rules are checked in the same order as the relay's `validate_identifier`, so
 * a value breaking two of them is diagnosed the same way on both sides.
 *
 * The whitespace test is the Unicode `White_Space` property on the first and
 * last code point, which is exactly what the relay's `char::is_whitespace`
 * uses. `trim()` would be the obvious spelling and is the wrong one: its
 * character set is ECMAScript `WhiteSpace` plus `LineTerminator`, which omits
 * U+0085 NEXT LINE. A client that accepted a peer name the relay rejects at
 * `hello` would turn one configuration error, reportable once at startup, into
 * a connect-reject-reconnect loop.
 *
 * @returns the first rule broken, or `null` when the value is acceptable.
 */
export function identifierProblem(value: string): IdentifierProblem | null {
  if (value.length === 0) {
    return { kind: "empty" };
  }
  const bytes = utf8Length(value);
  if (bytes > MAX_IDENTIFIER_BYTES) {
    return { kind: "too_long", limit: MAX_IDENTIFIER_BYTES, found: bytes };
  }
  for (const character of value) {
    if (character === "/" || character === "@") {
      return { kind: "reserved_separator", separator: character };
    }
  }
  if (/^\p{White_Space}|\p{White_Space}$/u.test(value)) {
    return { kind: "surrounding_whitespace" };
  }
  return null;
}

/**
 * Validates a correlation token (`send.id`, `list.request_id`): length only,
 * because the relay treats it as an opaque sender-scoped value.
 *
 * @returns the first rule broken, or `null` when the value is acceptable.
 */
export function correlationProblem(value: string): IdentifierProblem | null {
  if (value.length === 0) {
    return { kind: "empty" };
  }
  const bytes = utf8Length(value);
  if (bytes > MAX_CORRELATION_BYTES) {
    return { kind: "too_long", limit: MAX_CORRELATION_BYTES, found: bytes };
  }
  return null;
}

/**
 * Reports the body length when it exceeds {@link MAX_BODY_BYTES}.
 *
 * @returns the offending UTF-8 length, or `null` when the body fits.
 */
export function bodyOverBudget(body: string): number | null {
  const bytes = utf8Length(body);
  return bytes > MAX_BODY_BYTES ? bytes : null;
}
