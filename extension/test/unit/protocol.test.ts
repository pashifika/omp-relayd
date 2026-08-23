/**
 * Codec, validation, and identifier rules.
 *
 * Every assertion here is about a way two implementations of the same contract
 * can silently disagree, so each test names the disagreement it prevents rather
 * than restating the code.
 */

import { describe, expect, test } from "bun:test";
import { encode as rawEncode } from "@msgpack/msgpack";

import {
  bodyOverBudget,
  correlationProblem,
  decodePayload,
  encodeFrame,
  encodePayload,
  identifierProblem,
  LENGTH_PREFIX_BYTES,
  MAX_BODY_BYTES,
  MAX_CORRELATION_BYTES,
  MAX_FRAME_BYTES,
  MAX_IDENTIFIER_BYTES,
  PROTOCOL_VERSION,
  validateServerFrame,
  type ClientFrame,
  type ServerFrame,
} from "../../src/protocol.ts";

const CLIENT_FRAMES: readonly ClientFrame[] = [
  {
    type: "hello",
    protocol: PROTOCOL_VERSION,
    room: { project: "omp-relayd", task: "implement-relay-client-library" },
    peer: "macbook-reviewer",
  },
  { type: "list", request_id: "req-1" },
  { type: "send", id: "msg-1", to: "windows-main", body: "review the diff" },
  {
    type: "send",
    id: "msg-2",
    to: "windows-main",
    body: "answering",
    reply_to: "msg-1",
  },
  { type: "ping" },
];

const SERVER_FRAMES: readonly ServerFrame[] = [
  { type: "ready", protocol: PROTOCOL_VERSION },
  { type: "peers", request_id: "req-1", peers: ["macbook-reviewer", "windows-main"] },
  { type: "message", id: "msg-1", from: "windows-main", body: "on it" },
  {
    type: "message",
    id: "msg-2",
    from: "windows-main",
    body: "answering",
    reply_to: "msg-1",
  },
  { type: "receipt", id: "msg-1", to: "windows-main", status: "routed" },
  { type: "pong" },
  { type: "error", code: "unsupported_protocol", message: "this relay speaks 1" },
  { type: "error", code: "invalid_identifier", request_id: "msg-1" },
];

describe("codec", () => {
  test("every frame in the v1 inventory round-trips through the codec", () => {
    for (const frame of [...CLIENT_FRAMES, ...SERVER_FRAMES]) {
      const decoded = decodePayload(encodePayload(frame));
      expect(decoded.ok).toBe(true);
      if (!decoded.ok) continue;
      expect(decoded.value).toEqual(frame);
    }
  });

  test("every payload is a MessagePack map, never a positional array", () => {
    // A positional encoder on either side couples both implementations to field
    // order, and the divergence stays invisible until a field is added.
    for (const frame of [...CLIENT_FRAMES, ...SERVER_FRAMES]) {
      const marker = encodePayload(frame)[0];
      expect(marker).toBeDefined();
      const isMap =
        (marker! >= 0x80 && marker! <= 0x8f) || marker === 0xde || marker === 0xdf;
      expect(isMap).toBe(true);
    }
  });

  test("an absent reply_to produces no key at all, rather than a nil value", () => {
    const payload = encodePayload({
      type: "send",
      id: "msg-1",
      to: "windows-main",
      body: "review the diff",
    });
    const bytes = [...payload];
    const needle = [...new TextEncoder().encode("reply_to")];
    const present = bytes.some((_, at) =>
      needle.every((byte, offset) => bytes[at + offset] === byte),
    );
    expect(present).toBe(false);
  });

  test("an explicit nil reply_to decodes as absent", () => {
    // The relay omits the key, but the contract obliges a decoder to accept
    // both forms so interoperability does not depend on the peer's encoder.
    const payload = rawEncode({
      type: "message",
      id: "msg-1",
      from: "windows-main",
      body: "hello",
      reply_to: null,
    });
    const decoded = decodePayload(payload);
    expect(decoded.ok).toBe(true);
    if (!decoded.ok) return;

    const outcome = validateServerFrame(decoded.value);
    expect(outcome.kind).toBe("frame");
    if (outcome.kind !== "frame") return;
    expect(outcome.frame).toEqual({
      type: "message",
      id: "msg-1",
      from: "windows-main",
      body: "hello",
    });
    expect("reply_to" in outcome.frame).toBe(false);
  });

  test("a framed payload carries its own length in four big-endian bytes", () => {
    const framed = encodeFrame({ type: "ping" });
    const payload = encodePayload({ type: "ping" });
    const declared = new DataView(
      framed.buffer,
      framed.byteOffset,
      LENGTH_PREFIX_BYTES,
    ).getUint32(0, false);

    expect(declared).toBe(payload.length);
    expect(framed.length).toBe(LENGTH_PREFIX_BYTES + payload.length);
    expect([...framed.subarray(LENGTH_PREFIX_BYTES)]).toEqual([...payload]);
  });

  test("a decode failure is returned, never thrown", () => {
    // Every caller of this is a socket callback; a throw would leave the client
    // and land in the host's uncaught handler.
    const truncated = encodePayload({ type: "ping" }).subarray(0, 1);
    const decoded = decodePayload(truncated);
    expect(decoded.ok).toBe(false);
    if (decoded.ok) return;
    expect(decoded.detail.length).toBeGreaterThan(0);
  });

  test("a payload with trailing bytes after a complete value is refused", () => {
    // `rmp_serde::from_slice` would accept this and discard the remainder;
    // rejecting it is what keeps the two implementations agreeing on where a
    // frame ends.
    const complete = encodePayload({ type: "pong" });
    const padded = new Uint8Array(complete.length + 1);
    padded.set(complete);
    padded[complete.length] = 0xc0; // a trailing nil

    expect(decodePayload(padded).ok).toBe(false);
  });
});

describe("server frame validation", () => {
  test("an array payload is rejected rather than read positionally", () => {
    const outcome = validateServerFrame(["receipt", "msg-1", "windows-main", "routed"]);
    expect(outcome.kind).toBe("invalid");
    if (outcome.kind !== "invalid") return;
    expect(outcome.reason).toContain("array");
  });

  test.each([
    ["nil", null],
    ["a string", "receipt"],
    ["an integer", 7],
  ])("a %s payload is rejected", (_label, value) => {
    expect(validateServerFrame(value).kind).toBe("invalid");
  });

  test("a message frame missing body is rejected and names the field", () => {
    const outcome = validateServerFrame({
      type: "message",
      id: "msg-1",
      from: "windows-main",
    });
    expect(outcome.kind).toBe("invalid");
    if (outcome.kind !== "invalid") return;
    expect(outcome.reason).toBe("message.body is absent, expected a string");
  });

  test("a non-string receipt status is rejected and names the field", () => {
    // The relay encodes the status as a snake_case string. An ordinal here
    // would mean one side switched to an integer representation.
    const outcome = validateServerFrame({
      type: "receipt",
      id: "msg-1",
      to: "windows-main",
      status: 2,
    });
    expect(outcome.kind).toBe("invalid");
    if (outcome.kind !== "invalid") return;
    expect(outcome.reason).toBe(
      "receipt.status is a number, expected a non-empty string",
    );
  });

  test("a peers frame whose roster holds a non-string is rejected", () => {
    const outcome = validateServerFrame({
      type: "peers",
      request_id: "req-1",
      peers: ["macbook-reviewer", 7],
    });
    expect(outcome.kind).toBe("invalid");
    if (outcome.kind !== "invalid") return;
    expect(outcome.reason).toContain("an array of strings");
  });

  test("an unknown frame type is ignorable, not invalid", () => {
    // The two outcomes drive different behavior: ignorable keeps the
    // connection, invalid fails it. Collapsing them would turn a relay's
    // additive change into a reconnect loop.
    const outcome = validateServerFrame({ type: "broadcast", body: "hi" });
    expect(outcome.kind).toBe("ignorable");
    if (outcome.kind !== "ignorable") return;
    expect(outcome.type).toBe("broadcast");
  });

  test("an unrecognized field on a known frame is ignored", () => {
    const outcome = validateServerFrame({
      type: "receipt",
      id: "msg-1",
      to: "windows-main",
      status: "routed",
      priority: 9,
    });
    expect(outcome.kind).toBe("frame");
    if (outcome.kind !== "frame") return;
    expect(outcome.frame).toEqual({
      type: "receipt",
      id: "msg-1",
      to: "windows-main",
      status: "routed",
    });
  });

  test("a receipt status this version does not know is still surfaced", () => {
    // Openness is deliberate: discarding an unrecognized status would turn a
    // relay's additive change into a request that never settles.
    const outcome = validateServerFrame({
      type: "receipt",
      id: "msg-1",
      to: "windows-main",
      status: "deferred",
    });
    expect(outcome.kind).toBe("frame");
  });

  test("every frame the relay may send validates", () => {
    for (const frame of SERVER_FRAMES) {
      const decoded = decodePayload(encodePayload(frame));
      expect(decoded.ok).toBe(true);
      if (!decoded.ok) continue;
      const outcome = validateServerFrame(decoded.value);
      expect(outcome.kind).toBe("frame");
      if (outcome.kind !== "frame") continue;
      expect(outcome.frame).toEqual(frame);
    }
  });
});

describe("identifier rules", () => {
  test("an identifier at the byte limit is accepted", () => {
    expect(identifierProblem("a".repeat(MAX_IDENTIFIER_BYTES))).toBeNull();
  });

  test("an identifier one byte over the limit is refused", () => {
    const problem = identifierProblem("a".repeat(MAX_IDENTIFIER_BYTES + 1));
    expect(problem).toEqual({
      kind: "too_long",
      limit: MAX_IDENTIFIER_BYTES,
      found: MAX_IDENTIFIER_BYTES + 1,
    });
  });

  test("length is counted in UTF-8 bytes, not code units", () => {
    // 22 characters, 66 UTF-8 bytes: a UTF-16 length check would accept it and
    // the relay would then reject the handshake.
    const value = "あ".repeat(22);
    expect(value.length).toBeLessThan(MAX_IDENTIFIER_BYTES);
    expect(identifierProblem(value)).toEqual({
      kind: "too_long",
      limit: MAX_IDENTIFIER_BYTES,
      found: 66,
    });
  });

  test.each(["/", "@"])("the reserved separator %s is refused", (separator) => {
    expect(identifierProblem(`peer${separator}name`)).toEqual({
      kind: "reserved_separator",
      separator,
    });
  });

  test.each([" leading", "trailing ", "\tboth\t"])(
    "surrounding whitespace in %p is refused",
    (value) => {
      expect(identifierProblem(value)).toEqual({ kind: "surrounding_whitespace" });
    },
  );

  test("an empty identifier is refused", () => {
    expect(identifierProblem("")).toEqual({ kind: "empty" });
  });

  test("a correlation token is bounded at its own, larger limit", () => {
    expect(correlationProblem("t".repeat(MAX_CORRELATION_BYTES))).toBeNull();
    expect(correlationProblem("t".repeat(MAX_CORRELATION_BYTES + 1))).toEqual({
      kind: "too_long",
      limit: MAX_CORRELATION_BYTES,
      found: MAX_CORRELATION_BYTES + 1,
    });
  });

  test("a correlation token may carry characters an identifier may not", () => {
    // Opaque to the relay, which validates length only.
    expect(correlationProblem("room/task@peer")).toBeNull();
  });
});

describe("body budget", () => {
  test("a body at the budget is accepted and one byte over is not", () => {
    expect(bodyOverBudget("b".repeat(MAX_BODY_BYTES))).toBeNull();
    expect(bodyOverBudget("b".repeat(MAX_BODY_BYTES + 1))).toBe(MAX_BODY_BYTES + 1);
  });

  test("the budget leaves room for the message envelope the relay will build", () => {
    // The relay does not forward the frame it received: it builds a `message`
    // with longer key names. This is the arithmetic that keeps a body which fit
    // an inbound `send` from producing an outbound `message` that does not fit.
    const worstCase = encodePayload({
      type: "message",
      id: "i".repeat(MAX_CORRELATION_BYTES),
      from: "f".repeat(MAX_IDENTIFIER_BYTES),
      body: "b".repeat(MAX_BODY_BYTES),
      reply_to: "r".repeat(MAX_CORRELATION_BYTES),
    });
    expect(worstCase.length).toBeLessThanOrEqual(MAX_FRAME_BYTES);
    console.log(
      `worst-case message envelope at the body budget: ${worstCase.length} bytes, ${MAX_FRAME_BYTES - worstCase.length} inside the cap`,
    );
  });
});
