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
  { type: "announce", id: "ann-1", body: "the schema landed" },
  {
    type: "announce",
    id: "ann-2",
    body: "and the migration with it",
    reply_to: "ann-1",
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
  { type: "notice", id: "ann-1", from: "windows-main", body: "the schema landed" },
  {
    type: "notice",
    id: "ann-2",
    from: "windows-main",
    body: "and the migration with it",
    reply_to: "ann-1",
  },
  { type: "receipt", id: "msg-1", to: "windows-main", status: "routed" },
  { type: "accepted", id: "ann-1", delivered: 2, shed: 1 },
  { type: "accepted", id: "ann-2", delivered: 0, shed: 0 },
  { type: "pong" },
  { type: "error", code: "unsupported_protocol", message: "this relay speaks 1" },
  { type: "error", code: "invalid_identifier", request_id: "msg-1" },
];

/**
 * Names one frame so that no sibling shares the name.
 *
 * Six of the types below appear twice — the pairs are exactly the ones the
 * codec tests exist to tell apart, `reply_to` present against absent — so the
 * type alone would report two cases under one name and identify neither.
 */
function frameScenario(frame: ClientFrame | ServerFrame): string {
  const parts: string[] = [frame.type];
  if ("code" in frame) parts.push(frame.code);
  if ("id" in frame) parts.push(frame.id);
  if ("request_id" in frame && frame.request_id !== undefined) parts.push(frame.request_id);
  if ("reply_to" in frame && frame.reply_to !== undefined) parts.push(`replying to ${frame.reply_to}`);
  if (frame.type === "accepted") parts.push(`${frame.delivered} delivered, ${frame.shed} shed`);
  return `a ${parts.join(" ")} frame`;
}

describe("codec", () => {
  const inventory = [...CLIENT_FRAMES, ...SERVER_FRAMES].map((frame) => ({
    scenario: frameScenario(frame),
    frame,
  }));

  test("every frame in the inventory has a scenario no other frame shares", () => {
    // The control on both tables below: a duplicate name reports two cases as
    // one, so a failure would identify neither of them.
    const names = new Set(inventory.map((entry) => entry.scenario));
    expect(names.size).toBe(inventory.length);
    console.log(`${inventory.length} frames, ${names.size} distinct scenarios`);
  });

  test.each(inventory)("$scenario round-trips through the codec", ({ frame }) => {
    const decoded = decodePayload(encodePayload(frame));
    expect(decoded.ok).toBe(true);
    if (!decoded.ok) return;
    expect(decoded.value).toEqual(frame);
  });

  test.each(inventory)("$scenario encodes as a MessagePack map, never a positional array", ({ frame }) => {
    // A positional encoder on either side couples both implementations to field
    // order, and the divergence stays invisible until a field is added.
    const marker = encodePayload(frame)[0];
    expect(marker).toBeDefined();
    const isMap = (marker! >= 0x80 && marker! <= 0x8f) || marker === 0xde || marker === 0xdf;
    expect(isMap).toBe(true);
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

  test("an announce carries no target key at all", () => {
    // The absence of a peer component *is* the room-wide address, so there is
    // no reserved value for a peer to register under and capture. A `to` key
    // here, of any value, would be that reservation.
    const payload = encodePayload({ type: "announce", id: "ann-1", body: "everyone" });
    const bytes = [...payload];
    const needle = [...new TextEncoder().encode("to")];
    const present = bytes.some((_, at) =>
      needle.every((byte, offset) => bytes[at + offset] === byte),
    );
    expect(present).toBe(false);
  });

  test("an announce assembled with a target is refused rather than silently stripped", () => {
    // `AnnounceFrame.to` is `never`, so a literal carrying one does not
    // compile. This is the case types cannot reach: a frame built by spreading
    // a wider object. A caller that supplied a target believed it was
    // addressing one peer, so broadcasting silently instead would be worse than
    // a stated refusal.
    const smuggled = { type: "announce", id: "ann-1", body: "everyone", to: "windows-main" };
    expect(() => encodeFrame(smuggled as ClientFrame)).toThrow(/no target/);
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

  const outsideProfile = [
    {
      scenario: "a bin8 value carrying bytes is refused by its size bound",
      kind: "bin8",
      payload: new Uint8Array([0xc4, 0x01, 0x00]),
    },
    {
      scenario: "a timestamp32 value carrying bytes is refused by its size bound",
      kind: "timestamp32",
      payload: new Uint8Array([0xd6, 0xff, 0x00, 0x00, 0x00, 0x00]),
    },
  ];

  test.each(outsideProfile)("$scenario", ({ kind, payload }) => {
    // `spec.md:126` puts binary, extension-codec, and timestamp values outside
    // the compatibility profile. Both of these decoded into a value before the
    // decoder was bounded — a `Uint8Array` and a `Date` respectively — leaving
    // a rule stated in prose as the only thing keeping them off the wire. The
    // bound is on size, not on the marker: a zero-length `bin` or `ext` still
    // decodes and is discarded by validation like any other unknown field.
    const decoded = decodePayload(payload);
    expect(decoded.ok).toBe(false);
    if (decoded.ok) return;
    console.log(`refused a ${kind} value: ${decoded.detail}`);
  });

  test("an unknown extra field decodes and leaves the known fields intact", () => {
    // Schema evolution within one major version: "receivers must ignore unknown
    // map fields", and the Rust side is forbidden `deny_unknown_fields` for the
    // same reason (design record:253-260). A decoder bounded on map entries
    // would fail the whole connection on exactly this input, so the additive
    // case is asserted here rather than left to Serde's default on one side.
    const decoded = decodePayload(
      rawEncode({
        type: "message",
        id: "msg-1",
        from: "windows-main",
        body: "hello",
        reply_to: "msg-0",
        priority: 7, // what a later minor version adds
      }),
    );
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
      reply_to: "msg-0",
    });
    expect("priority" in outcome.frame).toBe(false);
    console.log(
      `decoded a 6-field message and kept all 5 known fields, dropping "priority"`,
    );
  });

  test("a peer roster far past any plausible room still decodes", () => {
    // The bounds above must not have become a peer ceiling. The design record
    // leaves `peers` unbounded on purpose (:1047-1054), so a roster larger than
    // any real room has to survive decoding rather than be refused by policy
    // this client invented.
    const frame = {
      type: "peers",
      request_id: "req-1",
      peers: Array.from({ length: 4096 }, (_, at) => `peer-${at}`),
    };
    const payload = rawEncode(frame);
    expect(payload.length).toBeLessThanOrEqual(MAX_FRAME_BYTES);

    const decoded = decodePayload(payload);
    expect(decoded.ok).toBe(true);
    if (!decoded.ok) return;
    expect(decoded.value).toEqual(frame);
    console.log(
      `decoded a ${frame.peers.length}-peer roster from ${payload.length} bytes`,
    );
  });
});

describe("server frame validation", () => {
  test("an array payload is rejected rather than read positionally", () => {
    const outcome = validateServerFrame(["receipt", "msg-1", "windows-main", "routed"]);
    expect(outcome.kind).toBe("invalid");
    if (outcome.kind !== "invalid") return;
    expect(outcome.reason).toContain("array");
  });

  const nonMapPayloads = [
    { scenario: "a nil payload is rejected", value: null as unknown },
    { scenario: "a string payload is rejected", value: "receipt" as unknown },
    { scenario: "an integer payload is rejected", value: 7 as unknown },
  ];

  test.each(nonMapPayloads)("$scenario", ({ value }) => {
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

  test("a granted reservation stating no lifetime is rejected and names the field", () => {
    // The mirror of the refusal-with-a-lifetime rule, and the half a caller
    // feels: accepting this would leave the client inventing the number and a
    // sender stating that invention to a recipient as the relay's own answer.
    const outcome = validateServerFrame({
      type: "reserved",
      request_id: "res-1",
      status: "granted",
    });
    expect(outcome.kind).toBe("invalid");
    if (outcome.kind !== "invalid") return;
    expect(outcome.reason).toBe(
      "reserved.expires_in is absent on a granted reservation, which must state a lifetime",
    );
    console.log(`granted without a lifetime: ${outcome.kind} -- ${outcome.reason}`);

    // The refusal it must not have broken: no lifetime is exactly right there.
    const refused = validateServerFrame({
      type: "reserved",
      request_id: "res-1",
      status: "room_full",
    });
    expect(refused.kind).toBe("frame");
    console.log(`room_full without a lifetime: ${refused.kind}`);
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

  const relayFrames = SERVER_FRAMES.map((frame) => ({
    scenario: `${frameScenario(frame)} the relay may send validates`,
    frame,
  }));

  test.each(relayFrames)("$scenario", ({ frame }) => {
    const decoded = decodePayload(encodePayload(frame));
    expect(decoded.ok).toBe(true);
    if (!decoded.ok) return;
    const outcome = validateServerFrame(decoded.value);
    expect(outcome.kind).toBe("frame");
    if (outcome.kind !== "frame") return;
    expect(outcome.frame).toEqual(frame);
  });

  test("a notice is surfaced as its own class, not folded into a message", () => {
    // The class is the discriminator and nothing else, so a validator that
    // shared the `message` branch by rewriting `type` would pass every field
    // assertion and lose the one thing the host must branch on.
    const outcome = validateServerFrame({
      type: "notice",
      id: "ann-1",
      from: "windows-main",
      body: "the schema landed",
    });
    expect(outcome.kind).toBe("frame");
    if (outcome.kind !== "frame") return;
    expect(outcome.frame.type).toBe("notice");
  });

  const fractionalCounts = [
    { scenario: "an accepted frame with a fractional delivered is rejected and names it", field: "delivered" },
    { scenario: "an accepted frame with a fractional shed is rejected and names it", field: "shed" },
  ];

  test.each(fractionalCounts)("$scenario", ({ field }) => {
    // A fractional count is not a smaller number of recipients; it is a frame
    // this client should not have believed.
    const outcome = validateServerFrame({
      type: "accepted",
      id: "ann-1",
      delivered: 2,
      shed: 0,
      [field]: 1.5,
    });
    expect(outcome.kind).toBe("invalid");
    if (outcome.kind !== "invalid") return;
    expect(outcome.reason).toContain(field);
  });

  test("an accepted frame with a negative count is rejected", () => {
    const outcome = validateServerFrame({
      type: "accepted",
      id: "ann-1",
      delivered: -1,
      shed: 0,
    });
    expect(outcome.kind).toBe("invalid");
    if (outcome.kind !== "invalid") return;
    expect(outcome.reason).toContain("delivered");
  });

  test("a zero-count acceptance is a valid frame, not an omission", () => {
    // An empty room is a fact about the room. Treating two zeroes as a missing
    // field would turn the commonest first announcement into a dropped reply.
    const outcome = validateServerFrame({
      type: "accepted",
      id: "ann-1",
      delivered: 0,
      shed: 0,
    });
    expect(outcome.kind).toBe("frame");
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

  const nextLine = [
    { scenario: "U+0085 at the start is refused, where trim() would have accepted it", value: "\u0085peer" },
    { scenario: "U+0085 at the end is refused, where trim() would have accepted it", value: "peer\u0085" },
  ];

  test.each(nextLine)("$scenario", ({ value }) => {
    // Rust's `char::is_whitespace` uses the Unicode `White_Space` property,
    // which includes U+0085 NEXT LINE; JavaScript's `trim()` does not. Accepting
    // one of these turns a configuration error reportable once at startup into
    // a connect-reject-reconnect loop against the relay's own `hello` check.
    expect(value.trim()).toBe(value);
    expect(identifierProblem(value)).toEqual({ kind: "surrounding_whitespace" });
    console.log(
      `refused code points [${[...value].map((c) => c.codePointAt(0)).join(", ")}] ` +
        `that trim() left unchanged`,
    );
  });

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
