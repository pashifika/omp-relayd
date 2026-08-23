/**
 * Byte-stream reassembly.
 *
 * A socket delivers part of a frame, exactly one frame, or several frames per
 * `data` event, and picking any one of those as "the" case is how a stream
 * reassembler ships broken. Every delivery pattern the contract names has a
 * test here, driven by synthesized chunks so the split points are exact rather
 * than whatever the kernel happened to do.
 */

import { describe, expect, test } from "bun:test";
import { encode as rawEncode } from "@msgpack/msgpack";

import {
  encodePayload,
  FrameAccumulator,
  LENGTH_PREFIX_BYTES,
  MAX_FRAME_BYTES,
  type ClientFrame,
  type ServerFrame,
} from "../../src/protocol.ts";

/** One frame as it appears on the wire. Prefixed here, independently of `encodeFrame`. */
function wire(frame: ClientFrame | ServerFrame): Uint8Array {
  const payload = encodePayload(frame);
  const framed = new Uint8Array(LENGTH_PREFIX_BYTES + payload.length);
  new DataView(framed.buffer).setUint32(0, payload.length, false);
  framed.set(payload, LENGTH_PREFIX_BYTES);
  return framed;
}

/** A length prefix declaring `declared` bytes, followed by `payload`. */
function declaring(declared: number, payload: Uint8Array = new Uint8Array(0)): Uint8Array {
  const framed = new Uint8Array(LENGTH_PREFIX_BYTES + payload.length);
  new DataView(framed.buffer).setUint32(0, declared, false);
  framed.set(payload, LENGTH_PREFIX_BYTES);
  return framed;
}

function join(...parts: readonly Uint8Array[]): Uint8Array {
  const total = parts.reduce((sum, part) => sum + part.length, 0);
  const joined = new Uint8Array(total);
  let offset = 0;
  for (const part of parts) {
    joined.set(part, offset);
    offset += part.length;
  }
  return joined;
}

const READY: ServerFrame = { type: "ready", protocol: 1 };
const PONG: ServerFrame = { type: "pong" };
const RECEIPT: ServerFrame = {
  type: "receipt",
  id: "msg-1",
  to: "windows-main",
  status: "routed",
};

describe("delivery patterns", () => {
  test("one frame split across two chunks emits after the second", () => {
    const framed = wire(RECEIPT);
    const split = Math.floor(framed.length / 2);
    const accumulator = new FrameAccumulator();

    const first = accumulator.push(framed.subarray(0, split));
    expect(first.values).toEqual([]);
    expect(first.failure).toBeNull();
    expect(accumulator.buffered).toBe(split);

    const second = accumulator.push(framed.subarray(split));
    expect(second.values).toEqual([RECEIPT]);
    expect(second.failure).toBeNull();
    expect(accumulator.buffered).toBe(0);
  });

  test("a length prefix split mid-way is held until the fourth byte arrives", () => {
    const framed = wire(READY);
    const accumulator = new FrameAccumulator();

    // Two of the four length bytes. Reading a length now would read past the
    // prefix into the payload, or into nothing at all.
    expect(accumulator.push(framed.subarray(0, 2)).values).toEqual([]);
    expect(accumulator.buffered).toBe(2);

    expect(accumulator.push(framed.subarray(2, 4)).values).toEqual([]);
    expect(accumulator.buffered).toBe(4);

    const rest = accumulator.push(framed.subarray(4));
    expect(rest.values).toEqual([READY]);
    expect(accumulator.buffered).toBe(0);
  });

  test("three frames coalesced into one chunk emit in arrival order", () => {
    const accumulator = new FrameAccumulator();
    const outcome = accumulator.push(join(wire(READY), wire(RECEIPT), wire(PONG)));

    expect(outcome.values).toEqual([READY, RECEIPT, PONG]);
    expect(outcome.failure).toBeNull();
    expect(accumulator.buffered).toBe(0);
  });

  test("a complete frame followed by a partial one emits the complete frame only", () => {
    const partial = wire(RECEIPT);
    const accumulator = new FrameAccumulator();

    const first = accumulator.push(join(wire(READY), partial.subarray(0, 6)));
    expect(first.values).toEqual([READY]);
    expect(accumulator.buffered).toBe(6);

    const second = accumulator.push(partial.subarray(6));
    expect(second.values).toEqual([RECEIPT]);
    expect(accumulator.buffered).toBe(0);
  });

  test("a whole frame delivered one byte at a time emits exactly once", () => {
    const framed = wire(RECEIPT);
    const accumulator = new FrameAccumulator();
    const emitted: unknown[] = [];

    for (const byte of framed) {
      const outcome = accumulator.push(new Uint8Array([byte]));
      expect(outcome.failure).toBeNull();
      emitted.push(...outcome.values);
    }

    expect(emitted).toEqual([RECEIPT]);
    expect(accumulator.buffered).toBe(0);
  });

  test("a chunk carrying two frames and half of a third keeps only the remainder", () => {
    const third = wire(PONG);
    const accumulator = new FrameAccumulator();

    const outcome = accumulator.push(
      join(wire(READY), wire(RECEIPT), third.subarray(0, 3)),
    );
    expect(outcome.values).toEqual([READY, RECEIPT]);
    expect(accumulator.buffered).toBe(3);

    expect(accumulator.push(third.subarray(3)).values).toEqual([PONG]);
  });

  test("a retained tail survives the caller reusing its chunk buffer", () => {
    // Node and Bun hand out a fresh buffer per `data` event today. This asserts
    // the accumulator does not depend on that: a retained tail is copied, so
    // overwriting the delivered chunk afterwards cannot corrupt a frame.
    const framed = wire(RECEIPT);
    const reused = new Uint8Array(framed.length);
    reused.set(framed);

    const accumulator = new FrameAccumulator();
    const head = reused.subarray(0, 5);
    expect(accumulator.push(head).values).toEqual([]);
    reused.fill(0xff, 0, 5);

    expect(accumulator.push(framed.subarray(5)).values).toEqual([RECEIPT]);
  });
});

describe("malformed declarations fail the connection", () => {
  test("a zero-length declaration fails and consumes nothing afterwards", () => {
    const accumulator = new FrameAccumulator();
    const outcome = accumulator.push(declaring(0));

    expect(outcome.failure?.reason).toBe("zero_length");
    expect(outcome.values).toEqual([]);

    // No resynchronization: once framing is untrustworthy, no later offset is
    // trustworthy either, and guessing would turn a clean failure into silent
    // corruption.
    const after = accumulator.push(wire(PONG));
    expect(after.values).toEqual([]);
    expect(after.failure?.reason).toBe("zero_length");
  });

  test("an oversized declaration fails without waiting for the payload", () => {
    const accumulator = new FrameAccumulator();
    const outcome = accumulator.push(declaring(MAX_FRAME_BYTES + 1));

    expect(outcome.failure?.reason).toBe("oversized");
    expect(outcome.failure?.detail).toContain(String(MAX_FRAME_BYTES + 1));
    // Nothing was buffered against the declared length: four bytes of lie must
    // not reserve the payload they claim.
    expect(accumulator.buffered).toBe(0);
  });

  test("a declaration at exactly the cap is accepted", () => {
    // The cap is inclusive on both sides. An off-by-one here would reject
    // frames the relay considers legal — so the payload has to sit *on* the
    // boundary rather than merely under it. At 65,521 bytes this test stayed
    // green against a `>=` guard, which is the off-by-one it exists to catch.
    const shell = { type: "message", id: "msg-1", from: "windows-main" };
    // A `str16` body header is three bytes for every length from 256 to 65535,
    // so the overhead measured at one such length is the overhead at the one
    // that lands the payload exactly on the cap.
    const probe = "x".repeat(1024);
    const overhead = rawEncode({ ...shell, body: probe }).length - probe.length;
    const payload = rawEncode({ ...shell, body: "x".repeat(MAX_FRAME_BYTES - overhead) });
    expect(payload.length).toBe(MAX_FRAME_BYTES);

    const accumulator = new FrameAccumulator();
    const outcome = accumulator.push(declaring(payload.length, payload));
    expect(outcome.failure).toBeNull();
    expect(outcome.values).toHaveLength(1);
    console.log(
      `accepted a ${payload.length}-byte payload against the ${MAX_FRAME_BYTES}-byte cap`,
    );
  });

  test("a payload that is not one MessagePack value fails the connection", () => {
    const accumulator = new FrameAccumulator();
    const outcome = accumulator.push(declaring(3, new Uint8Array([0xc1, 0xc1, 0xc1])));

    expect(outcome.failure?.reason).toBe("undecodable");
    expect(outcome.values).toEqual([]);
  });

  test("frames received before a bad length are still delivered", () => {
    // They were validly framed. Discarding correctly received data because of
    // what followed it would lose a message the relay already accounted for.
    const accumulator = new FrameAccumulator();
    const outcome = accumulator.push(join(wire(READY), declaring(0)));

    expect(outcome.values).toEqual([READY]);
    expect(outcome.failure?.reason).toBe("zero_length");
  });
});
