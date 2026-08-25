/**
 * Cross-language fixtures.
 *
 * Two assertions in opposite directions. This file decodes the bytes the Rust
 * relay committed, and produces the `ts-*` bytes that `server/tests/fixtures.rs`
 * decodes in return. Neither side is checking its own output, which is the
 * whole point: a round-trip through one library agrees with itself no matter
 * what it does.
 *
 * Comparison is of decoded values, not bytes. The two libraries need not agree
 * byte for byte, only semantically — except where a fixture exists specifically
 * to pin an encoding, which is the `reply_to` absence and the `status` spelling
 * asserted individually below.
 *
 * Regenerate the `ts-*` fixtures deliberately with `UPDATE_FIXTURES=1 bun test`.
 * A missing fixture is written and then fails, so a fresh checkout cannot pass
 * by generating what it was supposed to verify.
 */

import { describe, expect, test } from "bun:test";
import { encode as rawEncode } from "@msgpack/msgpack";
import { readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";

import {
  asRecord,
  decodePayload,
  digestProblem,
  encodePayload,
  PROTOCOL_VERSION,
  validateServerFrame,
  type ClientFrame,
  type ServerFrame,
} from "../../src/protocol.ts";
import { FIXTURE_DIR } from "../support/paths.ts";

const updating = process.env["UPDATE_FIXTURES"] !== undefined;

/**
 * The address every attachment fixture uses: the SHA-256 of the empty payload,
 * in unpadded base64url. A real digest rather than 43 arbitrary characters, so
 * the value is one either side could have produced.
 */
const FIXTURE_DIGEST = "47DEQpj8HBSa-_TImW-5JCeuQeRkm5NMpJWZG3hSuFU";

/** Each frame, and the interoperability risk it exists to catch. */
const TS_FIXTURES: readonly {
  readonly name: string;
  readonly frame: ClientFrame | ServerFrame;
  readonly risk: string;
}[] = [
  {
    name: "ts-hello.msgpack",
    frame: {
      type: "hello",
      protocol: PROTOCOL_VERSION,
      room: { project: "omp-relayd", task: "implement-relay-client-library" },
      peer: "macbook-reviewer",
    },
    risk: "a nested room map rather than a flattened or combined room string",
  },
  {
    name: "ts-send.msgpack",
    frame: {
      type: "send",
      id: "msg-1",
      to: "windows-main",
      body: "review the diff",
    },
    risk: "an absent optional field omitted entirely rather than encoded as nil",
  },
  {
    name: "ts-receipt.msgpack",
    frame: {
      type: "receipt",
      id: "msg-1",
      to: "windows-main",
      status: "recipient_backpressure",
    },
    risk: "an enum encoded as a snake_case string rather than as an integer",
  },
  {
    name: "ts-announce.msgpack",
    frame: {
      type: "announce",
      id: "ann-1",
      body: "the schema landed",
    },
    risk: "a room-wide address expressed as the absence of a target field, with no reserved value for a peer to capture",
  },
  {
    name: "ts-notice.msgpack",
    frame: {
      type: "notice",
      id: "ann-2",
      from: "macbook-reviewer",
      body: "and the migration with it",
      reply_to: "ann-1",
    },
    risk: "a second delivery class carried by the discriminator over a field set identical to `message`",
  },
  {
    name: "ts-accepted.msgpack",
    frame: {
      type: "accepted",
      id: "ann-1",
      delivered: 2,
      shed: 1,
    },
    risk: "an aggregate outcome as two integer counts rather than as a receipt status",
  },
  {
    name: "ts-reserve.msgpack",
    frame: {
      type: "reserve",
      request_id: "res-1",
      digest: FIXTURE_DIGEST,
      bytes: 301_824,
    },
    risk: "a byte count as a MessagePack integer rather than a string, beside a 43-character base64url address",
  },
  {
    name: "ts-reserved.msgpack",
    frame: {
      type: "reserved",
      request_id: "res-1",
      status: "granted",
      expires_in: 7200,
    },
    risk: "a reservation status as a snake_case string with the payload's stated lifetime beside it",
  },
  {
    name: "ts-send-attachment.msgpack",
    frame: {
      type: "send",
      id: "msg-2",
      to: "windows-main",
      body: "the failing test's output is attached",
      attachment: FIXTURE_DIGEST,
    },
    risk: "a reference as a bare string rather than a map, so no location, size, or filename travels with it",
  },
];

async function readFixture(name: string): Promise<Uint8Array | null> {
  try {
    return new Uint8Array(await readFile(join(FIXTURE_DIR, name)));
  } catch (error) {
    if (
      typeof error === "object" &&
      error !== null &&
      "code" in error &&
      error.code === "ENOENT"
    ) {
      return null;
    }
    throw error;
  }
}

/**
 * Reads a fixture the *other* implementation owns.
 *
 * Fails with the command that produces it rather than with a bare null, and
 * deliberately does not generate it: a side that regenerated its counterpart's
 * fixture would be checking itself again, which is the one thing this whole
 * directory exists to avoid.
 */
async function requireRustFixture(name: string): Promise<Uint8Array> {
  const bytes = await readFixture(name);
  if (bytes === null) {
    throw new Error(
      `${name} is missing. It is produced by \`cargo test --test fixtures\` in \`server/\`; run that and commit the result.`,
    );
  }
  return bytes;
}

/** Whether `payload` contains `key` as a literal byte sequence. */
function containsKey(payload: Uint8Array, key: string): boolean {
  const needle = new TextEncoder().encode(key);
  outer: for (let at = 0; at + needle.length <= payload.length; at += 1) {
    for (let offset = 0; offset < needle.length; offset += 1) {
      if (payload[at + offset] !== needle[offset]) continue outer;
    }
    return true;
  }
  return false;
}

describe("this implementation decodes the Rust relay's fixtures", () => {
  test("rust-hello.msgpack carries a nested room map", async () => {
    const committed = await requireRustFixture("rust-hello.msgpack");

    const decoded = decodePayload(committed);
    expect(decoded.ok).toBe(true);
    if (!decoded.ok) return;

    expect(decoded.value).toEqual({
      type: "hello",
      protocol: PROTOCOL_VERSION,
      room: { project: "omp-relayd", task: "implement-tcp-relay-server" },
      peer: "macbook-reviewer",
    });

    // The property the fixture exists for, asserted structurally: a flattened
    // `room_project` key would still *contain* the bytes "room" and "project",
    // so a substring scan would pass on the very encoding this rejects.
    const map = asRecord(decoded.value);
    expect(map).not.toBeNull();
    const room = asRecord(map?.["room"]);
    expect(room).not.toBeNull();
    expect(Object.keys(room ?? {}).sort()).toEqual(["project", "task"]);
    console.log(
      `rust-hello.msgpack: ${committed.length} bytes, room keys ${Object.keys(room ?? {}).join("+")}`,
    );
  });

  test("rust-send.msgpack has no reply_to property at all", async () => {
    const committed = await requireRustFixture("rust-send.msgpack");

    const decoded = decodePayload(committed);
    expect(decoded.ok).toBe(true);
    if (!decoded.ok) return;

    expect(decoded.value).toEqual({
      type: "send",
      id: "msg-1",
      to: "windows-main",
      body: "review the diff",
    });

    const map = asRecord(decoded.value);
    expect(map).not.toBeNull();
    expect(map !== null && "reply_to" in map).toBe(false);
    expect(containsKey(committed, "reply_to")).toBe(false);
    console.log(`rust-send.msgpack: ${committed.length} bytes, no reply_to key`);
  });

  test("rust-receipt.msgpack carries its status as a string", async () => {
    const committed = await requireRustFixture("rust-receipt.msgpack");

    const decoded = decodePayload(committed);
    expect(decoded.ok).toBe(true);
    if (!decoded.ok) return;

    // Validated rather than only compared, because this is a frame the client
    // will actually receive: passing validation is the contract, not a bonus.
    const outcome = validateServerFrame(decoded.value);
    expect(outcome.kind).toBe("frame");
    if (outcome.kind !== "frame") return;
    expect(outcome.frame).toEqual({
      type: "receipt",
      id: "msg-1",
      to: "windows-main",
      status: "recipient_backpressure",
    });

    const map = asRecord(decoded.value);
    expect(typeof map?.["status"]).toBe("string");
    console.log(
      `rust-receipt.msgpack: ${committed.length} bytes, status typeof ${typeof map?.["status"]}`,
    );
  });

  test("rust-announce.msgpack carries no target field at all", async () => {
    const committed = await requireRustFixture("rust-announce.msgpack");

    const decoded = decodePayload(committed);
    expect(decoded.ok).toBe(true);
    if (!decoded.ok) return;

    expect(decoded.value).toEqual({
      type: "announce",
      id: "ann-1",
      body: "the schema landed",
    });

    // Structural, not a substring scan: the absence of the key is the property,
    // and this side must not have invented one while decoding.
    const map = asRecord(decoded.value);
    expect(map).not.toBeNull();
    expect(map !== null && "to" in map).toBe(false);
    expect(Object.keys(map ?? {}).sort()).toEqual(["body", "id", "type"]);
    console.log(
      `rust-announce.msgpack: ${committed.length} bytes, keys ${Object.keys(map ?? {}).join("+")}`,
    );
  });

  test("rust-notice.msgpack validates as a notice, not as a message", async () => {
    const committed = await requireRustFixture("rust-notice.msgpack");

    const decoded = decodePayload(committed);
    expect(decoded.ok).toBe(true);
    if (!decoded.ok) return;

    const outcome = validateServerFrame(decoded.value);
    expect(outcome.kind).toBe("frame");
    if (outcome.kind !== "frame") return;
    expect(outcome.frame).toEqual({
      type: "notice",
      id: "ann-2",
      from: "macbook-reviewer",
      body: "and the migration with it",
      reply_to: "ann-1",
    });

    // The class relation the shared body budget rests on: identical fields, and
    // a `type` one byte shorter. Measured here rather than asserted, so the two
    // implementations are shown to agree on the number.
    const asMessage = encodePayload({
      type: "message",
      id: "ann-2",
      from: "macbook-reviewer",
      body: "and the migration with it",
      reply_to: "ann-1",
    });
    expect(committed.length + 1).toBe(asMessage.length);
    console.log(
      `rust-notice.msgpack: ${committed.length} bytes, exactly 1 below the same fields as a message (${asMessage.length})`,
    );
  });

  test("rust-accepted.msgpack carries two integer counts and no status", async () => {
    const committed = await requireRustFixture("rust-accepted.msgpack");

    const decoded = decodePayload(committed);
    expect(decoded.ok).toBe(true);
    if (!decoded.ok) return;

    const outcome = validateServerFrame(decoded.value);
    expect(outcome.kind).toBe("frame");
    if (outcome.kind !== "frame") return;
    expect(outcome.frame).toEqual({
      type: "accepted",
      id: "ann-1",
      delivered: 2,
      shed: 1,
    });

    const map = asRecord(decoded.value);
    expect(map !== null && "status" in map).toBe(false);
    expect(typeof map?.["delivered"]).toBe("number");
    expect(typeof map?.["shed"]).toBe("number");
    console.log(
      `rust-accepted.msgpack: ${committed.length} bytes, delivered/shed typeof ${typeof map?.["delivered"]}/${typeof map?.["shed"]}, no status key`,
    );
  });

  const reencodable = [
    "rust-announce.msgpack",
    "rust-notice.msgpack",
    "rust-accepted.msgpack",
    "rust-reserve.msgpack",
    "rust-reserved.msgpack",
    "rust-send-attachment.msgpack",
  ].map((name) => ({ scenario: `field order does not decide what ${name} means`, name }));

  test.each(reencodable)("$scenario", async ({ name }) => {
    // Named maps, not positional tuples: the whole set depends on it, and the
    // cheapest proof is to re-encode the decoded value with its keys reversed
    // and require the same reading back.
    const committed = await requireRustFixture(name);
    const decoded = decodePayload(committed);
    expect(decoded.ok).toBe(true);
    if (!decoded.ok) return;
    const map = asRecord(decoded.value);
    expect(map).not.toBeNull();
    if (map === null) return;

    const reversed = Object.fromEntries(Object.entries(map).reverse());
    const roundTrip = decodePayload(rawEncode(reversed));
    expect(roundTrip.ok).toBe(true);
    if (!roundTrip.ok) return;
    expect(roundTrip.value).toEqual(decoded.value);
  });

  test("an unknown extra field on an announce is ignored, not refused", async () => {
    // Additive evolution within major version 1: a relay one version ahead may
    // add a field, and a client that refused the frame would turn an additive
    // change into a dropped request.
    const committed = await requireRustFixture("rust-announce.msgpack");
    const decoded = decodePayload(committed);
    expect(decoded.ok).toBe(true);
    if (!decoded.ok) return;
    const map = asRecord(decoded.value);
    expect(map).not.toBeNull();
    if (map === null) return;

    const widened = decodePayload(rawEncode({ ...map, priority: 9, from: "impersonated" }));
    expect(widened.ok).toBe(true);
    if (!widened.ok) return;
    const widenedMap = asRecord(widened.value);
    expect(widenedMap?.["id"]).toBe("ann-1");
    expect(widenedMap?.["body"]).toBe("the schema landed");
    expect(widenedMap !== null && "to" in widenedMap).toBe(false);
  });

  test("an unknown extra field on a reserve is ignored, not refused", async () => {
    // The same additive rule on the frame this change introduces, which is where
    // a field is most likely to be added next: a bound, a hint, a priority.
    const committed = await requireRustFixture("rust-reserve.msgpack");
    const decoded = decodePayload(committed);
    expect(decoded.ok).toBe(true);
    if (!decoded.ok) return;
    const map = asRecord(decoded.value);
    expect(map).not.toBeNull();
    if (map === null) return;

    const widened = decodePayload(
      rawEncode({ ...map, host: "198.51.100.7", filename: "../../evil" }),
    );
    expect(widened.ok).toBe(true);
    if (!widened.ok) return;
    const widenedMap = asRecord(widened.value);
    expect(widenedMap?.["digest"]).toBe(FIXTURE_DIGEST);
    expect(widenedMap?.["bytes"]).toBe(301_824);
  });

  test("rust-reserve.msgpack carries its byte count as a number", async () => {
    const committed = await requireRustFixture("rust-reserve.msgpack");
    const decoded = decodePayload(committed);
    expect(decoded.ok).toBe(true);
    if (!decoded.ok) return;

    expect(decoded.value).toEqual({
      type: "reserve",
      request_id: "res-1",
      digest: FIXTURE_DIGEST,
      bytes: 301_824,
    });

    // A count encoded as text would make the field's type differ between
    // implementations while both still worked against their own encoder.
    const map = asRecord(decoded.value);
    expect(typeof map?.["bytes"]).toBe("number");
    expect(digestProblem(String(map?.["digest"]))).toBeNull();
    console.log(
      `rust-reserve.msgpack: ${committed.length} bytes, bytes typeof ${typeof map?.["bytes"]}`,
    );
  });

  test("rust-reserved.msgpack states its lifetime beside a grant", async () => {
    const committed = await requireRustFixture("rust-reserved.msgpack");
    const decoded = decodePayload(committed);
    expect(decoded.ok).toBe(true);
    if (!decoded.ok) return;

    const outcome = validateServerFrame(decoded.value);
    expect(outcome.kind).toBe("frame");
    if (outcome.kind !== "frame") return;
    expect(outcome.frame).toEqual({
      type: "reserved",
      request_id: "res-1",
      status: "granted",
      expires_in: 7200,
    });

    // The other half of the rule, which the granted fixture cannot show: a
    // lifetime beside a refusal is rejected rather than accepted with the number
    // ignored.
    const map = asRecord(decoded.value);
    expect(map).not.toBeNull();
    if (map === null) return;
    const refusedWithLifetime = validateServerFrame({ ...map, status: "room_full" });
    expect(refusedWithLifetime.kind).toBe("invalid");
    console.log(
      `rust-reserved.msgpack: ${committed.length} bytes; a refusal carrying expires_in is ${refusedWithLifetime.kind}`,
    );
  });

  test("rust-send-attachment.msgpack carries a bare digest and no reply_to", async () => {
    const committed = await requireRustFixture("rust-send-attachment.msgpack");
    const decoded = decodePayload(committed);
    expect(decoded.ok).toBe(true);
    if (!decoded.ok) return;

    expect(decoded.value).toEqual({
      type: "send",
      id: "msg-2",
      to: "windows-main",
      body: "the failing test's output is attached",
      attachment: FIXTURE_DIGEST,
    });

    // The shape, asserted against what a map-valued reference would have
    // carried: a location would let a sender aim a recipient's fetch at an
    // arbitrary host, and a filename would make a path component out of another
    // peer's text.
    const map = asRecord(decoded.value);
    expect(typeof map?.["attachment"]).toBe("string");
    for (const rejected of ["host", "port", "path", "filename", "name", "bytes"]) {
      expect(containsKey(committed, rejected)).toBe(false);
    }
    expect(containsKey(committed, "reply_to")).toBe(false);
  });

  test("a delivered attachment is validated as a digest, not as free text", async () => {
    // A relay that sent something else would otherwise have its value carried
    // into a URL path component and into the name of a local file.
    const base = {
      type: "message",
      id: "msg-2",
      from: "macbook-reviewer",
      body: "attached",
    };
    expect(validateServerFrame({ ...base, attachment: FIXTURE_DIGEST }).kind).toBe("frame");
    for (const bad of ["", "short", `${FIXTURE_DIGEST}x`, "../../etc/passwd", 7, {}]) {
      expect(validateServerFrame({ ...base, attachment: bad }).kind).toBe("invalid");
    }
  });
});

describe("this implementation produces fixtures for the Rust relay", () => {
  const produced = TS_FIXTURES.map((fixture) => ({
    scenario: `${fixture.name} is committed and still matches a fresh encode`,
    name: fixture.name,
    fixture,
  }));

  test.each(produced)("$scenario", async ({ name, fixture }) => {
    const fresh = encodePayload(fixture.frame);
    const committed = await readFixture(name);

    if (updating || committed === null) {
      await writeFile(join(FIXTURE_DIR, name), fresh);
      expect(updating).toBe(true);
      console.log(`${name}: written, ${fresh.length} bytes; commit it and re-run`);
      return;
    }

    // The committed bytes must still decode to the value they document. That
    // is the semantic contract the Rust side depends on.
    const decoded = decodePayload(committed);
    expect(decoded.ok).toBe(true);
    if (!decoded.ok) return;
    expect(decoded.value).toEqual(fixture.frame);

    // And a fresh encode must still equal them, so silent encoding drift
    // fails the suite instead of quietly rewriting the file.
    expect([...committed]).toEqual([...fresh]);
    console.log(`${name}: ${committed.length} bytes, covering ${fixture.risk}`);
  });

  test("ts-send.msgpack omits reply_to and ts-receipt.msgpack spells its status out", async () => {
    // The two encoding properties the Rust side asserts from its own direction,
    // pinned here as bytes so a change to either fails on this side too.
    const send = await readFixture("ts-send.msgpack");
    const receipt = await readFixture("ts-receipt.msgpack");
    if (send === null || receipt === null) {
      // The generating test above has already failed and said what to commit.
      return;
    }

    expect(containsKey(send, "reply_to")).toBe(false);
    expect(containsKey(receipt, "recipient_backpressure")).toBe(true);
  });
});
