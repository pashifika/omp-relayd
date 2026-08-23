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
import { readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";

import {
  asRecord,
  decodePayload,
  encodePayload,
  PROTOCOL_VERSION,
  validateServerFrame,
  type ClientFrame,
  type ServerFrame,
} from "../../src/protocol.ts";
import { FIXTURE_DIR } from "../support/paths.ts";

const updating = process.env["UPDATE_FIXTURES"] !== undefined;

/** The three frames, and the interoperability risk each one exists to catch. */
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
});

describe("this implementation produces fixtures for the Rust relay", () => {
  test.each(TS_FIXTURES.map((fixture) => [fixture.name, fixture] as const))(
    "%s is committed and still matches a fresh encode",
    async (name, fixture) => {
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
    },
  );

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
