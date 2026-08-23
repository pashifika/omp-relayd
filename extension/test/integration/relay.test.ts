/**
 * The TypeScript client against the real relay binary.
 *
 * Everything here could be faked, and faking it would prove nothing: a double
 * agrees with whatever this side believes about the protocol, and that belief is
 * the assumption under test. So the relay is built from `server/`, spawned, and
 * talked to over a real socket.
 *
 * These tests also cover the one risk the unit suite cannot: Bun's `node:net`
 * chunking is *assumed* by the accumulator's synthesized-chunk tests. Here the
 * kernel decides where the boundaries fall.
 *
 * Timers run on a {@link FakeScheduler} wherever the test needs to control
 * reconnect timing, and on the ambient ones in the first test, so the default
 * path is exercised against a real relay too.
 */

import { afterAll, beforeAll, describe, expect, test } from "bun:test";

import { connect } from "node:net";

import {
  RelayClient,
  RequestFailed,
  type Report,
  type Scheduler,
} from "../../src/client.ts";
import type { RelayConfig } from "../../src/config.ts";
import {
  encodeFrame,
  FrameAccumulator,
  MAX_IDENTIFIER_BYTES,
  PROTOCOL_VERSION,
  validateServerFrame,
  type MessageFrame,
  type ServerFrame,
} from "../../src/protocol.ts";
import { FakeScheduler } from "../support/fake-scheduler.ts";
import {
  buildRelay,
  RELAY_SETUP_TIMEOUT_MS,
  startRelay,
  type RelayProcess,
} from "../support/relay-process.ts";
import { settlement } from "../support/settlement.ts";
import { Signal } from "../support/signal.ts";

const ROOM = { project: "omp-relayd", task: "implement-relay-client-library" };

interface Peer {
  readonly client: RelayClient;
  readonly scheduler: FakeScheduler;
  readonly ready: Signal;
  readonly disconnects: Signal<string>;
  readonly reports: Signal<Report>;
  readonly messages: Signal<MessageFrame>;
}

function configFor(port: number, peer: string): RelayConfig {
  return { transport: { mode: "local", host: "127.0.0.1", port }, room: ROOM, peer };
}

function peerFor(config: RelayConfig, scheduler?: Scheduler): Peer {
  const fake = new FakeScheduler();
  const ready = new Signal();
  const disconnects = new Signal<string>();
  const reports = new Signal<Report>();
  const messages = new Signal<MessageFrame>();

  const client = new RelayClient({
    config,
    scheduler: scheduler ?? fake,
    handlers: {
      onReady: () => ready.fire(undefined),
      onDisconnect: (reason) => disconnects.fire(reason),
      onReport: (report) => reports.fire(report),
      onMessage: (message) => messages.fire(message),
    },
  });
  return { client, scheduler: fake, ready, disconnects, reports, messages };
}

const escaped: unknown[] = [];
const record = (error: unknown): void => void escaped.push(error);

let relay: RelayProcess | null = null;

beforeAll(async () => {
  process.on("unhandledRejection", record);
  process.on("uncaughtException", record);
  const binary = await buildRelay();
  relay = await startRelay();
  console.log(`relay: ${binary} bound to 127.0.0.1:${relay.port}`);
}, RELAY_SETUP_TIMEOUT_MS);

afterAll(async () => {
  // Guarded: when setup fails there is no relay to stop, and an unguarded
  // teardown replaces the real failure with a `TypeError` from this line.
  await relay?.stop();
  process.off("unhandledRejection", record);
  process.off("uncaughtException", record);
  expect(escaped).toEqual([]);
}, RELAY_SETUP_TIMEOUT_MS);

/** The shared relay, or a clear failure when setup did not produce one. */
function shared(): RelayProcess {
  if (relay === null) {
    throw new Error("the shared relay is not running; setup failed");
  }
  return relay;
}

/**
 * Completes one handshake without the client and returns the frame the relay
 * answered `hello` with.
 *
 * The client does not expose `ready.protocol`, and adding that surface for a
 * test's benefit would be the wrong trade. Reading the bytes off the wire is
 * what makes the version assertion an observation rather than a restatement of
 * this side's own constant.
 */
async function readyFrameFromWire(port: number, peer: string): Promise<ServerFrame> {
  const socket = connect({ host: "127.0.0.1", port });
  const accumulator = new FrameAccumulator();
  const answered = Promise.withResolvers<ServerFrame>();

  socket.setNoDelay(true);
  socket.on("error", (error) => answered.reject(error));
  socket.on("close", () =>
    answered.reject(new Error("the relay closed without answering hello")),
  );
  socket.on("connect", () => {
    socket.write(encodeFrame({ type: "hello", protocol: PROTOCOL_VERSION, room: ROOM, peer }));
  });
  socket.on("data", (chunk: Buffer) => {
    const outcome = accumulator.push(chunk);
    for (const value of outcome.values) {
      const validated = validateServerFrame(value);
      if (validated.kind === "frame") {
        answered.resolve(validated.frame);
      } else {
        answered.reject(new Error(`the relay sent a ${validated.kind} value`));
      }
      return;
    }
    if (outcome.failure !== null) {
      answered.reject(new Error(`framing failure: ${outcome.failure.detail}`));
    }
  });

  try {
    return await answered.promise;
  } finally {
    socket.removeAllListeners();
    socket.on("error", () => {});
    socket.destroy();
  }
}

describe("handshake", () => {
  test("connect, hello, ready against the real relay", async () => {
    // The one test here on the ambient scheduler, so the default timer path is
    // exercised end to end and not only against a scripted double.
    const ready = new Signal();
    const client = new RelayClient({
      config: configFor(shared().port, "handshake-peer"),
      handlers: { onReady: () => ready.fire(undefined) },
    });

    client.start();
    await ready.until(1);

    expect(client.state).toBe("ready");
    console.log(`handshake completed against the real relay, state ${client.state}`);

    await client.stop();
  });

  test("a peer name the relay refuses surfaces the relay's own error code", async () => {
    // Constructed from a hand-made config, not a loaded one: `config.ts` would
    // refuse this peer name before a connection existed, which is correct and
    // is exactly why the relay's own rejection needs a way to be reached.
    const peer = peerFor(configFor(shared().port, "invalid@peer"));

    peer.client.start();
    await peer.disconnects.until(1);

    expect(peer.disconnects.last).toContain("invalid_identifier");
    expect(peer.disconnects.last).toContain("rejected the handshake");
    console.log(`disconnect reason: ${peer.disconnects.last}`);

    await peer.client.stop();
  });

  test("an identifier at the 64-byte limit is accepted by the relay", async () => {
    // The boundary both implementations agree on, checked against the relay
    // rather than only against this side's own validator.
    const name = "b".repeat(MAX_IDENTIFIER_BYTES);
    const peer = peerFor(configFor(shared().port, name));

    peer.client.start();
    await peer.ready.until(1);
    expect(peer.client.state).toBe("ready");

    await peer.client.stop();
  });
});

describe("roster and routing", () => {
  test("list returns both peers, correlated by request_id", async () => {
    const first = peerFor(configFor(shared().port, "list-alpha"));
    const second = peerFor(configFor(shared().port, "list-beta"));

    first.client.start();
    second.client.start();
    await Promise.all([first.ready.until(1), second.ready.until(1)]);

    const roster = await first.client.list("roster-request-1");

    expect(roster.request_id).toBe("roster-request-1");
    expect([...roster.peers].sort()).toEqual(["list-alpha", "list-beta"]);
    console.log(`roster for ${roster.request_id}: ${roster.peers.join(", ")}`);

    await Promise.all([first.client.stop(), second.client.stop()]);
  });

  test("a send is delivered with the sender's registered name and receipted as routed", async () => {
    const sender = peerFor(configFor(shared().port, "route-sender"));
    const recipient = peerFor(configFor(shared().port, "route-recipient"));

    sender.client.start();
    recipient.client.start();
    await Promise.all([sender.ready.until(1), recipient.ready.until(1)]);

    const receipt = await sender.client.send({
      to: "route-recipient",
      body: "review the diff",
      id: "routed-1",
    });
    await recipient.messages.until(1);

    expect(receipt).toEqual({ type: "receipt", id: "routed-1", to: "route-recipient", status: "routed" });
    // `from` is derived by the relay from the registered peer name, never taken
    // from the sender's frame.
    expect(recipient.messages.last).toEqual({
      type: "message",
      id: "routed-1",
      from: "route-sender",
      body: "review the diff",
    });
    expect(recipient.messages.last && "reply_to" in recipient.messages.last).toBe(false);

    // "Exactly once" needs a barrier rather than a sleep: a completed round
    // trip through the relay after the message arrived means any duplicate
    // would already have been delivered by now.
    await recipient.client.list("delivery-barrier");
    expect(recipient.messages.count).toBe(1);

    await Promise.all([sender.client.stop(), recipient.client.stop()]);
  });

  test("a reply carries reply_to through to the recipient", async () => {
    // The optional field in its present form, since every other assertion here
    // covers its absent form.
    const sender = peerFor(configFor(shared().port, "reply-sender"));
    const recipient = peerFor(configFor(shared().port, "reply-recipient"));

    sender.client.start();
    recipient.client.start();
    await Promise.all([sender.ready.until(1), recipient.ready.until(1)]);

    await sender.client.send({
      to: "reply-recipient",
      body: "answering",
      id: "reply-2",
      replyTo: "reply-1",
    });
    await recipient.messages.until(1);

    expect(recipient.messages.last?.reply_to).toBe("reply-1");

    await Promise.all([sender.client.stop(), recipient.client.stop()]);
  });

  test("a self-addressed send is delivered to the sender", async () => {
    const peer = peerFor(configFor(shared().port, "self-addressed"));

    peer.client.start();
    await peer.ready.until(1);

    const receipt = await peer.client.send({
      to: "self-addressed",
      body: "note to self",
      id: "self-1",
    });
    await peer.messages.until(1);

    expect(receipt.status).toBe("routed");
    expect(peer.messages.last?.from).toBe("self-addressed");
    expect(peer.messages.last?.body).toBe("note to self");

    await peer.client.stop();
  });

  test("a send to an absent peer is receipted peer_offline, not an error", async () => {
    // A receipt, so the request resolves rather than rejecting: the relay
    // answered, and the answer is that nobody was there.
    const peer = peerFor(configFor(shared().port, "offline-sender"));

    peer.client.start();
    await peer.ready.until(1);

    const receipt = await peer.client.send({
      to: "nobody-is-registered-here",
      body: "hello?",
      id: "offline-1",
    });

    expect(receipt.status).toBe("peer_offline");
    expect(receipt.to).toBe("nobody-is-registered-here");
    console.log(`receipt for an absent peer: ${receipt.status}`);

    await peer.client.stop();
  });

  test("a body larger than a single TCP segment survives reassembly intact", async () => {
    // The accumulator's unit tests synthesize chunk boundaries; this one lets
    // the kernel choose them, which is the assumption those tests cannot check.
    const sender = peerFor(configFor(shared().port, "bulk-sender"));
    const recipient = peerFor(configFor(shared().port, "bulk-recipient"));

    sender.client.start();
    recipient.client.start();
    await Promise.all([sender.ready.until(1), recipient.ready.until(1)]);

    // Not a repeated byte: a body that is the same character throughout would
    // survive a reassembly that dropped or duplicated a chunk.
    const body = Array.from({ length: 60_000 }, (_, index) =>
      String.fromCharCode(0x20 + (index % 95)),
    ).join("");

    const receipt = await sender.client.send({
      to: "bulk-recipient",
      body,
      id: "bulk-1",
    });
    await recipient.messages.until(1);

    expect(receipt.status).toBe("routed");
    expect(recipient.messages.last?.body).toBe(body);
    console.log(
      `relayed ${body.length} bytes intact across kernel-chosen chunk boundaries`,
    );

    await Promise.all([sender.client.stop(), recipient.client.stop()]);
  });
});

describe("outages", () => {
  test("no relay listening is reported once, retried, and never thrown", async () => {
    // A port that was bound and released, so nothing is listening on it.
    const spare = await startRelay();
    const port = spare.port;
    await spare.stop();

    const peer = peerFor(configFor(port, "no-relay"));
    peer.client.start();
    await peer.disconnects.until(1);

    expect(peer.reports.count).toBe(1);
    expect(peer.reports.last?.level).toBe("warn");
    expect(peer.scheduler.liveOf("timeout")).toHaveLength(1);
    expect(peer.client.state).toBe("connecting");

    // Retried, and still reported once.
    peer.scheduler.fireNewest("timeout");
    await peer.disconnects.until(2);
    expect(peer.reports.count).toBe(1);
    console.log(
      `${peer.disconnects.count} failed attempts, ${peer.reports.count} report(s): ${peer.reports.last?.message}`,
    );

    await peer.client.stop();
  });

  test("the client reconnects and completes a fresh handshake after the relay restarts", async () => {
    // The relay is restarted on the same port, because that is where the client
    // is still trying to reach it.
    const own = await startRelay();
    const port = own.port;
    const peer = peerFor(configFor(port, "reconnecting-peer"));

    peer.client.start();
    await peer.ready.until(1);

    const roster = await peer.client.list("before-restart");
    expect(roster.peers).toEqual(["reconnecting-peer"]);

    await own.stop();
    await peer.disconnects.until(1);
    expect(peer.client.state).toBe("connecting");

    const restarted = await startRelay(port);
    expect(restarted.port).toBe(port);

    peer.scheduler.fireNewest("timeout");
    await peer.ready.until(2);

    // A *fresh* handshake, not a resumed session: the restarted relay persists
    // nothing, so the roster proves this peer registered again.
    const after = await peer.client.list("after-restart");
    expect(after.request_id).toBe("after-restart");
    expect(after.peers).toEqual(["reconnecting-peer"]);
    console.log(
      `reconnected to a restarted relay on port ${port}; ready fired ${peer.ready.count} times`,
    );

    await peer.client.stop();
    await restarted.stop();
  });

  test("requests outstanding when the relay dies settle with a stated reason", async () => {
    const own = await startRelay();
    const peer = peerFor(configFor(own.port, "settling-peer"));

    peer.client.start();
    await peer.ready.until(1);

    // A send to a peer that will never answer, so the request is outstanding
    // when the relay goes away. Captured before the trigger; see `settlement`.
    const outstanding = settlement(
      peer.client.send({ to: "settling-peer", body: "x", id: "outstanding-1" }),
    );
    await own.stop();
    await peer.disconnects.until(1);

    const outcome = await outstanding;
    // Either the receipt arrived before the relay died, or the request settled
    // as a disconnect. Both are correct; what must not happen is neither.
    if (outcome.status === "rejected") {
      expect(outcome.reason).toBeInstanceOf(RequestFailed);
      expect((outcome.reason as RequestFailed).reason).toBe("disconnected");
      console.log(`outstanding request settled as ${(outcome.reason as RequestFailed).reason}`);
    } else {
      expect(outcome.value.status).toBe("routed");
      console.log("outstanding request settled with its receipt before the relay died");
    }

    await peer.client.stop();
  });
});

describe("protocol version", () => {
  test("the relay reports the protocol version this client speaks", async () => {
    // `ready.protocol` is the negotiated version. A mismatch here would mean
    // the two sides disagree about which contract they are following, so the
    // value has to be the one the relay put on the wire: resolving it from the
    // local constant would assert that constant against itself.
    const frame = await readyFrameFromWire(shared().port, "version-peer");

    expect(frame.type).toBe("ready");
    if (frame.type !== "ready") return;
    console.log(`ready.protocol observed on the wire: ${frame.protocol}`);
    expect(frame.protocol).toBe(PROTOCOL_VERSION);
  });
});
