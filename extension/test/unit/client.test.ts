/**
 * Connection lifecycle, heartbeat, reconnect, correlation, and containment.
 *
 * Timers run on a {@link FakeScheduler} throughout, because every claim here is
 * about *which* timer exists and whether it was cancelled, not about how long
 * it takes. Sockets, by contrast, are real: the point of a scripted relay on a
 * real port is that Bun's `node:net` delivery is exercised rather than assumed.
 */

import { afterEach, beforeEach, describe, expect, test } from "bun:test";

import {
  backoffDelay,
  HANDSHAKE_TIMEOUT_MS,
  HEARTBEAT_INTERVAL_MS,
  RECONNECT_CAP_MS,
  RECONNECT_INITIAL_MS,
  RECONNECT_JITTER,
  RelayClient,
  REQUEST_TIMEOUT_MS,
  RequestFailed,
  type Report,
} from "../../src/client.ts";
import type { RelayConfig } from "../../src/config.ts";
import {
  encodePayload,
  MAX_BODY_BYTES,
  PROTOCOL_VERSION,
  type MessageFrame,
  type ServerFrame,
} from "../../src/protocol.ts";
import { FakeScheduler } from "../support/fake-scheduler.ts";
import { framePayload, ScriptedRelay, type Script } from "../support/scripted-relay.ts";
import { settlement } from "../support/settlement.ts";
import { Signal } from "../support/signal.ts";

const ROOM = { project: "omp-relayd", task: "implement-relay-client-library" };
const PEER = "macbook-reviewer";

function configFor(port: number, peer: string = PEER): RelayConfig {
  return { transport: { mode: "local", host: "127.0.0.1", port }, room: ROOM, peer };
}

interface Harness {
  readonly client: RelayClient;
  readonly scheduler: FakeScheduler;
  readonly ready: Signal;
  readonly disconnects: Signal<string>;
  readonly reports: Signal<Report>;
  readonly messages: Signal<MessageFrame>;
}

function harnessFor(config: RelayConfig): Harness {
  const scheduler = new FakeScheduler();
  const ready = new Signal();
  const disconnects = new Signal<string>();
  const reports = new Signal<Report>();
  const messages = new Signal<MessageFrame>();

  const client = new RelayClient({
    config,
    scheduler,
    handlers: {
      onReady: () => ready.fire(undefined),
      onDisconnect: (reason) => disconnects.fire(reason),
      onReport: (report) => reports.fire(report),
      onMessage: (message) => messages.fire(message),
    },
  });
  return { client, scheduler, ready, disconnects, reports, messages };
}

/** A script that completes the handshake and does nothing else. */
const ADMIT: Script = (frame, session) => {
  if (isFrame(frame, "hello")) {
    session.send({ type: "ready", protocol: PROTOCOL_VERSION });
  }
};

/**
 * A script that answers `send` with a `message` and that send's `receipt` in
 * one socket write, so both frames reach the client in a single chunk.
 */
const COALESCE: Script = (frame, session) => {
  if (isFrame(frame, "hello")) {
    session.send({ type: "ready", protocol: PROTOCOL_VERSION });
  }
  if (isFrame(frame, "send")) {
    const message = framePayload(
      encodePayload({ type: "message", id: "m1", from: "windows-main", body: "hi" }),
    );
    const receipt = framePayload(
      encodePayload({
        type: "receipt",
        id: String(frame["id"]),
        to: "windows-main",
        status: "routed",
      }),
    );
    const coalesced = new Uint8Array(message.length + receipt.length);
    coalesced.set(message);
    coalesced.set(receipt, message.length);
    session.sendRaw(coalesced);
  }
};

function isFrame(value: unknown, type: string): value is Record<string, unknown> {
  return (
    typeof value === "object" &&
    value !== null &&
    (value as { type?: unknown }).type === type
  );
}

/** A port with nothing listening on it: bound, its port noted, then released. */
async function closedPort(): Promise<number> {
  const relay = await ScriptedRelay.start(() => {});
  const port = relay.port;
  await relay.close();
  return port;
}

const escaped: unknown[] = [];
const onUnhandledRejection = (error: unknown): void => void escaped.push(error);
const onUncaughtException = (error: unknown): void => void escaped.push(error);

beforeEach(() => {
  escaped.length = 0;
  process.on("unhandledRejection", onUnhandledRejection);
  process.on("uncaughtException", onUncaughtException);
});

afterEach(() => {
  process.off("unhandledRejection", onUnhandledRejection);
  process.off("uncaughtException", onUncaughtException);
  expect(escaped).toEqual([]);
});

describe("handshake", () => {
  test("hello is the first frame and carries the configured room and peer", async () => {
    const relay = await ScriptedRelay.start(ADMIT);
    const { client, ready } = harnessFor(configFor(relay.port));

    client.start();
    await ready.until(1);

    expect(relay.received[0]).toEqual({
      type: "hello",
      protocol: PROTOCOL_VERSION,
      room: ROOM,
      peer: PEER,
    });
    expect(client.state).toBe("ready");

    await client.stop();
    await relay.close();
  });

  test("the connection is not usable before ready arrives", async () => {
    // The relay accepts the connection and says nothing. A client that treated
    // an open socket as a usable connection would report ready here.
    const relay = await ScriptedRelay.start(() => {});
    const { client, ready } = harnessFor(configFor(relay.port));

    client.start();
    await relay.awaitReceived(1);

    expect(client.state).toBe("connecting");
    expect(ready.count).toBe(0);

    await client.stop();
    await relay.close();
  });

  test("a request issued before readiness is not written to an unready socket", async () => {
    // The frame must wait, not be written early and not fail outright: the
    // contract permits either waiting or a stated failure, and waiting inside
    // the request's own deadline is strictly more useful to a caller.
    let admit: (() => void) | null = null;
    const relay = await ScriptedRelay.start((frame, session) => {
      if (isFrame(frame, "hello")) {
        admit = () => session.send({ type: "ready", protocol: PROTOCOL_VERSION });
      }
      if (isFrame(frame, "list")) {
        session.send({ type: "peers", request_id: "req-1", peers: [PEER] });
      }
    });
    const { client, ready } = harnessFor(configFor(relay.port));

    client.start();
    await relay.awaitReceived(1);

    const pending = client.list("req-1");
    // Still exactly one frame received: the `hello`. The `list` is held.
    expect(relay.received).toHaveLength(1);
    expect(client.pendingRequests).toBe(1);

    expect(admit).not.toBeNull();
    admit!();
    await ready.until(1);

    const peers = await pending;
    expect(peers.peers).toEqual([PEER]);
    expect(relay.received[1]).toEqual({ type: "list", request_id: "req-1" });

    await client.stop();
    await relay.close();
  });

  test("a peers frame arriving before ready does not settle the request it names", async () => {
    // The `list` was deliberately held back, so a reply naming it answers
    // nothing: before admission only the handshake's own frames mean anything.
    let admit: (() => void) | null = null;
    let impersonate: (() => void) | null = null;
    const relay = await ScriptedRelay.start((frame, session) => {
      if (isFrame(frame, "hello")) {
        admit = () => session.send({ type: "ready", protocol: PROTOCOL_VERSION });
        impersonate = () =>
          session.send({ type: "peers", request_id: "req-1", peers: ["ghost"] });
      }
      if (isFrame(frame, "list")) {
        session.send({
          type: "peers",
          request_id: String(frame["request_id"]),
          peers: [PEER],
        });
      }
    });
    const { client, ready, reports } = harnessFor(configFor(relay.port));

    client.start();
    await relay.awaitReceived(1);

    const pending = client.list("req-1");
    expect(impersonate).not.toBeNull();
    impersonate!();
    await reports.until(1);

    const ignored = reports.last?.message;
    expect(ignored).toContain("peers");
    expect(ignored).toContain("before ready");
    expect(client.pendingRequests).toBe(1);

    expect(admit).not.toBeNull();
    admit!();
    await ready.until(1);

    // It resolves after readiness, with the relay's real answer.
    const peers = await pending;
    expect(peers.peers).toEqual([PEER]);
    console.log(
      `pre-ready peers: "${ignored}"; the held list then resolved with ${peers.peers.join(", ")}`,
    );

    await client.stop();
    await relay.close();
  });

  test("a message frame arriving before ready does not reach the host", async () => {
    // Ordered on one connection, so by the time the admitted message arrives
    // the early one has definitively been handled: no sleep, nothing guessed.
    const relay = await ScriptedRelay.start((frame, session) => {
      if (isFrame(frame, "hello")) {
        session.send({ type: "message", id: "before", from: "windows-main", body: "early" });
        session.send({ type: "ready", protocol: PROTOCOL_VERSION });
        session.send({ type: "message", id: "after", from: "windows-main", body: "admitted" });
      }
    });
    const { client, messages, reports } = harnessFor(configFor(relay.port));

    client.start();
    await messages.until(1);

    expect(messages.observed.map((message) => message.id)).toEqual(["after"]);
    const notices = reports.observed.map((report) => report.message);
    expect(notices.some((message) => message.includes("message"))).toBe(true);
    console.log(
      `delivered: ${messages.observed.map((message) => message.id).join(", ")}; reported: ${notices.join(" | ")}`,
    );

    await client.stop();
    await relay.close();
  });

  test("a second ready fails the connection rather than re-running admission", async () => {
    const relay = await ScriptedRelay.start((frame, session) => {
      if (isFrame(frame, "hello")) {
        session.send({ type: "ready", protocol: PROTOCOL_VERSION });
        session.send({ type: "ready", protocol: PROTOCOL_VERSION });
      }
    });
    const { client, scheduler, ready, disconnects } = harnessFor(configFor(relay.port));

    client.start();
    await disconnects.until(1);

    expect(ready.count).toBe(1);
    expect(disconnects.last).toContain("second ready");
    expect(client.state).toBe("connecting");
    // Failed through the ordinary close path, so the backoff takes over.
    expect(scheduler.liveOf("timeout")).toHaveLength(1);
    console.log(
      `duplicate ready: onReady fired ${ready.count} time(s), disconnected with "${disconnects.last}"`,
    );

    await client.stop();
    await relay.close();
  });

  test("a peer that accepts the connection and never says ready is abandoned", async () => {
    // Such a peer sends no `error` and no `close`, so without a deadline of its
    // own the client stays `connecting` for the life of the process: nothing
    // else can ever schedule a reconnect.
    const relay = await ScriptedRelay.start(() => {});
    const { client, scheduler, disconnects } = harnessFor(configFor(relay.port));

    client.start();
    await relay.awaitReceived(1);

    const deadline = scheduler.liveOf("timeout", HANDSHAKE_TIMEOUT_MS);
    expect(deadline).toHaveLength(1);

    scheduler.fire(deadline[0]!);
    await disconnects.until(1);

    expect(disconnects.last).toContain(`no ready within ${HANDSHAKE_TIMEOUT_MS} ms`);
    expect(client.state).toBe("connecting");

    const retry = scheduler.liveOf("timeout");
    expect(retry).toHaveLength(1);
    expect(retry[0]!.delay).toBeLessThanOrEqual(
      RECONNECT_INITIAL_MS * (1 + RECONNECT_JITTER),
    );
    console.log(
      `silent peer abandoned: "${disconnects.last}"; retrying in ${retry[0]!.delay} ms`,
    );

    await client.stop();
    await relay.close();
  });

  test("a rejected handshake is reported with the relay's own code", async () => {
    // Not "the relay closed the connection": a client that reported an
    // anonymous disconnect would leave a version mismatch indistinguishable
    // from a network fault.
    const relay = await ScriptedRelay.start((frame, session) => {
      if (isFrame(frame, "hello")) {
        session.send({
          type: "error",
          code: "unsupported_protocol",
          message: "this relay speaks protocol 1",
        });
        session.drop();
      }
    });
    const { client, disconnects } = harnessFor(configFor(relay.port));

    client.start();
    await disconnects.until(1);

    expect(disconnects.last).toContain("unsupported_protocol");
    console.log(`disconnect reason observed: ${disconnects.last}`);

    await client.stop();
    await relay.close();
  });
});

describe("heartbeat", () => {
  test("an idle ready connection sends ping, and pong does not reach the caller", async () => {
    const relay = await ScriptedRelay.start((frame, session) => {
      if (isFrame(frame, "hello")) {
        session.send({ type: "ready", protocol: PROTOCOL_VERSION });
      }
      if (isFrame(frame, "ping")) {
        session.send({ type: "pong" });
      }
      if (isFrame(frame, "list")) {
        session.send({
          type: "peers",
          request_id: String(frame["request_id"]),
          peers: [PEER],
        });
      }
    });
    const { client, scheduler, ready, messages, reports } = harnessFor(
      configFor(relay.port),
    );

    client.start();
    await ready.until(1);

    const heartbeat = scheduler.liveOf("interval", HEARTBEAT_INTERVAL_MS);
    expect(heartbeat).toHaveLength(1);

    scheduler.fireNewest("interval", HEARTBEAT_INTERVAL_MS);
    await relay.awaitReceived(2);
    expect(relay.received[1]).toEqual({ type: "ping" });

    // Asserting that the pong produced *nothing* needs a barrier, because there
    // is no signal to await for an event that must not happen. A `list` issued
    // after the pong resolves only once its `peers` reply has been processed,
    // and TCP ordering on one connection puts the pong ahead of that reply --
    // so by this line the pong has definitively been handled.
    await client.list("after-pong");
    expect(messages.count).toBe(0);
    expect(reports.observed.map((report) => report.message)).toEqual([]);

    await client.stop();
    await relay.close();
  });

  test("an outbound frame defers the next ping to a full interval later", async () => {
    // A `send` 25 seconds into an idle interval must push the next `ping` to 30
    // seconds after that frame, not 5 seconds later. With the interval reset on
    // every outbound frame, the observable form of that is: the old interval is
    // cancelled and a fresh one of the same length replaces it.
    const relay = await ScriptedRelay.start((frame, session) => {
      if (isFrame(frame, "hello")) {
        session.send({ type: "ready", protocol: PROTOCOL_VERSION });
      }
      if (isFrame(frame, "send")) {
        session.send({
          type: "receipt",
          id: String(frame["id"]),
          to: "windows-main",
          status: "routed",
        });
      }
    });
    const { client, scheduler, ready } = harnessFor(configFor(relay.port));

    client.start();
    await ready.until(1);

    const before = scheduler.liveOf("interval", HEARTBEAT_INTERVAL_MS).at(-1);
    expect(before).toBeDefined();

    await client.send({ to: "windows-main", body: "review the diff", id: "msg-1" });

    const after = scheduler.liveOf("interval", HEARTBEAT_INTERVAL_MS).at(-1);
    expect(after).toBeDefined();
    expect(before!.cancelled).toBe(true);
    expect(after!.handle).not.toBe(before!.handle);
    expect(after!.delay).toBe(HEARTBEAT_INTERVAL_MS);
    console.log(
      `heartbeat rearmed: interval ${before!.handle} cancelled, ${after!.handle} armed at ${after!.delay} ms`,
    );

    await client.stop();
    await relay.close();
  });
});

describe("reconnect", () => {
  test("the delay grows, carries jitter, and never exceeds the cap", () => {
    // Asserted arithmetically with a supplied `random`, because this is the one
    // part of the policy that is a formula rather than a behavior.
    const low = (attempt: number): number => backoffDelay(attempt, () => 0);
    const high = (attempt: number): number => backoffDelay(attempt, () => 1);

    expect(low(1)).toBe(RECONNECT_INITIAL_MS * (1 - RECONNECT_JITTER));
    expect(high(1)).toBe(RECONNECT_INITIAL_MS * (1 + RECONNECT_JITTER));

    for (let attempt = 1; attempt < 12; attempt += 1) {
      expect(high(attempt)).toBeLessThanOrEqual(RECONNECT_CAP_MS);
      expect(low(attempt)).toBeGreaterThan(0);
      // Each attempt's whole range sits above the previous attempt's, which is
      // what "the delay increases between attempts" requires and what full
      // jitter over [0, base) would not give.
      if (high(attempt) < RECONNECT_CAP_MS) {
        expect(low(attempt + 1)).toBeGreaterThan(high(attempt));
      }
    }

    expect(high(20)).toBe(RECONNECT_CAP_MS);
    console.log(
      `backoff ranges: ${[1, 2, 3, 4, 20].map((n) => `${low(n)}-${high(n)}`).join(", ")} ms (cap ${RECONNECT_CAP_MS})`,
    );
  });

  test("a relay down at startup is reported once and retried, not thrown", async () => {
    const port = await closedPort();
    const { client, scheduler, reports, disconnects } = harnessFor(configFor(port));

    client.start();
    await disconnects.until(1);

    expect(reports.count).toBe(1);
    expect(reports.last?.level).toBe("warn");
    expect(client.state).toBe("connecting");

    const scheduled = scheduler.liveOf("timeout");
    expect(scheduled).toHaveLength(1);
    expect(scheduled[0]!.delay).toBeGreaterThanOrEqual(
      RECONNECT_INITIAL_MS * (1 - RECONNECT_JITTER),
    );
    expect(scheduled[0]!.delay).toBeLessThanOrEqual(
      RECONNECT_INITIAL_MS * (1 + RECONNECT_JITTER),
    );

    // Retried, and the outage is not re-reported on every attempt: a relay down
    // for an hour must not produce a log line every thirty seconds.
    scheduler.fireNewest("timeout");
    await disconnects.until(2);
    expect(reports.count).toBe(1);
    console.log(
      `after ${disconnects.count} failed attempts the outage was reported ${reports.count} time(s)`,
    );

    await client.stop();
  });

  test("the delay grows across consecutive failures and resets after ready", async () => {
    // Reset happens on `ready`, not on a successful TCP connect: a relay that
    // accepts and then rejects the handshake must not reset the sequence, or a
    // version mismatch becomes a tight reconnect loop.
    let admitting = false;
    const relay = await ScriptedRelay.start((frame, session) => {
      if (!isFrame(frame, "hello")) return;
      if (admitting) {
        session.send({ type: "ready", protocol: PROTOCOL_VERSION });
      } else {
        session.send({ type: "error", code: "invalid_hello" });
        session.drop();
      }
    });
    const { client, scheduler, ready, disconnects } = harnessFor(configFor(relay.port));

    client.start();
    await disconnects.until(1);
    scheduler.fireNewest("timeout");
    await disconnects.until(2);
    scheduler.fireNewest("timeout");
    await disconnects.until(3);

    // The handshake deadline is a timeout as well, so the reconnect delays are
    // the ones that are not it.
    const rising = scheduler
      .delaysOf("timeout")
      .filter((delay) => delay !== HANDSHAKE_TIMEOUT_MS);
    expect(rising).toHaveLength(3);
    expect(rising[1]!).toBeGreaterThan(rising[0]!);
    expect(rising[2]!).toBeGreaterThan(rising[1]!);

    admitting = true;
    scheduler.fireNewest("timeout");
    await ready.until(1);

    // Now a fresh outage starts from the initial delay again.
    admitting = false;
    await relay.close();
    await disconnects.until(4);

    const afterReset = scheduler
      .delaysOf("timeout")
      .filter((delay) => delay !== HANDSHAKE_TIMEOUT_MS)
      .at(-1);
    expect(afterReset).toBeLessThanOrEqual(
      RECONNECT_INITIAL_MS * (1 + RECONNECT_JITTER),
    );
    console.log(
      `delays before reset: ${rising.join(", ")} ms; after ready and a fresh outage: ${afterReset} ms`,
    );

    await client.stop();
  });

  test("shutdown cancels a pending reconnect and attempts nothing further", async () => {
    const port = await closedPort();
    const { client, scheduler, disconnects } = harnessFor(configFor(port));

    client.start();
    await disconnects.until(1);
    const pendingReconnect = scheduler.liveOf("timeout");
    expect(pendingReconnect).toHaveLength(1);

    await client.stop();

    expect(pendingReconnect[0]!.cancelled).toBe(true);
    expect(scheduler.live).toHaveLength(0);
    expect(client.state).toBe("stopped");
    // Firing a cancelled timer is refused by the fake scheduler, which is the
    // assertion: there is no live callback left to run.
    expect(() => scheduler.fire(pendingReconnect[0]!)).toThrow();
  });

  test("a reconnect attempt that fails to connect schedules the next one", async () => {
    const port = await closedPort();
    const { client, scheduler, disconnects } = harnessFor(configFor(port));

    client.start();
    await disconnects.until(1);
    scheduler.fireNewest("timeout");
    await disconnects.until(2);

    expect(scheduler.liveOf("timeout")).toHaveLength(1);
    await client.stop();
  });
});

describe("request correlation", () => {
  test("receipts arriving out of order resolve their own requests", async () => {
    const relay = await ScriptedRelay.start((frame, session) => {
      if (isFrame(frame, "hello")) {
        session.send({ type: "ready", protocol: PROTOCOL_VERSION });
      }
      if (isFrame(frame, "send") && frame["id"] === "msg-2") {
        // Both answers, second request first.
        session.send({ type: "receipt", id: "msg-2", to: "b", status: "peer_offline" });
        session.send({ type: "receipt", id: "msg-1", to: "a", status: "routed" });
      }
    });
    const { client, ready } = harnessFor(configFor(relay.port));

    client.start();
    await ready.until(1);

    const first = client.send({ to: "a", body: "one", id: "msg-1" });
    const second = client.send({ to: "b", body: "two", id: "msg-2" });
    const [one, two] = await Promise.all([first, second]);

    expect(one.status).toBe("routed");
    expect(two.status).toBe("peer_offline");
    expect(client.pendingRequests).toBe(0);

    await client.stop();
    await relay.close();
  });

  test("connection loss settles every pending request with a stated reason", async () => {
    const relay = await ScriptedRelay.start((frame, session) => {
      if (isFrame(frame, "hello")) {
        session.send({ type: "ready", protocol: PROTOCOL_VERSION });
      }
      if (isFrame(frame, "send")) {
        session.drop();
      }
    });
    const { client, ready } = harnessFor(configFor(relay.port));

    client.start();
    await ready.until(1);

    const first = client.send({ to: "a", body: "one", id: "msg-1" });
    const roster = client.list("req-1");

    const failures = await Promise.allSettled([first, roster]);
    for (const outcome of failures) {
      expect(outcome.status).toBe("rejected");
      if (outcome.status !== "rejected") continue;
      expect(outcome.reason).toBeInstanceOf(RequestFailed);
      expect((outcome.reason as RequestFailed).reason).toBe("disconnected");
      expect((outcome.reason as RequestFailed).message.length).toBeGreaterThan(0);
    }
    expect(client.pendingRequests).toBe(0);

    await client.stop();
    await relay.close();
  });

  test("an unanswered request times out after the named constant", async () => {
    const relay = await ScriptedRelay.start(ADMIT);
    const { client, scheduler, ready } = harnessFor(configFor(relay.port));

    client.start();
    await ready.until(1);

    const pending = client.list("req-1");
    const timer = scheduler.liveOf("timeout", REQUEST_TIMEOUT_MS);
    expect(timer).toHaveLength(1);

    scheduler.fire(timer[0]!);
    await expect(pending).rejects.toThrow(`no reply within ${REQUEST_TIMEOUT_MS} ms`);
    expect(client.pendingRequests).toBe(0);

    await client.stop();
    await relay.close();
  });

  test("shutdown settles an outstanding request with a stated reason", async () => {
    // Distinct from the connection-loss case: shutdown is a host action, not a
    // transport event, and it is the one settlement path whose reason a caller
    // can act on by not retrying. Previously this was only asserted incidentally
    // by the duplicate-token test, which names something else.
    const relay = await ScriptedRelay.start(ADMIT);
    const { client, ready } = harnessFor(configFor(relay.port));

    client.start();
    await ready.until(1);

    const pending = client.list("req-1");
    expect(client.pendingRequests).toBe(1);

    const settles = settlement(pending);
    await client.stop();

    const outcome = await settles;
    expect(outcome.status).toBe("rejected");
    if (outcome.status !== "rejected") return;
    expect(outcome.reason).toBeInstanceOf(RequestFailed);
    expect((outcome.reason as RequestFailed).reason).toBe("stopped");
    expect(client.pendingRequests).toBe(0);

    await relay.close();
  });

  test("a reply matching no pending request is discarded without growing the map", async () => {
    const relay = await ScriptedRelay.start((frame, session) => {
      if (isFrame(frame, "hello")) {
        session.send({ type: "ready", protocol: PROTOCOL_VERSION });
        // A late receipt for a request that timed out is expected and harmless.
        session.send({ type: "receipt", id: "long-gone", to: "a", status: "routed" });
      }
    });
    const { client, ready, reports, disconnects } = harnessFor(configFor(relay.port));

    client.start();
    await ready.until(1);
    // Awaiting the discard notice itself, which is the observable. The previous
    // spelling awaited one microtask and hoped the receipt's chunk had already
    // been read -- it had not, roughly one run in five.
    await reports.until(1);

    expect(reports.last?.message).toContain("long-gone");
    expect(client.pendingRequests).toBe(0);
    expect(client.state).toBe("ready");
    expect(disconnects.count).toBe(0);

    await client.stop();
    await relay.close();
  });

  test("an error naming a request settles that request instead of waiting out its timeout", async () => {
    // `wire-protocol` obliges the relay to echo the correlation token of a
    // recoverable rejection. Honouring the echo is what turns a five-second
    // wait into an immediate answer.
    const relay = await ScriptedRelay.start((frame, session) => {
      if (isFrame(frame, "hello")) {
        session.send({ type: "ready", protocol: PROTOCOL_VERSION });
      }
      if (isFrame(frame, "send")) {
        session.send({
          type: "error",
          code: "invalid_identifier",
          message: "reply_to must be at most 128 UTF-8 bytes",
          request_id: String(frame["id"]),
        });
      }
    });
    const { client, ready, disconnects } = harnessFor(configFor(relay.port));

    client.start();
    await ready.until(1);

    const attempt = client.send({ to: "a", body: "one", id: "msg-1" });
    await expect(attempt).rejects.toBeInstanceOf(RequestFailed);
    await attempt.catch((error: RequestFailed) => {
      expect(error.reason).toBe("relay_error");
      expect(error.code).toBe("invalid_identifier");
    });

    // Recoverable: the connection stays open.
    expect(client.state).toBe("ready");
    expect(disconnects.count).toBe(0);

    await client.stop();
    await relay.close();
  });

  test("a duplicate correlation token is refused rather than displacing the first request", async () => {
    // Two entries under one key would leave the first request unsettled and its
    // timer answering the second.
    const relay = await ScriptedRelay.start(ADMIT);
    const { client, ready } = harnessFor(configFor(relay.port));

    client.start();
    await ready.until(1);

    const first = client.list("req-1");
    const duplicate = client.list("req-1");

    await expect(duplicate).rejects.toThrow("already outstanding");
    expect(client.pendingRequests).toBe(1);

    // Captured, not asserted, before the shutdown that rejects it: see
    // `settlement` for why neither `await expect(...).rejects` before nor after
    // the trigger works here.
    const firstSettles = settlement(first);
    await client.stop();
    const outcome = await firstSettles;
    expect(outcome.status).toBe("rejected");
    if (outcome.status !== "rejected") return;
    expect(outcome.reason).toBeInstanceOf(RequestFailed);
    await relay.close();
  });

  test("a correlation token that timed out is refused rather than reused", async () => {
    // The first request's reply can still arrive. Settling this one with it
    // would hand the caller a receipt describing a different message.
    const relay = await ScriptedRelay.start(ADMIT);
    const { client, scheduler, ready } = harnessFor(configFor(relay.port));

    client.start();
    await ready.until(1);

    const first = client.send({ to: "a", body: "one", id: "msg-1" });
    scheduler.fireNewest("timeout", REQUEST_TIMEOUT_MS);
    await expect(first).rejects.toThrow(`no reply within ${REQUEST_TIMEOUT_MS} ms`);

    const reused = client.send({ to: "a", body: "one again", id: "msg-1" });
    await expect(reused).rejects.toThrow("timed out on this connection");

    // A fresh token is still accepted: the refusal is about the reused one.
    const fresh = settlement(client.send({ to: "a", body: "two", id: "msg-2" }));
    await relay.awaitReceived(3);

    expect(relay.received).toHaveLength(3); // hello, msg-1, msg-2: not the reuse
    expect(relay.received[2]).toEqual({ type: "send", id: "msg-2", to: "a", body: "two" });
    expect(client.pendingRequests).toBe(1);
    console.log(
      `after msg-1 timed out, reuse was refused and msg-2 was written; frames received: ${relay.received.length}`,
    );

    await client.stop();
    expect((await fresh).status).toBe("rejected");
    await relay.close();
  });

  test("a request that timed out before it was ever written may be reissued", async () => {
    // Nothing reached the socket, so no late reply can exist to settle the
    // reissue: refusing the token would deny a caller a retry that is safe.
    let admit: (() => void) | null = null;
    const relay = await ScriptedRelay.start((frame, session) => {
      if (isFrame(frame, "hello")) {
        admit = () => session.send({ type: "ready", protocol: PROTOCOL_VERSION });
      }
      if (isFrame(frame, "list")) {
        session.send({
          type: "peers",
          request_id: String(frame["request_id"]),
          peers: [PEER],
        });
      }
    });
    const { client, scheduler, ready } = harnessFor(configFor(relay.port));

    client.start();
    await relay.awaitReceived(1);

    // Issued while connecting, so it is held rather than written.
    const early = client.list("req-1");
    expect(relay.received).toHaveLength(1);
    scheduler.fireNewest("timeout", REQUEST_TIMEOUT_MS);
    await expect(early).rejects.toThrow(`no reply within ${REQUEST_TIMEOUT_MS} ms`);

    // Readiness still arrives well inside the handshake deadline.
    expect(admit).not.toBeNull();
    admit!();
    await ready.until(1);

    const retried = await client.list("req-1");
    expect(retried.peers).toEqual([PEER]);
    console.log(
      `an unwritten req-1 timed out; the same token was accepted again and resolved with ${retried.peers.join(", ")}`,
    );

    await client.stop();
    await relay.close();
  });

  test("an over-budget body is refused locally, before anything is written", async () => {
    // The relay answers an over-budget body by closing the connection, so
    // sending it would cost an outage rather than a rejected promise.
    const relay = await ScriptedRelay.start(ADMIT);
    const { client, ready } = harnessFor(configFor(relay.port));

    client.start();
    await ready.until(1);

    const attempt = client.send({ to: "a", body: "b".repeat(MAX_BODY_BYTES + 1) });
    await expect(attempt).rejects.toThrow(`over the ${MAX_BODY_BYTES}-byte budget`);

    expect(relay.received).toHaveLength(1); // the hello, and nothing else
    expect(client.state).toBe("ready");

    await client.stop();
    await relay.close();
  });
});

describe("failures stay inside the client", () => {
  test("a malformed frame fails the connection and schedules a reconnect", async () => {
    const relay = await ScriptedRelay.start((frame, session) => {
      if (isFrame(frame, "hello")) {
        // A positional encoding: valid MessagePack, forbidden by the contract.
        session.sendRaw(framePayload(new Uint8Array([0x92, 0xa4, 0x70, 0x6f, 0x6e, 0x67, 0x01])));
      }
    });
    const { client, scheduler, disconnects } = harnessFor(configFor(relay.port));

    client.start();
    await disconnects.until(1);

    expect(disconnects.last).toContain("malformed frame");
    expect(scheduler.liveOf("timeout")).toHaveLength(1);

    await client.stop();
    await relay.close();
  });

  test("bytes that do not decode fail the connection without escaping", async () => {
    const relay = await ScriptedRelay.start((frame, session) => {
      if (isFrame(frame, "hello")) {
        session.sendRaw(framePayload(new Uint8Array([0xc1, 0xc1, 0xc1])));
      }
    });
    const { client, disconnects } = harnessFor(configFor(relay.port));

    client.start();
    await disconnects.until(1);

    expect(disconnects.last).toContain("undecodable");

    await client.stop();
    await relay.close();
  });

  test("a zero-length declaration fails the connection", async () => {
    const relay = await ScriptedRelay.start((frame, session) => {
      if (isFrame(frame, "hello")) {
        session.sendRaw(new Uint8Array([0, 0, 0, 0]));
      }
    });
    const { client, disconnects } = harnessFor(configFor(relay.port));

    client.start();
    await disconnects.until(1);

    expect(disconnects.last).toContain("zero_length");

    await client.stop();
    await relay.close();
  });

  test("an unrecognized frame type is ignored and the connection stays open", async () => {
    const relay = await ScriptedRelay.start((frame, session) => {
      if (isFrame(frame, "hello")) {
        session.send({ type: "ready", protocol: PROTOCOL_VERSION });
        // `broadcast` is not in the v1 inventory. A relay one version ahead
        // could send it, and dropping the connection over it would turn an
        // additive change into an outage.
        session.sendRaw(framePayload(new Uint8Array([0x81, 0xa4, 0x74, 0x79, 0x70, 0x65, 0xa9, 0x62, 0x72, 0x6f, 0x61, 0x64, 0x63, 0x61, 0x73, 0x74])));
        session.send({ type: "pong" });
      }
    });
    const { client, ready, disconnects, reports } = harnessFor(configFor(relay.port));

    client.start();
    await ready.until(1);
    await reports.until(1);

    expect(reports.last?.message).toContain("broadcast");
    expect(client.state).toBe("ready");
    expect(disconnects.count).toBe(0);

    await client.stop();
    await relay.close();
  });

  test("a handler that throws is contained rather than allowed to escape", async () => {
    const relay = await ScriptedRelay.start((frame, session) => {
      if (isFrame(frame, "hello")) {
        session.send({ type: "ready", protocol: PROTOCOL_VERSION });
        session.send({ type: "message", id: "m1", from: "windows-main", body: "hi" });
      }
    });
    const reports = new Signal<Report>();
    const client = new RelayClient({
      config: configFor(relay.port),
      scheduler: new FakeScheduler(),
      handlers: {
        onMessage: () => {
          throw new Error("the host's message handler is broken");
        },
        onReport: (report) => reports.fire(report),
      },
    });

    client.start();
    await reports.until(1);

    expect(reports.last?.level).toBe("error");
    expect(reports.last?.message).toContain("the host's message handler is broken");
    expect(client.state).toBe("ready");

    await client.stop();
    await relay.close();
  });

  test("a handler that returns a rejected promise is reported, not left unhandled", async () => {
    // The handler types return `void`, and TypeScript accepts an `async`
    // function there, so this is a shape a host can write by accident. The
    // synchronous guard around the socket event cannot see the rejection.
    const relay = await ScriptedRelay.start((frame, session) => {
      if (isFrame(frame, "hello")) {
        session.send({ type: "ready", protocol: PROTOCOL_VERSION });
        session.send({ type: "message", id: "m1", from: "windows-main", body: "hi" });
      }
    });
    const reports = new Signal<Report>();
    const client = new RelayClient({
      config: configFor(relay.port),
      scheduler: new FakeScheduler(),
      handlers: {
        onMessage: async () => {
          await Promise.resolve();
          throw new Error("the host's async message handler rejected");
        },
        onReport: (report) => reports.fire(report),
      },
    });

    client.start();
    await reports.until(1);

    expect(reports.last?.level).toBe("error");
    expect(reports.last?.message).toContain("the host's async message handler rejected");
    expect(client.state).toBe("ready");
    // `escaped` is asserted empty in `afterEach`: that is the no-unhandled half.
    console.log(`async rejection contained as: ${reports.last?.message}`);

    await client.stop();
    await relay.close();
  });

  test("a throwing message handler does not cost a coalesced receipt its request", async () => {
    // One socket write carrying a `message` and the pending `send`'s `receipt`.
    // A throw that unwound the receive loop would discard the receipt: the
    // accumulator has already consumed those bytes, so nothing re-emits them
    // and the request would settle five seconds later as a timeout instead.
    const relay = await ScriptedRelay.start(COALESCE);
    const scheduler = new FakeScheduler();
    const ready = new Signal();
    const reports = new Signal<Report>();
    const client = new RelayClient({
      config: configFor(relay.port),
      scheduler,
      handlers: {
        onReady: () => ready.fire(undefined),
        onMessage: () => {
          throw new Error("the host's message handler is broken");
        },
        onReport: (report) => reports.fire(report),
      },
    });

    client.start();
    await ready.until(1);

    const receipt = await client.send({ to: "windows-main", body: "one", id: "msg-1" });

    expect(receipt.id).toBe("msg-1");
    expect(receipt.status).toBe("routed");
    // Settled by its receipt, not by its deadline: no request timeout is left
    // live, and this scheduler's clock never advanced.
    expect(scheduler.liveOf("timeout", REQUEST_TIMEOUT_MS)).toHaveLength(0);
    expect(reports.last?.message).toContain("the host's message handler is broken");
    console.log(
      `coalesced chunk: the handler threw and ${receipt.id} still settled as ${receipt.status}`,
    );

    await client.stop();
    await relay.close();
  });

  test("a handler returning a hostile thenable is contained and costs no frame", async () => {
    // `then` is host code too. Detecting the thenable outside the guard would
    // let a throwing getter unwind the receive loop from just outside the
    // callback, losing the receipt coalesced behind the message -- the same
    // failure as a throwing handler, through a different door.
    const relay = await ScriptedRelay.start(COALESCE);
    const scheduler = new FakeScheduler();
    const ready = new Signal();
    const reports = new Signal<Report>();
    const client = new RelayClient({
      config: configFor(relay.port),
      scheduler,
      handlers: {
        onReady: () => ready.fire(undefined),
        onMessage: () => ({
          get then(): never {
            throw new Error("the host's thenable is broken");
          },
        }),
        onReport: (report) => reports.fire(report),
      },
    });

    client.start();
    await ready.until(1);

    const receipt = await client.send({ to: "windows-main", body: "one", id: "msg-1" });

    expect(receipt.id).toBe("msg-1");
    expect(receipt.status).toBe("routed");
    expect(scheduler.liveOf("timeout", REQUEST_TIMEOUT_MS)).toHaveLength(0);
    expect(reports.last?.message).toContain("the host's thenable is broken");
    console.log(
      `hostile thenable contained as "${reports.last?.message}"; ${receipt.id} still settled as ${receipt.status}`,
    );

    await client.stop();
    await relay.close();
  });

  test("a handler throwing an undescribable value is contained and costs no frame", async () => {
    // Containment has to survive describing what it caught. Two values are
    // thrown here rather than one, because they fail at different steps and an
    // earlier version of the guard caught only the first: a bare
    // null-prototype object raises inside the conversion, while an `Error`
    // whose `message` is one converts without complaint and then raises in the
    // caller's own template literal -- outside the guard, unwinding the receive
    // loop and losing the receipt coalesced behind the message.
    const thrown: unknown[] = [
      Object.create(null),
      Object.assign(new Error("x"), { message: Object.create(null) as unknown as string }),
    ];
    for (const [index, value] of thrown.entries()) {
      const relay = await ScriptedRelay.start(COALESCE);
      const scheduler = new FakeScheduler();
      const ready = new Signal();
      const reports = new Signal<Report>();
      const client = new RelayClient({
        config: configFor(relay.port),
        scheduler,
        handlers: {
          onReady: () => ready.fire(undefined),
          onMessage: () => {
            throw value;
          },
          onReport: (report) => reports.fire(report),
        },
      });

      client.start();
      await ready.until(1);

      const receipt = await client.send({ to: "windows-main", body: "one", id: `msg-${index}` });

      expect(receipt.id).toBe(`msg-${index}`);
      expect(receipt.status).toBe("routed");
      expect(scheduler.liveOf("timeout", REQUEST_TIMEOUT_MS)).toHaveLength(0);
      expect(reports.last?.message).toContain("could not be described");
      console.log(
        `undescribable throw ${index} contained as "${reports.last?.message}"; ${receipt.id} still settled as ${receipt.status}`,
      );

      await client.stop();
      await relay.close();
    }
  });

  test("a throwing onReport getter is contained and costs no frame", async () => {
    // Every other containment site funnels its diagnostic through `#report`,
    // so reading `onReport` is host code on the recovery path. Read outside the
    // guard, a throwing getter escaped the catch that was handling the first
    // failure and took the coalesced receipt with it.
    const relay = await ScriptedRelay.start(COALESCE);
    const scheduler = new FakeScheduler();
    const ready = new Signal();
    let lookups = 0;
    const client = new RelayClient({
      config: configFor(relay.port),
      scheduler,
      handlers: {
        onReady: () => ready.fire(undefined),
        onMessage: () => {
          throw new Error("the host's message handler threw");
        },
        get onReport(): never {
          lookups += 1;
          throw new Error("the host's reporter getter is broken");
        },
      },
    });

    client.start();
    await ready.until(1);

    const receipt = await client.send({ to: "windows-main", body: "one", id: "msg-1" });

    expect(receipt.id).toBe("msg-1");
    expect(receipt.status).toBe("routed");
    expect(lookups).toBeGreaterThan(0);
    console.log(
      `throwing onReport getter read ${lookups} time(s) and contained; ${receipt.id} still settled as ${receipt.status}`,
    );

    await client.stop();
    await relay.close();
  });

  test("a handler promise rejecting after a restart reports nothing", async () => {
    // The stale rejection belongs to the run that `stop()` ended. `#stopped`
    // alone cannot see that: `start()` sets it back to false, so the rejection
    // would be reported against the run that replaced it.
    //
    // The message goes out on the first admission only. Sending it on both
    // would have run B attach the same held promise, making its rejection a
    // legitimate report and the assertion below meaningless.
    let admissions = 0;
    const relay = await ScriptedRelay.start((frame, session) => {
      if (isFrame(frame, "hello")) {
        admissions += 1;
        session.send({ type: "ready", protocol: PROTOCOL_VERSION });
        if (admissions === 1) {
          session.send({ type: "message", id: "m1", from: "windows-main", body: "hi" });
        }
      }
    });
    const held = Promise.withResolvers<void>();
    const delivered = new Signal();
    const ready = new Signal();
    const reports = new Signal<Report>();
    const client = new RelayClient({
      config: configFor(relay.port),
      scheduler: new FakeScheduler(),
      handlers: {
        onReady: () => ready.fire(undefined),
        onMessage: () => {
          delivered.fire(undefined);
          return held.promise;
        },
        onReport: (report) => reports.fire(report),
      },
    });

    client.start();
    await ready.until(1);
    await delivered.until(1);
    await client.stop();
    client.start();
    // Run B's own readiness, not run A's: waiting for one would return at once
    // and leave the rejection below racing the restart rather than following it.
    await ready.until(2);

    const before = reports.count;
    held.reject(new Error("the host's handler rejected across a restart"));
    await held.promise.catch(() => {});

    expect(admissions).toBe(2);
    expect(client.state).toBe("ready");
    expect(reports.count).toBe(before);
    console.log(
      `across a restart (${admissions} admissions) a late rejection produced ${reports.count - before} further report(s)`,
    );

    await client.stop();
    await relay.close();
  });

  test("a handler promise that rejects after shutdown reports nothing", async () => {
    // `stop()` tells its caller that no callback fires afterwards. A promise
    // the host is still holding is the one path that can outlive it.
    const relay = await ScriptedRelay.start((frame, session) => {
      if (isFrame(frame, "hello")) {
        session.send({ type: "ready", protocol: PROTOCOL_VERSION });
        session.send({ type: "message", id: "m1", from: "windows-main", body: "hi" });
      }
    });
    const held = Promise.withResolvers<void>();
    const delivered = new Signal();
    const reports = new Signal<Report>();
    const client = new RelayClient({
      config: configFor(relay.port),
      scheduler: new FakeScheduler(),
      handlers: {
        onMessage: () => {
          delivered.fire(undefined);
          return held.promise;
        },
        onReport: (report) => reports.fire(report),
      },
    });

    client.start();
    await delivered.until(1);
    await client.stop();

    const before = reports.count;
    held.reject(new Error("the host's handler rejected after shutdown"));
    // The client attached its rejection handler first, so awaiting the same
    // promise here resumes after it: a barrier rather than a guessed tick.
    await held.promise.catch(() => {});

    expect(reports.count).toBe(before);
    console.log(
      `after stop() a late rejection produced ${reports.count - before} further report(s)`,
    );

    await relay.close();
  });

  test("a throwing disconnect handler does not stop the reconnect", async () => {
    // The throw used to be swallowed by the outer guard, which sits above the
    // only call that arms a reconnect: the client was left with no socket, no
    // timer, and no event that could ever arrive.
    const relay = await ScriptedRelay.start((frame, session) => {
      if (isFrame(frame, "hello")) {
        session.send({ type: "ready", protocol: PROTOCOL_VERSION });
        session.drop();
      }
    });
    const scheduler = new FakeScheduler();
    const disconnects = new Signal<string>();
    const reports = new Signal<Report>();
    const client = new RelayClient({
      config: configFor(relay.port),
      scheduler,
      handlers: {
        onDisconnect: (reason) => {
          disconnects.fire(reason);
          throw new Error("the host's disconnect handler is broken");
        },
        onReport: (report) => reports.fire(report),
      },
    });

    client.start();
    await disconnects.until(1);

    const retry = scheduler.liveOf("timeout");
    expect(retry).toHaveLength(1);
    expect(client.state).toBe("connecting");
    expect(
      reports.observed.some((report) =>
        report.message.includes("the host's disconnect handler is broken"),
      ),
    ).toBe(true);
    console.log(
      `the disconnect handler threw; a reconnect is still armed for ${retry[0]!.delay} ms`,
    );

    await client.stop();
    await relay.close();
  });
});

describe("the host owns every timer", () => {
  test("a client given fake timers never schedules on the ambient ones", async () => {
    // Ambient timers are wrapped rather than removed, because `node:net` itself
    // uses them and removing them would break the socket under test. What is
    // asserted is that no ambient call carries one of the client's own delays,
    // and that all three of those delays reached the supplied scheduler.
    const ambientDelays: number[] = [];
    const realSetTimeout = globalThis.setTimeout;
    const realSetInterval = globalThis.setInterval;

    const relay = await ScriptedRelay.start(ADMIT);
    const clientDelays = new Set([
      REQUEST_TIMEOUT_MS,
      HEARTBEAT_INTERVAL_MS,
      HANDSHAKE_TIMEOUT_MS,
    ]);

    // @ts-expect-error deliberately replacing an ambient global for the duration
    globalThis.setTimeout = (callback: () => void, delay?: number) => {
      ambientDelays.push(delay ?? 0);
      return realSetTimeout(callback, delay);
    };
    // @ts-expect-error deliberately replacing an ambient global for the duration
    globalThis.setInterval = (callback: () => void, delay?: number) => {
      ambientDelays.push(delay ?? 0);
      return realSetInterval(callback, delay);
    };

    try {
      const { client, scheduler, ready } = harnessFor(configFor(relay.port));
      client.start();
      await ready.until(1);
      const pending = client.list("req-1");

      expect(scheduler.delaysOf("interval")).toContain(HEARTBEAT_INTERVAL_MS);
      expect(scheduler.delaysOf("timeout")).toContain(REQUEST_TIMEOUT_MS);
      expect(scheduler.delaysOf("timeout")).toContain(HANDSHAKE_TIMEOUT_MS);

      const leaked = ambientDelays.filter((delay) => clientDelays.has(delay));
      expect(leaked).toEqual([]);
      console.log(
        `ambient timer calls during the run: ${ambientDelays.length === 0 ? "none at all" : `${ambientDelays.join(", ")} ms`}; none of them the client's ${[...clientDelays].join("/")} ms`,
      );

      scheduler.fireNewest("timeout", REQUEST_TIMEOUT_MS);
      await expect(pending).rejects.toBeInstanceOf(RequestFailed);
      await client.stop();
    } finally {
      globalThis.setTimeout = realSetTimeout;
      globalThis.setInterval = realSetInterval;
      await relay.close();
    }
  });

  test("shutdown cancels an outstanding heartbeat, reconnect delay, and request timeout", async () => {
    // All three at once, because cancelling two of the three is the plausible
    // bug and a per-timer test would not catch it. Leaking a timer past
    // shutdown in a long-lived host is a slow leak plus a callback firing
    // against torn-down state.
    const relay = await ScriptedRelay.start(ADMIT);
    const { client, scheduler, ready, disconnects } = harnessFor(configFor(relay.port));

    client.start();
    await ready.until(1);
    const request = client.list("req-1");

    expect(scheduler.liveOf("interval", HEARTBEAT_INTERVAL_MS)).toHaveLength(1);
    expect(scheduler.liveOf("timeout", REQUEST_TIMEOUT_MS)).toHaveLength(1);

    // Captured before the disconnect that rejects it.
    const requestSettles = settlement(request);

    // Force a reconnect delay to be outstanding too.
    await relay.close();
    await disconnects.until(1);
    expect((await requestSettles).status).toBe("rejected");

    const reconnect = scheduler.liveOf("timeout");
    expect(reconnect).toHaveLength(1);

    const before = scheduler.history.length;
    await client.stop();

    expect(scheduler.live).toHaveLength(0);
    expect(scheduler.history).toHaveLength(before);
    console.log(
      `shutdown cancelled all ${before} timer(s) created; ${scheduler.live.length} remain live`,
    );
  });

  test("shutdown cancels a handshake deadline that never came due", async () => {
    // The one timer that exists only while connecting, which the test above --
    // running from a ready connection -- cannot cover.
    const relay = await ScriptedRelay.start(() => {});
    const { client, scheduler } = harnessFor(configFor(relay.port));

    client.start();
    await relay.awaitReceived(1);

    const deadline = scheduler.liveOf("timeout", HANDSHAKE_TIMEOUT_MS);
    expect(deadline).toHaveLength(1);

    await client.stop();

    expect(deadline[0]!.cancelled).toBe(true);
    expect(scheduler.live).toHaveLength(0);
    // Firing a cancelled timer is refused, which is the assertion: no live
    // callback is left to fail a connection that is already gone.
    expect(() => scheduler.fire(deadline[0]!)).toThrow();
    console.log(
      `handshake deadline of ${deadline[0]!.delay} ms cancelled by shutdown; ${scheduler.live.length} timer(s) remain live`,
    );

    await relay.close();
  });

  test("without a supplied scheduler the client uses the ambient timers and behaves the same", async () => {
    const relay = await ScriptedRelay.start(ADMIT);
    const ready = new Signal();
    const client = new RelayClient({
      config: configFor(relay.port),
      handlers: { onReady: () => ready.fire(undefined) },
    });

    client.start();
    await ready.until(1);
    expect(client.state).toBe("ready");

    await client.stop();
    await relay.close();
  });
});
