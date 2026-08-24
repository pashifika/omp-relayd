/**
 * The client's attachment surface: the reservation, the transfer, and the rules
 * that keep a remote peer from steering either one.
 *
 * Timers run on a {@link FakeScheduler}, because every claim about a bound here
 * is about *which* timer exists rather than about how long it takes. Sockets are
 * real: the scripted relay answers both protocols on one port, exactly as the
 * relay does, so a test that reached the transfer routes on a second port would
 * be exercising a topology that does not exist.
 */

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { encode as rawEncode } from "@msgpack/msgpack";

import {
  RelayClient,
  RequestFailed,
  TRANSFER_CHUNK_BYTES,
  TRANSFER_STALL_MS,
  type Report,
} from "../../src/client.ts";
import type { RelayConfig } from "../../src/config.ts";
import { digestOf, PROTOCOL_VERSION, type DeliveryFrame } from "../../src/protocol.ts";
import { FakeScheduler } from "../support/fake-scheduler.ts";
import {
  frameField,
  framePayload,
  isFrame,
  ScriptedRelay,
  type BodyWriter,
  type Script,
} from "../support/scripted-relay.ts";
import { settlement } from "../support/settlement.ts";
import { Signal } from "../support/signal.ts";

const ROOM = { project: "omp-relayd", task: "extend-long-payloads" };
const PEER = "macbook-reviewer";

/** The route the client must build for `digest`, from its own configuration. */
const route = (digest: string): string =>
  `/blob/omp-relayd/extend-long-payloads/${digest}`;

function configFor(port: number): RelayConfig {
  return { transport: { mode: "local", host: "127.0.0.1", port }, room: ROOM, peer: PEER };
}

interface Harness {
  readonly client: RelayClient;
  readonly scheduler: FakeScheduler;
  readonly ready: Signal;
  readonly deliveries: Signal<DeliveryFrame>;
  readonly reports: Signal<Report>;
}

function harnessFor(config: RelayConfig): Harness {
  const scheduler = new FakeScheduler();
  const ready = new Signal();
  const deliveries = new Signal<DeliveryFrame>();
  const reports = new Signal<Report>();
  const client = new RelayClient({
    config,
    scheduler,
    handlers: {
      onReady: () => ready.fire(undefined),
      onDelivery: (delivery) => deliveries.fire(delivery),
      onReport: (report) => reports.fire(report),
    },
  });
  return { client, scheduler, ready, deliveries, reports };
}

/** Admits, then grants every reservation with a two-hour lifetime. */
const GRANT: Script = (frame, session) => {
  if (isFrame(frame, "hello")) {
    session.send({ type: "ready", protocol: PROTOCOL_VERSION });
  }
  if (isFrame(frame, "reserve")) {
    session.send({
      type: "reserved",
      request_id: String(frameField(frame, "request_id")),
      status: "granted",
      expires_in: 7200,
    });
  }
  if (isFrame(frame, "send")) {
    session.send({
      type: "receipt",
      id: String(frameField(frame, "id")),
      to: String(frameField(frame, "to")),
      status: "routed",
    });
  }
};

/** Admits, then refuses every reservation with `status`. */
const refuseWith =
  (status: string): Script =>
  (frame, session) => {
    if (isFrame(frame, "hello")) {
      session.send({ type: "ready", protocol: PROTOCOL_VERSION });
    }
    if (isFrame(frame, "reserve")) {
      session.send({
        type: "reserved",
        request_id: String(frameField(frame, "request_id")),
        status,
      });
    }
  };

/** Admits, then answers every reservation with `code`. */
const errorWith =
  (code: string, message: string): Script =>
  (frame, session) => {
    if (isFrame(frame, "hello")) {
      session.send({ type: "ready", protocol: PROTOCOL_VERSION });
    }
    if (isFrame(frame, "reserve")) {
      session.send({
        type: "error",
        code,
        message,
        request_id: String(frameField(frame, "request_id")),
      } as never);
    }
  };

const escaped: unknown[] = [];
const onUnhandledRejection = (error: unknown): void => void escaped.push(error);

beforeEach(() => {
  escaped.length = 0;
  process.on("unhandledRejection", onUnhandledRejection);
});

afterEach(() => {
  process.off("unhandledRejection", onUnhandledRejection);
  expect(escaped).toEqual([]);
});

/** A started relay and a joined client, torn down by the caller. */
interface Joined {
  readonly relay: ScriptedRelay;
  readonly harness: Harness;
  readonly close: () => Promise<void>;
}

async function joined(script: Script): Promise<Joined> {
  const relay = await ScriptedRelay.start(script);
  const harness = harnessFor(configFor(relay.port));
  harness.client.start();
  await harness.ready.until(1);
  return {
    relay,
    harness,
    close: async () => {
      await harness.client.stop();
      await relay.close();
    },
  };
}

describe("reserving", () => {
  test("a granted reservation states the lifetime the sender must pass on", async () => {
    const { relay, harness, close } = await joined(GRANT);
    try {
      const payload = new Uint8Array([1, 2, 3, 4]);
      const attached = await harness.client.attach(payload);

      expect(attached.digest).toBe(await digestOf(payload));
      expect(attached.bytes).toBe(4);
      expect(attached.expiresIn).toBe(7200);

      // The reservation went first, and it declared what the upload would carry.
      const reserve = relay.received.find((frame) => isFrame(frame, "reserve"));
      expect(reserve).toBeDefined();
      expect(frameField(reserve, "bytes")).toBe(4);
      expect(frameField(reserve, "digest")).toBe(attached.digest);
      console.log(
        `reserve declared ${String(frameField(reserve, "bytes"))} bytes; the grant stated ${attached.expiresIn} s`,
      );
    } finally {
      await close();
    }
  });

  test("the address is computed from the bytes, never supplied", async () => {
    const { relay, harness, close } = await joined(GRANT);
    try {
      const payload = new TextEncoder().encode("the failing test's output");
      const attached = await harness.client.attach(payload);

      // The one number that makes a fetch verifiable. Were this a caller's value,
      // a defect upstream would put a payload at an address that does not
      // describe it, and every recipient trusting the address would be misled.
      expect(attached.digest).toBe(await digestOf(payload));
      expect(relay.transfers.map((request) => request.path)).toEqual([
        route(attached.digest),
      ]);
      expect([...(relay.transfers[0]?.body ?? [])]).toEqual([...payload]);
    } finally {
      await close();
    }
  });

  test.each(["payload_too_large", "room_full", "store_full"])(
    "a %s refusal names its bound and uploads nothing",
    async (status) => {
      const { relay, harness, close } = await joined(refuseWith(status));
      try {
        const outcome = await settlement(harness.client.attach(new Uint8Array([9])));

        expect(outcome.status).toBe("rejected");
        if (outcome.status !== "rejected") return;
        expect(outcome.reason).toBeInstanceOf(RequestFailed);
        expect((outcome.reason as RequestFailed).reason).toBe("refused");
        expect((outcome.reason as RequestFailed).status).toBe(status);
        // The whole point of checking a ceiling before transferring anything.
        expect(relay.transfers).toEqual([]);
      } finally {
        await close();
      }
    },
  );

  test("a relay that does not know the frame is reported as unsupported", async () => {
    // What an older relay does: an unrecognized frame is answered
    // `unsupported_frame` on a connection that stays open.
    const { relay, harness, close } = await joined(
      errorWith("unsupported_frame", "this protocol version does not implement that frame"),
    );
    try {
      const outcome = await settlement(harness.client.attach(new Uint8Array([1])));

      expect(outcome.status).toBe("rejected");
      if (outcome.status !== "rejected") return;
      expect((outcome.reason as RequestFailed).reason).toBe("unsupported");
      expect((outcome.reason as RequestFailed).code).toBe("unsupported_frame");
      expect(relay.transfers).toEqual([]);
      // The connection survives, which is what makes this a capability answer
      // rather than an outage.
      expect(harness.client.state).toBe("ready");
    } finally {
      await close();
    }
  });

  test("an error naming a reservation settles it rather than timing out", async () => {
    // What the reply-space enumeration buys: with the spaces listed by hand, a
    // `reserve`'s error frame matched nothing and its caller waited out the full
    // request deadline for an answer that had already arrived.
    const { harness, close } = await joined(
      errorWith("invalid_identifier", "reserve.digest is wrong"),
    );
    try {
      const outcome = await settlement(
        harness.client.reserve(await digestOf(new Uint8Array([1])), 1),
      );

      expect(outcome.status).toBe("rejected");
      if (outcome.status !== "rejected") return;
      expect((outcome.reason as RequestFailed).reason).toBe("relay_error");
      // Settled by the error, so no timer was left waiting for it.
      expect(harness.client.pendingRequests).toBe(0);
    } finally {
      await close();
    }
  });

  test("a malformed digest is refused before anything is written", async () => {
    const { relay, harness, close } = await joined(GRANT);
    try {
      for (const bad of ["", "short", "A".repeat(44), `${"A".repeat(42)}=`]) {
        const outcome = await settlement(harness.client.reserve(bad, 1));
        expect(outcome.status).toBe("rejected");
        if (outcome.status !== "rejected") continue;
        expect((outcome.reason as RequestFailed).reason).toBe("invalid_request");
      }
      expect(relay.received.filter((frame) => isFrame(frame, "reserve"))).toEqual([]);
    } finally {
      await close();
    }
  });
});

describe("attaching to a message", () => {
  test("the reference is carried only after a grant and an upload", async () => {
    const { relay, harness, close } = await joined(GRANT);
    try {
      const payload = new TextEncoder().encode("a diff");
      const attached = await harness.client.attach(payload);
      await harness.client.send({
        to: "windows-main",
        body: "attached",
        attachment: attached.digest,
      });

      // Order is the contract: reserve, upload, then the frame that refers to it.
      // A frame that overtook its own upload would reference a payload the
      // recipient could not fetch.
      const order = [
        ...relay.received.filter((frame) => isFrame(frame, "reserve")).map(() => "reserve"),
        ...relay.transfers.map((request) => `transfer:${request.method}`),
        ...relay.received.filter((frame) => isFrame(frame, "send")).map(() => "send"),
      ];
      expect(order).toEqual(["reserve", "transfer:PUT", "send"]);

      const sent = relay.received.find((frame) => isFrame(frame, "send"));
      expect(frameField(sent, "attachment")).toBe(attached.digest);
    } finally {
      await close();
    }
  });

  test("a refused reservation writes no frame carrying a reference", async () => {
    const { relay, harness, close } = await joined(refuseWith("room_full"));
    try {
      const outcome = await settlement(harness.client.attach(new Uint8Array([1])));
      expect(outcome.status).toBe("rejected");
      expect(relay.received.filter((frame) => isFrame(frame, "send"))).toEqual([]);
      expect(relay.transfers).toEqual([]);
    } finally {
      await close();
    }
  });

  test("a caller's malformed reference is refused before the frame is written", async () => {
    const { relay, harness, close } = await joined(GRANT);
    try {
      const sent = await settlement(
        harness.client.send({ to: "windows-main", body: "x", attachment: "not-a-digest" }),
      );
      const announced = await settlement(
        harness.client.announce({ body: "x", attachment: "not-a-digest" }),
      );

      expect(sent.status).toBe("rejected");
      expect(announced.status).toBe("rejected");
      expect(relay.received.filter((frame) => isFrame(frame, "send"))).toEqual([]);
      expect(relay.received.filter((frame) => isFrame(frame, "announce"))).toEqual([]);
    } finally {
      await close();
    }
  });
});

describe("fetching", () => {
  test("every request is built from local configuration and the local room", async () => {
    const { relay, harness, close } = await joined(GRANT);
    try {
      const payload = new TextEncoder().encode("held by the relay");
      const digest = await digestOf(payload);
      relay.hold(route(digest), payload);

      const fetched = await harness.client.fetchAttachment(digest);
      expect([...fetched]).toEqual([...payload]);

      // The room in the path is this connection's own, and the host and port are
      // this client's own configuration. Nothing from a frame reaches the URL but
      // the digest.
      expect(relay.transfers.map((request) => request.path)).toEqual([route(digest)]);
    } finally {
      await close();
    }
  });

  test("extra location fields on a delivery reach neither the host nor the URL", async () => {
    const payload = new TextEncoder().encode("held by the relay");
    const digest = await digestOf(payload);

    const { relay, harness, close } = await joined((frame, session) => {
      if (isFrame(frame, "hello")) {
        session.send({ type: "ready", protocol: PROTOCOL_VERSION });
        // A hostile sender's best attempt: fields that would redirect a fetch or
        // name a local file if anything read them.
        session.sendRaw(
          framePayload(
            rawEncode({
              type: "message",
              id: "msg-1",
              from: "windows-main",
              body: "attached",
              attachment: digest,
              host: "198.51.100.7",
              port: 9,
              path: "/etc/passwd",
              filename: "../../evil",
            }),
          ),
        );
      }
    });
    try {
      relay.hold(route(digest), payload);
      await harness.deliveries.until(1);

      // The frame the host sees carries the reference and nothing the sender
      // added beside it.
      expect(harness.deliveries.last).toEqual({
        type: "message",
        id: "msg-1",
        from: "windows-main",
        body: "attached",
        attachment: digest,
      });

      await harness.client.fetchAttachment(digest);
      expect(relay.transfers.map((request) => request.path)).toEqual([route(digest)]);
    } finally {
      await close();
    }
  });

  test("a delivered reference starts no transfer while it is delivered", async () => {
    const payload = new TextEncoder().encode("never fetched");
    const digest = await digestOf(payload);

    const { relay, harness, close } = await joined((frame, session) => {
      if (isFrame(frame, "hello")) {
        session.send({ type: "ready", protocol: PROTOCOL_VERSION });
        session.send({
          type: "message",
          id: "msg-1",
          from: "windows-main",
          body: "attached",
          attachment: digest,
        });
      }
    });
    try {
      relay.hold(route(digest), payload);
      await harness.deliveries.until(1);
      expect(harness.deliveries.last?.attachment).toBe(digest);

      // Auto-fetching would let a remote peer cause this machine to download
      // arbitrary bytes at a moment nothing local chose.
      expect(relay.transfers).toEqual([]);
    } finally {
      await close();
    }
  });

  test("a payload that does not match its address is a stated failure", async () => {
    const payload = new TextEncoder().encode("what was asked for");
    const digest = await digestOf(payload);

    const { relay, harness, close } = await joined(GRANT);
    try {
      // A relay, an interposed proxy, or a defect in either implementation
      // returning other bytes under this address.
      const substituted = new TextEncoder().encode("something else entirely");
      relay.retransfer(() => ({ status: 200, body: substituted }));

      const outcome = await settlement(harness.client.fetchAttachment(digest));

      expect(outcome.status).toBe("rejected");
      if (outcome.status !== "rejected") return;
      expect((outcome.reason as RequestFailed).reason).toBe("transfer_failed");
      expect((outcome.reason as RequestFailed).message).toContain(digest);
    } finally {
      await close();
    }
  });

  test("a truncated download is a stated failure and delivers nothing", async () => {
    const payload = new TextEncoder().encode("a payload of some length");
    const digest = await digestOf(payload);

    const { relay, harness, close } = await joined(GRANT);
    try {
      relay.retransfer(() => ({ status: 200, body: payload.subarray(0, 8) }));

      const outcome = await settlement(harness.client.fetchAttachment(digest));

      // Nothing is handed back: a truncated payload is not a shorter version of
      // what was asked for.
      expect(outcome.status).toBe("rejected");
      if (outcome.status !== "rejected") return;
      expect((outcome.reason as RequestFailed).reason).toBe("transfer_failed");
    } finally {
      await close();
    }
  });

  test("an absent payload is reported as unavailable rather than as a fault", async () => {
    const { harness, close } = await joined(GRANT);
    try {
      const digest = await digestOf(new Uint8Array([7]));
      const outcome = await settlement(harness.client.fetchAttachment(digest));

      expect(outcome.status).toBe("rejected");
      if (outcome.status !== "rejected") return;
      expect((outcome.reason as RequestFailed).reason).toBe("unavailable");
    } finally {
      await close();
    }
  });

  test("the length request answers without transferring the payload", async () => {
    const payload = new TextEncoder().encode("fifty thousand bytes, notionally");
    const digest = await digestOf(payload);

    const { relay, harness, close } = await joined(GRANT);
    try {
      relay.hold(route(digest), payload);

      expect(await harness.client.lengthOf(digest)).toBe(payload.byteLength);
      expect(relay.transfers.map((request) => request.method)).toEqual(["HEAD"]);
      expect(relay.transfers[0]?.body.byteLength).toBe(0);

      // Absent and present are distinguished, which a reference carrying a size
      // fixed at send time cannot do.
      expect(await harness.client.lengthOf(await digestOf(new Uint8Array([0])))).toBeNull();
    } finally {
      await close();
    }
  });

  test("a payload over the caller's ceiling transfers nothing and reports its size", async () => {
    const payload = new TextEncoder().encode("x".repeat(4096));
    const digest = await digestOf(payload);

    const { relay, harness, close } = await joined(GRANT);
    try {
      relay.hold(route(digest), payload);

      const outcome = await settlement(
        harness.client.fetchAttachment(digest, { maxBytes: 1024 }),
      );

      expect(outcome.status).toBe("rejected");
      if (outcome.status !== "rejected") return;
      expect((outcome.reason as RequestFailed).reason).toBe("over_ceiling");
      expect((outcome.reason as RequestFailed).bytes).toBe(4096);
      // The length request happened; the transfer did not.
      expect(relay.transfers.map((request) => request.method)).toEqual(["HEAD"]);
    } finally {
      await close();
    }
  });
});

describe("bounding a transfer", () => {
  test("a stalled transfer fails on its own bound, not the request deadline", async () => {
    const { relay, harness, close } = await joined(GRANT);
    try {
      const digest = await digestOf(new Uint8Array([5]));
      // Silence: the double accepts the request and answers nothing at all.
      relay.retransfer(() => null);

      const pending = settlement(harness.client.fetchAttachment(digest));
      // Armed before the request is written, so this is observation rather than a
      // wait. A transfer's duration scales with the payload, so the frame
      // exchange's deadline would fail a large payload *because it was working*.
      const armed = harness.scheduler.liveOf("timeout", TRANSFER_STALL_MS);
      expect(armed).toHaveLength(1);
      expect(harness.scheduler.delaysOf("timeout")).not.toContain(5_000);

      await relay.awaitTransfers(1);
      harness.scheduler.fireNewest("timeout", TRANSFER_STALL_MS);

      const outcome = await pending;
      expect(outcome.status).toBe("rejected");
      if (outcome.status !== "rejected") return;
      expect((outcome.reason as RequestFailed).reason).toBe("transfer_failed");
    } finally {
      await close();
    }
  });

  test("a download that keeps arriving is not failed for taking longer than the bound", async () => {
    // The requirement's central guarantee: "a transfer that is moving is allowed
    // to take as long as its size requires". A single deadline over the whole
    // request satisfies every other test in this file and violates exactly this
    // one, which is why it exists.
    const payload = new TextEncoder().encode("chunk".repeat(2000));
    const digest = await digestOf(payload);
    const slices = [payload.subarray(0, 3000), payload.subarray(3000, 7000), payload.subarray(7000)];

    const handed = Promise.withResolvers<BodyWriter>();
    const { relay, harness, close } = await joined(GRANT);
    try {
      relay.retransfer(() => ({
        status: 200,
        trickle: (control) => handed.resolve(control),
      }));

      const pending = settlement(harness.client.fetchAttachment(digest));
      const writer = await handed.promise;

      const armedFirst = harness.scheduler.liveOf("timeout", TRANSFER_STALL_MS)[0];
      expect(armedFirst).toBeDefined();

      // Each slice arrives, and between slices the bound is allowed to elapse in
      // full. Under a whole-request deadline the transfer would already be dead
      // by the second slice.
      let armedBefore = armedFirst;
      let bounds = harness.scheduler.delaysOf("timeout").filter((d) => d === TRANSFER_STALL_MS).length;
      for (const slice of slices) {
        writer.push(slice);
        bounds += 1;
        await harness.scheduler.until("timeout", bounds, TRANSFER_STALL_MS);
        // The timer counting before this slice is cancelled and a fresh one
        // replaces it: that is what "restarted on progress" means.
        expect(armedBefore?.cancelled).toBe(true);
        armedBefore = harness.scheduler.liveOf("timeout", TRANSFER_STALL_MS)[0];
        expect(armedBefore).toBeDefined();
      }
      writer.end();

      const outcome = await pending;
      expect(outcome.status).toBe("fulfilled");
      if (outcome.status !== "fulfilled") return;
      expect([...outcome.value]).toEqual([...payload]);

      // One bound per chunk plus the one armed before the request: a single
      // whole-request deadline would have created exactly one.
      expect(bounds).toBeGreaterThan(slices.length);
      console.log(
        `${slices.length} chunk(s) totalling ${payload.byteLength} bytes restarted the ${TRANSFER_STALL_MS} ms bound ${bounds} time(s)`,
      );
    } finally {
      await close();
    }
  });

  test("a download that stops partway fails once the gap exceeds the bound", async () => {
    // The other half of the same rule. Progress resets the bound; absence of
    // progress must still end the transfer, and a partial body is not a result.
    const payload = new TextEncoder().encode("chunk".repeat(2000));
    const digest = await digestOf(payload);

    const handed = Promise.withResolvers<BodyWriter>();
    const { relay, harness, close } = await joined(GRANT);
    try {
      relay.retransfer(() => ({
        status: 200,
        trickle: (control) => handed.resolve(control),
      }));

      const pending = settlement(harness.client.fetchAttachment(digest));
      const writer = await handed.promise;

      const before = harness.scheduler
        .delaysOf("timeout")
        .filter((d) => d === TRANSFER_STALL_MS).length;
      writer.push(payload.subarray(0, 4000));
      await harness.scheduler.until("timeout", before + 1, TRANSFER_STALL_MS);

      // Nothing more arrives. The bound armed by the last chunk expires.
      harness.scheduler.fireNewest("timeout", TRANSFER_STALL_MS);

      const outcome = await pending;
      expect(outcome.status).toBe("rejected");
      if (outcome.status !== "rejected") return;
      expect((outcome.reason as RequestFailed).reason).toBe("transfer_failed");
    } finally {
      await close();
    }
  });

  test("an upload restarts the bound as the socket takes each chunk", async () => {
    // The request side of the same requirement. `fetch` reports no upload
    // progress directly, but a `pull` on a stream body is the runtime asking for
    // more, which it does once the previous chunk has left.
    const { relay, harness, close } = await joined(GRANT);
    try {
      // Several chunks' worth, so more than one pull is required.
      const payload = new Uint8Array(TRANSFER_CHUNK_BYTES * 3 + 17);
      for (let index = 0; index < payload.length; index += 1) {
        payload[index] = index % 251;
      }

      const attached = await harness.client.attach(payload);
      expect(attached.bytes).toBe(payload.byteLength);
      expect([...(relay.transfers[0]?.body ?? [])]).toEqual([...payload]);

      const bounds = harness.scheduler
        .delaysOf("timeout")
        .filter((delay) => delay === TRANSFER_STALL_MS).length;
      expect(bounds).toBeGreaterThan(1);
      console.log(
        `uploading ${payload.byteLength} bytes in ${TRANSFER_CHUNK_BYTES}-byte chunks restarted the bound ${bounds} time(s)`,
      );
    } finally {
      await close();
    }
  });

  test("shutdown cancels a transfer in flight", async () => {
    const { relay, harness } = await joined(GRANT);
    try {
      const digest = await digestOf(new Uint8Array([6]));
      relay.retransfer(() => null);

      const pending = settlement(harness.client.fetchAttachment(digest));
      await relay.awaitTransfers(1);
      await harness.client.stop();

      const outcome = await pending;
      expect(outcome.status).toBe("rejected");
      if (outcome.status !== "rejected") return;
      expect((outcome.reason as RequestFailed).reason).toBe("transfer_failed");
      // A transfer runs on its own socket rather than the client's, so nothing
      // else would have ended it, and its bound is released with it.
      expect(harness.scheduler.live).toEqual([]);
    } finally {
      await relay.close();
    }
  });

  test("a transfer on a stopped client is refused before a socket is opened", async () => {
    const relay = await ScriptedRelay.start(GRANT);
    const harness = harnessFor(configFor(relay.port));
    try {
      const outcome = await settlement(
        harness.client.fetchAttachment(await digestOf(new Uint8Array([1]))),
      );
      expect(outcome.status).toBe("rejected");
      if (outcome.status !== "rejected") return;
      expect((outcome.reason as RequestFailed).reason).toBe("stopped");
      expect(relay.transfers).toEqual([]);
    } finally {
      await relay.close();
    }
  });
});
