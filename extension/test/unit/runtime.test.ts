/**
 * The extension's runtime half, and the one connection failure that must not be
 * retried.
 *
 * `index.test.ts` covers registration and the pure helpers. What is exercised
 * here is everything that only exists once a session is running: which
 * scheduler the client is handed, which injection API an inbound message
 * reaches, and what happens when shutdown races session start. Those are claims
 * about wiring, so they are driven through the registered handlers against a
 * real socket rather than by calling the helpers directly — a helper called
 * directly proves nothing about whether the runtime ever calls it.
 */

import { afterEach, describe, expect, test } from "bun:test";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import type { ExtensionAPI, ExtensionContext } from "@oh-my-pi/pi-coding-agent";

import {
  HANDSHAKE_TIMEOUT_MS,
  HEARTBEAT_INTERVAL_MS,
  PEER_REPLACED_REPORT,
  RelayClient,
  REQUEST_TIMEOUT_MS,
  type Report,
} from "../../src/client.ts";
import { CONFIG_PATH_ENV, type RelayConfig } from "../../src/config.ts";
import ompRelay, { INBOUND_MESSAGE_TYPE } from "../../src/index.ts";
import { PROTOCOL_VERSION } from "../../src/protocol.ts";
import { FakeScheduler } from "../support/fake-scheduler.ts";
import { ScriptedRelay, type RelaySession, type Script } from "../support/scripted-relay.ts";
import { Signal } from "../support/signal.ts";

const ROOM = { project: "omp-relayd", task: "implement-omp-extension" };
const PEER = "fixture-peer";

/** The delays that belong to the client rather than to `node:net` or Bun. */
const CLIENT_DELAYS = [HANDSHAKE_TIMEOUT_MS, HEARTBEAT_INTERVAL_MS, REQUEST_TIMEOUT_MS];

/** The ambient timers, captured before any test replaces them. */
const realSetTimeout = globalThis.setTimeout;
const realSetInterval = globalThis.setInterval;
const realClearTimeout = globalThis.clearTimeout;
const realClearInterval = globalThis.clearInterval;

function isHello(value: unknown): boolean {
  return (
    typeof value === "object" &&
    value !== null &&
    (value as Record<string, unknown>)["type"] === "hello"
  );
}

// ---------------------------------------------------------------------------
// Displacement
// ---------------------------------------------------------------------------

describe("a peer name taken by another session", () => {
  /**
   * The registry behaviour a displacement needs, and nothing else: the name
   * goes to whichever connection registered last, the connection that held it
   * is told `peer_replaced`, and it is then closed
   * (`server/src/relay.rs:619-627`).
   *
   * The close is exposed to the test rather than performed inside the script,
   * because `send` followed immediately by `destroy` can discard the frame that
   * was just written. The test closes it once the client has demonstrably
   * processed the error, which is also the ordering the real relay produces.
   */
  function displacingRelay(): { script: Script; displaced: () => RelaySession | null } {
    let holder: RelaySession | null = null;
    let displaced: RelaySession | null = null;
    const script: Script = (frame, session) => {
      if (!isHello(frame)) return;
      if (holder !== null) {
        displaced = holder;
        holder.send({
          type: "error",
          code: "peer_replaced",
          message: "a newer connection registered this peer name",
        });
      }
      holder = session;
      session.send({ type: "ready", protocol: PROTOCOL_VERSION });
    };
    return { script, displaced: () => displaced };
  }

  test("displaces the loser permanently instead of starting an eviction loop", async () => {
    const { script, displaced } = displacingRelay();
    const relay = await ScriptedRelay.start(script);
    const config: RelayConfig = {
      transport: { mode: "local", host: "127.0.0.1", port: relay.port },
      room: ROOM,
      peer: PEER,
    };

    const loserScheduler = new FakeScheduler();
    const loserReports = new Signal<Report>();
    const loserDisconnects = new Signal<string>();
    const loser = new RelayClient({
      config,
      scheduler: loserScheduler,
      handlers: {
        onReport: (report) => loserReports.fire(report),
        onDisconnect: (reason) => loserDisconnects.fire(reason),
      },
    });

    const winnerReady = new Signal();
    const winner = new RelayClient({
      config,
      scheduler: new FakeScheduler(),
      handlers: { onReady: () => winnerReady.fire(undefined) },
    });

    try {
      loser.start();
      await relay.awaitReceived(1);
      winner.start();
      await winnerReady.until(1);
      // The report is the one signal both a fixed and an unfixed client emit on
      // receiving the error, so waiting on it keeps this test failing by
      // assertion rather than by timing out.
      await loserReports.until(1);
      // Only now, so the frame cannot be discarded with the socket.
      displaced()?.drop();
      await loserDisconnects.until(1);

      // Terminal, not an outage. Before the fix this read "connecting".
      expect(loser.state).toBe("stopped");

      // The decisive assertion: a scheduled reconnect would be a live timer on
      // the scheduler the client was given. There is none, so no attempt is
      // pending and the loser cannot take the name back.
      expect(loserScheduler.live).toEqual([]);
      expect(relay.connections).toBe(2);

      // Reported once, and as the configuration collision it is rather than as
      // a relay code the operator would have to look up.
      expect(loserReports.observed).toEqual([
        { level: "error", message: PEER_REPLACED_REPORT },
      ]);
      expect(PEER_REPLACED_REPORT).toContain("peer name");
      expect(PEER_REPLACED_REPORT).toContain("configuration");
      console.log(
        `displaced client: state=${loser.state}, live timers=${loserScheduler.live.length}, reports=${loserReports.count}, relay connections=${relay.connections}`,
      );

      // The session that took the name keeps it.
      expect(winner.state).toBe("ready");
    } finally {
      await loser.stop();
      await winner.stop();
      await relay.close();
    }
  });
});

// ---------------------------------------------------------------------------
// Session runtime
// ---------------------------------------------------------------------------

/** What one fake `ExtensionAPI` observed. */
interface RuntimeCalls {
  readonly customMessages: unknown[];
  readonly userMessages: Array<{ content: unknown; options: unknown }>;
  readonly entries: Array<{ customType: string; data: unknown }>;
  readonly injected: Signal<void>;
}

/** What one fake `ExtensionContext` was asked to schedule. */
interface ContextTimers {
  readonly timeouts: number[];
  readonly intervals: number[];
}

interface SessionHarness {
  readonly api: ExtensionAPI;
  readonly handlers: Map<string, (event: unknown, ctx: ExtensionContext) => Promise<void> | void>;
  readonly calls: RuntimeCalls;
  readonly contextTimers: ContextTimers;
  readonly ctx: ExtensionContext;
  readonly notifications: string[];
}

function sessionHarness(): SessionHarness {
  const handlers = new Map<string, (event: unknown, ctx: ExtensionContext) => Promise<void> | void>();
  const calls: RuntimeCalls = {
    customMessages: [],
    userMessages: [],
    entries: [],
    injected: new Signal<void>(),
  };
  const contextTimers: ContextTimers = { timeouts: [], intervals: [] };
  const notifications: string[] = [];

  const chain = {
    describe() {
      return chain;
    },
    optional() {
      return chain;
    },
  };
  const api = {
    zod: {
      enum() {
        return chain;
      },
      string() {
        return chain;
      },
      object() {
        return chain;
      },
    },
    logger: { error() {} },
    registerTool() {},
    on(event: string, handler: (event: unknown, ctx: ExtensionContext) => Promise<void> | void) {
      handlers.set(event, handler);
    },
    sendMessage(message: unknown) {
      calls.customMessages.push(message);
      calls.injected.fire(undefined);
    },
    sendUserMessage(content: unknown, options: unknown) {
      calls.userMessages.push({ content, options });
      calls.injected.fire(undefined);
    },
    appendEntry(customType: string, data: unknown) {
      calls.entries.push({ customType, data });
    },
  } as unknown as ExtensionAPI;

  // The real ambient timers, captured at module load, so replacing the globals
  // in a test does not turn the context's own delegation into an ambient call.
  const ctx = {
    mode: "tui",
    ui: {
      notify(message: string) {
        notifications.push(message);
      },
    },
    setTimeout(callback: () => void, milliseconds: number) {
      contextTimers.timeouts.push(milliseconds);
      return realSetTimeout(callback, milliseconds);
    },
    setInterval(callback: () => void, milliseconds: number) {
      contextTimers.intervals.push(milliseconds);
      return realSetInterval(callback, milliseconds);
    },
    clearTimer(handle: unknown) {
      realClearTimeout(handle as Parameters<typeof realClearTimeout>[0]);
      realClearInterval(handle as Parameters<typeof realClearInterval>[0]);
    },
  } as unknown as ExtensionContext;

  return { api, handlers, calls, contextTimers, ctx, notifications };
}

const originalConfigPath = process.env[CONFIG_PATH_ENV];

afterEach(() => {
  if (originalConfigPath === undefined) {
    delete process.env[CONFIG_PATH_ENV];
  } else {
    process.env[CONFIG_PATH_ENV] = originalConfigPath;
  }
});

/** Writes a configuration the loader accepts, pointing at `port`. */
function configFileFor(port: number): string {
  const path = join(mkdtempSync(join(tmpdir(), "omp-relay-runtime-")), "omp-relay.yml");
  writeFileSync(
    path,
    [
      "transport:",
      "  mode: local",
      `  address: 127.0.0.1:${port}`,
      "room:",
      `  project: ${ROOM.project}`,
      `  task: ${ROOM.task}`,
      `peer: ${PEER}`,
      "",
    ].join("\n"),
    "utf8",
  );
  return path;
}

/** The one inbound frame these tests deliver, and the text it must render to. */
const INBOUND = {
  type: "message",
  id: "message-42",
  from: "alpha",
  body: "Please review the parser.",
  reply_to: "message-3",
} as const;

const INBOUND_TEXT = [
  "Remote message from alpha",
  `Project: ${ROOM.project}`,
  `Task: ${ROOM.task}`,
  "Message ID: message-42",
  "Reply to: message-3",
  "",
  "> Please review the parser.",
].join("\n");

/**
 * Admits the connection and delivers {@link INBOUND} on the same handshake.
 *
 * Both frames are written in `ready`-then-`message` order, which is what makes
 * the delivery deterministic: the client dispatches a chunk's frames in order,
 * so the connection is ready by the time the message is routed.
 */
const ADMIT_AND_DELIVER: Script = (frame, session) => {
  if (!isHello(frame)) return;
  session.send({ type: "ready", protocol: PROTOCOL_VERSION });
  session.send(INBOUND);
};

/** Starts a session against a scripted relay and returns its harness. */
async function startSession(
  relay: ScriptedRelay,
): Promise<{ harness: SessionHarness; shutdown: () => Promise<void> }> {
  const harness = sessionHarness();
  ompRelay(harness.api);
  process.env[CONFIG_PATH_ENV] = configFileFor(relay.port);

  await harness.handlers.get("session_start")?.({ type: "session_start" }, harness.ctx);

  return {
    harness,
    shutdown: async () => {
      await harness.handlers.get("session_shutdown")?.({ type: "session_shutdown" }, harness.ctx);
    },
  };
}

describe("the session runtime", () => {
  test("schedules every client timer through the context, never the ambient timers", async () => {
    // The listener is started before the globals are replaced, and the wrappers
    // forward every argument: `node:net` schedules its own callbacks with extra
    // arguments, and dropping them breaks the socket under test.
    const relay = await ScriptedRelay.start(ADMIT_AND_DELIVER);
    const ambientDelays: number[] = [];
    // @ts-expect-error deliberately replacing an ambient global for the duration
    globalThis.setTimeout = (callback: () => void, delay?: number, ...args: unknown[]) => {
      ambientDelays.push(delay ?? 0);
      return realSetTimeout(callback, delay, ...args);
    };
    // @ts-expect-error deliberately replacing an ambient global for the duration
    globalThis.setInterval = (callback: () => void, delay?: number, ...args: unknown[]) => {
      ambientDelays.push(delay ?? 0);
      return realSetInterval(callback, delay, ...args);
    };

    try {
      const { harness, shutdown } = await startSession(relay);
      // Waiting on the injection is waiting on readiness: the heartbeat is armed
      // by the same `ready` that admits the message.
      await harness.calls.injected.until(1);

      // Counts, not a flag. The handshake deadline is armed once; the heartbeat
      // is armed twice, because every outbound frame restarts it and a ready
      // connection has written `hello` and then received `ready`.
      expect(harness.contextTimers.timeouts.filter((delay) => delay === HANDSHAKE_TIMEOUT_MS)).toEqual([
        HANDSHAKE_TIMEOUT_MS,
      ]);
      expect(harness.contextTimers.intervals).toEqual([
        HEARTBEAT_INTERVAL_MS,
        HEARTBEAT_INTERVAL_MS,
      ]);

      // Dropping the context scheduler would move both of those onto the
      // ambient timers, which is what this filter would then find.
      const leaked = ambientDelays.filter((delay) => CLIENT_DELAYS.includes(delay));
      expect(leaked).toEqual([]);
      console.log(
        `context timers: ${harness.contextTimers.timeouts.length} timeout(s), ${harness.contextTimers.intervals.length} interval(s); ambient calls carrying a client delay: ${leaked.length} of ${ambientDelays.length}`,
      );

      await shutdown();
    } finally {
      globalThis.setTimeout = realSetTimeout;
      globalThis.setInterval = realSetInterval;
      await relay.close();
    }
  });

  test("delivers an inbound message as a user prompt and persists its provenance", async () => {
    const relay = await ScriptedRelay.start(ADMIT_AND_DELIVER);
    try {
      const { harness, shutdown } = await startSession(relay);
      await harness.calls.injected.until(1);

      // The delivery option is asserted, not treated as incidental: it is the
      // property that keeps remote text off the prompt-preprocessing path. A
      // bare `sendUserMessage` routes to `prompt()`, which auto-reads `@path`
      // file mentions out of the remote body, and `followUp` would never start
      // a turn on a fresh transcript. A reviewer deleting the option to
      // "simplify" would reintroduce the file-mention Blocker, and this
      // assertion is what stops that.
      // Limitation, stated plainly: this asserts the argument the extension
      // passes. It does not execute OMP's prompt path, so it cannot itself
      // demonstrate that an `@path` in the body goes unexpanded.
      expect(harness.calls.userMessages).toEqual([
        { content: INBOUND_TEXT, options: { deliverAs: "steer" } },
      ]);
      // The exact frame is kept in a session entry, which the runtime documents
      // as state persistence that never reaches the LLM.
      expect(harness.calls.entries).toEqual([
        {
          customType: INBOUND_MESSAGE_TYPE,
          data: {
            id: "message-42",
            from: "alpha",
            project: ROOM.project,
            task: ROOM.task,
            body: "Please review the parser.",
            reply_to: "message-3",
          },
        },
      ]);

      await shutdown();
    } finally {
      await relay.close();
    }
  });

  test("never routes inbound relay content through the custom-message API", async () => {
    // The provider-role contract. The runtime converts an extension custom
    // message to the provider role `developer`, above the local operator's own
    // `user` messages, so remote peer content must not travel that path. This
    // asserts the API choice at the call boundary rather than the converted
    // role, because reaching the runtime's own converter would mean importing a
    // package this project does not depend on.
    const relay = await ScriptedRelay.start(ADMIT_AND_DELIVER);
    try {
      const { harness, shutdown } = await startSession(relay);
      await harness.calls.injected.until(1);

      expect(harness.calls.customMessages).toEqual([]);
      expect(harness.calls.userMessages).toHaveLength(1);

      await shutdown();
    } finally {
      await relay.close();
    }
  });

  test("a shutdown arriving while session start reads configuration opens no socket", async () => {
    const relay = await ScriptedRelay.start(ADMIT_AND_DELIVER);
    try {
      const harness = sessionHarness();
      ompRelay(harness.api);
      process.env[CONFIG_PATH_ENV] = configFileFor(relay.port);

      // Not awaited: the handler suspends on the configuration read, which is
      // the window the generation guard exists for.
      const starting = harness.handlers.get("session_start")?.(
        { type: "session_start" },
        harness.ctx,
      );
      await harness.handlers.get("session_shutdown")?.(
        { type: "session_shutdown" },
        harness.ctx,
      );
      await starting;

      // A barrier rather than a sleep: a second session connects to the same
      // listener strictly after the raced handler resumed, so once that session
      // has been admitted, a socket the raced handler had opened would already
      // be counted ahead of it.
      const second = await startSession(relay);
      await second.harness.calls.injected.until(1);

      expect(relay.connections).toBe(1);
      expect(harness.calls.userMessages).toEqual([]);
      expect(harness.contextTimers.timeouts).toEqual([]);
      expect(harness.notifications).toEqual([]);
      console.log(
        `shutdown raced session start: ${relay.connections} connection(s) accepted in total, all of them the barrier session's; ${harness.contextTimers.timeouts.length} timer(s) requested by the raced one`,
      );

      await second.shutdown();
    } finally {
      await relay.close();
    }
  });
});
