/**
 * The extension's runtime half: which start path opens a socket, what a join
 * does to a live connection, and where an inbound message lands.
 *
 * `index.test.ts` covers registration and the pure helpers. What is exercised
 * here is everything that only exists once a session is running: which
 * scheduler the client is handed, which injection API an inbound message
 * reaches, whether `manual` really opens nothing, and what happens when a
 * shutdown or a second join races the first. Those are claims about wiring, so
 * they are driven through the registered handlers and the registered tool
 * against a real socket rather than by calling the helpers directly — a helper
 * called directly proves nothing about whether the runtime ever calls it.
 */

import { afterEach, describe, expect, test } from "bun:test";
import { mkdirSync, mkdtempSync, writeFileSync } from "node:fs";
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
import {
  AGENT_DIR_ENV,
  CONFIG_FILE_NAME,
  PROJECT_ROOT_ENV,
  projectConfigPath,
  type RelayConfig,
} from "../../src/config.ts";
import ompRelay, {
  INBOUND_MESSAGE_TYPE,
  OUTBOUND_MESSAGE_TYPE,
  type MeshToolResult,
} from "../../src/index.ts";
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

function frameField(value: unknown, field: string): unknown {
  return typeof value === "object" && value !== null
    ? (value as Record<string, unknown>)[field]
    : undefined;
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

type ToolExecute = (
  toolCallId: string,
  args: Record<string, unknown>,
  signal: undefined,
  onUpdate: undefined,
  ctx: ExtensionContext,
) => Promise<MeshToolResult>;

interface SessionHarness {
  readonly api: ExtensionAPI;
  readonly handlers: Map<string, (event: unknown, ctx: ExtensionContext) => Promise<void> | void>;
  readonly calls: RuntimeCalls;
  readonly contextTimers: ContextTimers;
  readonly ctx: ExtensionContext;
  readonly notifications: string[];
  /** Invokes the registered `mesh` tool exactly as the runtime would. */
  mesh(args: Record<string, unknown>, ctx?: ExtensionContext): Promise<MeshToolResult>;
}

function sessionHarness(cwd: string, mode = "tui"): SessionHarness {
  const handlers = new Map<string, (event: unknown, ctx: ExtensionContext) => Promise<void> | void>();
  const calls: RuntimeCalls = {
    customMessages: [],
    userMessages: [],
    entries: [],
    injected: new Signal<void>(),
  };
  const contextTimers: ContextTimers = { timeouts: [], intervals: [] };
  const notifications: string[] = [];
  let execute: ToolExecute | null = null;

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
    registerTool(tool: { execute: ToolExecute }) {
      execute = tool.execute;
    },
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
    mode,
    cwd,
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

  return {
    api,
    handlers,
    calls,
    contextTimers,
    ctx,
    notifications,
    mesh(args, override) {
      if (execute === null) throw new Error("the extension registered no tool");
      return execute("call-1", args, undefined, undefined, override ?? ctx);
    },
  };
}

const originalAgentDir = process.env[AGENT_DIR_ENV];
const originalProjectRoot = process.env[PROJECT_ROOT_ENV];

afterEach(() => {
  for (const [key, value] of [
    [AGENT_DIR_ENV, originalAgentDir],
    [PROJECT_ROOT_ENV, originalProjectRoot],
  ] as const) {
    if (value === undefined) delete process.env[key];
    else process.env[key] = value;
  }
});

/** One scratch machine: both layers written, and `process.env` pointed at them. */
interface Layers {
  readonly agentDir: string;
  readonly projectRoot: string;
}

function layers(
  options: {
    readonly port: number;
    readonly startup?: "manual" | "auto";
    readonly purpose?: string;
    readonly task?: string;
  },
): Layers {
  const agentDir = mkdtempSync(join(tmpdir(), "omp-relay-agent-"));
  const projectRoot = mkdtempSync(join(tmpdir(), "omp-relay-root-"));
  writeFileSync(
    join(agentDir, CONFIG_FILE_NAME),
    [
      "transport:",
      "  mode: local",
      `  address: 127.0.0.1:${options.port}`,
      ...(options.startup === undefined ? [] : [`startup: ${options.startup}`]),
      "peer:",
      `  name: ${PEER}`,
      ...(options.purpose === undefined ? [] : [`  purpose: ${JSON.stringify(options.purpose)}`]),
      "",
    ].join("\n"),
    "utf8",
  );
  mkdirSync(join(projectRoot, ".omp"), { recursive: true });
  writeFileSync(
    projectConfigPath(projectRoot),
    ["room:", `  project: ${ROOM.project}`, `  task: ${options.task ?? ROOM.task}`, ""].join("\n"),
    "utf8",
  );

  process.env[AGENT_DIR_ENV] = agentDir;
  process.env[PROJECT_ROOT_ENV] = projectRoot;
  return { agentDir, projectRoot };
}

/** The one inbound frame these tests deliver, and the text it must render to. */
const INBOUND = {
  type: "message",
  id: "message-42",
  from: "alpha",
  body: "Please review the parser.",
  reply_to: "message-3",
} as const;

/** The same message with no correlation, for the preamble tests. */
const PLAIN_INBOUND = {
  type: "message",
  id: "message-42",
  from: "alpha",
  body: "Please review the parser.",
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
 * A relay that admits every connection, answers `list`, and can deliver
 * messages on demand.
 *
 * `list` is answered because a join is not complete until the roster comes
 * back, so every join-driven test needs it; the hellos are recorded because
 * "the relay saw the new room" is what makes a rejoin observable from the far
 * side rather than only from the caller's own report.
 */
interface Recorder {
  readonly script: Script;
  readonly hellos: Array<{ project: string; task: string; peer: string }>;
  readonly sends: unknown[];
  /** Delivers a frame on the most recent connection. */
  deliver(frame: Parameters<RelaySession["send"]>[0]): void;
  peers: readonly string[];
}

function recordingRelay(options: { deliverOnReady?: readonly unknown[] } = {}): Recorder {
  const hellos: Array<{ project: string; task: string; peer: string }> = [];
  const sends: unknown[] = [];
  let latest: RelaySession | null = null;
  const recorder: Recorder = {
    hellos,
    sends,
    peers: [PEER],
    deliver(frame) {
      latest?.send(frame);
    },
    script: (frame, session) => {
      latest = session;
      const type = frameField(frame, "type");
      if (type === "hello") {
        const room = frameField(frame, "room") as Record<string, unknown> | undefined;
        hellos.push({
          project: String(room?.["project"]),
          task: String(room?.["task"]),
          peer: String(frameField(frame, "peer")),
        });
        session.send({ type: "ready", protocol: PROTOCOL_VERSION });
        for (const extra of options.deliverOnReady ?? []) {
          session.send(extra as Parameters<RelaySession["send"]>[0]);
        }
        return;
      }
      if (type === "list") {
        session.send({
          type: "peers",
          request_id: String(frameField(frame, "request_id")),
          peers: recorder.peers,
        });
        return;
      }
      if (type === "send") {
        sends.push(frame);
        session.send({
          type: "receipt",
          id: String(frameField(frame, "id")),
          to: String(frameField(frame, "to")),
          status: "routed",
        });
      }
    },
  };
  return recorder;
}

/** Admits the connection and delivers {@link INBOUND} on the same handshake. */
const ADMIT_AND_DELIVER: Script = (frame, session) => {
  if (!isHello(frame)) return;
  session.send({ type: "ready", protocol: PROTOCOL_VERSION });
  session.send(INBOUND);
};

/** Starts an `auto` session against a scripted relay and returns its harness. */
async function startAutoSession(
  relay: ScriptedRelay,
  options: { purpose?: string } = {},
): Promise<{ harness: SessionHarness; shutdown: () => Promise<void> }> {
  const written = layers({
    port: relay.port,
    startup: "auto",
    ...(options.purpose === undefined ? {} : { purpose: options.purpose }),
  });
  const harness = sessionHarness(written.projectRoot);
  ompRelay(harness.api);

  await harness.handlers.get("session_start")?.({ type: "session_start" }, harness.ctx);

  return {
    harness,
    shutdown: async () => {
      await harness.handlers.get("session_shutdown")?.({ type: "session_shutdown" }, harness.ctx);
    },
  };
}

describe("the startup mode decides whether session start connects", () => {
  test("manual opens no socket at session start, and exactly one after a join", async () => {
    const recorder = recordingRelay();
    const relay = await ScriptedRelay.start(recorder.script);
    try {
      const written = layers({ port: relay.port });
      const harness = sessionHarness(written.projectRoot);
      ompRelay(harness.api);

      await harness.handlers.get("session_start")?.({ type: "session_start" }, harness.ctx);

      // Nothing resolved and nothing opened: not a timing claim but a state
      // one, because `session_start` has already returned.
      expect(relay.connections).toBe(0);
      expect(harness.contextTimers.timeouts).toEqual([]);
      expect(harness.notifications).toEqual([]);

      const joined = await harness.mesh({ action: "join" });

      expect(relay.connections).toBe(1);
      expect(recorder.hellos).toEqual([{ ...ROOM, peer: PEER }]);
      expect(joined.details["status"]).toBeUndefined();
      expect(joined.content[0]?.text).toContain(`Joined ${ROOM.project}/${ROOM.task} as ${PEER}`);
      console.log(
        `manual startup: ${0} connection(s) at session start, ${relay.connections} after join; relay saw room ${recorder.hellos[0]?.project}/${recorder.hellos[0]?.task}`,
      );

      await harness.handlers.get("session_shutdown")?.({ type: "session_shutdown" }, harness.ctx);
    } finally {
      await relay.close();
    }
  });

  test("auto connects at session start with no tool call at all", async () => {
    const recorder = recordingRelay();
    const relay = await ScriptedRelay.start(recorder.script);
    try {
      const { harness, shutdown } = await startAutoSession(relay);
      await relay.awaitReceived(1);

      expect(relay.connections).toBe(1);
      expect(recorder.hellos).toEqual([{ ...ROOM, peer: PEER }]);
      console.log(
        `auto startup: relay saw ${recorder.hellos.length} hello with no tool invocation`,
      );

      await shutdown();
      void harness;
    } finally {
      await relay.close();
    }
  });

  test("a non-interactive session neither connects at start nor may join", async () => {
    const recorder = recordingRelay();
    const relay = await ScriptedRelay.start(recorder.script);
    try {
      const written = layers({ port: relay.port, startup: "auto" });
      const harness = sessionHarness(written.projectRoot, "rpc");
      ompRelay(harness.api);

      await harness.handlers.get("session_start")?.({ type: "session_start" }, harness.ctx);
      const refused = await harness.mesh({ action: "join" });

      expect(relay.connections).toBe(0);
      expect(refused.details["reason"]).toBe("not_interactive");
      console.log(
        `rpc session: ${relay.connections} connection(s), join refused as ${refused.details["reason"]}`,
      );
    } finally {
      await relay.close();
    }
  });
});

describe("joining a live session", () => {
  test("a different task reconnects, and the relay sees the new room", async () => {
    const recorder = recordingRelay();
    const relay = await ScriptedRelay.start(recorder.script);
    try {
      const written = layers({ port: relay.port });
      const harness = sessionHarness(written.projectRoot);
      ompRelay(harness.api);
      await harness.handlers.get("session_start")?.({ type: "session_start" }, harness.ctx);

      await harness.mesh({ action: "join" });
      const rejoined = await harness.mesh({ action: "join", task: "pr-471-review" });

      expect(relay.connections).toBe(2);
      expect(relay.open).toBe(1);
      expect(recorder.hellos).toEqual([
        { ...ROOM, peer: PEER },
        { project: ROOM.project, task: "pr-471-review", peer: PEER },
      ]);
      expect(rejoined.content[0]?.text).toContain(`Joined ${ROOM.project}/pr-471-review`);
      expect(rejoined.details["sources"]).toEqual({
        project: "project-file",
        task: "parameter",
        peer: "global-file",
      });
      console.log(
        `rejoin: relay saw rooms ${recorder.hellos.map((h) => `${h.project}/${h.task}`).join(" then ")}; ${relay.open} connection open of ${relay.connections} accepted`,
      );

      await harness.handlers.get("session_shutdown")?.({ type: "session_shutdown" }, harness.ctx);
    } finally {
      await relay.close();
    }
  });

  test("the room already held is a no-op that does not close the socket", async () => {
    // Reconnecting would make this peer briefly vanish from every other roster
    // for no gain, so the identical case is answered from the live connection.
    const recorder = recordingRelay();
    const relay = await ScriptedRelay.start(recorder.script);
    try {
      const written = layers({ port: relay.port });
      const harness = sessionHarness(written.projectRoot);
      ompRelay(harness.api);
      await harness.handlers.get("session_start")?.({ type: "session_start" }, harness.ctx);

      await harness.mesh({ action: "join" });
      const again = await harness.mesh({ action: "join", task: ROOM.task });

      expect(relay.connections).toBe(1);
      expect(relay.open).toBe(1);
      expect(recorder.hellos).toHaveLength(1);
      expect(again.details["unchanged"]).toBe(true);
      expect(again.content[0]?.text).toStartWith("Already joined");
      console.log(
        `identical rejoin: ${recorder.hellos.length} hello total, unchanged=${again.details["unchanged"]}`,
      );

      await harness.handlers.get("session_shutdown")?.({ type: "session_shutdown" }, harness.ctx);
    } finally {
      await relay.close();
    }
  });

  test("two concurrent joins leave exactly one client", async () => {
    const recorder = recordingRelay();
    const relay = await ScriptedRelay.start(recorder.script);
    try {
      const written = layers({ port: relay.port });
      const harness = sessionHarness(written.projectRoot);
      ompRelay(harness.api);
      await harness.handlers.get("session_start")?.({ type: "session_start" }, harness.ctx);

      // Not awaited in turn: the generation counter exists for exactly this
      // window, and awaiting the first would close it.
      const [first, second] = await Promise.all([
        harness.mesh({ action: "join", task: "first-room" }),
        harness.mesh({ action: "join", task: "second-room" }),
      ]);

      const superseded = [first, second].filter((r) =>
        (r.content[0]?.text ?? "").includes("superseded"),
      );
      const succeeded = [first, second].filter((r) => r.details["action"] === "join" && r.details["status"] === undefined);

      expect(superseded).toHaveLength(1);
      expect(succeeded).toHaveLength(1);
      expect(relay.open).toBe(1);
      expect(recorder.hellos).toHaveLength(relay.connections);
      expect(succeeded[0]?.details["task"]).toBe("second-room");
      console.log(
        `concurrent joins: ${relay.connections} connection(s) accepted, ${relay.open} open; winner joined ${succeeded[0]?.details["task"]}, loser reported "${superseded[0]?.content[0]?.text}"`,
      );

      await harness.handlers.get("session_shutdown")?.({ type: "session_shutdown" }, harness.ctx);
    } finally {
      await relay.close();
    }
  });

  test("a routed send through the tool leaves a session entry beside the receipt", async () => {
    const recorder = recordingRelay();
    const relay = await ScriptedRelay.start(recorder.script);
    try {
      const written = layers({ port: relay.port });
      const harness = sessionHarness(written.projectRoot);
      ompRelay(harness.api);
      await harness.handlers.get("session_start")?.({ type: "session_start" }, harness.ctx);
      await harness.mesh({ action: "join" });

      const sent = await harness.mesh({
        action: "send",
        to: "alpha",
        message: "Run the suite and report.",
        reply_to: "message-3",
      });

      expect(sent.details["status"]).toBe("routed");
      expect(harness.calls.entries).toEqual([
        {
          customType: OUTBOUND_MESSAGE_TYPE,
          data: {
            id: sent.details["id"],
            to: "alpha",
            project: ROOM.project,
            task: ROOM.task,
            body: "Run the suite and report.",
            status: "routed",
            reply_to: "message-3",
          },
        },
      ]);
      console.log(`outbound entry persisted: ${JSON.stringify(harness.calls.entries[0])}`);

      await harness.handlers.get("session_shutdown")?.({ type: "session_shutdown" }, harness.ctx);
    } finally {
      await relay.close();
    }
  });
});

describe("the machine's purpose under automatic startup", () => {
  test("rides the first inbound message and is not repeated on the second", async () => {
    // There is no join result to carry it and no operator present at connect
    // time, so the moment work arrives is the moment the policy matters. Once
    // is enough: the text is then in the transcript.
    const purpose = "Run Linux builds here. Decline Windows work.";
    const recorder = recordingRelay();
    const relay = await ScriptedRelay.start(recorder.script);
    try {
      const { harness, shutdown } = await startAutoSession(relay, { purpose });
      await relay.awaitReceived(1);

      recorder.deliver({ ...PLAIN_INBOUND, id: "message-1" });
      await harness.calls.injected.until(1);
      recorder.deliver({ ...PLAIN_INBOUND, id: "message-2" });
      await harness.calls.injected.until(2);

      const first = String(harness.calls.userMessages[0]?.content);
      const second = String(harness.calls.userMessages[1]?.content);
      expect(first.split("\n").slice(0, 3)).toEqual([
        "This terminal's configured purpose, from its own operator:",
        purpose,
        "",
      ]);
      expect(second).not.toContain(purpose);
      expect(second.split("\n")[0]).toBe("Remote message from alpha");
      console.log(
        `auto session: message 1 carried the purpose preamble (${first.split("\n").length} lines), message 2 did not (${second.split("\n").length} lines)`,
      );

      await shutdown();
    } finally {
      await relay.close();
    }
  });

  test("no configured purpose injects no preamble", async () => {
    const recorder = recordingRelay();
    const relay = await ScriptedRelay.start(recorder.script);
    try {
      const { harness, shutdown } = await startAutoSession(relay);
      await relay.awaitReceived(1);

      recorder.deliver(PLAIN_INBOUND);
      await harness.calls.injected.until(1);

      const text = String(harness.calls.userMessages[0]?.content);
      expect(text.split("\n")[0]).toBe("Remote message from alpha");

      await shutdown();
    } finally {
      await relay.close();
    }
  });

  test("a manual join returns the purpose instead of injecting it later", async () => {
    const purpose = "Answer review requests only.";
    const recorder = recordingRelay();
    const relay = await ScriptedRelay.start(recorder.script);
    try {
      const written = layers({ port: relay.port, purpose });
      const harness = sessionHarness(written.projectRoot);
      ompRelay(harness.api);
      await harness.handlers.get("session_start")?.({ type: "session_start" }, harness.ctx);

      const joined = await harness.mesh({ action: "join" });
      expect(joined.details["purpose"]).toBe(purpose);

      recorder.deliver(PLAIN_INBOUND);
      await harness.calls.injected.until(1);
      const text = String(harness.calls.userMessages[0]?.content);
      expect(text).not.toContain(purpose);
      console.log(
        `manual startup: purpose delivered in the join result, and absent from the ${text.split("\n").length}-line inbound injection`,
      );

      await harness.handlers.get("session_shutdown")?.({ type: "session_shutdown" }, harness.ctx);
    } finally {
      await relay.close();
    }
  });
});

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
      const { harness, shutdown } = await startAutoSession(relay);
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
      const { harness, shutdown } = await startAutoSession(relay);
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
      const { harness, shutdown } = await startAutoSession(relay);
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
      const written = layers({ port: relay.port, startup: "auto" });
      const harness = sessionHarness(written.projectRoot);
      ompRelay(harness.api);

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
      const second = await startAutoSession(relay);
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
