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
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
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
  INBOUND_NOTICE_TYPE,
  OUTBOUND_ANNOUNCE_TYPE,
  OUTBOUND_MESSAGE_TYPE,
  type MeshToolResult,
} from "../../src/index.ts";
import { PROTOCOL_VERSION } from "../../src/protocol.ts";
import { FakeScheduler } from "../support/fake-scheduler.ts";
import {
  frameField,
  isFrame,
  ScriptedRelay,
  type RelaySession,
  type Script,
} from "../support/scripted-relay.ts";
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
      if (!isFrame(frame, "hello")) return;
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
  /**
   * Fires on every `ctx.ui.notify`, so a test can wait for a report the client
   * raised on its own instead of polling for it.
   */
  readonly notified: Signal<string>;
  readonly editorReads: string[];
  readonly editorWrites: string[];
  /** Replaces editor text as operator input or host behavior, without recording an extension write. */
  setEditorText(text: string): void;
  editorText(): string;
  messageStart(content: unknown, role?: string): Promise<void>;
  pendingTimeouts(): number;
  runNextTimeout(): void;
  /** Changes what `ctx.isIdle()` reports to an inbound-delivery handler. */
  setIdle(idle: boolean): void;
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
  const notified = new Signal<string>();
  let execute: ToolExecute | null = null;
  let idle = true;
  const editorReads: string[] = [];
  const editorWrites: string[] = [];
  const deferredTimeouts: Array<() => void> = [];
  let editorText = "";

  // Mirrors only the builder methods this extension calls, so a schema the real
  // facade cannot build fails registration here rather than at load time.
  const chain = {
    describe() {
      return chain;
    },
    optional() {
      return chain;
    },
    int() {
      return chain;
    },
    nonnegative() {
      return chain;
    },
  };
  const api = {
    pi: {
      getAgentDir() {
        return process.env[AGENT_DIR_ENV] ?? join(cwd, ".omp-relay-test-agent");
      },
    },
    zod: {
      enum() {
        return chain;
      },
      string() {
        return chain;
      },
      number() {
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
    isIdle() {
      return idle;
    },
    mode,
    cwd,
    ui: {
      notify(message: string) {
        notifications.push(message);
        notified.fire(message);
      },
      getEditorText() {
        editorReads.push(editorText);
        return editorText;
      },
      setEditorText(text: string) {
        editorWrites.push(text);
        editorText = text;
      },
    },
    setTimeout(callback: () => void, milliseconds: number) {
      contextTimers.timeouts.push(milliseconds);
      if (milliseconds === 0) {
        deferredTimeouts.push(callback);
        return 0;
      }
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
    notified,
    editorReads,
    editorWrites,
    setEditorText(text) {
      editorText = text;
    },
    editorText() {
      return editorText;
    },
    async messageStart(content, role = "user") {
      const handler = handlers.get("message_start");
      if (handler === undefined) throw new Error("the extension registered no message_start handler");
      await handler({ type: "message_start", message: { role, content } }, ctx);
    },
    pendingTimeouts() {
      return deferredTimeouts.length;
    },
    runNextTimeout() {
      const callback = deferredTimeouts.shift();
      if (callback === undefined) throw new Error("no deferred context timeout");
      callback();
    },
    setIdle(value) {
      idle = value;
    },
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

/** What the global layer says. The only layer a test ever rewrites in place. */
interface GlobalLayer {
  readonly port: number;
  readonly startup?: "manual" | "auto";
  readonly purpose?: string;
}

/** Writes the global layer into `agentDir`, replacing whatever is there. */
function writeGlobal(agentDir: string, options: GlobalLayer): void {
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
}

function layers(
  options: GlobalLayer & {
    readonly task?: string;
    /**
     * Whether to write the project layer at all. Omitting it is a checkout that
     * never committed one, which is what leaves `auto` startup with no room.
     */
    readonly projectFile?: boolean;
  },
): Layers {
  const agentDir = mkdtempSync(join(tmpdir(), "omp-relay-agent-"));
  const projectRoot = mkdtempSync(join(tmpdir(), "omp-relay-root-"));
  writeGlobal(agentDir, options);
  if (options.projectFile !== false) {
    mkdirSync(join(projectRoot, ".omp"), { recursive: true });
    writeFileSync(
      projectConfigPath(projectRoot),
      ["room:", `  project: ${ROOM.project}`, `  task: ${options.task ?? ROOM.task}`, ""].join("\n"),
      "utf8",
    );
  }

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

/** One inbound room announcement, with the text and entry it must produce. */
const NOTICE = {
  type: "notice",
  id: "announcement-7",
  from: "alpha",
  body: "The schema has landed.",
  reply_to: "message-42",
} as const;

const PLAIN_NOTICE = {
  type: "notice",
  id: "announcement-8",
  from: "alpha",
  body: "No reply reference.",
} as const;

const NOTICE_TEXT = [
  "Room announcement from alpha, addressed to everyone in this room",
  `Project: ${ROOM.project}`,
  `Task: ${ROOM.task}`,
  "Announcement ID: announcement-7",
  "Reply to: message-42",
  "",
  "> The schema has landed.",
].join("\n");

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
 * A relay that admits every connection, answers `list`, accepts announcements,
 * and can deliver server frames on demand.
 *
 * `list` is answered because a join is not complete until the roster comes
 * back, so every join-driven test needs it; hellos and outbound frames are
 * retained because observing the far side is stronger than observing only the
 * extension's own result.
 */
interface Recorder {
  readonly script: Script;
  readonly hellos: Array<{ project: string; task: string; peer: string }>;
  readonly sends: unknown[];
  readonly announcements: unknown[];
  /** Delivers a frame on the most recent connection. */
  deliver(frame: Parameters<RelaySession["send"]>[0]): void;
  peers: readonly string[];
}

function recordingRelay(options: { deliverOnReady?: readonly unknown[] } = {}): Recorder {
  const hellos: Array<{ project: string; task: string; peer: string }> = [];
  const sends: unknown[] = [];
  const announcements: unknown[] = [];
  let latest: RelaySession | null = null;
  const recorder: Recorder = {
    hellos,
    sends,
    announcements,
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
      if (type === "announce") {
        announcements.push(frame);
        session.send({
          type: "accepted",
          id: String(frameField(frame, "id")),
          delivered: 2,
          shed: 0,
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
  if (!isFrame(frame, "hello")) return;
  session.send({ type: "ready", protocol: PROTOCOL_VERSION });
  session.send(INBOUND);
};

/**
 * Starts an `auto` session against a scripted relay.
 *
 * The layers are returned as well, because the startup mode is re-read on every
 * join: a test that flips the file mid-session needs the directory it is in.
 */
async function startAutoSession(
  relay: ScriptedRelay,
  options: { purpose?: string; idle?: boolean } = {},
): Promise<{ harness: SessionHarness; shutdown: () => Promise<void>; written: Layers }> {
  const written = layers({
    port: relay.port,
    startup: "auto",
    ...(options.purpose === undefined ? {} : { purpose: options.purpose }),
  });
  const harness = sessionHarness(written.projectRoot);
  if (options.idle !== undefined) harness.setIdle(options.idle);
  ompRelay(harness.api);

  await harness.handlers.get("session_start")?.({ type: "session_start" }, harness.ctx);
  return {
    harness,
    written,
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

  test("renaming the peer reconnects and the relay reports the new name", async () => {
    // The sibling of the changed-task case, and the one the capability states
    // separately: `as` is the other half of what `join` may change, and the
    // roster the relay answers with is where the new name has to appear.
    const recorder = recordingRelay();
    const relay = await ScriptedRelay.start(recorder.script);
    try {
      const written = layers({ port: relay.port });
      const harness = sessionHarness(written.projectRoot);
      ompRelay(harness.api);
      await harness.handlers.get("session_start")?.({ type: "session_start" }, harness.ctx);

      await harness.mesh({ action: "join" });
      recorder.peers = ["second-terminal"];
      const renamed = await harness.mesh({ action: "join", as: "second-terminal" });

      expect(relay.connections).toBe(2);
      expect(relay.open).toBe(1);
      // The room is unchanged; only the identity registered under it moved.
      expect(recorder.hellos).toEqual([
        { ...ROOM, peer: PEER },
        { ...ROOM, peer: "second-terminal" },
      ]);
      expect(renamed.details["unchanged"]).toBe(false);
      expect(renamed.details["peer"]).toBe("second-terminal");
      expect(renamed.details["peers"]).toEqual(["second-terminal"]);
      expect(renamed.details["sources"]).toEqual({
        project: "project-file",
        task: "project-file",
        peer: "parameter",
      });
      console.log(
        `rename: relay saw peers ${recorder.hellos.map((h) => h.peer).join(" then ")}; roster after rename ${JSON.stringify(renamed.details["peers"])}`,
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

  test("an announcement through the tool leaves its own session entry", async () => {
    const recorder = recordingRelay();
    const relay = await ScriptedRelay.start(recorder.script);
    try {
      const written = layers({ port: relay.port });
      const harness = sessionHarness(written.projectRoot);
      ompRelay(harness.api);
      await harness.handlers.get("session_start")?.({ type: "session_start" }, harness.ctx);
      await harness.mesh({ action: "join" });

      const announced = await harness.mesh({
        action: "announce",
        message: "The schema has landed.",
        reply_to: "message-42",
      });

      expect(announced.details["delivered"]).toBe(2);
      expect(announced.details["shed"]).toBe(0);
      expect(recorder.announcements).toHaveLength(1);
      expect(frameField(recorder.announcements[0], "to")).toBeUndefined();
      expect(harness.calls.entries).toEqual([
        {
          customType: OUTBOUND_ANNOUNCE_TYPE,
          data: {
            id: announced.details["id"],
            project: ROOM.project,
            task: ROOM.task,
            body: "The schema has landed.",
            delivered: 2,
            shed: 0,
            reply_to: "message-42",
          },
        },
      ]);

      await harness.handlers.get("session_shutdown")?.(
        { type: "session_shutdown" },
        harness.ctx,
      );
    } finally {
      await relay.close();
    }
  });

  test("a rejoin after the relay displaced this peer reconnects", async () => {
    // `peer_replaced` is terminal by design: the displaced client stops for
    // good rather than fighting for the name. The identical rejoin is then the
    // operator's one recovery move, and answering it from the retained object
    // would report an open connection while nothing is connected and nothing
    // is trying to be.
    const recorder = recordingRelay();
    const relay = await ScriptedRelay.start(recorder.script);
    try {
      const written = layers({ port: relay.port });
      const harness = sessionHarness(written.projectRoot);
      ompRelay(harness.api);
      await harness.handlers.get("session_start")?.({ type: "session_start" }, harness.ctx);
      await harness.mesh({ action: "join" });

      recorder.deliver({
        type: "error",
        code: "peer_replaced",
        message: "a newer connection registered this peer name",
      });
      // The client's own report, raised where it abandons the connection for
      // good, so this waits on the state change rather than on a sleep.
      await harness.notified.until(1);
      expect(harness.notifications).toEqual([PEER_REPLACED_REPORT]);

      const again = await harness.mesh({ action: "join", task: ROOM.task });

      // The decisive assertion: the relay saw a second `hello`, so the room was
      // rejoined rather than reported from a client that had stopped.
      expect(relay.connections).toBe(2);
      expect(recorder.hellos).toEqual([
        { ...ROOM, peer: PEER },
        { ...ROOM, peer: PEER },
      ]);
      expect(again.details["unchanged"]).toBe(false);
      expect(again.details["status"]).toBeUndefined();
      expect(again.content[0]?.text).toStartWith(
        `Joined ${ROOM.project}/${ROOM.task} as ${PEER}`,
      );
      console.log(
        `after displacement: relay saw ${recorder.hellos.length} hello(s), rejoin unchanged=${again.details["unchanged"]}, ${relay.open} connection open`,
      );

      await harness.handlers.get("session_shutdown")?.({ type: "session_shutdown" }, harness.ctx);
    } finally {
      await relay.close();
    }
  });
});

describe("a join the relay never answered", () => {
  test("reports an unconfirmed connection rather than a completed join", async () => {
    // The common first run: the operator has not started the relay yet. The
    // connect is refused, the roster request fails with it, and a first line
    // saying `Joined` is read as permission to send.
    const listener = await ScriptedRelay.start(() => {});
    const port = listener.port;
    await listener.close();

    const written = layers({ port });
    const harness = sessionHarness(written.projectRoot);
    ompRelay(harness.api);
    await harness.handlers.get("session_start")?.({ type: "session_start" }, harness.ctx);

    try {
      const joined = await harness.mesh({ action: "join" });
      const lines = (joined.content[0]?.text ?? "").split("\n");

      // Neither headline that asserts a join happened.
      expect(lines[0]).not.toStartWith("Joined ");
      expect(lines[0]).not.toStartWith("Already joined ");
      expect(lines[0]).toContain("has not confirmed");
      expect(joined.details["status"]).toBe("unconfirmed");
      expect(String(joined.details["roster_failure"])).toContain("roster request");

      // What was resolved is still reported: which room this client will be in
      // once the relay is up is exactly what the operator has to be able to see.
      expect(joined.details["project"]).toBe(ROOM.project);
      expect(joined.details["task"]).toBe(ROOM.task);
      expect(joined.details["peer"]).toBe(PEER);
      expect(joined.details["sources"]).toEqual({
        project: "project-file",
        task: "project-file",
        peer: "global-file",
      });
      console.log(`unreachable relay: status=${joined.details["status"]}, first line "${lines[0]}"`);
    } finally {
      await harness.handlers.get("session_shutdown")?.({ type: "session_shutdown" }, harness.ctx);
    }
  });

  test("a roster the relay refused reports an unknown roster, not an unconfirmed join", async () => {
    // `ready` arrived, so the relay registered this peer and a send can go out;
    // only the roster is missing. Reporting that as an unconfirmed join sends
    // the caller to recover a connection that is fine, instead of retrying the
    // one request that failed.
    const relay = await ScriptedRelay.start((frame, session) => {
      const type = frameField(frame, "type");
      if (type === "hello") {
        session.send({ type: "ready", protocol: PROTOCOL_VERSION });
        return;
      }
      if (type === "list") {
        // `wire-protocol` obliges the relay to echo the correlation token of a
        // recoverable rejection, so this settles the roster request and leaves
        // the connection exactly as ready as it already was.
        session.send({
          type: "error",
          code: "malformed_frame",
          message: "the roster is unavailable",
          request_id: String(frameField(frame, "request_id")),
        });
      }
    });
    try {
      const written = layers({ port: relay.port });
      const harness = sessionHarness(written.projectRoot);
      ompRelay(harness.api);
      await harness.handlers.get("session_start")?.({ type: "session_start" }, harness.ctx);

      const joined = await harness.mesh({ action: "join" });
      const lines = (joined.content[0]?.text ?? "").split("\n");

      expect(lines[0]).toStartWith(`Joined ${ROOM.project}/${ROOM.task} as ${PEER}`);
      expect(lines[0]).not.toContain("has not confirmed");
      expect(joined.details["status"]).toBe("roster_unknown");
      expect(String(joined.details["roster_failure"])).toContain("roster request");
      // The roster is still reported as unknown rather than as an empty room.
      expect(joined.content[0]?.text).toContain("The roster is unknown");
      console.log(
        `ready connection, refused roster: status=${joined.details["status"]}, first line "${lines[0]}"`,
      );

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

  test("a deferred notice leaves the purpose for the next turn-starting delivery", async () => {
    const purpose = "Run Linux builds here. Decline Windows work.";
    const recorder = recordingRelay();
    const relay = await ScriptedRelay.start(recorder.script);
    try {
      const { harness, shutdown } = await startAutoSession(relay, {
        purpose,
        idle: false,
      });
      await relay.awaitReceived(1);

      recorder.deliver(NOTICE);
      await harness.calls.injected.until(1);

      const deferred = String(harness.calls.userMessages[0]?.content);
      expect(harness.calls.userMessages[0]?.options).toEqual({ deliverAs: "followUp" });
      expect(deferred).not.toContain(purpose);

      // The run has finished. A directed delivery starts or steers the next turn,
      // so this is where the still-owed preamble belongs.
      harness.setIdle(true);
      recorder.deliver({ ...PLAIN_INBOUND, id: "message-after-notice" });
      await harness.calls.injected.until(2);

      const starting = String(harness.calls.userMessages[1]?.content);
      expect(harness.calls.userMessages[1]?.options).toEqual({ deliverAs: "steer" });
      expect(starting.split("\n").slice(0, 3)).toEqual([
        "This terminal's configured purpose, from its own operator:",
        purpose,
        "",
      ]);

      await shutdown();
    } finally {
      await relay.close();
    }
  });

  test("an idle notice may carry the purpose, and a later join does not repeat it", async () => {
    const purpose = "Run Linux builds here. Decline Windows work.";
    const recorder = recordingRelay();
    const relay = await ScriptedRelay.start(recorder.script);
    try {
      const { harness, shutdown, written } = await startAutoSession(relay, {
        purpose,
        idle: true,
      });
      await relay.awaitReceived(1);

      recorder.deliver(NOTICE);
      await harness.calls.injected.until(1);

      const injected = String(harness.calls.userMessages[0]?.content);
      expect(harness.calls.userMessages[0]?.options).toEqual({ deliverAs: "steer" });
      expect(injected).toContain(purpose);

      // Flip to manual so the join result is a channel that *could* carry the
      // purpose. It must not: the notice already delivered it to this session.
      writeGlobal(written.agentDir, {
        port: relay.port,
        startup: "manual",
        purpose,
      });
      const joined = await harness.mesh({ action: "join" });
      expect(joined.content[0]?.text).not.toContain(purpose);

      recorder.deliver({ ...PLAIN_INBOUND, id: "message-after-join" });
      await harness.calls.injected.until(2);
      expect(String(harness.calls.userMessages[1]?.content)).not.toContain(purpose);

      await shutdown();
    } finally {
      await relay.close();
    }
  });

  test("a rejoin does not re-owe the purpose the session was already paid", async () => {
    // "Once per session", not once per connection. A `join` opens a new
    // connection within the same session, and arming the debt at connect made
    // the same operator text arrive a second time -- which the second
    // assertion above cannot see, because it never rejoins.
    const purpose = "Run Linux builds here. Decline Windows work.";
    const recorder = recordingRelay();
    const relay = await ScriptedRelay.start(recorder.script);
    try {
      const { harness, shutdown } = await startAutoSession(relay, { purpose });
      await relay.awaitReceived(1);

      recorder.deliver({ ...PLAIN_INBOUND, id: "message-1" });
      await harness.calls.injected.until(1);

      await harness.mesh({ action: "join", task: "another-room" });
      expect(relay.connections).toBe(2);

      recorder.deliver({ ...PLAIN_INBOUND, id: "message-2" });
      await harness.calls.injected.until(2);

      const carried = harness.calls.userMessages.map((entry) =>
        String(entry.content).includes(purpose),
      );
      expect(carried).toEqual([true, false]);
      console.log(
        `across a rejoin (${relay.connections} connections, 1 session): purpose carried by message ${carried.map((c, i) => (c ? i + 1 : null)).filter((v) => v !== null).join(", ")} only`,
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

  test("a join recovering a failed auto start still owes the session's purpose", async () => {
    // The plausible failure this covers: an `auto` machine in a checkout that
    // never committed a project file. Session start resolves no room, so the
    // first connection the session gets is the one the operator's parameterized
    // join opens -- and the purpose has still never been delivered.
    const purpose = "Run Linux builds here. Decline Windows work.";
    const recorder = recordingRelay();
    const relay = await ScriptedRelay.start(recorder.script);
    try {
      const written = layers({
        port: relay.port,
        startup: "auto",
        purpose,
        projectFile: false,
      });
      const harness = sessionHarness(written.projectRoot);
      ompRelay(harness.api);
      await harness.handlers.get("session_start")?.({ type: "session_start" }, harness.ctx);

      // The precondition, asserted rather than assumed: auto startup resolved
      // nothing and opened no socket. The project derives from the root's
      // directory name, so the task is the half with no source.
      expect(relay.connections).toBe(0);
      expect(harness.notifications[0]).toContain("room.task");

      const joined = await harness.mesh({
        action: "join",
        project: ROOM.project,
        task: ROOM.task,
      });
      expect(joined.details["status"]).toBeUndefined();
      // Under `auto` the result does not carry the text; the first message does.
      expect(joined.details["purpose"]).toBeUndefined();

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
      console.log(
        `auto start that resolved no room: the recovering join's session carried the purpose on message 1 (${first.split("\n").length} lines) and not on message 2 (${second.split("\n").length} lines)`,
      );

      await harness.handlers.get("session_shutdown")?.({ type: "session_shutdown" }, harness.ctx);
    } finally {
      await relay.close();
    }
  });

  test("a purpose the join result carried is not re-owed when the mode flips to auto", async () => {
    // "Once per session" counts deliveries, not connections: the manual join
    // handed this text to the caller that asked for it, so flipping the machine
    // to `auto` and rejoining must not queue it again for the first message.
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

      writeGlobal(written.agentDir, { port: relay.port, startup: "auto", purpose });
      await harness.mesh({ action: "join", task: "another-room" });

      recorder.deliver(PLAIN_INBOUND);
      await harness.calls.injected.until(1);

      const text = String(harness.calls.userMessages[0]?.content);
      expect(text).not.toContain(purpose);
      expect(text.split("\n")[0]).toBe("Remote message from alpha");
      console.log(
        `mode flipped to auto after a manual join carried the purpose: the ${text.split("\n").length}-line injection repeats none of it`,
      );

      await harness.handlers.get("session_shutdown")?.({ type: "session_shutdown" }, harness.ctx);
    } finally {
      await relay.close();
    }
  });

  test("flipping to manual before the first inbound pays the purpose exactly once", async () => {
    // The debt an `auto` start armed is owed to the first inbound message. An
    // operator who flips the file to `manual` moves that obligation to the join
    // result, and leaving both standing paid the same operator text twice --
    // the one thing "once per session" forbids.
    const purpose = "Run Linux builds here. Decline Windows work.";
    const recorder = recordingRelay();
    const relay = await ScriptedRelay.start(recorder.script);
    try {
      const { harness, shutdown, written } = await startAutoSession(relay, { purpose });
      // A join whose roster came back is the readiness barrier: the no-op branch
      // this test needs is reachable only from a `ready` client.
      await harness.mesh({ action: "join" });
      const hellos = recorder.hellos.length;

      writeGlobal(written.agentDir, { port: relay.port, startup: "manual", purpose });
      const joined = await harness.mesh({ action: "join" });

      // The path under test: nothing changed, so no handshake and no `connect`.
      expect(recorder.hellos).toHaveLength(hellos);
      expect(joined.details["unchanged"]).toBe(true);
      const inResult = joined.details["purpose"] === purpose;

      recorder.deliver(PLAIN_INBOUND);
      await harness.calls.injected.until(1);
      const inMessage = String(harness.calls.userMessages[0]?.content).includes(purpose);

      expect([inResult, inMessage]).toEqual([true, false]);
      console.log(
        `auto then manual on an identical rejoin: join result carried the purpose: ${inResult}; first inbound ALSO carried the purpose: ${inMessage}; deliveries in one session: ${[inResult, inMessage].filter(Boolean).length}`,
      );

      await shutdown();
    } finally {
      await relay.close();
    }
  });

  test("flipping to auto owes the purpose even when the rejoin changes nothing", async () => {
    // The mirror. Arming at `connect` covered no identical rejoin, because that
    // is the one join that opens no connection -- so a machine the operator
    // turned on mid-session, stating why in the same edit, was never told.
    const purpose = "Run Linux builds here. Decline Windows work.";
    const recorder = recordingRelay();
    const relay = await ScriptedRelay.start(recorder.script);
    try {
      // Manual and silent: nothing has been owed or paid yet.
      const written = layers({ port: relay.port });
      const harness = sessionHarness(written.projectRoot);
      ompRelay(harness.api);
      await harness.handlers.get("session_start")?.({ type: "session_start" }, harness.ctx);

      const first = await harness.mesh({ action: "join" });
      expect(first.details["purpose"]).toBeUndefined();

      writeGlobal(written.agentDir, { port: relay.port, startup: "auto", purpose });
      const again = await harness.mesh({ action: "join" });

      expect(recorder.hellos).toHaveLength(1);
      expect(again.details["unchanged"]).toBe(true);
      // Under `auto` the result must not carry it; the first message must.
      expect(again.details["purpose"]).toBeUndefined();

      recorder.deliver(PLAIN_INBOUND);
      await harness.calls.injected.until(1);
      const text = String(harness.calls.userMessages[0]?.content);
      expect(text.split("\n").slice(0, 3)).toEqual([
        "This terminal's configured purpose, from its own operator:",
        purpose,
        "",
      ]);
      console.log(
        `manual then auto on an identical rejoin (${recorder.hellos.length} hello, unchanged=${again.details["unchanged"]}): the first inbound carried the purpose`,
      );

      await harness.handlers.get("session_shutdown")?.({ type: "session_shutdown" }, harness.ctx);
    } finally {
      await relay.close();
    }
  });

  test("a manual join superseded before it reported leaves the purpose owed", async () => {
    // The delivery is the report, not the resolution. A `manual` join that
    // resolved the text and was then superseded before it could return handed
    // it to nobody, so recording the delivery at resolution lost it for the
    // rest of the session: the join that actually landed saw a debt already
    // paid and armed nothing.
    const purpose = "Run Linux builds here. Decline Windows work.";
    const recorder = recordingRelay();
    // The first roster request is withheld, which is the window the second join
    // supersedes the first in. It is settled all the same, and without a timer,
    // by the stop the winning join performs on the connection it replaces.
    let rosterRequests = 0;
    const relay = await ScriptedRelay.start((frame, session) => {
      if (frameField(frame, "type") === "list") {
        rosterRequests += 1;
        if (rosterRequests === 1) return;
      }
      recorder.script(frame, session);
    });
    try {
      const written = layers({ port: relay.port, purpose });
      const harness = sessionHarness(written.projectRoot);
      ompRelay(harness.api);
      await harness.handlers.get("session_start")?.({ type: "session_start" }, harness.ctx);

      // Not awaited: the hello and the roster request are the two frames that
      // prove it is parked inside the window, past resolving the purpose.
      const parked = harness.mesh({ action: "join" });
      await relay.awaitReceived(2);

      // The operator flips the machine to `auto` and joins another room, which
      // stops the connection the parked join is still waiting on.
      writeGlobal(written.agentDir, { port: relay.port, startup: "auto", purpose });
      const [superseded, winner] = await Promise.all([
        parked,
        harness.mesh({ action: "join", task: "second-room" }),
      ]);

      expect(String(superseded.content[0]?.text)).toContain("superseded");
      // Neither report carried the text: the loser returned nothing, and `auto`
      // never carries it. So it is still the session's debt, owed to the first
      // message the connection that survived receives -- exactly once.
      expect(superseded.details["purpose"]).toBeUndefined();
      expect(winner.details["status"]).toBeUndefined();
      expect(winner.details["purpose"]).toBeUndefined();

      recorder.deliver({ ...PLAIN_INBOUND, id: "message-1" });
      await harness.calls.injected.until(1);
      recorder.deliver({ ...PLAIN_INBOUND, id: "message-2" });
      await harness.calls.injected.until(2);
      const [paid, next] = harness.calls.userMessages.map((entry) => String(entry.content));

      expect(paid?.split("\n").slice(0, 3)).toEqual([
        "This terminal's configured purpose, from its own operator:",
        purpose,
        "",
      ]);
      expect(next).not.toContain(purpose);
      console.log(
        `manual join superseded at the roster (${relay.connections} connections, loser reported "${superseded.content[0]?.text}"): no report carried the purpose, and the surviving auto connection paid it on message 1 only`,
      );

      await harness.handlers.get("session_shutdown")?.({ type: "session_shutdown" }, harness.ctx);
    } finally {
      await relay.close();
    }
  });

  test("a purpose an inbound message already paid is not re-carried by a later manual join", async () => {
    // The mirror of the flip covered above, and the last path out of "once per
    // session" (spec.md: "The machine's purpose reaches the agent once per
    // session"). The `auto` debt was paid by the first message; an operator who
    // then flips the file to `manual` and rejoins must not be handed the same
    // operator text a second time as a tool result. The manual branch therefore
    // consults the flag it does not set.
    const purpose = "Run Linux builds here. Decline Windows work.";
    const recorder = recordingRelay();
    const relay = await ScriptedRelay.start(recorder.script);
    try {
      const { harness, shutdown, written } = await startAutoSession(relay, { purpose });
      await relay.awaitReceived(1);

      recorder.deliver({ ...PLAIN_INBOUND, id: "message-1" });
      await harness.calls.injected.until(1);
      const inMessage = String(harness.calls.userMessages[0]?.content).includes(purpose);

      writeGlobal(written.agentDir, { port: relay.port, startup: "manual", purpose });
      const joined = await harness.mesh({ action: "join" });
      const inResult = joined.details["purpose"] === purpose;

      // Nothing reconnected, so this is the same session and the same debt.
      expect(recorder.hellos).toHaveLength(1);
      expect(joined.details["unchanged"]).toBe(true);
      expect([inMessage, inResult]).toEqual([true, false]);
      console.log(
        `auto paid then flipped to manual (${recorder.hellos.length} hello, unchanged=${joined.details["unchanged"]}): first inbound carried the purpose: ${inMessage}; the later manual join result ALSO carried it: ${inResult}; deliveries in one session: ${[inMessage, inResult].filter(Boolean).length}`,
      );

      await shutdown();
    } finally {
      await relay.close();
    }
  });

  test("a superseded join does not erase the purpose the live connection still owes", async () => {
    // The same defect in the write rather than the read: the `manual` branch
    // used to clear the debt at resolution, and the join that resolved may not
    // be the join that wins. Arming under `auto` is idempotent, so a loser
    // doing it is harmless; erasing a debt is not, and here the loser is the
    // only join that ever reconciled — the join that superseded it failed to
    // resolve a room, so it reconciled nothing, and the connection both of them
    // left running still owed the text.
    const purpose = "Run Linux builds here. Decline Windows work.";
    const recorder = recordingRelay();
    let held: { readonly frame: unknown; readonly session: RelaySession } | null = null;
    const relay = await ScriptedRelay.start(recorder.script);
    try {
      const { harness, written } = await startAutoSession(relay, { purpose });
      // A join whose roster came back is the readiness barrier: the no-op branch
      // this test needs is reachable only from a `ready` client.
      await harness.mesh({ action: "join" });

      // From here the roster is withheld rather than refused, so the next join
      // parks inside its own generation window. It is released by hand below:
      // nothing else will settle it, because the join that supersedes it stops
      // no connection.
      relay.rescript((frame, session) => {
        if (frameField(frame, "type") === "list" && held === null) {
          held = { frame, session };
          return;
        }
        recorder.script(frame, session);
      });

      writeGlobal(written.agentDir, { port: relay.port, startup: "manual", purpose });
      const parkedAt = relay.received.length + 1;
      const parked = harness.mesh({ action: "join" });
      await relay.awaitReceived(parkedAt);

      // The superseding join resolves nothing: the room it would have needed is
      // no longer committed. It still took the generation, which is the whole
      // race -- it bumps the counter before it reads a single file.
      rmSync(projectConfigPath(written.projectRoot));
      const failed = await harness.mesh({ action: "join" });
      expect(failed.details["status"]).toBe("failed");

      if (held === null) throw new Error("no roster request was withheld");
      const withheld = held as { readonly frame: unknown; readonly session: RelaySession };
      withheld.session.send({
        type: "peers",
        request_id: String(frameField(withheld.frame, "request_id")),
        peers: [PEER],
      });
      const superseded = await parked;
      expect(String(superseded.content[0]?.text)).toContain("superseded");
      expect(superseded.details["purpose"]).toBeUndefined();

      // Nothing reconnected and nothing was reported, so the debt is exactly
      // where it was: owed to the next message this connection receives.
      recorder.deliver({ ...PLAIN_INBOUND, id: "message-1" });
      await harness.calls.injected.until(1);
      const text = String(harness.calls.userMessages[0]?.content);
      expect(text.split("\n").slice(0, 3)).toEqual([
        "This terminal's configured purpose, from its own operator:",
        purpose,
        "",
      ]);
      console.log(
        `manual no-op join superseded by a join that resolved nothing (${relay.connections} connections, loser reported "${superseded.content[0]?.text}"): the surviving connection still paid the purpose on its next message`,
      );

      await harness.handlers.get("session_shutdown")?.({ type: "session_shutdown" }, harness.ctx);
    } finally {
      await relay.close();
    }
  });

  test("a purpose paid while a manual join waited is not handed back by its result", async () => {
    // The defect in the read: the text a `manual` join returns used to be
    // decided at resolution and then carried across the roster await. An
    // inbound message arriving inside that window pays the debt, so the result
    // that settles afterwards hands back text this session has already been
    // given -- two deliveries, which is the one thing "once per session"
    // forbids. The flag is therefore read at the instant the report is built.
    const purpose = "Run Linux builds here. Decline Windows work.";
    const recorder = recordingRelay();
    let held: { readonly frame: unknown; readonly session: RelaySession } | null = null;
    const relay = await ScriptedRelay.start(recorder.script);
    try {
      const { harness, shutdown, written } = await startAutoSession(relay, { purpose });
      // A join whose roster came back is the readiness barrier: the no-op branch
      // this test needs is reachable only from a `ready` client.
      await harness.mesh({ action: "join" });

      // The next roster answer is withheld, which parks the join under test
      // exactly where the inbound message can overtake it.
      relay.rescript((frame, session) => {
        if (frameField(frame, "type") === "list" && held === null) {
          held = { frame, session };
          return;
        }
        recorder.script(frame, session);
      });

      writeGlobal(written.agentDir, { port: relay.port, startup: "manual", purpose });
      const parkedAt = relay.received.length + 1;
      const parked = harness.mesh({ action: "join" });
      await relay.awaitReceived(parkedAt);

      // Paid, while the join that would have paid it is still waiting. The
      // injection is what proves the handler ran and took the debt with it.
      recorder.deliver({ ...PLAIN_INBOUND, id: "message-1" });
      await harness.calls.injected.until(1);
      const inMessage = String(harness.calls.userMessages[0]?.content).includes(purpose);

      if (held === null) throw new Error("no roster request was withheld");
      const withheld = held as { readonly frame: unknown; readonly session: RelaySession };
      withheld.session.send({
        type: "peers",
        request_id: String(frameField(withheld.frame, "request_id")),
        peers: [PEER],
      });
      const joined = await parked;
      const inResult = joined.details["purpose"] === purpose;

      // A successful no-op join: it reported, so it would have committed the
      // delivery -- and it must find there is nothing left to deliver.
      expect(joined.details["status"]).toBeUndefined();
      expect(joined.details["unchanged"]).toBe(true);
      expect([inMessage, inResult]).toEqual([true, false]);
      console.log(
        `inbound overtook a parked manual join (unchanged=${joined.details["unchanged"]}): message carried the purpose: ${inMessage}; the join result ALSO carried it: ${inResult}; deliveries in one session: ${[inMessage, inResult].filter(Boolean).length}`,
      );

      await shutdown();
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

  test("an idle notice starts a turn without entering prompt preprocessing", async () => {
    const recorder = recordingRelay({ deliverOnReady: [NOTICE] });
    const relay = await ScriptedRelay.start(recorder.script);
    try {
      const { harness, shutdown } = await startAutoSession(relay, { idle: true });
      await harness.calls.injected.until(1);

      // Explicit `steer`, even while idle. Omitting `deliverAs` would enter
      // `AgentSession.prompt()`, whose non-streaming path auto-reads `@filepath`
      // mentions (agent-session.ts:6045-6056 in OMP 18.0.11). A queued steer
      // schedules the idle drain and may resume from any transcript tail
      // (agent-session.ts:6474-6506, 6515-6521), so it still starts a turn.
      expect(harness.calls.userMessages).toEqual([
        { content: NOTICE_TEXT, options: { deliverAs: "steer" } },
      ]);
      expect(harness.calls.entries).toEqual([
        {
          customType: INBOUND_NOTICE_TYPE,
          data: {
            id: "announcement-7",
            from: "alpha",
            project: ROOM.project,
            task: ROOM.task,
            body: "The schema has landed.",
            reply_to: "message-42",
          },
        },
      ]);

      await shutdown();
    } finally {
      await relay.close();
    }
  });

  test("a notice while streaming is deferred rather than steered or aborted", async () => {
    const recorder = recordingRelay({ deliverOnReady: [NOTICE] });
    const relay = await ScriptedRelay.start(recorder.script);
    try {
      const { harness, shutdown } = await startAutoSession(relay, { idle: false });
      await harness.calls.injected.until(1);

      expect(harness.calls.userMessages).toEqual([
        { content: NOTICE_TEXT, options: { deliverAs: "followUp" } },
      ]);
      expect(harness.calls.customMessages).toEqual([]);

      await shutdown();
    } finally {
      await relay.close();
    }
  });

  test("several notices during one run remain separate follow-up messages", async () => {
    const recorder = recordingRelay({ deliverOnReady: [NOTICE, PLAIN_NOTICE] });
    const relay = await ScriptedRelay.start(recorder.script);
    try {
      const { harness, shutdown } = await startAutoSession(relay, { idle: false });
      await harness.calls.injected.until(2);

      expect(harness.calls.userMessages).toHaveLength(2);
      expect(harness.calls.userMessages.map((call) => call.options)).toEqual([
        { deliverAs: "followUp" },
        { deliverAs: "followUp" },
      ]);
      expect(harness.calls.userMessages.map((call) => String(call.content))).toEqual([
        NOTICE_TEXT,
        [
          "Room announcement from alpha, addressed to everyone in this room",
          `Project: ${ROOM.project}`,
          `Task: ${ROOM.task}`,
          "Announcement ID: announcement-8",
          "",
          "> No reply reference.",
        ].join("\n"),
      ]);
      expect(harness.calls.entries.map((entry) => entry.customType)).toEqual([
        INBOUND_NOTICE_TYPE,
        INBOUND_NOTICE_TYPE,
      ]);

      await shutdown();
    } finally {
      await relay.close();
    }
  });

  const directedDraftCases = [
    { scenario: "an idle directed message preserves the unsent draft", idle: true },
    { scenario: "a steering directed message preserves the unsent draft", idle: false },
  ];

  test.each(directedDraftCases)("$scenario", async ({ idle }) => {
    const recorder = recordingRelay({ deliverOnReady: [INBOUND] });
    const relay = await ScriptedRelay.start(recorder.script);
    try {
      const { harness, shutdown } = await startAutoSession(relay, { idle });
      await harness.calls.injected.until(1);
      harness.setEditorText("review draft");

      await harness.messageStart([{ type: "text", text: INBOUND_TEXT }]);
      expect(harness.editorReads).toEqual(["review draft"]);
      expect(harness.pendingTimeouts()).toBe(1);

      // OMP clears after extension handlers and before the deferred callback.
      harness.setEditorText("");
      harness.runNextTimeout();

      expect(harness.editorText()).toBe("review draft");
      expect(harness.editorWrites).toEqual(["review draft"]);
      expect(harness.calls.userMessages[0]?.options).toEqual({ deliverAs: "steer" });

      await shutdown();
    } finally {
      await relay.close();
    }
  });

  test("a deferred notice restores the draft present when the notice starts", async () => {
    const recorder = recordingRelay({ deliverOnReady: [NOTICE] });
    const relay = await ScriptedRelay.start(recorder.script);
    try {
      const written = layers({ port: relay.port, startup: "auto" });
      const harness = sessionHarness(written.projectRoot);
      harness.setIdle(false);
      harness.setEditorText("draft when the notice arrived");
      ompRelay(harness.api);

      await harness.handlers.get("session_start")?.({ type: "session_start" }, harness.ctx);
      await harness.calls.injected.until(1);
      harness.setEditorText("latest draft before notice start");
      await harness.messageStart([{ type: "text", text: NOTICE_TEXT }]);
      harness.setEditorText("");
      harness.runNextTimeout();

      expect(harness.editorText()).toBe("latest draft before notice start");
      expect(harness.editorWrites).toEqual(["latest draft before notice start"]);
      expect(harness.calls.userMessages[0]?.options).toEqual({ deliverAs: "followUp" });

      await harness.handlers.get("session_shutdown")?.({ type: "session_shutdown" }, harness.ctx);
    } finally {
      await relay.close();
    }
  });

  test("text entered after the host clear is appended to the protected draft", async () => {
    const recorder = recordingRelay({ deliverOnReady: [INBOUND] });
    const relay = await ScriptedRelay.start(recorder.script);
    try {
      const { harness, shutdown } = await startAutoSession(relay);
      await harness.calls.injected.until(1);
      harness.setEditorText("review draft");

      await harness.messageStart([{ type: "text", text: INBOUND_TEXT }]);
      harness.setEditorText("");
      harness.setEditorText(" plus a new thought");
      harness.runNextTimeout();

      expect(harness.editorText()).toBe("review draft plus a new thought");
      expect(harness.editorWrites).toEqual(["review draft plus a new thought"]);

      await shutdown();
    } finally {
      await relay.close();
    }
  });

  test("an already restored draft prefix is not duplicated", async () => {
    const recorder = recordingRelay({ deliverOnReady: [INBOUND] });
    const relay = await ScriptedRelay.start(recorder.script);
    try {
      const { harness, shutdown } = await startAutoSession(relay);
      await harness.calls.injected.until(1);
      harness.setEditorText("review draft");

      await harness.messageStart([{ type: "text", text: INBOUND_TEXT }]);
      harness.setEditorText("");
      harness.setEditorText("review draft plus a new thought");
      harness.runNextTimeout();

      expect(harness.editorText()).toBe("review draft plus a new thought");
      expect(harness.editorWrites).toEqual([]);

      await shutdown();
    } finally {
      await relay.close();
    }
  });

  test("a later steer may start before an earlier deferred notice", async () => {
    const recorder = recordingRelay({ deliverOnReady: [NOTICE, INBOUND] });
    const relay = await ScriptedRelay.start(recorder.script);
    try {
      const { harness, shutdown } = await startAutoSession(relay, { idle: false });
      await harness.calls.injected.until(2);
      harness.setEditorText("review draft");

      await harness.messageStart([{ type: "text", text: INBOUND_TEXT }]);
      harness.setEditorText("");
      harness.runNextTimeout();
      await harness.messageStart([{ type: "text", text: NOTICE_TEXT }]);
      harness.setEditorText("");
      harness.runNextTimeout();

      expect(harness.editorText()).toBe("review draft");
      expect(harness.editorWrites).toEqual(["review draft", "review draft"]);
      expect(harness.calls.userMessages.map((call) => call.options)).toEqual([
        { deliverAs: "followUp" },
        { deliverAs: "steer" },
      ]);

      await shutdown();
    } finally {
      await relay.close();
    }
  });

  test("repeated identical relay injections retain separate correlation counts", async () => {
    const recorder = recordingRelay({ deliverOnReady: [INBOUND, INBOUND] });
    const relay = await ScriptedRelay.start(recorder.script);
    try {
      const { harness, shutdown } = await startAutoSession(relay);
      await harness.calls.injected.until(2);
      harness.setEditorText("review draft");

      await harness.messageStart([{ type: "text", text: INBOUND_TEXT }]);
      harness.setEditorText("");
      harness.runNextTimeout();
      await harness.messageStart([{ type: "text", text: INBOUND_TEXT }]);
      harness.setEditorText("");
      harness.runNextTimeout();

      expect(harness.editorText()).toBe("review draft");
      expect(harness.editorWrites).toEqual(["review draft", "review draft"]);

      await shutdown();
    } finally {
      await relay.close();
    }
  });

  test("overlapping restores let only the newest callback write", async () => {
    const recorder = recordingRelay({ deliverOnReady: [INBOUND, NOTICE] });
    const relay = await ScriptedRelay.start(recorder.script);
    try {
      const { harness, shutdown } = await startAutoSession(relay, { idle: false });
      await harness.calls.injected.until(2);
      harness.setEditorText("review draft");

      await harness.messageStart([{ type: "text", text: INBOUND_TEXT }]);
      harness.setEditorText("");
      await harness.messageStart([{ type: "text", text: NOTICE_TEXT }]);
      harness.setEditorText("");
      expect(harness.pendingTimeouts()).toBe(2);

      harness.runNextTimeout();
      expect(harness.editorText()).toBe("");
      harness.runNextTimeout();

      expect(harness.editorText()).toBe("review draft");
      expect(harness.editorWrites).toEqual(["review draft"]);

      await shutdown();
    } finally {
      await relay.close();
    }
  });

  test("an unmatched local user message does not inspect or restore the editor", async () => {
    const harness = sessionHarness(process.cwd());
    ompRelay(harness.api);
    harness.setEditorText("local draft");

    await harness.messageStart("local submission");

    expect(harness.editorReads).toEqual([]);
    expect(harness.editorWrites).toEqual([]);
    expect(harness.pendingTimeouts()).toBe(0);
  });

  test("an unmatched external user message does not consume relay correlation", async () => {
    const recorder = recordingRelay({ deliverOnReady: [INBOUND] });
    const relay = await ScriptedRelay.start(recorder.script);
    try {
      const { harness, shutdown } = await startAutoSession(relay);
      await harness.calls.injected.until(1);
      harness.setEditorText("review draft");

      await harness.messageStart([{ type: "text", text: "external user message" }]);
      expect(harness.editorReads).toEqual([]);
      expect(harness.pendingTimeouts()).toBe(0);

      await harness.messageStart([{ type: "text", text: INBOUND_TEXT }]);
      expect(harness.editorReads).toEqual(["review draft"]);
      expect(harness.pendingTimeouts()).toBe(1);

      await shutdown();
    } finally {
      await relay.close();
    }
  });

  test("a matching user message with non-text content does not consume relay correlation", async () => {
    const recorder = recordingRelay({ deliverOnReady: [INBOUND] });
    const relay = await ScriptedRelay.start(recorder.script);
    try {
      const { harness, shutdown } = await startAutoSession(relay);
      await harness.calls.injected.until(1);
      harness.setEditorText("review draft");

      await harness.messageStart([
        { type: "text", text: INBOUND_TEXT },
        { type: "image", data: "AA==", mimeType: "image/png" },
      ]);
      expect(harness.editorReads).toEqual([]);
      expect(harness.pendingTimeouts()).toBe(0);

      await harness.messageStart([{ type: "text", text: INBOUND_TEXT }]);
      expect(harness.editorReads).toEqual(["review draft"]);
      expect(harness.pendingTimeouts()).toBe(1);

      await shutdown();
    } finally {
      await relay.close();
    }
  });

  test("session shutdown invalidates pending correlation and restoration", async () => {
    const recorder = recordingRelay({ deliverOnReady: [INBOUND] });
    const relay = await ScriptedRelay.start(recorder.script);
    try {
      const { harness, shutdown } = await startAutoSession(relay);
      await harness.calls.injected.until(1);
      harness.setEditorText("review draft");
      await harness.messageStart([{ type: "text", text: INBOUND_TEXT }]);
      expect(harness.pendingTimeouts()).toBe(1);

      await shutdown();
      harness.setEditorText("");
      harness.runNextTimeout();
      expect(harness.editorText()).toBe("");
      expect(harness.editorWrites).toEqual([]);

      harness.setEditorText("new local draft");
      await harness.messageStart([{ type: "text", text: INBOUND_TEXT }]);
      expect(harness.editorReads).toEqual(["review draft"]);
      expect(harness.pendingTimeouts()).toBe(0);
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
