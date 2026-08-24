import type {
  ExtensionAPI,
  ExtensionContext,
  ExtensionMode,
} from "@oh-my-pi/pi-coding-agent";

import {
  RelayClient,
  RequestFailed,
  type ClientState,
  type Scheduler,
  type SendRequest,
} from "./client.ts";
import {
  loadGlobalConfig,
  resolveClient,
  resolveWithGlobal,
  type ConfigProblem,
  type JoinParameters,
  type RelayConfig,
  type ResolvedClient,
  type ResolvedSources,
  type StartupMode,
  type ValueSource,
} from "./config.ts";
import {
  correlationProblem,
  describe,
  describeIdentifierProblem,
  identifierProblem,
  type MessageFrame,
  type PeersFrame,
  type ReceiptFrame,
  type RoomId,
} from "./protocol.ts";

export const INBOUND_MESSAGE_TYPE = "io.github.pashifika.omp-relay.message";

/**
 * Session entry for one outbound message, the mirror of
 * {@link INBOUND_MESSAGE_TYPE}.
 *
 * A distinct type rather than a direction field on the inbound one: a consumer
 * reading the transcript back wants one of the two far more often than both, and
 * a shared type would make every such read filter.
 */
export const OUTBOUND_MESSAGE_TYPE = "io.github.pashifika.omp-relay.sent";

export interface MeshArguments {
  readonly action?: unknown;
  readonly to?: unknown;
  readonly message?: unknown;
  readonly reply_to?: unknown;
  readonly project?: unknown;
  readonly task?: unknown;
  readonly as?: unknown;
}

export interface MeshClient {
  readonly state: ClientState;
  list(): Promise<PeersFrame>;
  send(request: SendRequest): Promise<ReceiptFrame>;
}

export interface MeshToolResult {
  content: Array<{ type: "text"; text: string }>;
  details: Record<string, unknown>;
}

/** The exact frame values for one inbound message, kept out of the model's context. */
export interface InboundDetails {
  readonly id: string;
  readonly from: string;
  readonly project: string;
  readonly task: string;
  readonly body: string;
  readonly reply_to?: string;
}

/** The exact values of one outbound message and the receipt that acknowledged it. */
export interface OutboundDetails {
  readonly id: string;
  readonly to: string;
  readonly project: string;
  readonly task: string;
  readonly body: string;
  readonly status: string;
  readonly reply_to?: string;
}

/** What one validated inbound `message` frame contributes to the session. */
export interface InboundInjection {
  /** Rendered provenance and quoted body, delivered to the session as a user prompt. */
  readonly text: string;
  /** The unmodified frame values, persisted as a session entry rather than sent to the model. */
  readonly details: InboundDetails;
}

/** What one successful join resolved, and who was in the room when it did. */
export interface JoinReport {
  readonly room: RoomId;
  readonly peer: string;
  readonly sources: ResolvedSources;
  /** The roster exactly as the relay reported it, unneutralized. */
  readonly peers: readonly string[];
  /** The machine's configured purpose, carried only under `manual` startup. */
  readonly purpose: string | null;
  /** True when the resolved room and peer already matched the live connection. */
  readonly unchanged: boolean;
  /** Present when the connection opened but the roster request did not settle. */
  readonly rosterFailure: string | null;
  /**
   * Whether the relay confirmed this join: the connection was still `ready`
   * when the roster request settled.
   *
   * Read only alongside {@link rosterFailure}, which does not decide it. A
   * request the relay never answered on a `ready` connection leaves a
   * registered peer with an unknown roster; a connection that never reached
   * `ready` leaves a join nothing has acknowledged. Absent reads as the second,
   * the conservative of the two.
   */
  readonly confirmed?: boolean;
}

/** Outcome of one join. A failure names the field responsible where there is one. */
export type JoinOutcome =
  | { readonly ok: true; readonly report: JoinReport }
  | { readonly ok: false; readonly problem: ConfigProblem };

/**
 * What {@link executeMesh} needs from the session runtime.
 *
 * An interface rather than the client alone, because two of the three actions
 * now reach past the connection: `join` replaces it, and `send` persists a
 * record beside it. Keeping those behind named members is what lets the tool's
 * behaviour — including its refusal to register a non-interactive session — be
 * exercised without a socket.
 */
export interface MeshHost {
  /**
   * Whether this session may register as a peer: a top-level interactive one.
   *
   * Checked in the join path because a tool call is not a lifecycle event, so
   * `session_start`'s guard no longer covers every way a client can start.
   */
  readonly interactive: boolean;
  /** The live client, or `null` when nothing is connected. */
  readonly client: MeshClient | null;
  /** The room the live client joined, for recording an outbound message. */
  readonly room: RoomId | null;
  join(parameters: JoinParameters): Promise<JoinOutcome>;
  recordSend(details: OutboundDetails): void;
}

// ---------------------------------------------------------------------------
// Rendering untrusted text
// ---------------------------------------------------------------------------

/**
 * Every character a line-oriented terminal sink must not receive verbatim: C0,
 * DEL, C1, and the Unicode line and paragraph separators.
 *
 * The relay's identifier validation rejects only an empty or overlong name, `/`,
 * `@`, and outer whitespace, so a protocol-valid peer name may carry an internal
 * newline or an escape sequence, and a message body is unrestricted apart from
 * its byte budget. Both end up in a Markdown renderer that deliberately
 * preserves ANSI, so neutralizing them here is the only place it happens.
 */
const CONTROL_CHARACTERS = /[\u0000-\u001f\u007f-\u009f\u2028\u2029]/gu;

/** The same set less `\t` and `\n`, which a body may legitimately contain. */
const CONTROL_CHARACTERS_OUTSIDE_TEXT = /[\u0000-\u0008\u000b-\u001f\u007f-\u009f\u2028\u2029]/gu;

/** Stands in for one neutralized character: visible, inert, one column wide. */
const NEUTRALIZED = "\uFFFD";

/** Prefix on every rendered body line. See {@link buildInboundInjection}. */
const BODY_QUOTE = "> ";

/** Opens the purpose preamble. See {@link buildInboundInjection}. */
const PURPOSE_HEADING = "This terminal's configured purpose, from its own operator:";

/** `value` as one line that can neither move the cursor nor forge a second one. */
function singleLine(value: string): string {
  return value.replace(CONTROL_CHARACTERS, NEUTRALIZED);
}

/**
 * `value` as quoted body lines: newlines and tabs kept, every other control
 * character neutralized, and every line prefixed so no body line can occupy
 * column zero, where the provenance header lives.
 */
function quotedBody(value: string): string[] {
  return value
    .replace(CONTROL_CHARACTERS_OUTSIDE_TEXT, NEUTRALIZED)
    .split("\n")
    .map((line) => `${BODY_QUOTE}${line}`);
}

function result(text: string, details: Record<string, unknown>): MeshToolResult {
  return { content: [{ type: "text", text }], details };
}

function validationFailure(message: string): MeshToolResult {
  return result(`Invalid mesh arguments: ${message}`, {
    action: "invalid",
    status: "validation_error",
  });
}

function requestFailure(error: unknown): MeshToolResult {
  if (error instanceof RequestFailed) {
    const text =
      error.reason === "stopped"
        ? "OMP Relay is not ready; no request was sent."
        : error.reason === "disconnected"
          ? "OMP Relay disconnected before the request completed."
          : error.reason === "timeout"
            ? "OMP Relay did not answer before the request deadline."
            : `OMP Relay request failed: ${error.message}`;
    return result(text, { status: "request_failed", reason: error.reason });
  }
  return result(`OMP Relay request failed: ${describe(error)}`, {
    status: "request_failed",
    reason: "unknown",
  });
}

function receiptResult(receipt: ReceiptFrame): MeshToolResult {
  let text: string;
  switch (receipt.status) {
    case "routed":
      text = `Message ${receipt.id} was queued for ${receipt.to}; this does not mean the recipient has read, accepted, or answered it.`;
      break;
    case "peer_offline":
      text = `Message ${receipt.id} was not queued because peer ${receipt.to} is offline.`;
      break;
    case "recipient_backpressure":
      text = `Message ${receipt.id} was not queued because peer ${receipt.to}'s queue is full; retry later.`;
      break;
    case "invalid_target":
      text = `Message ${receipt.id} was rejected because ${receipt.to} is not a valid target.`;
      break;
    default:
      text = `Relay returned receipt status ${JSON.stringify(receipt.status)} for message ${receipt.id}.`;
      break;
  }
  return result(text, {
    action: "send",
    id: receipt.id,
    to: receipt.to,
    status: receipt.status,
  });
}

/** How each resolved value's origin is named to the operator. */
const SOURCE_LABEL: Record<ValueSource, string> = {
  parameter: "this join's parameter",
  "project-file": "the project file",
  "global-file": "the global file",
  derivation: "the host name",
};

/**
 * The first line of a join result: what happened, and never more than happened.
 *
 * Three states rather than two, because a roster that did not come back does
 * not say which of them this is. On a connection the relay never confirmed --
 * the common first run, where the operator has not started it yet -- `Joined`
 * would assert a handshake that did not happen, and a model reading it would go
 * on to send. On a `ready` connection the peer is registered and can send, and
 * only the roster is unknown; calling that unconfirmed sends the caller to
 * recover a connection that is fine. The failure is named on its own line in
 * both cases; this is what keeps the headline from contradicting it.
 */
function joinHeadline(report: JoinReport): string {
  const room = `${singleLine(report.room.project)}/${singleLine(report.room.task)}`;
  const as = `as ${singleLine(report.peer)}`;
  if (report.rosterFailure === null) {
    return report.unchanged
      ? `Already joined ${room} ${as}; the connection was left open.`
      : `Joined ${room} ${as}.`;
  }
  if (report.confirmed !== true) {
    return report.unchanged
      ? `The connection to ${room} ${as} was left open, but the relay has not confirmed this join, so nothing can be sent yet.`
      : `Opened a connection to ${room} ${as}, but the relay has not confirmed the join, so nothing can be sent yet.`;
  }
  return report.unchanged
    ? `Already joined ${room} ${as}; the connection was left open, but this join did not learn who else is in the room.`
    : `Joined ${room} ${as}, but this join did not learn who else is in the room.`;
}

/**
 * Renders a join report.
 *
 * The sources and the roster are the substance rather than decoration. Join
 * parameters make two mistyped rooms two *successful* joins that never meet,
 * with an empty roster as the only symptom, so the resolved values are stated
 * with where each came from — and an empty room is said outright rather than
 * left to be inferred from a later routing failure.
 */
function joinResult(report: JoinReport): MeshToolResult {
  const others = report.peers.filter((name) => name !== report.peer);
  const lines = [
    joinHeadline(report),
    `Room project came from ${SOURCE_LABEL[report.sources.project]}, task from ${SOURCE_LABEL[report.sources.task]}, peer name from ${SOURCE_LABEL[report.sources.peer]}.`,
    report.rosterFailure !== null
      ? `The roster is unknown: ${report.rosterFailure}`
      : others.length === 0
        ? "No other peer is in this room; nobody will receive a message sent now."
        : `Other peers in this room: ${others.map(singleLine).join(", ")}`,
  ];
  if (report.purpose !== null) {
    // Operator-authored text from the global file, so it is rendered as written.
    lines.push("", PURPOSE_HEADING, report.purpose);
  }
  return result(lines.join("\n"), {
    action: "join",
    project: report.room.project,
    task: report.room.task,
    peer: report.peer,
    sources: { ...report.sources },
    peers: [...report.peers],
    unchanged: report.unchanged,
    ...(report.purpose === null ? {} : { purpose: report.purpose }),
    // Carried as a status, the same way a refusal and a failure are, because
    // `details` is what a caller dispatches on: an unconfirmed connection read
    // as an unqualified success is the text defect again, one layer down. The
    // two are separate statuses because they are separate dispatches: one
    // retries a request, the other recovers a connection.
    ...(report.rosterFailure === null
      ? {}
      : {
          status: report.confirmed === true ? "roster_unknown" : "unconfirmed",
          roster_failure: report.rosterFailure,
        }),
  });
}

/** Validates one optional join parameter, naming it as a parameter. */
function joinParameter(
  value: unknown,
  name: string,
): { readonly ok: true; readonly value: string | undefined } | { readonly ok: false; readonly failure: MeshToolResult } {
  if (value === undefined) return { ok: true, value: undefined };
  if (typeof value !== "string") {
    return { ok: false, failure: validationFailure(`${name} must be a string when provided`) };
  }
  const broken = identifierProblem(value);
  if (broken !== null) {
    return { ok: false, failure: validationFailure(`${name} ${describeIdentifierProblem(broken)}`) };
  }
  return { ok: true, value };
}

export async function executeMesh(host: MeshHost, args: MeshArguments): Promise<MeshToolResult> {
  if (args.action !== "join" && args.action !== "list" && args.action !== "send") {
    return validationFailure('action must be "join", "list", or "send"');
  }

  if (args.action === "join") {
    const project = joinParameter(args.project, "project");
    if (!project.ok) return project.failure;
    const task = joinParameter(args.task, "task");
    if (!task.ok) return task.failure;
    const as = joinParameter(args.as, "as");
    if (!as.ok) return as.failure;

    // A tool call is not a lifecycle event, so this is the only place that can
    // keep a local subagent out of the roster once a client can start from one.
    if (!host.interactive) {
      return result(
        "OMP Relay registers only a top-level interactive session as a peer, and this session is not one; no connection was opened.",
        { action: "join", status: "refused", reason: "not_interactive" },
      );
    }

    const outcome = await host.join({
      ...(project.value === undefined ? {} : { project: project.value }),
      ...(task.value === undefined ? {} : { task: task.value }),
      ...(as.value === undefined ? {} : { as: as.value }),
    });
    if (!outcome.ok) {
      return result(`OMP Relay could not join: ${outcome.problem.reason}`, {
        action: "join",
        status: "failed",
        ...(outcome.problem.field === null ? {} : { field: outcome.problem.field }),
      });
    }
    return joinResult(outcome.report);
  }

  if (args.action === "send") {
    if (typeof args.to !== "string") {
      return validationFailure("send requires a string to");
    }
    const targetProblem = identifierProblem(args.to);
    if (targetProblem !== null) {
      return validationFailure(`to ${describeIdentifierProblem(targetProblem)}`);
    }
    if (typeof args.message !== "string") {
      return validationFailure("send requires a string message");
    }
    if (args.reply_to !== undefined) {
      if (typeof args.reply_to !== "string") {
        return validationFailure("reply_to must be a string when provided");
      }
      const replyProblem = correlationProblem(args.reply_to);
      if (replyProblem !== null) {
        return validationFailure(`reply_to ${describeIdentifierProblem(replyProblem)}`);
      }
    }
  }

  const client = host.client;
  if (client === null || client.state !== "ready") {
    return result(
      'OMP Relay is not ready; no request was sent. Call mesh with action "join" to connect.',
      { action: args.action, status: "unavailable" },
    );
  }

  try {
    if (args.action === "list") {
      const peers = await client.list();
      const text =
        peers.peers.length === 0
          ? "No peers are connected in this room."
          : // Names come from other peers' own registrations, so they are
            // neutralized for the same reason inbound provenance is. `details`
            // keeps them verbatim: the runtime documents it as UI and log data,
            // not provider content.
            `Peers in this room: ${peers.peers.map(singleLine).join(", ")}`;
      return result(text, { action: "list", peers: [...peers.peers] });
    }

    const id = crypto.randomUUID();
    const to = args.to as string;
    const body = args.message as string;
    const replyTo = args.reply_to as string | undefined;
    const receipt = await client.send({
      id,
      to,
      body,
      ...(replyTo === undefined ? {} : { replyTo }),
    });

    // Recorded with the relay's own verdict rather than only when it routed.
    // Inbound gets a durable entry and outbound previously got only a tool call
    // and result, both in classes a context maintenance pass may prune -- so
    // over a long exchange the initiator lost its record of what it asked before
    // it lost the reply, and an inbound `reply_to` then named a request nothing
    // could resolve. Carrying the status keeps a refused send from reading as a
    // delivered one.
    const room = host.room;
    if (room !== null) {
      host.recordSend({
        id: receipt.id,
        to: receipt.to,
        project: room.project,
        task: room.task,
        body,
        status: receipt.status,
        ...(replyTo === undefined ? {} : { reply_to: replyTo }),
      });
    }
    return receiptResult(receipt);
  } catch (error) {
    return requestFailure(error);
  }
}

/**
 * Renders one inbound message for delivery, and the frame values to persist.
 *
 * The rendered text is what the model and the operator see, so every value
 * interpolated into it is neutralized first: the provenance fields to a single
 * line each, the body to text without terminal control sequences. Provenance
 * occupies column zero and every body line is quoted, which is what makes the
 * boundary unambiguous — a body line can no longer be read as a provenance
 * header, whatever it contains.
 *
 * `purpose` is the exception, and only because of where it comes from: it is
 * accepted from the global file alone, so it is first-party operator text with
 * the same trust as a context file they wrote, and neutralizing it would mangle
 * prose nobody untrusted authored. It is rendered as a labelled block ahead of
 * the provenance header, so it is distinguishable from both the header and the
 * remote body rather than reading as part of either.
 */
export function buildInboundInjection(
  message: MessageFrame,
  room: RoomId,
  purpose: string | null = null,
): InboundInjection {
  const details: InboundDetails = {
    id: message.id,
    from: message.from,
    project: room.project,
    task: room.task,
    body: message.body,
    ...(message.reply_to === undefined ? {} : { reply_to: message.reply_to }),
  };
  const lines = [
    ...(purpose === null ? [] : [PURPOSE_HEADING, purpose, ""]),
    `Remote message from ${singleLine(message.from)}`,
    `Project: ${singleLine(room.project)}`,
    `Task: ${singleLine(room.task)}`,
    `Message ID: ${singleLine(message.id)}`,
    ...(message.reply_to === undefined ? [] : [`Reply to: ${singleLine(message.reply_to)}`]),
    "",
    ...quotedBody(message.body),
  ];
  return { text: lines.join("\n"), details };
}

function schedulerFrom(ctx: ExtensionContext): Scheduler {
  type ContextTimer = Timer;
  return {
    setTimeout(callback, milliseconds) {
      return ctx.setTimeout(callback, milliseconds);
    },
    clearTimeout(handle) {
      ctx.clearTimer(handle as ContextTimer);
    },
    setInterval(callback, milliseconds) {
      return ctx.setInterval(callback, milliseconds);
    },
    clearInterval(handle) {
      ctx.clearTimer(handle as ContextTimer);
    },
  };
}

/** The mode in which a session is allowed to register itself as a peer. */
const INTERACTIVE_MODE: ExtensionMode = "tui";

export default function ompRelay(pi: ExtensionAPI): void {
  let client: RelayClient | null = null;
  /** What the live client joined, for no-op detection and outbound records. */
  let live: { readonly config: RelayConfig; readonly startup: StartupMode } | null = null;
  let generation = 0;
  const notified = new Set<string>();

  /**
   * The purpose still owed to this session, under `auto` only.
   *
   * Under `manual` nothing is ever owed, because the join result carries the
   * text to the caller that asked. Under `auto` there is no call to return it
   * from and no operator present at connect time, so it rides the first
   * message: the moment work arrives is the moment the policy matters, and once
   * is enough because the text is then in the transcript.
   *
   * Armed by {@link armPurpose} under `auto`, and cleared only where it is
   * settled: by the inbound handler that pays it, or by a `manual` join at the
   * point its own report carries the text instead. Never at resolution time —
   * a join that resolved is not yet a join that reported.
   */
  let pendingPurpose: string | null = null;

  /**
   * Whether this session has already been given its purpose text, through
   * either channel.
   *
   * The debt belongs to the session, not to the connection: a rejoin is a new
   * connection within one session, so re-arming there would deliver the same
   * operator text twice — which the capability forbids in as many words.
   *
   * A flag rather than "arm at session start only", because the connection that
   * starts a session is not always the one `session_start` opened. An `auto`
   * start that cannot resolve a room — a checkout with no committed project
   * file — leaves the session's first successful connection to the join that
   * recovers it, and that join owes the purpose exactly as much.
   *
   * It is set where a delivery actually happens, and nowhere earlier: by the
   * inbound handler that consumes the preamble, and by a `manual` join at the
   * point it returns a report carrying the text. Setting it at resolution
   * instead claimed a delivery a later generation check could still cancel,
   * turning the join into a `superseded` return that handed the text to
   * nobody and left the session owing a debt it believed paid.
   */
  let purposeDelivered = false;

  /**
   * Arms the debt an `auto` resolution incurs, ahead of the socket that pays it.
   *
   * It runs at every successful resolution rather than inside `connect`,
   * because the mode is re-read from the file on every join and an identical
   * `manual` -> `auto` rejoin opens no connection at all, so nothing in
   * `connect` would ever arm it. It runs before `connect` rather than after the
   * roster, because a relay may push a message on the very handshake `connect`
   * opens.
   *
   * This is the only purpose write left that happens before a join has won, and
   * the only one that may: it is idempotent, so a join that goes on to be
   * superseded writes the value the winner would write anyway, and the winner
   * re-runs it regardless.
   *
   * Everything the `manual` path does — reading {@link purposeDelivered},
   * deciding the text, clearing the debt, recording the delivery — happens at
   * the commit site in {@link performJoin}, past the final generation check.
   * That split is the load-bearing property here, not an arrangement of
   * convenience: state read early and acted on late is what put four separate
   * lost- and double-delivery races in this file, because the join that
   * resolves is not always the join that reports, and `purposeDelivered` can
   * flip under any await in between. Under `manual` the text is decided at the
   * instant the report that carries it is built, and nowhere else.
   */
  const armPurpose = (resolved: ResolvedClient): void => {
    if (resolved.startup !== "auto") return;
    pendingPurpose = purposeDelivered ? null : resolved.purpose;
  };

  const notifyOnce = (
    ctx: ExtensionContext,
    message: string,
    type: "info" | "warning" | "error",
  ): void => {
    if (notified.has(message)) return;
    notified.add(message);
    try {
      ctx.ui.notify(message, type);
    } catch (error) {
      pi.logger.error("OMP Relay notification failed", { error: describe(error), message });
    }
  };

  /**
   * The one place a client is constructed and started.
   *
   * Both start paths reach it, so there is no second connect path that could
   * drift: `session_start` under `auto`, and every `join`. The caller owns the
   * generation check on either side of its own awaits; this function performs
   * none, because it does not await.
   */
  const connect = (ctx: ExtensionContext, resolved: ResolvedClient): RelayClient => {
    const config = resolved.config;
    const next = new RelayClient({
      config,
      scheduler: schedulerFrom(ctx),
      handlers: {
        onMessage(message) {
          const purpose = pendingPurpose;
          pendingPurpose = null;
          if (purpose !== null) {
            purposeDelivered = true;
          }
          const injection = buildInboundInjection(message, config.room, purpose);
          // A session entry, which the runtime documents as state persistence
          // that never reaches the LLM. It is where the exact frame values live,
          // so the rendered text can be neutralized without losing them.
          pi.appendEntry(INBOUND_MESSAGE_TYPE, injection.details);
          // The call shape is load-bearing in all three of its parts.
          // Not a custom message: the runtime converts one to provider role
          // `developer` (compaction/messages.ts:194-211), ranking a remote peer
          // above the local operator. Not a bare `sendUserMessage`: with no
          // `deliverAs` it takes the prompt path, which auto-reads `@path` file
          // mentions out of the remote body (agent-session.ts:5789-5800) — and
          // neither `expandPromptTemplates: false` nor the `> ` body quoting
          // stops that, because a preceding space already satisfies the mention
          // boundary. Not `followUp`: its drain is gated on an
          // `assistant`/`toolResult` transcript tail (agent-session.ts:6244-6266),
          // so a fresh session would never start the turn; `steer` passes that
          // gate from any tail via the steering-queue check.
          pi.sendUserMessage(injection.text, { deliverAs: "steer" });
        },
        onReport(report) {
          const type = report.level === "warn" ? "warning" : report.level;
          notifyOnce(ctx, report.message, type);
        },
      },
    });
    client = next;
    live = { config, startup: resolved.startup };
    next.start();
    return next;
  };

  /**
   * Resolves configuration, reconnects if needed, and reports what it resolved.
   *
   * A room is fixed for a connection's lifetime by its `hello` and no frame
   * changes it, so a rejoin is a stop and a fresh handshake rather than a new
   * frame type. Nothing is persisted or replayed, so re-entry costs only the
   * handshake. When the resolved room and peer already match the live
   * connection, nothing is closed: reconnecting would make this peer briefly
   * vanish from every other roster for no gain.
   */
  const performJoin = async (
    ctx: ExtensionContext,
    parameters: JoinParameters,
  ): Promise<JoinOutcome> => {
    const thisGeneration = ++generation;
    const outcome = await resolveClient({ env: process.env, cwd: ctx.cwd, parameters });
    if (thisGeneration !== generation) {
      return {
        ok: false,
        problem: { field: null, reason: "a newer join superseded this one before it connected" },
      };
    }
    if (!outcome.ok) {
      return { ok: false, problem: outcome.problem };
    }

    const resolved = outcome.resolved;
    // Armed before the branch below rather than after the roster, for the two
    // reasons that put it here at all: the no-op branch opens no connection to
    // arm it in, and under `auto` a relay may push a message on the same
    // handshake, so the debt has to exist before the socket starts.
    armPurpose(resolved);
    const current = live;
    const unchanged =
      current !== null &&
      client !== null &&
      // Liveness, not object existence. A client the relay displaced with
      // `peer_replaced` is `stopped` for good, and one between reconnect
      // attempts holds no registration either, so answering an identical
      // join — the operator's one recovery move — from either would report a
      // connection nobody can reach. Only `ready` is the live connection the
      // no-op is scoped to; anything else reconnects, which costs a handshake
      // and no roster churn, because a client that is not ready is in no
      // roster to churn.
      client.state === "ready" &&
      current.config.room.project === resolved.config.room.project &&
      current.config.room.task === resolved.config.room.task &&
      current.config.peer === resolved.config.peer;

    let active: RelayClient;
    if (unchanged) {
      active = client as RelayClient;
    } else {
      const previous = client;
      client = null;
      live = null;
      if (previous !== null) {
        // A host-requested shutdown, which the client contract already settles
        // pending `list` and `send` requests through with a stated failure
        // rather than leaving them to hang on a socket nobody will answer.
        await previous.stop();
        if (thisGeneration !== generation) {
          return {
            ok: false,
            problem: { field: null, reason: "a newer join superseded this one before it connected" },
          };
        }
      }
      active = connect(ctx, resolved);
    }

    // Issued straight away rather than after a readiness signal: the client
    // holds a request written before `ready` and flushes it on the handshake, so
    // this is one round trip either way, and its own deadline bounds a relay
    // that never answers.
    let peers: readonly string[] = [];
    let rosterFailure: string | null = null;
    try {
      peers = (await active.list()).peers;
    } catch (error) {
      rosterFailure =
        error instanceof RequestFailed
          ? `the relay did not answer the roster request (${error.reason})`
          : `the roster request failed: ${describe(error)}`;
    }
    // What the roster failure above does not say: whether the relay confirmed
    // the join. A client still in `ready` completed its handshake and holds a
    // registration, so only the roster is missing; one that is not never got
    // that far. See {@link joinHeadline}.
    const confirmed = active.state === "ready";
    if (thisGeneration !== generation) {
      return {
        ok: false,
        problem: { field: null, reason: "a newer join superseded this one before it completed" },
      };
    }

    // The purpose, decided here and only here under `manual`. Every generation
    // check above can still end this join as a `superseded` return that reports
    // nothing, and `purposeDelivered` can flip under any await above — the
    // roster request most of all, which an inbound message paying the debt can
    // land inside. So the flag is read, the text chosen, the debt cleared and
    // the delivery recorded together, at the instant the report below is built.
    // Deciding it at resolution time instead lost the debt to a join that was
    // superseded, erased it for the connection that survived, and handed back
    // text a message that arrived while this join waited had already paid.
    let purpose: string | null = null;
    if (resolved.startup !== "auto") {
      purpose = purposeDelivered ? null : resolved.purpose;
      // Under `manual` nothing rides an inbound message: this report is the
      // delivery, and it is this join's report, so it is now a fact.
      pendingPurpose = null;
      if (purpose !== null) {
        purposeDelivered = true;
      }
    }

    return {
      ok: true,
      report: {
        room: resolved.config.room,
        peer: resolved.config.peer,
        sources: resolved.sources,
        peers,
        purpose,
        unchanged,
        rosterFailure,
        confirmed,
      },
    };
  };

  const parameters = pi.zod.object({
    action: pi.zod
      .enum(["join", "list", "send"])
      .describe("Connect to a room, list connected peers, or send a message"),
    project: pi.zod
      .string()
      .optional()
      .describe("join only: room project, overriding the project configuration file"),
    task: pi.zod
      .string()
      .optional()
      .describe("join only: room task, overriding the project configuration file"),
    as: pi.zod.string().optional().describe("join only: this session's peer name"),
    to: pi.zod.string().optional().describe("Peer name; required for send"),
    message: pi.zod.string().optional().describe("Message body; required for send"),
    reply_to: pi.zod.string().optional().describe("Message identifier being answered"),
  });

  pi.registerTool({
    name: "mesh",
    label: "OMP Relay Mesh",
    description:
      "Join a relay room, list the peers in it, or send work to one of them. Join first: nothing else works until this session has joined, and the join result reports the room it resolved, where each part of it came from, and who else is present. A routed send result means the message was queued for the recipient; it does not mean the recipient read, accepted, or answered it.",
    parameters,
    async execute(_toolCallId, args, _signal, _onUpdate, ctx) {
      const host: MeshHost = {
        interactive: ctx.mode === INTERACTIVE_MODE,
        client,
        room: live?.config.room ?? null,
        join: (request) => performJoin(ctx, request),
        recordSend: (details) => pi.appendEntry(OUTBOUND_MESSAGE_TYPE, details),
      };
      return executeMesh(host, args);
    },
  });

  pi.on("session_start", async (_event, ctx) => {
    const thisGeneration = ++generation;
    notified.clear();
    pendingPurpose = null;
    purposeDelivered = false;

    const previous = client;
    client = null;
    live = null;
    if (previous !== null) {
      await previous.stop();
    }
    if (ctx.mode !== INTERACTIVE_MODE || thisGeneration !== generation) {
      return;
    }

    // Only the global layer, and only to learn the startup mode. Under `manual`
    // nothing further is read and no socket is opened, so a session in a
    // checkout this machine never meant to join stays inert.
    const global = await loadGlobalConfig(process.env);
    if (thisGeneration !== generation) {
      return;
    }
    if (!global.ok) {
      // An absent global file is the resting state, not a fault: the operator
      // has granted this machine no participation, and every session on the
      // machine would otherwise open with the same complaint. A file that exists
      // and does not validate is the opposite -- an intent was stated, possibly
      // `auto`, and frustrated -- so that is reported.
      if (!global.absent) {
        notifyOnce(ctx, global.problem.reason, "error");
      }
      return;
    }
    if (global.config.startup !== "auto") {
      return;
    }

    const outcome = await resolveWithGlobal(global.config, global.path, {
      env: process.env,
      cwd: ctx.cwd,
    });
    if (thisGeneration !== generation) {
      return;
    }
    if (!outcome.ok) {
      notifyOnce(ctx, outcome.problem.reason, "error");
      return;
    }
    // The debt this start incurs. `session_start` reaches here only under
    // `auto`, so this arms it and never delivers it, and it is armed before the
    // socket that pays it opens.
    armPurpose(outcome.resolved);
    connect(ctx, outcome.resolved);
  });

  pi.on("session_shutdown", async () => {
    generation += 1;
    const active = client;
    client = null;
    live = null;
    pendingPurpose = null;
    if (active !== null) {
      await active.stop();
    }
  });
}
