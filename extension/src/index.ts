import type {
  ExtensionAPI,
  ExtensionContext,
} from "@oh-my-pi/pi-coding-agent";

import {
  RelayClient,
  RequestFailed,
  type ClientState,
  type Scheduler,
  type SendRequest,
} from "./client.ts";
import { loadConfig, type RelayConfig } from "./config.ts";
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

export interface MeshArguments {
  readonly action?: unknown;
  readonly to?: unknown;
  readonly message?: unknown;
  readonly reply_to?: unknown;
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

/** What one validated inbound `message` frame contributes to the session. */
export interface InboundInjection {
  /** Rendered provenance and quoted body, delivered to the session as a user prompt. */
  readonly text: string;
  /** The unmodified frame values, persisted as a session entry rather than sent to the model. */
  readonly details: InboundDetails;
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

export async function executeMesh(
  client: MeshClient | null,
  args: MeshArguments,
): Promise<MeshToolResult> {
  if (args.action !== "list" && args.action !== "send") {
    return validationFailure('action must be "list" or "send"');
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

  if (client === null || client.state !== "ready") {
    return result("OMP Relay is not ready; no request was sent.", {
      action: args.action,
      status: "unavailable",
    });
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
    const receipt = await client.send({
      id,
      to: args.to as string,
      body: args.message as string,
      ...(args.reply_to === undefined ? {} : { replyTo: args.reply_to as string }),
    });
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
 */
export function buildInboundInjection(message: MessageFrame, room: RoomId): InboundInjection {
  const details: InboundDetails = {
    id: message.id,
    from: message.from,
    project: room.project,
    task: room.task,
    body: message.body,
    ...(message.reply_to === undefined ? {} : { reply_to: message.reply_to }),
  };
  const lines = [
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

export default function ompRelay(pi: ExtensionAPI): void {
  let client: RelayClient | null = null;
  let generation = 0;
  const notified = new Set<string>();

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

  const parameters = pi.zod.object({
    action: pi.zod.enum(["list", "send"]).describe("List connected peers or send a message"),
    to: pi.zod.string().optional().describe("Peer name; required for send"),
    message: pi.zod.string().optional().describe("Message body; required for send"),
    reply_to: pi.zod.string().optional().describe("Message identifier being answered"),
  });

  pi.registerTool({
    name: "mesh",
    label: "OMP Relay Mesh",
    description:
      "List peers in the configured relay room or send work to one. A routed result means the message was queued for the recipient; it does not mean the recipient read, accepted, or answered it.",
    parameters,
    async execute(_toolCallId, args) {
      return executeMesh(client, args);
    },
  });

  pi.on("session_start", async (_event, ctx) => {
    const thisGeneration = ++generation;
    notified.clear();

    const previous = client;
    client = null;
    if (previous !== null) {
      await previous.stop();
    }
    if (ctx.mode !== "tui" || thisGeneration !== generation) {
      return;
    }

    const configured = await loadConfig(process.env);
    if (thisGeneration !== generation) {
      return;
    }
    if (!configured.ok) {
      notifyOnce(ctx, configured.problem.reason, "error");
      return;
    }

    const config: RelayConfig = configured.config;
    const next = new RelayClient({
      config,
      scheduler: schedulerFrom(ctx),
      handlers: {
        onMessage(message) {
          const injection = buildInboundInjection(message, config.room);
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
    next.start();
  });

  pi.on("session_shutdown", async () => {
    generation += 1;
    const active = client;
    client = null;
    if (active !== null) {
      await active.stop();
    }
  });
}
