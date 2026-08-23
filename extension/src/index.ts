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

export interface InboundDetails {
  readonly id: string;
  readonly from: string;
  readonly project: string;
  readonly task: string;
  readonly body: string;
  readonly reply_to?: string;
}

export interface InboundPayload {
  readonly customType: typeof INBOUND_MESSAGE_TYPE;
  readonly content: string;
  readonly display: true;
  readonly attribution: "agent";
  readonly details: InboundDetails;
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
          : `Peers in this room: ${peers.peers.join(", ")}`;
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

export function buildInboundPayload(message: MessageFrame, room: RoomId): InboundPayload {
  const details: InboundDetails = {
    id: message.id,
    from: message.from,
    project: room.project,
    task: room.task,
    body: message.body,
    ...(message.reply_to === undefined ? {} : { reply_to: message.reply_to }),
  };
  const lines = [
    `Remote message from ${message.from}`,
    `Project: ${room.project}`,
    `Task: ${room.task}`,
    `Message ID: ${message.id}`,
    ...(message.reply_to === undefined ? [] : [`Reply to: ${message.reply_to}`]),
    "",
    message.body,
  ];
  return {
    customType: INBOUND_MESSAGE_TYPE,
    content: lines.join("\n"),
    display: true,
    attribution: "agent",
    details,
  };
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
          pi.sendMessage(buildInboundPayload(message, config.room), {
            deliverAs: "followUp",
            triggerTurn: true,
          });
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
