import { afterEach, describe, expect, test } from "bun:test";
import { join } from "node:path";
import { tmpdir } from "node:os";

import type {
  ExtensionAPI,
  ExtensionContext,
} from "@oh-my-pi/pi-coding-agent";

import type { ClientState, SendRequest } from "../../src/client.ts";
import { CONFIG_PATH_ENV } from "../../src/config.ts";
import ompRelay, {
  buildInboundPayload,
  executeMesh,
  INBOUND_MESSAGE_TYPE,
  type MeshClient,
} from "../../src/index.ts";
import type { PeersFrame, ReceiptFrame } from "../../src/protocol.ts";

class RecordingClient implements MeshClient {
  state: ClientState = "ready";
  listCalls = 0;
  readonly sends: SendRequest[] = [];
  peers: readonly string[] = ["alpha", "beta"];
  receiptStatus: ReceiptFrame["status"] = "routed";

  async list(): Promise<PeersFrame> {
    this.listCalls += 1;
    return { type: "peers", request_id: "list-1", peers: this.peers };
  }

  async send(request: SendRequest): Promise<ReceiptFrame> {
    this.sends.push(request);
    return {
      type: "receipt",
      id: request.id ?? "generated-by-client",
      to: request.to,
      status: this.receiptStatus,
    };
  }
}

type SessionHandler = (event: unknown, ctx: ExtensionContext) => Promise<void> | void;

interface FactoryHarness {
  readonly api: ExtensionAPI;
  readonly handlers: Map<string, SessionHandler>;
  readonly tools: Array<Record<string, unknown>>;
  readonly runtimeCalls: { sendMessage: number };
}

function factoryHarness(): FactoryHarness {
  const handlers = new Map<string, SessionHandler>();
  const tools: Array<Record<string, unknown>> = [];
  const runtimeCalls = { sendMessage: 0 };
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
    registerTool(tool: Record<string, unknown>) {
      tools.push(tool);
    },
    on(event: string, handler: SessionHandler) {
      handlers.set(event, handler);
    },
    sendMessage() {
      runtimeCalls.sendMessage += 1;
    },
  } as unknown as ExtensionAPI;
  return { api, handlers, tools, runtimeCalls };
}

function context(overrides: Partial<ExtensionContext> = {}): ExtensionContext {
  return {
    mode: "tui",
    ui: { notify() {} },
    setInterval: globalThis.setInterval,
    setTimeout: globalThis.setTimeout,
    clearTimer: clearTimeout,
    ...overrides,
  } as unknown as ExtensionContext;
}

const originalConfigPath = process.env[CONFIG_PATH_ENV];

afterEach(() => {
  if (originalConfigPath === undefined) {
    delete process.env[CONFIG_PATH_ENV];
  } else {
    process.env[CONFIG_PATH_ENV] = originalConfigPath;
  }
});

describe("extension registration", () => {
  test("load registers mesh and lifecycle handlers without runtime actions", () => {
    const harness = factoryHarness();

    ompRelay(harness.api);

    expect(harness.tools).toHaveLength(1);
    expect(harness.tools[0]?.["name"]).toBe("mesh");
    expect(harness.handlers.has("session_start")).toBe(true);
    expect(harness.handlers.has("session_shutdown")).toBe(true);
    expect(harness.runtimeCalls.sendMessage).toBe(0);
  });

  test("a missing configuration is reported without failing session start", async () => {
    process.env[CONFIG_PATH_ENV] = join(tmpdir(), `missing-omp-relay-${crypto.randomUUID()}.yml`);
    const notifications: Array<{ message: string; type: string | undefined }> = [];
    const harness = factoryHarness();
    ompRelay(harness.api);
    const start = harness.handlers.get("session_start");
    expect(start).toBeDefined();

    await start?.(
      { type: "session_start" },
      context({
        ui: {
          notify(message, type) {
            notifications.push({ message, type });
          },
        } as ExtensionContext["ui"],
      }),
    );

    expect(notifications).toHaveLength(1);
    expect(notifications[0]?.message).toContain("does not exist");
    expect(notifications[0]?.type).toBe("error");
    expect(harness.runtimeCalls.sendMessage).toBe(0);
  });

  test("a non-interactive session does not read configuration or start a client", async () => {
    process.env[CONFIG_PATH_ENV] = join(tmpdir(), `missing-omp-relay-${crypto.randomUUID()}.yml`);
    const notifications: string[] = [];
    const harness = factoryHarness();
    ompRelay(harness.api);

    await harness.handlers.get("session_start")?.(
      { type: "session_start" },
      context({
        mode: "rpc",
        ui: {
          notify(message) {
            notifications.push(message);
          },
        } as ExtensionContext["ui"],
      }),
    );

    expect(notifications).toEqual([]);
  });
});

describe("mesh tool", () => {
  test.each([
    ["unknown action", { action: "join" }],
    ["empty target", { action: "send", to: "", message: "work" }],
    ["missing message", { action: "send", to: "beta" }],
  ])("rejects %s before contacting the client", async (_name, args) => {
    const client = new RecordingClient();

    const output = await executeMesh(client, args);

    expect(output.content[0]?.text).toStartWith("Invalid mesh arguments:");
    expect(client.listCalls).toBe(0);
    expect(client.sends).toEqual([]);
  });

  test("list reports every peer returned by the relay", async () => {
    const client = new RecordingClient();

    const output = await executeMesh(client, { action: "list" });

    expect(output.content[0]?.text).toBe("Peers in this room: alpha, beta");
    expect(output.details["peers"]).toEqual(["alpha", "beta"]);
    expect(client.listCalls).toBe(1);
  });

  test("send supplies an identifier and explains that routed only means queued", async () => {
    const client = new RecordingClient();

    const output = await executeMesh(client, {
      action: "send",
      to: "beta",
      message: "review this",
      reply_to: "original-id",
    });

    expect(client.sends).toHaveLength(1);
    expect(client.sends[0]?.id).toBeString();
    expect(client.sends[0]?.replyTo).toBe("original-id");
    expect(output.details["status"]).toBe("routed");
    expect(output.details["id"]).toBe(client.sends[0]?.id);
    expect(output.content[0]?.text).toContain("queued for beta");
    expect(output.content[0]?.text).toContain("does not mean");
  });

  test.each([
    ["peer_offline", "is offline"],
    ["recipient_backpressure", "queue is full"],
    ["invalid_target", "not a valid target"],
  ] as const)("gives %s a distinct result", async (status, wording) => {
    const client = new RecordingClient();
    client.receiptStatus = status;

    const output = await executeMesh(client, {
      action: "send",
      to: "beta",
      message: "review this",
    });

    expect(output.details["status"]).toBe(status);
    expect(output.content[0]?.text).toContain(wording);
  });

  test("an unavailable client fails before issuing a request", async () => {
    const client = new RecordingClient();
    client.state = "connecting";

    const output = await executeMesh(client, { action: "list" });

    expect(output.details["status"]).toBe("unavailable");
    expect(client.listCalls).toBe(0);
  });
});

test("inbound payload preserves provenance in text and details", () => {
  const payload = buildInboundPayload(
    {
      type: "message",
      id: "message-7",
      from: "alpha",
      body: "Please review the parser.",
      reply_to: "message-3",
    },
    { project: "omp-relayd", task: "implement-omp-extension" },
  );

  expect(payload).toEqual({
    customType: INBOUND_MESSAGE_TYPE,
    content: [
      "Remote message from alpha",
      "Project: omp-relayd",
      "Task: implement-omp-extension",
      "Message ID: message-7",
      "Reply to: message-3",
      "",
      "Please review the parser.",
    ].join("\n"),
    display: true,
    attribution: "agent",
    details: {
      id: "message-7",
      from: "alpha",
      project: "omp-relayd",
      task: "implement-omp-extension",
      body: "Please review the parser.",
      reply_to: "message-3",
    },
  });
});
