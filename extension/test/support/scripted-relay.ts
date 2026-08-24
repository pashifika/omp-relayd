/**
 * An in-process TCP server that answers protocol v1 frames from a script.
 *
 * A hand-written double rather than the real relay, deliberately and only for
 * the cases the real relay cannot be made to produce on demand: receipts
 * arriving out of order, a reply naming nothing, a malformed length prefix, and
 * silence. Everything the real relay *can* produce is tested against the real
 * relay in `integration.test.ts` — a double that agrees with whatever the
 * TypeScript side believes would be assuming the very thing under test.
 *
 * It is a real socket on a real port, so Bun's `node:net` delivery behavior is
 * exercised here too rather than only in the integration suite.
 *
 * Deframing reuses the production {@link FrameAccumulator}. That is a shared
 * component, not a shared assumption: the accumulator has its own tests against
 * synthesized chunk boundaries, and this double's job is to script *answers*.
 */

import { createServer, type Server, type Socket } from "node:net";

import {
  asRecord,
  encodePayload,
  FrameAccumulator,
  LENGTH_PREFIX_BYTES,
  type ClientFrame,
  type ServerFrame,
} from "../../src/protocol.ts";

/** What a script may do in response to a client frame. */
export interface RelaySession {
  /** Which connection this is, counting from 1 across the relay's lifetime. */
  readonly connection: number;
  /** Encodes and writes one server frame. */
  readonly send: (frame: ServerFrame) => void;
  /** Writes raw bytes, for a malformed prefix or a partial frame. */
  readonly sendRaw: (bytes: Uint8Array) => void;
  /** Destroys the socket without a close frame. */
  readonly drop: () => void;
}

/** Called for every decoded client frame. Throwing fails the test loudly. */
export type Script = (frame: unknown, session: RelaySession) => void;

/** One transfer request, as a script sees it. */
export interface TransferRequest {
  readonly method: string;
  /** Request target, percent-encoded exactly as the client sent it. */
  readonly path: string;
  readonly body: Uint8Array;
}

/** What a transfer script answers with. `null` means: answer nothing at all. */
export interface TransferResponse {
  readonly status: number;
  readonly headers?: Record<string, string>;
  readonly body?: Uint8Array;
}

/**
 * Answers one transfer request, or holds the connection open by returning
 * `null`.
 *
 * Silence is a scripted outcome rather than an accident: it is the only way to
 * exercise the client's stall bound, which is deliberately not its request
 * deadline.
 */
export type TransferScript = (
  request: TransferRequest,
  relay: ScriptedRelay,
) => TransferResponse | null;

/**
 * Whether `value` is a decoded frame map carrying `type`.
 *
 * Shared rather than re-declared per test file, because every script starts by
 * asking this and three copies had already drifted into three signatures. The
 * parameter is the client-frame union, so a script matching on a `type` no
 * client can send fails to compile instead of silently never firing.
 */
export function isFrame(
  value: unknown,
  type: ClientFrame["type"],
): value is Record<string, unknown> {
  return asRecord(value)?.["type"] === type;
}

/** One field of a decoded frame map, or `undefined` when there is no map. */
export function frameField(value: unknown, field: string): unknown {
  return asRecord(value)?.[field];
}

/**
 * Frames the length prefix independently of `encodeFrame`.
 *
 * The client's reader and this writer are separate implementations of the same
 * four-byte rule on purpose: a shared helper would agree with itself.
 */
export function framePayload(payload: Uint8Array): Uint8Array {
  const framed = new Uint8Array(LENGTH_PREFIX_BYTES + payload.length);
  framed[0] = (payload.length >>> 24) & 0xff;
  framed[1] = (payload.length >>> 16) & 0xff;
  framed[2] = (payload.length >>> 8) & 0xff;
  framed[3] = payload.length & 0xff;
  framed.set(payload, LENGTH_PREFIX_BYTES);
  return framed;
}

export class ScriptedRelay {
  readonly #server: Server;
  readonly #sockets = new Set<Socket>();
  readonly #waiters: { count: number; resolve: () => void }[] = [];
  readonly #transferWaiters: { count: number; resolve: () => void }[] = [];
  #script: Script;
  #transferScript: TransferScript = defaultTransferScript;
  #connections = 0;

  /** Every client frame received across every connection, decoded, in order. */
  readonly received: unknown[] = [];

  /** Every transfer request received, in order. */
  readonly transfers: TransferRequest[] = [];

  /**
   * Payloads this relay holds, keyed by request path.
   *
   * Keyed by the whole path rather than by digest, because the path carries the
   * room and a payload is room-scoped: a test that fetched across rooms and
   * succeeded would be asserting the opposite of the contract.
   */
  readonly blobs = new Map<string, Uint8Array>();

  private constructor(server: Server, script: Script) {
    this.#server = server;
    this.#script = script;
  }

  static async start(script: Script): Promise<ScriptedRelay> {
    const server = createServer();
    const relay = new ScriptedRelay(server, script);

    server.on("connection", (socket) => relay.#accept(socket));

    const listening = Promise.withResolvers<void>();
    server.once("error", (error) => listening.reject(error));
    server.listen(0, "127.0.0.1", () => listening.resolve());
    await listening.promise;
    return relay;
  }

  get port(): number {
    const address = this.#server.address();
    if (address === null || typeof address === "string") {
      throw new Error("the scripted relay is not bound to a TCP port");
    }
    return address.port;
  }

  /** Connections accepted since start, including ones already closed. */
  get connections(): number {
    return this.#connections;
  }

  /**
   * Connections still open right now.
   *
   * Distinct from {@link connections}, which only grows: a rejoin is a stop and
   * a fresh handshake, so "two connections were accepted" and "two clients are
   * live" are different claims and only the second is a defect.
   */
  get open(): number {
    return this.#sockets.size;
  }

  /** Replaces the script, so one test can change the relay's behavior mid-run. */
  rescript(script: Script): void {
    this.#script = script;
  }

  /** Replaces the transfer script, so one test can force a status or silence. */
  retransfer(script: TransferScript): void {
    this.#transferScript = script;
  }

  /** Seeds a payload at `path`, as an upload would have. */
  hold(path: string, bytes: Uint8Array): void {
    this.blobs.set(path, bytes);
  }

  /** Resolves once `count` client frames have been received in total. */
  awaitReceived(count: number): Promise<void> {
    const waiter = Promise.withResolvers<void>();
    if (this.received.length >= count) {
      waiter.resolve();
    } else {
      this.#waiters.push({ count, resolve: waiter.resolve });
    }
    return waiter.promise;
  }

  /**
   * Resolves once `count` transfer requests have arrived in total.
   *
   * The synchronization point a transfer test needs: that the client's request
   * reached the socket, rather than that some interval has passed.
   */
  awaitTransfers(count: number): Promise<void> {
    const waiter = Promise.withResolvers<void>();
    if (this.transfers.length >= count) {
      waiter.resolve();
    } else {
      this.#transferWaiters.push({ count, resolve: waiter.resolve });
    }
    return waiter.promise;
  }

  /** Closes every connection and the listener. */
  async close(): Promise<void> {
    for (const socket of this.#sockets) {
      socket.removeAllListeners();
      socket.on("error", () => {});
      socket.destroy();
    }
    this.#sockets.clear();

    const closed = Promise.withResolvers<void>();
    this.#server.close(() => closed.resolve());
    await closed.promise;
  }

  /**
   * Sends one accepted connection to the protocol its first byte names.
   *
   * The same discrimination the relay makes, and this double makes it for the
   * same reason: both protocols share one port, so a test that reached the
   * transfer routes on a second port would be exercising a topology the relay
   * does not have. Every valid frame length is at most 65536, so a length
   * prefix begins with a zero byte; every method of the transfer protocol begins
   * with printable ASCII.
   */
  #accept(socket: Socket): void {
    this.#connections += 1;
    const connection = this.#connections;
    this.#sockets.add(socket);
    socket.setNoDelay(true);

    const accumulator = new FrameAccumulator();
    const session: RelaySession = {
      connection,
      send: (frame) => {
        socket.write(framePayload(encodePayload(frame)));
      },
      sendRaw: (bytes) => {
        socket.write(bytes);
      },
      drop: () => {
        socket.destroy();
      },
    };

    let protocol: "unknown" | "frames" | "transfer" = "unknown";
    let pending: Buffer<ArrayBufferLike> = Buffer.alloc(0);

    socket.on("data", (chunk: Buffer) => {
      if (protocol === "unknown") {
        protocol = chunk[0] === 0 ? "frames" : "transfer";
      }
      if (protocol === "transfer") {
        pending = Buffer.concat([pending, chunk]);
        pending = this.#serveTransfers(socket, pending);
        return;
      }

      const outcome = accumulator.push(chunk);
      if (outcome.failure !== null) {
        throw new Error(
          `the scripted relay could not deframe a client chunk: ${outcome.failure.detail}`,
        );
      }
      for (const value of outcome.values) {
        this.received.push(value);
        this.#script(value, session);
      }
      this.#notify();
    });

    socket.on("error", () => {
      // A client that vanished mid-write is a scenario, not a fault.
    });
    socket.on("close", () => {
      this.#sockets.delete(socket);
    });
  }

  /**
   * Answers every complete transfer request in `pending`, returning what is left.
   *
   * Hand-parsed rather than run through a server library: the whole surface is a
   * request line, a `content-length`, and a body, and a library here would be a
   * second HTTP implementation for a double whose job is to script answers.
   */
  #serveTransfers(socket: Socket, pending: Buffer<ArrayBufferLike>): Buffer<ArrayBufferLike> {
    let rest = pending;
    for (;;) {
      const separator = rest.indexOf("\r\n\r\n");
      if (separator < 0) {
        return rest;
      }
      const head = rest.subarray(0, separator).toString("utf8");
      const lines = head.split("\r\n");
      const [method = "", path = ""] = (lines[0] ?? "").split(" ");
      const declared = Number(
        lines
          .slice(1)
          .map((line) => line.split(":"))
          .find(([name]) => name?.trim().toLowerCase() === "content-length")?.[1]
          ?.trim() ?? "0",
      );
      const bodyStart = separator + 4;
      if (rest.length < bodyStart + declared) {
        return rest;
      }
      const body = new Uint8Array(rest.subarray(bodyStart, bodyStart + declared));
      rest = rest.subarray(bodyStart + declared);

      const request: TransferRequest = { method, path, body };
      this.transfers.push(request);
      this.#notifyTransfers();
      const answer = this.#transferScript(request, this);
      if (answer !== null) {
        socket.write(renderResponse(answer, method === "HEAD"));
      }
    }
  }

  #notifyTransfers(): void {
    for (let index = this.#transferWaiters.length - 1; index >= 0; index -= 1) {
      const waiter = this.#transferWaiters[index];
      if (waiter !== undefined && this.transfers.length >= waiter.count) {
        this.#transferWaiters.splice(index, 1);
        waiter.resolve();
      }
    }
  }

  #notify(): void {
    for (let index = this.#waiters.length - 1; index >= 0; index -= 1) {
      const waiter = this.#waiters[index];
      if (waiter !== undefined && this.received.length >= waiter.count) {
        this.#waiters.splice(index, 1);
        waiter.resolve();
      }
    }
  }
}

/**
 * The default transfer behaviour: a content-addressed store keyed by path.
 *
 * Deliberately does not check a reservation. That rule belongs to the real relay
 * and is tested against it; a double that enforced it would be asserting this
 * side's belief about the rule rather than the rule.
 */
function defaultTransferScript(
  request: TransferRequest,
  relay: ScriptedRelay,
): TransferResponse | null {
  const held = relay.blobs.get(request.path);
  switch (request.method) {
    case "PUT":
      if (held !== undefined) {
        return { status: 204 };
      }
      relay.blobs.set(request.path, request.body);
      return { status: 201 };
    case "HEAD":
    case "GET":
      if (held === undefined) {
        return { status: 404 };
      }
      return {
        status: 200,
        headers: {
          "content-type": "application/octet-stream",
          "cache-control": "public, max-age=31536000, immutable",
        },
        body: held,
      };
    default:
      return { status: 405 };
  }
}

/** Renders one response, omitting the body for a `HEAD`. */
function renderResponse(answer: TransferResponse, headOnly: boolean): Uint8Array {
  const body = answer.body ?? new Uint8Array(0);
  const headers: Record<string, string> = {
    ...answer.headers,
    // Declared from the payload even on a `HEAD`, which is the whole point of
    // the length-only request.
    "content-length": String(body.byteLength),
    connection: "close",
  };
  const head = [
    `HTTP/1.1 ${answer.status} ${STATUS_TEXT[answer.status] ?? "Unknown"}`,
    ...Object.entries(headers).map(([name, value]) => `${name}: ${value}`),
    "",
    "",
  ].join("\r\n");

  const rendered = Buffer.concat([
    Buffer.from(head, "utf8"),
    headOnly ? Buffer.alloc(0) : Buffer.from(body),
  ]);
  return new Uint8Array(rendered);
}

/** Reason phrases for the statuses this double answers with. */
const STATUS_TEXT: Record<number, string> = {
  200: "OK",
  201: "Created",
  204: "No Content",
  400: "Bad Request",
  403: "Forbidden",
  404: "Not Found",
  405: "Method Not Allowed",
  413: "Content Too Large",
  500: "Internal Server Error",
};
