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
  encodePayload,
  FrameAccumulator,
  LENGTH_PREFIX_BYTES,
  type ServerFrame,
} from "../../src/protocol.ts";

/** What a script may do in response to a client frame. */
export interface RelaySession {
  /** Writes one server frame, length-prefixed. */
  send(frame: ServerFrame): void;
  /** Writes bytes verbatim, for malformed-framing cases. */
  sendRaw(bytes: Uint8Array): void;
  /** Closes this connection without saying anything further. */
  drop(): void;
  /** Which connection this is, 1-based, so a script can behave differently on a reconnect. */
  readonly connection: number;
}

/** Called for every decoded client frame. Throwing fails the test loudly. */
export type Script = (frame: unknown, session: RelaySession) => void;

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
  #script: Script;
  #connections = 0;

  /** Every client frame received across every connection, decoded, in order. */
  readonly received: unknown[] = [];

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

    socket.on("data", (chunk: Buffer) => {
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
