/**
 * The relay client: one TCP connection, the protocol v1 handshake, a heartbeat,
 * automatic reconnect, and request/reply correlation.
 *
 * Two properties shape every decision in this file.
 *
 * **Nothing escapes into the host.** Every socket callback, timer callback, and
 * detached promise catches its own errors. A host may be a long-lived
 * interactive session in which an unhandled rejection terminates the process,
 * so a decode failure or an unreachable relay has to stay a local event.
 *
 * **The host supplies the scheduler.** Timer functions arrive at construction
 * and default to the ambient ones. This is not gratuitous injection: the OMP
 * runtime documents that a throw from a raw `setInterval` callback escapes
 * handler dispatch and crashes the session, and that its own `ctx.setInterval`
 * exists to contain exactly that. A client reaching for ambient timers could
 * not be made safe by its host without monkey-patching globals.
 *
 * Nothing in this module imports OMP.
 */

import { connect, type Socket } from "node:net";

import type { RelayConfig } from "./config.ts";
import {
  bodyOverBudget,
  correlationProblem,
  describe,
  describeIdentifierProblem,
  encodeFrame,
  FrameAccumulator,
  MAX_BODY_BYTES,
  PROTOCOL_VERSION,
  validateServerFrame,
  type ClientFrame,
  type ErrorCode,
  type ErrorFrame,
  type MessageFrame,
  type PeersFrame,
  type ReceiptFrame,
  type ServerFrame,
} from "./protocol.ts";

/**
 * Deadline for every request, from issue to settlement.
 *
 * The relay does no work that can legitimately take longer: it validates a
 * frame, performs one hash lookup, and calls a non-blocking enqueue. A request
 * outstanding for five seconds means the relay is gone, wedged, or never saw
 * the frame, and waiting longer only delays a certain failure.
 *
 * This is a judgment about the relay rather than a measurement of it. If the
 * relay ever does real work per frame, this constant becomes wrong — which is
 * why it is named and referenced directly by its test.
 */
export const REQUEST_TIMEOUT_MS = 5_000;

/**
 * Outbound idle period after which the client sends `ping`.
 *
 * Comfortably inside the relay's 90-second idle deadline, so two consecutive
 * lost heartbeats still leave a third before eviction.
 */
export const HEARTBEAT_INTERVAL_MS = 30_000;

/** Delay before the first reconnect attempt of an outage. */
export const RECONNECT_INITIAL_MS = 500;

/** Ceiling on the reconnect delay, jitter included. */
export const RECONNECT_CAP_MS = 30_000;

/** Fraction of the computed delay that jitter may add or subtract. */
export const RECONNECT_JITTER = 0.2;

/**
 * Opaque timer identity: only the scheduler that produced a handle interprets
 * it. `number`, a Node `Timeout`, and an OMP context handle all pass through.
 */
export type TimerHandle = unknown;

/**
 * The timer functions the client is allowed to use.
 *
 * Declared with method syntax so a host can supply the ambient functions, whose
 * `clear*` parameters are narrower than {@link TimerHandle}, without a cast.
 */
export interface Scheduler {
  setTimeout(callback: () => void, milliseconds: number): TimerHandle;
  clearTimeout(handle: TimerHandle): void;
  setInterval(callback: () => void, milliseconds: number): TimerHandle;
  clearInterval(handle: TimerHandle): void;
}

/** The ambient timers, used only when a host supplies no scheduler. */
export const ambientScheduler: Scheduler = {
  setTimeout,
  clearTimeout,
  setInterval,
  clearInterval,
};

/** How urgent a {@link Report} is, for a host deciding whether to surface it. */
export type ReportLevel = "info" | "warn" | "error";

/** One operational diagnostic. The client never logs; the host does. */
export interface Report {
  readonly level: ReportLevel;
  readonly message: string;
}

/** Callbacks a host may install. Every one is optional. */
export interface RelayClientHandlers {
  /** A message relayed from another peer, already validated. */
  readonly onMessage?: (message: MessageFrame) => void;
  /** The connection became usable: `ready` arrived. */
  readonly onReady?: () => void;
  /** The connection ended, with the reason it ended for. */
  readonly onDisconnect?: (reason: string) => void;
  /** An operational diagnostic worth a log line. */
  readonly onReport?: (report: Report) => void;
}

/** Everything the client needs to run. */
export interface RelayClientOptions {
  /**
   * A validated configuration.
   *
   * Deliberately not a path: loading and validating belongs to `config.ts`, and
   * keeping them apart is what lets a test hand the client a value the config
   * validator would have refused, which is the only way to exercise the
   * relay's own rejection of a bad identifier.
   */
  readonly config: RelayConfig;
  /** Timer functions; defaults to {@link ambientScheduler}. */
  readonly scheduler?: Scheduler;
  /** Host callbacks. */
  readonly handlers?: RelayClientHandlers;
}

/** Why a request did not produce the reply its caller asked for. */
export type RequestFailureReason =
  /** The caller's own arguments were refused before anything was written. */
  | "invalid_request"
  /** The client was stopped when the request was issued, or while it waited. */
  | "stopped"
  /** The connection ended before the reply arrived. */
  | "disconnected"
  /** {@link REQUEST_TIMEOUT_MS} elapsed with no reply. */
  | "timeout"
  /** The relay answered with an `error` frame naming this request. */
  | "relay_error"
  /** The relay answered with a frame of the wrong type for this request. */
  | "unexpected_reply";

/** A request that will not be answered. Rejects the caller's promise. */
export class RequestFailed extends Error {
  override readonly name = "RequestFailed";
  /**
   * Which category of failure this is.
   *
   * Named `reason` rather than `cause` on purpose: `Error.cause` conventionally
   * carries the underlying error that produced this one, and a domain enum
   * sitting in that slot would mislead every generic error handler that reads
   * it.
   */
  readonly reason: RequestFailureReason;
  /** The relay's code, when {@link reason} is `relay_error`. */
  readonly code: ErrorCode | null;

  constructor(
    reason: RequestFailureReason,
    message: string,
    code: ErrorCode | null = null,
  ) {
    super(message);
    this.reason = reason;
    this.code = code;
  }
}

/** Observable connection state. */
export type ClientState = "stopped" | "connecting" | "ready";

/** What a `send` needs. `id` is generated when the caller does not supply one. */
export interface SendRequest {
  readonly to: string;
  readonly body: string;
  readonly replyTo?: string;
  readonly id?: string;
}

type RequestOutcome =
  | { readonly ok: true; readonly frame: PeersFrame | ReceiptFrame }
  | { readonly ok: false; readonly error: RequestFailed };

interface PendingRequest {
  /**
   * Which reply this entry accepts. A `list` and a `send` may legitimately
   * carry the same correlation token — the relay treats both as opaque and
   * scopes neither — so the token alone does not identify the reply space.
   */
  readonly kind: "list" | "send";
  readonly settle: (outcome: RequestOutcome) => void;
  timer: TimerHandle | null;
  /**
   * The encoded frame, held until `ready` arrives. Non-null means "not yet
   * written", which is how a request issued while connecting avoids being
   * written to an unready socket without failing its caller outright.
   */
  unwritten: Uint8Array | null;
}

/**
 * Delay before reconnect attempt number `attempt` (1-based).
 *
 * Exponential from {@link RECONNECT_INITIAL_MS}, capped at
 * {@link RECONNECT_CAP_MS}, with jitter of ±{@link RECONNECT_JITTER} applied to
 * the computed value and the cap re-applied afterwards so jitter cannot exceed
 * it.
 *
 * Jitter is proportional rather than the more common "full jitter" over
 * `[0, base)`, because the reconnect requirement asks for a delay that
 * *increases* between attempts. Full jitter does not: it lets attempt five wait
 * less than attempt two, which is fine for a thundering herd and wrong for the
 * behavior specified here. Proportional jitter keeps each attempt's range
 * disjoint from the previous one while still decorrelating two clients that
 * lost the same relay.
 *
 * Exported because it is the one part of the reconnect policy worth asserting
 * arithmetically, with a supplied `random`, rather than through a fake clock.
 */
export function backoffDelay(attempt: number, random: () => number): number {
  const exponential = RECONNECT_INITIAL_MS * 2 ** Math.max(0, attempt - 1);
  const base = Math.min(exponential, RECONNECT_CAP_MS);
  const jittered = base * (1 + RECONNECT_JITTER * (random() * 2 - 1));
  return Math.min(Math.round(jittered), RECONNECT_CAP_MS);
}

/**
 * A connection to one relay, for one room and one peer name.
 *
 * Construction does nothing observable. {@link start} begins connecting and
 * returns immediately; an unreachable relay is reported once and retried, never
 * raised. {@link stop} is final: it cancels every timer, settles every pending
 * request, and makes no further connection attempt.
 */
export class RelayClient {
  readonly #config: RelayConfig;
  readonly #scheduler: Scheduler;
  readonly #handlers: RelayClientHandlers;
  readonly #pending = new Map<string, PendingRequest>();

  #state: ClientState = "stopped";
  #stopped = true;
  #socket: Socket | null = null;
  #accumulator: FrameAccumulator | null = null;
  #heartbeat: TimerHandle | null = null;
  #reconnect: TimerHandle | null = null;
  #attempt = 0;
  /**
   * Whether the current outage has already been reported.
   *
   * "Report the condition once" is per outage, not per attempt: a relay down
   * for an hour would otherwise produce a log line every thirty seconds in a
   * host the user is reading. Cleared by a received `ready`.
   */
  #outageReported = false;
  /**
   * The relay's own stated cause for the connection ending, when it sent one.
   *
   * Kept so a close is reported as `unsupported_protocol` rather than as an
   * anonymous disconnect. Only errors that answer no pending request land here;
   * an error naming a request has already been delivered to that request.
   */
  #statedCause: string | null = null;

  constructor(options: RelayClientOptions) {
    this.#config = options.config;
    this.#scheduler = options.scheduler ?? ambientScheduler;
    this.#handlers = options.handlers ?? {};
  }

  /** Observable connection state. */
  get state(): ClientState {
    return this.#state;
  }

  /** Requests issued but not yet settled. Bounded; never grows from replies. */
  get pendingRequests(): number {
    return this.#pending.size;
  }

  /**
   * Begins connecting. Returns immediately and never throws.
   *
   * Idempotent while running: a second call on a live client does nothing.
   */
  start(): void {
    if (!this.#stopped) {
      return;
    }
    this.#stopped = false;
    this.#attempt = 0;
    this.#outageReported = false;
    this.#openConnection();
  }

  /**
   * Stops for good: cancels every timer, settles every pending request, closes
   * the socket, and schedules no further attempt.
   *
   * Resolves once the socket has actually closed, so a caller — a test, or a
   * host tearing down a session — can rely on no callback firing afterwards.
   */
  async stop(): Promise<void> {
    this.#stopped = true;
    this.#state = "stopped";
    this.#cancelReconnect();
    this.#clearHeartbeat();
    this.#failPending(new RequestFailed("stopped", "the client was stopped"));

    const socket = this.#socket;
    this.#socket = null;
    this.#accumulator = null;
    if (socket === null) {
      return;
    }

    socket.removeAllListeners();
    // An already-destroyed socket is not guaranteed to emit `close` again, so
    // awaiting one would hang shutdown forever. This is reachable: the peer's
    // FIN can destroy the socket before the `close` handler that clears
    // `#socket` has run, which leaves a live reference to a dead socket.
    if (socket.destroyed) {
      return;
    }

    const closed = Promise.withResolvers<void>();
    socket.once("close", () => closed.resolve());
    // A destroyed socket may still emit `error`; that also means it is gone.
    socket.once("error", () => closed.resolve());
    socket.destroy();
    await closed.promise;
  }

  /**
   * Requests the room's peer roster.
   *
   * @param requestId opaque correlation token; generated when omitted.
   */
  list(requestId: string = crypto.randomUUID()): Promise<PeersFrame> {
    const { promise, resolve, reject } = Promise.withResolvers<PeersFrame>();

    const problem = correlationProblem(requestId);
    if (problem !== null) {
      reject(
        new RequestFailed(
          "invalid_request",
          `list request_id ${describeIdentifierProblem(problem)}`,
        ),
      );
      return promise;
    }

    this.#issue(requestId, "list", { type: "list", request_id: requestId }, (outcome) => {
      if (!outcome.ok) {
        reject(outcome.error);
      } else if (outcome.frame.type === "peers") {
        resolve(outcome.frame);
      } else {
        reject(
          new RequestFailed(
            "unexpected_reply",
            `list ${requestId} was answered with a ${outcome.frame.type} frame`,
          ),
        );
      }
    });
    return promise;
  }

  /**
   * Relays `body` to another peer in the room.
   *
   * Resolves with the relay's receipt, including `peer_offline` and
   * `invalid_target`: those are answers, not failures of the request. The
   * caller branches on `status`.
   */
  send(request: SendRequest): Promise<ReceiptFrame> {
    const id = request.id ?? crypto.randomUUID();
    const { promise, resolve, reject } = Promise.withResolvers<ReceiptFrame>();

    const refuse = (message: string): Promise<ReceiptFrame> => {
      reject(new RequestFailed("invalid_request", message));
      return promise;
    };

    const idProblem = correlationProblem(id);
    if (idProblem !== null) {
      return refuse(`send id ${describeIdentifierProblem(idProblem)}`);
    }
    if (request.replyTo !== undefined) {
      const replyProblem = correlationProblem(request.replyTo);
      if (replyProblem !== null) {
        return refuse(`send reply_to ${describeIdentifierProblem(replyProblem)}`);
      }
    }
    // Checked here rather than left to the relay because the relay answers an
    // over-budget body by closing the connection. Refusing it locally costs the
    // caller one rejected promise instead of an outage.
    const oversized = bodyOverBudget(request.body);
    if (oversized !== null) {
      return refuse(
        `send body is ${oversized} UTF-8 bytes, over the ${MAX_BODY_BYTES}-byte budget`,
      );
    }

    this.#issue(
      id,
      "send",
      {
        type: "send",
        id,
        to: request.to,
        body: request.body,
        ...(request.replyTo === undefined ? {} : { reply_to: request.replyTo }),
      },
      (outcome) => {
        if (!outcome.ok) {
          reject(outcome.error);
        } else if (outcome.frame.type === "receipt") {
          resolve(outcome.frame);
        } else {
          reject(
            new RequestFailed(
              "unexpected_reply",
              `send ${id} was answered with a ${outcome.frame.type} frame`,
            ),
          );
        }
      },
    );
    return promise;
  }

  // -------------------------------------------------------------------------
  // Connection lifecycle
  // -------------------------------------------------------------------------

  #openConnection(): void {
    this.#cancelReconnect();
    this.#state = "connecting";
    this.#accumulator = new FrameAccumulator();
    this.#statedCause = null;

    let socket: Socket;
    try {
      socket = connect({
        host: this.#config.transport.host,
        port: this.#config.transport.port,
      });
    } catch (error) {
      // `connect` can throw synchronously on a malformed target. Treated as a
      // failed attempt rather than allowed to escape a timer callback.
      this.#scheduleReconnect(`could not open a connection: ${describe(error)}`);
      return;
    }

    this.#socket = socket;
    // This is a control protocol: frames are small, latency matters, and there
    // is nothing for Nagle to coalesce that waiting would improve.
    socket.setNoDelay(true);

    socket.on("connect", () => {
      this.#guard("connect", () => {
        this.#write({
          type: "hello",
          protocol: PROTOCOL_VERSION,
          room: this.#config.room,
          peer: this.#config.peer,
        });
      });
    });

    socket.on("data", (chunk: Buffer) => {
      this.#guard("data", () => this.#receive(socket, chunk));
    });

    socket.on("error", (error: Error) => {
      this.#guard("error", () => {
        this.#closeConnection(socket, this.#statedCause ?? describe(error));
      });
    });

    socket.on("close", () => {
      this.#guard("close", () => {
        this.#closeConnection(
          socket,
          this.#statedCause ?? "the relay closed the connection",
        );
      });
    });
  }

  #receive(socket: Socket, chunk: Buffer): void {
    if (socket !== this.#socket || this.#accumulator === null) {
      return;
    }
    const outcome = this.#accumulator.push(chunk);
    for (const value of outcome.values) {
      const validated = validateServerFrame(value);
      switch (validated.kind) {
        case "frame":
          this.#dispatch(validated.frame);
          break;
        case "ignorable":
          // Forward compatibility: a relay one version ahead may send a frame
          // this client has no code for. Ignoring it and staying connected is
          // the whole point of the additive-evolution rule.
          this.#report("info", `ignored an unrecognized ${validated.type} frame`);
          break;
        case "invalid":
          // A malformed frame is not recoverable the way an unknown type is: it
          // means the peer is not speaking this protocol, and continuing to
          // read it would be guessing.
          this.#closeConnection(socket, `malformed frame: ${validated.reason}`);
          return;
      }
      // A dispatched frame may have torn the connection down.
      if (socket !== this.#socket) {
        return;
      }
    }
    if (outcome.failure !== null) {
      this.#closeConnection(
        socket,
        `framing failure (${outcome.failure.reason}): ${outcome.failure.detail}`,
      );
    }
  }

  #dispatch(frame: ServerFrame): void {
    switch (frame.type) {
      case "ready":
        this.#state = "ready";
        this.#attempt = 0;
        this.#outageReported = false;
        this.#armHeartbeat();
        this.#flushUnwritten();
        this.#handlers.onReady?.();
        break;

      case "pong":
        // Consumed deliberately: a heartbeat answer is not a caller's business.
        break;

      case "message":
        this.#handlers.onMessage?.(frame);
        break;

      case "peers":
        this.#settle(frame.request_id, "list", { ok: true, frame });
        break;

      case "receipt":
        this.#settle(frame.id, "send", { ok: true, frame });
        break;

      case "error":
        this.#handleError(frame);
        break;
    }
  }

  /**
   * Routes an `error` frame to the request it answers, or to the connection.
   *
   * `wire-protocol` obliges the relay to echo the correlation token of a
   * recoverable rejection, so an error naming a pending request is that
   * request's answer and settles it immediately instead of leaving the caller
   * to wait out the full timeout.
   *
   * An error that names nothing, or that arrives before `ready`, is about the
   * connection. Every pre-readiness error is one: the relay rejects a handshake
   * and closes, so recording the code here is what turns the imminent close
   * into a stated cause rather than an anonymous disconnect.
   */
  #handleError(frame: ErrorFrame): void {
    const detail = frame.message === undefined ? frame.code : `${frame.code}: ${frame.message}`;

    if (frame.request_id !== undefined && this.#state === "ready") {
      const settled = this.#settleEither(frame.request_id, {
        ok: false,
        error: new RequestFailed("relay_error", `the relay rejected the request: ${detail}`, frame.code),
      });
      if (settled) {
        return;
      }
    }

    if (this.#state === "ready") {
      this.#statedCause = `the relay reported ${detail}`;
      this.#report("error", `the relay reported ${detail}`);
      return;
    }
    this.#statedCause = `the relay rejected the handshake: ${detail}`;
  }

  /**
   * Ends the current connection and, unless stopped, schedules a reconnect.
   *
   * Guarded by socket identity so the `error`-then-`close` pair a failed socket
   * emits tears down once, and so a stale socket's late event cannot disturb
   * the connection that replaced it.
   */
  #closeConnection(socket: Socket, reason: string): void {
    if (socket !== this.#socket) {
      return;
    }
    this.#socket = null;
    this.#accumulator = null;
    this.#clearHeartbeat();
    this.#state = this.#stopped ? "stopped" : "connecting";

    socket.removeAllListeners();
    // A destroyed socket may still emit `error`; absorb it rather than let it
    // reach the process as an unhandled event.
    socket.on("error", () => {});
    socket.destroy();

    this.#failPending(new RequestFailed("disconnected", reason));
    this.#handlers.onDisconnect?.(reason);

    if (!this.#stopped) {
      this.#scheduleReconnect(reason);
    }
  }

  #scheduleReconnect(reason: string): void {
    if (this.#stopped) {
      return;
    }
    this.#attempt += 1;
    const delay = backoffDelay(this.#attempt, Math.random);

    if (!this.#outageReported) {
      this.#outageReported = true;
      this.#report("warn", `${reason}; reconnecting in ${delay} ms`);
    }

    this.#cancelReconnect();
    this.#reconnect = this.#scheduler.setTimeout(() => {
      this.#reconnect = null;
      this.#guard("reconnect", () => {
        if (!this.#stopped) {
          this.#openConnection();
        }
      });
    }, delay);
  }

  #cancelReconnect(): void {
    if (this.#reconnect !== null) {
      this.#scheduler.clearTimeout(this.#reconnect);
      this.#reconnect = null;
    }
  }

  // -------------------------------------------------------------------------
  // Heartbeat
  // -------------------------------------------------------------------------

  /**
   * Restarts the outbound-idle countdown.
   *
   * Called after every outbound frame, which is what "any outbound frame resets
   * the heartbeat interval" means: a `send` 25 seconds into an idle period
   * pushes the next `ping` to 30 seconds after that frame, not 5 seconds later.
   * The `ping` itself goes through the same write path, so the interval is
   * re-armed by its own tick and never drifts.
   */
  #armHeartbeat(): void {
    this.#clearHeartbeat();
    this.#heartbeat = this.#scheduler.setInterval(() => {
      this.#guard("heartbeat", () => {
        if (this.#state === "ready") {
          this.#write({ type: "ping" });
        }
      });
    }, HEARTBEAT_INTERVAL_MS);
  }

  #clearHeartbeat(): void {
    if (this.#heartbeat !== null) {
      this.#scheduler.clearInterval(this.#heartbeat);
      this.#heartbeat = null;
    }
  }

  // -------------------------------------------------------------------------
  // Request correlation
  // -------------------------------------------------------------------------

  #issue(
    token: string,
    kind: "list" | "send",
    frame: ClientFrame,
    settle: (outcome: RequestOutcome) => void,
  ): void {
    if (this.#stopped) {
      settle({
        ok: false,
        error: new RequestFailed("stopped", "the client is not running"),
      });
      return;
    }
    const key = `${kind}:${token}`;
    if (this.#pending.has(key)) {
      settle({
        ok: false,
        error: new RequestFailed(
          "invalid_request",
          `a ${kind} with correlation token ${JSON.stringify(token)} is already outstanding`,
        ),
      });
      return;
    }

    let encoded: Uint8Array;
    try {
      encoded = encodeFrame(frame);
    } catch (error) {
      settle({
        ok: false,
        error: new RequestFailed("invalid_request", describe(error)),
      });
      return;
    }

    const entry: PendingRequest = { kind, settle, timer: null, unwritten: encoded };
    this.#pending.set(key, entry);

    // Started before the write, and running whether or not the frame has been
    // written yet. A request issued while connecting therefore waits for
    // readiness inside its own deadline instead of waiting forever.
    entry.timer = this.#scheduler.setTimeout(() => {
      entry.timer = null;
      this.#guard("request timeout", () => {
        this.#settleKey(key, {
          ok: false,
          error: new RequestFailed(
            "timeout",
            `no reply within ${REQUEST_TIMEOUT_MS} ms`,
          ),
        });
      });
    }, REQUEST_TIMEOUT_MS);

    if (this.#state === "ready") {
      this.#writeEncoded(encoded);
      entry.unwritten = null;
    }
  }

  /** Writes every request that was issued before `ready` arrived. */
  #flushUnwritten(): void {
    for (const entry of this.#pending.values()) {
      if (entry.unwritten !== null) {
        const encoded = entry.unwritten;
        entry.unwritten = null;
        this.#writeEncoded(encoded);
      }
    }
  }

  /**
   * Settles the entry `token` names within the `kind` reply space.
   *
   * A reply matching nothing is discarded rather than treated as an error: a
   * late reply after a timeout is expected and harmless, and the pending map
   * cannot grow from one.
   */
  #settle(token: string, kind: "list" | "send", outcome: RequestOutcome): void {
    if (!this.#settleKey(`${kind}:${token}`, outcome)) {
      this.#report(
        "info",
        `discarded a ${kind} reply for unknown correlation token ${JSON.stringify(token)}`,
      );
    }
  }

  /** Settles whichever reply space holds `token`. Used for `error` frames. */
  #settleEither(token: string, outcome: RequestOutcome): boolean {
    return (
      this.#settleKey(`send:${token}`, outcome) ||
      this.#settleKey(`list:${token}`, outcome)
    );
  }

  #settleKey(key: string, outcome: RequestOutcome): boolean {
    const entry = this.#pending.get(key);
    if (entry === undefined) {
      return false;
    }
    // Removed before settling, so a callback that re-enters cannot observe an
    // entry that has already been answered.
    this.#pending.delete(key);
    if (entry.timer !== null) {
      this.#scheduler.clearTimeout(entry.timer);
      entry.timer = null;
    }
    try {
      entry.settle(outcome);
    } catch (error) {
      this.#report("error", `a request callback threw: ${describe(error)}`);
    }
    return true;
  }

  #failPending(error: RequestFailed): void {
    for (const key of [...this.#pending.keys()]) {
      this.#settleKey(key, { ok: false, error });
    }
  }

  // -------------------------------------------------------------------------
  // Writing and containment
  // -------------------------------------------------------------------------

  #write(frame: ClientFrame): void {
    let encoded: Uint8Array;
    try {
      encoded = encodeFrame(frame);
    } catch (error) {
      this.#report("error", `refused to send a ${frame.type} frame: ${describe(error)}`);
      return;
    }
    this.#writeEncoded(encoded);
  }

  #writeEncoded(encoded: Uint8Array): void {
    const socket = this.#socket;
    if (socket === null) {
      return;
    }
    try {
      socket.write(encoded);
    } catch (error) {
      this.#closeConnection(socket, `write failed: ${describe(error)}`);
      return;
    }
    this.#armHeartbeat();
  }

  /**
   * Runs `work`, converting any throw into a report.
   *
   * Every socket callback and every timer callback goes through here. In a host
   * whose runtime lets a throw from a raw callback escape handler dispatch,
   * this is the difference between a logged diagnostic and a terminated
   * session.
   */
  #guard(site: string, work: () => void): void {
    try {
      work();
    } catch (error) {
      this.#report("error", `${site} handler failed: ${describe(error)}`);
    }
  }

  #report(level: ReportLevel, message: string): void {
    const handler = this.#handlers.onReport;
    if (handler === undefined) {
      return;
    }
    try {
      handler({ level, message });
    } catch {
      // A host whose own reporter throws cannot be told about it. Swallowing is
      // the only option that does not escalate a logging fault into a crash.
    }
  }
}
