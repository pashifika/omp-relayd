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

/**
 * Deadline for `ready` to arrive after the socket opens.
 *
 * Twice the relay's own five-second `hello` deadline, so a relay still inside
 * its own handshake window is never abandoned, while a peer that accepts the
 * connection and then says nothing becomes an ordinary reconnect instead of
 * leaving the client `connecting` for the life of the process.
 */
export const HANDSHAKE_TIMEOUT_MS = 10_000;

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
 * `value` as a promise, when it is one.
 *
 * Every host handler type returns `void`, and TypeScript accepts an `async`
 * function wherever a `void`-returning one is expected, so any of them may hand
 * back a promise whose rejection a synchronous `try`/`catch` cannot observe.
 * Tested by `then` rather than by `instanceof Promise`, because the promise may
 * come from another realm or another library.
 */
function asThenable(value: unknown): PromiseLike<unknown> | null {
  const candidate = value as PromiseLike<unknown> | null;
  return typeof candidate?.then === "function" ? candidate : null;
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
  /**
   * Correlation keys whose request timed out on the current connection.
   *
   * A reply later than the deadline still names one, so reissuing it would let
   * that reply settle a different request. Cleared when the connection ends,
   * which is the whole scope over which such a reply is possible.
   */
  readonly #timedOut = new Set<string>();

  #state: ClientState = "stopped";
  #stopped = true;
  /**
   * Which run of this client is current, incremented by every `stop()`.
   *
   * `#stopped` alone cannot isolate a detached rejection: a handler's promise
   * may still be pending when `stop()` resolves, and a later `start()` sets
   * that flag back to false, so the stale rejection would be reported against
   * the run that replaced it. Comparing the run it was attached in keeps
   * `stop()`'s promise that no callback fires afterwards.
   */
  #generation = 0;
  #socket: Socket | null = null;
  #accumulator: FrameAccumulator | null = null;
  #heartbeat: TimerHandle | null = null;
  #reconnect: TimerHandle | null = null;
  #handshake: TimerHandle | null = null;
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
    this.#generation += 1;
    this.#state = "stopped";
    this.#cancelReconnect();
    this.#clearHandshakeDeadline();
    this.#clearHeartbeat();
    this.#timedOut.clear();
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
        // Armed before the write, so nothing can leave the client waiting on a
        // peer that accepted the connection and then said nothing.
        this.#armHandshakeDeadline(socket);
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
          this.#dispatch(socket, validated.frame);
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

  /**
   * Routes one validated server frame, honouring the connection phase.
   *
   * Before `ready` the client has written nothing but `hello`, so a `peers`,
   * `receipt`, or `message` answers nothing: delivering one would settle a
   * request that was deliberately never written, or hand the host a message
   * from a connection that has not been admitted. Only the handshake's own two
   * answers mean anything there.
   *
   * A second `ready` is a protocol violation rather than a re-admission:
   * re-running the readiness path would re-fire `onReady`, re-arm the
   * heartbeat, and re-flush, so the connection is failed instead.
   */
  #dispatch(socket: Socket, frame: ServerFrame): void {
    if (this.#state === "ready") {
      if (frame.type === "ready") {
        this.#closeConnection(socket, "the relay sent a second ready frame");
        return;
      }
    } else if (frame.type !== "ready" && frame.type !== "error") {
      this.#report("warn", `ignored a ${frame.type} frame received before ready`);
      return;
    }

    switch (frame.type) {
      case "ready":
        this.#state = "ready";
        this.#attempt = 0;
        this.#outageReported = false;
        this.#clearHandshakeDeadline();
        this.#armHeartbeat();
        this.#flushUnwritten();
        this.#notify("onReady", () => this.#handlers.onReady?.());
        break;

      case "pong":
        // Consumed deliberately: a heartbeat answer is not a caller's business.
        break;

      case "message":
        this.#notify("onMessage", () => this.#handlers.onMessage?.(frame));
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
    this.#clearHandshakeDeadline();
    this.#clearHeartbeat();
    // A late reply can only name a token from the connection that is ending.
    this.#timedOut.clear();
    this.#state = this.#stopped ? "stopped" : "connecting";

    socket.removeAllListeners();
    // A destroyed socket may still emit `error`; absorb it rather than let it
    // reach the process as an unhandled event.
    socket.on("error", () => {});
    socket.destroy();

    this.#failPending(new RequestFailed("disconnected", reason));

    // Recovery is armed before the host is told, so that reconnecting cannot
    // depend on what the host's handler does.
    if (!this.#stopped) {
      this.#scheduleReconnect(reason);
    }
    this.#notify("onDisconnect", () => this.#handlers.onDisconnect?.(reason));
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

  /**
   * Arms the deadline for `ready` on a freshly opened connection.
   *
   * The heartbeat cannot serve as this deadline: its callback returns unless
   * the state is already `ready`, so it is inert during exactly the phase this
   * covers. Expiry goes through the ordinary close path, so a silent peer ends
   * up on the same backoff as an unreachable one.
   */
  #armHandshakeDeadline(socket: Socket): void {
    this.#clearHandshakeDeadline();
    this.#handshake = this.#scheduler.setTimeout(() => {
      this.#handshake = null;
      this.#guard("handshake deadline", () => {
        this.#closeConnection(
          socket,
          `no ready within ${HANDSHAKE_TIMEOUT_MS} ms of connecting`,
        );
      });
    }, HANDSHAKE_TIMEOUT_MS);
  }

  #clearHandshakeDeadline(): void {
    if (this.#handshake !== null) {
      this.#scheduler.clearTimeout(this.#handshake);
      this.#handshake = null;
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
    if (this.#timedOut.has(key)) {
      settle({
        ok: false,
        error: new RequestFailed(
          "invalid_request",
          `a ${kind} with correlation token ${JSON.stringify(token)} timed out on this connection; its late reply would settle this request instead`,
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
        // Tombstoned only once the frame has actually reached the socket. A
        // request still holding its encoding was never sent, so no late reply
        // can exist and its token stays free for a retry.
        if (entry.unwritten === null) {
          this.#timedOut.add(key);
        }
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

  /**
   * Invokes one host callback so that neither a synchronous throw nor a
   * rejected promise can reach the host process or the client's own control
   * flow.
   *
   * Containment at the socket-event boundary is not enough: a `#guard` around
   * the whole event still loses every frame that event had left to dispatch,
   * and it cannot see a rejection at all.
   */
  #notify(site: string, call: () => unknown): void {
    // Captured at attach time, not read at rejection time: see `#generation`.
    const generation = this.#generation;
    try {
      // Inspecting the return value stays inside the guard: the thenable is
      // host code too, so reading `then` or calling it can throw, and a throw
      // out here would unwind the frame loop exactly as one from the callback
      // itself would.
      void asThenable(call())?.then(undefined, (error: unknown) => {
        // `stop()` promises its caller that no callback fires afterwards, and
        // a detached rejection is the one path that can outlive it.
        if (this.#stopped || generation !== this.#generation) {
          return;
        }
        this.#report("error", `the host's ${site} handler rejected: ${describe(error)}`);
      });
    } catch (error) {
      this.#report("error", `the host's ${site} handler failed: ${describe(error)}`);
    }
  }

  #report(level: ReportLevel, message: string): void {
    try {
      // Read inside the guard: `onReport` may be a getter, and this is the one
      // path every other containment site funnels its diagnostic through, so a
      // throw here would escape the guard that called it.
      const handler = this.#handlers.onReport;
      if (handler === undefined) {
        return;
      }
      const returned: unknown = handler({ level, message });
      // Not routed through `#notify`: a reporter's own failure has nowhere to
      // be reported. Attaching to its rejection is still required, or an async
      // reporter turns a logging fault into an unhandled rejection.
      void asThenable(returned)?.then(undefined, () => {});
    } catch {
      // A host whose own reporter throws cannot be told about it. Swallowing is
      // the only option that does not escalate a logging fault into a crash.
    }
  }
}
