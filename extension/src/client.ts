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
  describeDigestProblem,
  describeIdentifierProblem,
  digestOf,
  digestProblem,
  encodeFrame,
  FrameAccumulator,
  MAX_BODY_BYTES,
  PROTOCOL_VERSION,
  validateServerFrame,
  type AcceptedFrame,
  type ClientFrame,
  type DeliveryFrame,
  type ErrorCode,
  type ErrorFrame,
  type PeersFrame,
  type ReceiptFrame,
  type ReservedFrame,
  type ReserveStatus,
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

/**
 * Ceiling on one payload transfer with no answer.
 *
 * Deliberately not {@link REQUEST_TIMEOUT_MS}. That deadline is right for a
 * frame exchange, where an answer either comes promptly or is not coming; a
 * transfer's duration scales with the payload, so the same bound would fail a
 * large payload on a slow link *because it was working*.
 *
 * Ten seconds is a bound on silence rather than on the transfer: `fetch`
 * resolves as soon as response headers arrive, so this covers the round trip to
 * the first byte and not the body that follows it. A relay that is up answers a
 * `reserve`d upload's headers in milliseconds on a LAN.
 */
export const TRANSFER_STALL_MS = 10_000;

/**
 * How much of an upload is offered to the socket at a time.
 *
 * Only a granularity for progress: it decides how often {@link TRANSFER_STALL_MS}
 * is restarted while uploading, not how fast the transfer runs. 64 KiB is the
 * frame cap, so a stalled upload is noticed within one frame's worth of bytes.
 */
export const TRANSFER_CHUNK_BYTES = 64 * 1024;

/** Delay before the first reconnect attempt of an outage. */
export const RECONNECT_INITIAL_MS = 500;

/** Ceiling on the reconnect delay, jitter included. */
export const RECONNECT_CAP_MS = 30_000;

/** Fraction of the computed delay that jitter may add or subtract. */
export const RECONNECT_JITTER = 0.2;

/**
 * What the host is told when the relay hands this peer name to another session.
 *
 * Phrased as an operator instruction rather than as a relay code, because the
 * condition is a configuration collision: two sessions were told to register
 * the same `peer`, and only changing one of them resolves it.
 */
export const PEER_REPLACED_REPORT =
  "another session registered this peer name in this room, so the relay displaced this one; " +
  "reconnecting would displace that session in turn, so OMP Relay has stopped. " +
  "Give each session its own peer name in its configuration.";

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
  /**
   * A delivery relayed from another peer, already validated.
   *
   * Carries both classes, and the class is `type`. One callback rather than two
   * so that a host cannot drop announcements by forgetting to install a second
   * handler: `wire-protocol` obliges the client to deliver every validated
   * `notice`, and an uninstalled callback would satisfy that obligation
   * nowhere. What the difference *means* is host policy; the client applies
   * none.
   */
  readonly onDelivery?: (delivery: DeliveryFrame) => void;
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
  | "unexpected_reply"
  /**
   * The relay does not implement attachments, so nothing can be reserved.
   *
   * Distinct from `relay_error` because it is the answer the capability probe
   * exists to produce: a relay one version behind answers `reserve` with
   * `unsupported_frame` on an open connection, and the caller reports that
   * attachments are unavailable rather than treating its connection as broken.
   */
  | "unsupported"
  /**
   * A ceiling refused the reservation. {@link RequestFailed.status} names which.
   *
   * An answer rather than a fault: the relay understood the request and acted on
   * it. Kept apart from `relay_error` so a caller can tell "wait, or send less"
   * from "this connection is not working".
   */
  | "refused"
  /** A payload transfer failed, stalled, or was cancelled. */
  | "transfer_failed"
  /**
   * A payload is larger than the ceiling the caller set for it. Carries the
   * size in {@link RequestFailed.bytes}, because deciding what to do next needs
   * the number.
   */
  | "over_ceiling"
  /** The address names no payload the relay holds, or it has expired. */
  | "unavailable";

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
  /** Which ceiling refused, when {@link reason} is `refused`. */
  readonly status: ReserveStatus | null;
  /** The payload's size, when {@link reason} is `over_ceiling`. */
  readonly bytes: number | null;

  constructor(
    reason: RequestFailureReason,
    message: string,
    detail: {
      readonly code?: ErrorCode;
      readonly status?: ReserveStatus;
      readonly bytes?: number;
    } = {},
  ) {
    super(message);
    this.reason = reason;
    this.code = detail.code ?? null;
    this.status = detail.status ?? null;
    this.bytes = detail.bytes ?? null;
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
  /**
   * Address of a payload already uploaded through {@link RelayClient.attach}.
   *
   * A caller that has not reserved and uploaded must not set this: the relay
   * relays it uninterpreted, so a reference to nothing arrives looking exactly
   * like a reference to something.
   */
  readonly attachment?: string;
}

/**
 * What an `announce` needs. `id` is generated when the caller does not supply
 * one.
 *
 * No target, and no field where one could go: the room-wide address is the
 * absence of a peer component.
 */
export interface AnnounceRequest {
  readonly body: string;
  readonly replyTo?: string;
  readonly id?: string;
  /** Same rules as {@link SendRequest.attachment}. */
  readonly attachment?: string;
}

/** A payload the relay now holds, as its sender needs to describe it. */
export interface Attachment {
  /** The address, to be carried on a `send` or an `announce`. */
  readonly digest: string;
  /** Byte length uploaded. */
  readonly bytes: number;
  /**
   * Seconds the reference remains resolvable, as the relay stated it.
   *
   * Passed on rather than kept, because the sender's job is to say so in the
   * body: a recipient reading late finds expiry, and expiry reported as failure
   * is somebody else's cause.
   */
  readonly expiresIn: number;
}

/**
 * What one transfer request produced, read under the transfer's own bound.
 *
 * `bytes` carries the body only for a drained `GET`; a `HEAD` or a `PUT` leaves
 * it empty. The body is read here rather than handed back as a `Response`
 * because reading it is part of the transfer, and a bound that ended when the
 * headers arrived would not cover the payload it was measuring.
 */
interface TransferOutcome {
  readonly status: number;
  readonly headers: Headers;
  readonly bytes: Uint8Array<ArrayBuffer>;
}

/** Which request a pending entry belongs to, for its diagnostics. */
export type RequestKind = "list" | "send" | "announce" | "reserve";

/**
 * The reply spaces a correlation token may occupy.
 *
 * Enumerated rather than left implicit, so every place that has to consider all
 * of them — routing an `error` frame that names a token but not a class — covers
 * each by construction instead of by a list someone remembered to extend.
 */
export const REPLY_SPACES = ["list", "delivery", "reserve"] as const;

/** One reply space. */
export type ReplySpace = (typeof REPLY_SPACES)[number];

/**
 * Which reply space a request's correlation token occupies.
 *
 * `send` and `announce` share one, because their tokens do. The relay validates
 * length and deduplicates nothing, so it will route a `send` and an `announce`
 * carrying one `id`; this client is then the only party that could tell the
 * resulting `receipt` and `accepted` apart. It could, by `type` — but an `error`
 * frame naming that token could not, and an error is the reply either request
 * may get. Sharing the space is what turns that ambiguity into a refusal of the
 * second request instead of a reply settling the wrong caller.
 *
 * `list` and `reserve` keep their own. Each carries its token in a different
 * field answered by a different frame, and nothing is gained by forbidding a
 * coincidence between them.
 */
function replySpace(kind: RequestKind): ReplySpace {
  switch (kind) {
    case "list":
      return "list";
    case "reserve":
      return "reserve";
    case "send":
    case "announce":
      return "delivery";
  }
}

type RequestOutcome =
  | {
      readonly ok: true;
      readonly frame: PeersFrame | ReceiptFrame | AcceptedFrame | ReservedFrame;
    }
  | { readonly ok: false; readonly error: RequestFailed };

interface PendingRequest {
  /**
   * Which request this entry belongs to, for the diagnostic a collision or a
   * timeout reports. The reply space it occupies is {@link replySpace} of this,
   * which is coarser: a `list` and a `send` may legitimately carry the same
   * correlation token, and a `send` and an `announce` may not.
   */
  readonly kind: RequestKind;
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
 * Percent-encodes one room component for a transfer route.
 *
 * Beyond `encodeURIComponent` in one respect: `.` is encoded too. A room named
 * `..`/`..` is admissible under every identifier rule, and `encodeURIComponent`
 * leaves the dot alone -- which would put a literal `..` in the path. The relay
 * hashes a room's components, so nothing can traverse either way; what a literal
 * `..` would break is reachability, because any intermediary that normalizes a
 * path would rewrite the route out from under the request.
 */
function pathSegment(value: string): string {
  return encodeURIComponent(value).replaceAll(".", "%2E");
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

  /**
   * Transfers in flight, so shutdown can cancel them.
   *
   * A transfer outlives the frame exchange that authorized it and runs on no
   * connection this client owns, so nothing else would end one: a host tearing
   * down a session would return from `stop()` with a download still writing.
   */
  #transfers = new Set<AbortController>();

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

    // Cancelled before anything can return early, because a transfer is the one
    // thing here that does not end when the connection does: it runs on its own
    // socket, and a client with no frame connection may still have one in
    // flight.
    for (const controller of this.#transfers) {
      controller.abort(new Error("the client was stopped"));
    }
    this.#transfers.clear();

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
    if (request.attachment !== undefined) {
      const broken = digestProblem(request.attachment);
      if (broken !== null) {
        return refuse(`send attachment ${describeDigestProblem(broken)}`);
      }
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
        ...(request.attachment === undefined ? {} : { attachment: request.attachment }),
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

  /**
   * Relays `body` to every other peer in the room.
   *
   * Resolves with the relay's acceptance, including two zero counts: an empty
   * room is a fact about the room, not a failure of the request. The caller
   * reads the counts.
   *
   * The announcer never receives its own announcement, so a caller must not
   * wait to see one as confirmation.
   */
  announce(request: AnnounceRequest): Promise<AcceptedFrame> {
    const id = request.id ?? crypto.randomUUID();
    const { promise, resolve, reject } = Promise.withResolvers<AcceptedFrame>();

    const refuse = (message: string): Promise<AcceptedFrame> => {
      reject(new RequestFailed("invalid_request", message));
      return promise;
    };

    const idProblem = correlationProblem(id);
    if (idProblem !== null) {
      return refuse(`announce id ${describeIdentifierProblem(idProblem)}`);
    }
    if (request.replyTo !== undefined) {
      const replyProblem = correlationProblem(request.replyTo);
      if (replyProblem !== null) {
        return refuse(`announce reply_to ${describeIdentifierProblem(replyProblem)}`);
      }
    }
    // The same budget as `send`, and checked here for the same reason: the relay
    // answers an over-budget body by closing the connection, which would turn
    // one bad argument into an outage for every other request on it.
    const oversized = bodyOverBudget(request.body);
    if (oversized !== null) {
      return refuse(
        `announce body is ${oversized} UTF-8 bytes, over the ${MAX_BODY_BYTES}-byte budget`,
      );
    }
    if (request.attachment !== undefined) {
      const broken = digestProblem(request.attachment);
      if (broken !== null) {
        return refuse(`announce attachment ${describeDigestProblem(broken)}`);
      }
    }

    this.#issue(
      id,
      "announce",
      {
        type: "announce",
        id,
        body: request.body,
        ...(request.replyTo === undefined ? {} : { reply_to: request.replyTo }),
        ...(request.attachment === undefined ? {} : { attachment: request.attachment }),
      },
      (outcome) => {
        if (!outcome.ok) {
          reject(outcome.error);
        } else if (outcome.frame.type === "accepted") {
          resolve(outcome.frame);
        } else {
          reject(
            new RequestFailed(
              "unexpected_reply",
              `announce ${id} was answered with a ${outcome.frame.type} frame`,
            ),
          );
        }
      },
    );
    return promise;
  }

  // -------------------------------------------------------------------------
  // Attachments
  // -------------------------------------------------------------------------

  /**
   * Reserves room for a payload and uploads it, returning its reference.
   *
   * The order is the contract. The reservation comes first because it is what
   * authorizes the upload at all — the transfer route carries no credential, so
   * write authority is the `hello` handshake this connection already completed —
   * and because it is the capability probe: a relay that does not implement
   * attachments answers `unsupported_frame`, and this rejects with
   * `unsupported` so a caller reports that rather than attaching a reference
   * nothing can resolve.
   *
   * The address is computed here, from the bytes, and never taken from a caller.
   * A caller-supplied digest would let a defect anywhere upstream put a payload
   * at an address that does not describe it, and every recipient trusting the
   * address would receive content the address does not name.
   *
   * @throws RequestFailed with `unsupported`, `refused`, or `transfer_failed`.
   */
  async attach(bytes: Uint8Array<ArrayBuffer>): Promise<Attachment> {
    const digest = await digestOf(bytes);
    const reserved = await this.reserve(digest, bytes.byteLength);

    if (reserved.status !== "granted") {
      throw new RequestFailed(
        "refused",
        `the relay refused to hold ${bytes.byteLength} bytes: ${reserved.status}`,
        { status: reserved.status },
      );
    }
    // A grant always states a lifetime; `protocol.ts` rejects one that does not,
    // so this is a total function rather than a default standing in for a
    // missing field.
    const expiresIn = reserved.expires_in ?? 0;

    await this.#transfer("PUT", digest, {
      body: bytes,
      headers: { "content-length": String(bytes.byteLength) },
      expect: [201, 204],
    });

    return { digest, bytes: bytes.byteLength, expiresIn };
  }

  /**
   * Asks the relay to hold `bytes` for the payload addressed `digest`.
   *
   * Exposed separately from {@link attach} so a caller can probe without
   * uploading, and because the reply carries the lifetime a sender has to state.
   */
  reserve(digest: string, bytes: number): Promise<ReservedFrame> {
    const requestId = crypto.randomUUID();
    const { promise, resolve, reject } = Promise.withResolvers<ReservedFrame>();

    const broken = digestProblem(digest);
    if (broken !== null) {
      reject(
        new RequestFailed("invalid_request", `reserve digest ${describeDigestProblem(broken)}`),
      );
      return promise;
    }
    if (!Number.isInteger(bytes) || bytes < 0) {
      reject(
        new RequestFailed("invalid_request", `reserve bytes must be a non-negative integer, got ${bytes}`),
      );
      return promise;
    }

    this.#issue(
      requestId,
      "reserve",
      { type: "reserve", request_id: requestId, digest, bytes },
      (outcome) => {
        if (!outcome.ok) {
          // A relay that does not know the frame answers `unsupported_frame`,
          // which arrives here as a `relay_error`. Translated so the caller
          // branches on the capability rather than on a code.
          if (outcome.error.reason === "relay_error" && outcome.error.code === "unsupported_frame") {
            reject(
              new RequestFailed(
                "unsupported",
                "this relay does not implement attachments, so nothing can be reserved",
                { code: outcome.error.code },
              ),
            );
            return;
          }
          reject(outcome.error);
        } else if (outcome.frame.type === "reserved") {
          resolve(outcome.frame);
        } else {
          reject(
            new RequestFailed(
              "unexpected_reply",
              `reserve ${requestId} was answered with a ${outcome.frame.type} frame`,
            ),
          );
        }
      },
    );
    return promise;
  }

  /**
   * The byte length of a stored payload, without transferring it.
   *
   * Strictly more informative than a size carried on the frame that referenced
   * it: this reports the size *and* whether the payload still exists, which a
   * value fixed at send time cannot.
   *
   * @returns the length, or `null` when the relay holds no such payload.
   */
  async lengthOf(digest: string): Promise<number | null> {
    const broken = digestProblem(digest);
    if (broken !== null) {
      throw new RequestFailed(
        "invalid_request",
        `attachment digest ${describeDigestProblem(broken)}`,
      );
    }
    const response = await this.#transfer("HEAD", digest, { expect: [200, 404] });
    if (response.status === 404) {
      return null;
    }
    const declared = Number(response.headers.get("content-length"));
    return Number.isInteger(declared) && declared >= 0 ? declared : null;
  }

  /**
   * Downloads a stored payload and verifies it against its own address.
   *
   * Verification is not optional here. The server hashes what it receives, so an
   * address cannot lie about content; this side hashes what it downloads, so a
   * truncated response, an interposed proxy, or a defect in either
   * implementation is caught rather than delivered. One hashing pass against a
   * network transfer of the same bytes.
   *
   * `maxBytes` is checked with the length-only request first, so a payload over
   * the ceiling costs no transfer at all.
   *
   * @throws RequestFailed with `unavailable`, `over_ceiling`, or
   * `transfer_failed`.
   */
  async fetchAttachment(
    digest: string,
    options: { readonly maxBytes?: number } = {},
  ): Promise<Uint8Array<ArrayBuffer>> {
    if (options.maxBytes !== undefined) {
      const length = await this.lengthOf(digest);
      if (length === null) {
        throw new RequestFailed(
          "unavailable",
          `the relay holds no payload at ${digest}; it was never uploaded or its time to live has elapsed`,
        );
      }
      if (length > options.maxBytes) {
        throw new RequestFailed(
          "over_ceiling",
          `the payload is ${length} bytes, over the ${options.maxBytes}-byte ceiling; nothing was transferred`,
          { bytes: length },
        );
      }
    }

    // Drained inside the transfer, so the stall bound covers the payload it
    // exists to measure rather than ending when the headers arrived.
    const response = await this.#transfer("GET", digest, {
      expect: [200, 404],
      drain: true,
    });
    if (response.status === 404) {
      throw new RequestFailed(
        "unavailable",
        `the relay holds no payload at ${digest}; it was never uploaded or its time to live has elapsed`,
      );
    }

    const bytes = response.bytes;
    const computed = await digestOf(bytes);
    if (computed !== digest) {
      // Nothing is handed back. A payload that does not match its address is not
      // a smaller or older version of what was asked for; it is content the
      // address does not describe.
      throw new RequestFailed(
        "transfer_failed",
        `the downloaded payload hashes to ${computed}, not to the address ${digest} it was fetched from`,
      );
    }
    return bytes;
  }

  /**
   * Performs one transfer request against this client's own relay.
   *
   * Every part of the URL comes from local state: the host and port from this
   * client's configuration, and the room from the connection this client
   * established. Nothing from a delivered frame reaches it but the digest, so
   * the worst a hostile sender can do is name an address the local relay does
   * not hold. A location supplied over the wire would let it aim this fetch at a
   * host of its choosing.
   *
   * Bounded by *progress* rather than by {@link REQUEST_TIMEOUT_MS}. That
   * deadline is right for a frame exchange, where an answer either comes
   * promptly or is not coming; a transfer's duration scales with the payload, so
   * one deadline over the whole request would fail a large payload on a slow
   * link *because it was working*. At the 4 MiB payload ceiling a 10-second
   * whole-request bound demands 3.36 Mbit/s sustained, which a congested LAN
   * does not always provide.
   *
   * So the bound is on the gaps, not on the total: every chunk that arrives, and
   * every chunk the socket accepts, restarts it. A transfer that is moving may
   * take as long as its size requires; one that has stopped fails within the
   * interval. Every timer comes from the host's scheduler so a test drives them.
   */
  async #transfer(
    method: "PUT" | "GET" | "HEAD",
    digest: string,
    options: {
      readonly body?: Uint8Array<ArrayBuffer>;
      readonly headers?: Record<string, string>;
      readonly expect: readonly number[];
      /** Read the response body under the same bound, for `GET`. */
      readonly drain?: boolean;
    },
  ): Promise<TransferOutcome> {
    if (this.#stopped) {
      throw new RequestFailed("stopped", "the client is not running");
    }
    const room = this.#config.room;
    const { host, port } = this.#config.transport;
    // Bracketed for an IPv6 literal, which a URL requires and a `host:port`
    // configuration value does not carry.
    const authority = host.includes(":") ? `[${host}]` : host;
    const url = `http://${authority}:${port}/blob/${pathSegment(room.project)}/${pathSegment(room.task)}/${digest}`;

    const controller = new AbortController();
    this.#transfers.add(controller);

    let stall: TimerHandle | null = null;
    const stopWaiting = (): void => {
      if (stall !== null) {
        this.#scheduler.clearTimeout(stall);
        stall = null;
      }
    };
    // Named for what it measures: the gap since the last byte moved, not the
    // age of the request.
    const restart = (): void => {
      stopWaiting();
      stall = this.#scheduler.setTimeout(() => {
        controller.abort(new Error(`no progress within ${TRANSFER_STALL_MS} ms`));
      }, TRANSFER_STALL_MS);
    };
    restart();

    try {
      const response = await fetch(url, {
        method,
        signal: controller.signal,
        ...(options.body === undefined ? {} : { body: this.#progressing(options.body, restart) }),
        ...(options.headers === undefined ? {} : { headers: options.headers }),
        // Required by the Streams specification for a stream request body, and
        // not in the DOM types Bun compiles against.
        ...(options.body === undefined ? {} : ({ duplex: "half" } as Record<string, string>)),
      });
      if (!options.expect.includes(response.status)) {
        throw new RequestFailed(
          "transfer_failed",
          `${method} ${digest} was answered ${response.status}`,
        );
      }

      const bytes =
        options.drain === true && response.status === 200
          ? await this.#drain(response, restart)
          : new Uint8Array(0);

      return { status: response.status, headers: response.headers, bytes };
    } catch (error) {
      if (error instanceof RequestFailed) {
        throw error;
      }
      throw new RequestFailed(
        "transfer_failed",
        `${method} ${digest} failed: ${describe(error)}`,
      );
    } finally {
      stopWaiting();
      this.#transfers.delete(controller);
    }
  }

  /**
   * `payload` as a stream that reports progress as the socket drains it.
   *
   * A `pull` is the runtime asking for more, which it does once the previous
   * chunk has left for the socket — so a pull is evidence the transfer moved,
   * and it is the only such evidence `fetch` offers on the request side.
   */
  #progressing(
    payload: Uint8Array<ArrayBuffer>,
    restart: () => void,
  ): ReadableStream<Uint8Array> {
    let offset = 0;
    return new ReadableStream<Uint8Array>({
      pull: (controller) => {
        if (offset >= payload.byteLength) {
          controller.close();
          return;
        }
        const end = Math.min(offset + TRANSFER_CHUNK_BYTES, payload.byteLength);
        controller.enqueue(payload.subarray(offset, end));
        offset = end;
        restart();
      },
    });
  }

  /** Reads `response`'s body chunk by chunk, restarting the bound on each. */
  async #drain(response: Response, restart: () => void): Promise<Uint8Array<ArrayBuffer>> {
    const body = response.body;
    if (body === null) {
      return new Uint8Array(0);
    }
    const reader = body.getReader();
    const chunks: Uint8Array[] = [];
    let total = 0;
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      chunks.push(value);
      total += value.byteLength;
      restart();
    }
    const joined = new Uint8Array(total);
    let at = 0;
    for (const chunk of chunks) {
      joined.set(chunk, at);
      at += chunk.byteLength;
    }
    return joined;
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

      // Both classes reach the host through one callback, carrying `type` as
      // the class. The client applies no policy to the difference.
      case "message":
      case "notice":
        this.#notify("onDelivery", () => this.#handlers.onDelivery?.(frame));
        break;

      case "peers":
        this.#settle(frame.request_id, "list", { ok: true, frame });
        break;

      case "receipt":
        this.#settle(frame.id, "send", { ok: true, frame });
        break;

      case "accepted":
        this.#settle(frame.id, "announce", { ok: true, frame });
        break;

      case "reserved":
        this.#settle(frame.request_id, "reserve", { ok: true, frame });
        break;

      case "error":
        this.#handleError(socket, frame);
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
   *
   * `peer_replaced` is the one code that ends the run rather than the
   * connection. See the branch below.
   */
  #handleError(socket: Socket, frame: ErrorFrame): void {
    const detail = frame.message === undefined ? frame.code : `${frame.code}: ${frame.message}`;

    // Displacement is terminal, not an outage. The relay gave this peer name to
    // a newer connection, so reconnecting would take the name back and displace
    // that connection in turn: two sessions configured with the same name evict
    // each other for as long as both run. Entering the stopped state is what
    // stops `#closeConnection` from arming the next attempt.
    //
    // Checked before the request branch because losing the name is never one
    // request's answer, and the connection is closed here rather than left to
    // the relay's own close so that `state` stops claiming to be ready while a
    // half-dead socket accepts requests no one will answer.
    if (frame.code === "peer_replaced") {
      this.#stopped = true;
      this.#report("error", PEER_REPLACED_REPORT);
      this.#closeConnection(socket, PEER_REPLACED_REPORT);
      return;
    }

    if (frame.request_id !== undefined && this.#state === "ready") {
      const settled = this.#settleEither(frame.request_id, {
        ok: false,
        error: new RequestFailed("relay_error", `the relay rejected the request: ${detail}`, {
          code: frame.code,
        }),
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
    kind: RequestKind,
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
    const key = `${replySpace(kind)}:${token}`;
    const outstanding = this.#pending.get(key);
    if (outstanding !== undefined) {
      // Names the *outstanding* request rather than this one, because a
      // cross-class collision is the interesting case: a caller announcing with
      // the `id` of an unsettled `send` needs to be told which request it
      // collided with.
      settle({
        ok: false,
        error: new RequestFailed(
          "invalid_request",
          `a ${outstanding.kind} with correlation token ${JSON.stringify(token)} is already outstanding`,
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
   * Settles the entry `token` names within `kind`'s reply space.
   *
   * A reply matching nothing is discarded rather than treated as an error: a
   * late reply after a timeout is expected and harmless, and the pending map
   * cannot grow from one.
   */
  #settle(token: string, kind: RequestKind, outcome: RequestOutcome): void {
    if (!this.#settleKey(`${replySpace(kind)}:${token}`, outcome)) {
      this.#report(
        "info",
        `discarded a ${kind} reply for unknown correlation token ${JSON.stringify(token)}`,
      );
    }
  }

  /**
   * Settles whichever reply space holds `token`. Used for `error` frames.
   *
   * An `error` names a token and not a class, which is precisely why `send` and
   * `announce` share one space: with both outstanding under one token this
   * function would have to guess, and it would guess in silence.
   *
   * Iterates {@link REPLY_SPACES} rather than naming the spaces, so a request
   * class added later is covered by construction. `reserve` was the case that
   * showed why: with the spaces listed here by hand, a refused reservation's
   * `error` frame would have matched nothing and its caller would have waited
   * out the full request deadline for an answer that had already arrived.
   */
  #settleEither(token: string, outcome: RequestOutcome): boolean {
    for (const space of REPLY_SPACES) {
      if (this.#settleKey(`${space}:${token}`, outcome)) {
        return true;
      }
    }
    return false;
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
