import type {
  ExtensionAPI,
  ExtensionContext,
  ExtensionMode,
} from "@oh-my-pi/pi-coding-agent";

import { lstat, mkdir, readFile, rename, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import {
  RelayClient,
  RequestFailed,
  type AnnounceRequest,
  type Attachment,
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
  describeDigestProblem,
  describeIdentifierProblem,
  digestProblem,
  identifierProblem,
  type AcceptedFrame,
  type DeliveryFrame,
  type PeersFrame,
  type ReceiptFrame,
  type RoomId,
} from "./protocol.ts";

export const INBOUND_MESSAGE_TYPE = "io.github.pashifika.omp-relay.message";

/**
 * Session entry for one inbound announcement.
 *
 * A distinct type from {@link INBOUND_MESSAGE_TYPE} rather than a class field on
 * it, for the reason {@link OUTBOUND_MESSAGE_TYPE} is distinct too: a later
 * reader wants one class far more often than both, and a shared type would make
 * every such read filter. It also means the class survives in the transcript
 * without anyone parsing rendered text for it.
 */
export const INBOUND_NOTICE_TYPE = "io.github.pashifika.omp-relay.notice";

/**
 * Session entry for one outbound message, the mirror of
 * {@link INBOUND_MESSAGE_TYPE}.
 *
 * A distinct type rather than a direction field on the inbound one: a consumer
 * reading the transcript back wants one of the two far more often than both, and
 * a shared type would make every such read filter.
 */
export const OUTBOUND_MESSAGE_TYPE = "io.github.pashifika.omp-relay.sent";

/**
 * Session entry for one outbound announcement, the mirror of
 * {@link INBOUND_NOTICE_TYPE}.
 *
 * The `send` record exists because an initiator that loses its own record of
 * what it asked cannot resolve the `reply_to` that answers it. An announcement
 * carries `reply_to` on the same terms and can be answered by a directed
 * message naming it, so the same loss is reachable through this class -- which
 * is why the record is not left to the directed class alone.
 */
export const OUTBOUND_ANNOUNCE_TYPE = "io.github.pashifika.omp-relay.announced";

/**
 * Where a fetched payload lands, under the process's temporary directory.
 *
 * Owner-only, and named for the product rather than for a room: the file's own
 * name is the payload's address, which is unique across every room by
 * construction.
 */
const ATTACHMENT_DIR = "omp-relay-attachments";

/** Distinguishes one process's temporaries from another's in that directory. */
let nextAttachmentTemp = 0;

/**
 * Writes a fetched payload to a file named for its address, and returns it.
 *
 * Under the process's temporary directory rather than the working tree: a
 * fetched payload is material to inspect, not a file to commit, and writing
 * into the repository would put another peer's bytes where a careless
 * `git add` picks them up.
 *
 * Published by rename rather than written in place. The path is derived from
 * the digest, so an earlier fetch of the same payload has already handed it to
 * a caller: writing straight to it would truncate a complete, digest-verified
 * file for the duration of the rewrite, and would leave a partial one behind if
 * the write failed -- at a name whose entire meaning is that its content hashes
 * to it. Verifying the bytes in memory buys nothing if the file the caller
 * opens is a different, shorter thing. The relay's store publishes by rename
 * for the same reason (`server/src/blob.rs`), and undoing that at the last step
 * would spend its care for nothing.
 *
 * The temporary sits in the same directory, which is what makes the rename
 * atomic, and takes the leading dot and counter the store uses for its own. The
 * pid joins them because every session on this machine shares this directory,
 * where the store owns each room's.
 *
 * The directory is verified rather than trusted, and that is separate from the
 * rename. `tmpdir()` is the shared `/tmp` on Linux, so this name is predictable
 * and its parent is writable by every local user, while a recursive `mkdir`
 * reports success for whatever already occupies it -- a symlink included, and
 * with the owner and mode it already had, because its `mode` applies only to
 * directories it creates. Unchecked, another local user pre-positions the name
 * and every payload lands where they chose while the path handed back still
 * reads as ours. The store's `create_private_dir` records why that matters
 * (`server/src/blob.rs`): on a trusted host a traversable temporary directory
 * exposes to every local user exactly the payload content the logging rules
 * forbid recording.
 *
 * The temporary is created exclusively, which is what keeps it honest once the
 * directory is. Its name is guessable -- the digest is relayed in plaintext to
 * every recipient of an announcement, and a pid and a small counter are not
 * secrets -- and a plain `writeFile` follows a symlink standing at it, where
 * the rename only protects the final name. An exclusive create fails when the
 * name exists at all, symlink included, so it needs no separate no-follow flag.
 * It also refuses a temporary this pid abandoned in an earlier crash, which is
 * right on the same terms: that file is not this fetch's to truncate, and the
 * counter has already moved on for the next attempt.
 */
export async function saveFetchedPayload(
  digest: string,
  bytes: Uint8Array<ArrayBuffer>,
): Promise<string> {
  const directory = join(tmpdir(), ATTACHMENT_DIR);
  await mkdir(directory, { recursive: true, mode: 0o700 });

  // `lstat` rather than `stat`: the question is what this name *is*, not what
  // it points at, and resolving the link is exactly what must not happen.
  const stats = await lstat(directory);
  // Absent on Windows, which has neither of the two properties below. The
  // symlink and directory checks still hold there.
  const uid = process.getuid?.();
  const wrong = stats.isSymbolicLink()
    ? "a symbolic link rather than a directory"
    : !stats.isDirectory()
      ? "not a directory"
      : uid !== undefined && stats.uid !== uid
        ? `owned by uid ${String(stats.uid)} rather than by this process's uid ${String(uid)}`
        : uid !== undefined && (stats.mode & 0o022) !== 0
          ? `writable beyond its owner, at mode 0${(stats.mode & 0o777).toString(8)}`
          : null;
  if (wrong !== null) {
    throw new Error(
      `the attachment directory ${directory} is ${wrong}, so nothing was written to it`,
    );
  }

  const path = join(directory, digest);
  const temp = join(directory, `.${digest}.${process.pid}.${++nextAttachmentTemp}`);
  try {
    await writeFile(temp, bytes, { mode: 0o600, flag: "wx" });
    await rename(temp, path);
  } catch (error) {
    // Removed on the way out so a failed fetch leaves nothing behind, and
    // `force` because the failure may well be that the temporary was never
    // created.
    await rm(temp, { force: true });
    throw error;
  }
  return path;
}

export interface MeshArguments {
  readonly action?: unknown;
  readonly to?: unknown;
  readonly message?: unknown;
  readonly reply_to?: unknown;
  readonly project?: unknown;
  readonly task?: unknown;
  readonly as?: unknown;
  /** Local file to attach to a `send` or an `announce`. */
  readonly attach?: unknown;
  /** The reference a `fetch` resolves. */
  readonly reference?: unknown;
  /** Byte ceiling above which a `fetch` transfers nothing. */
  readonly max_bytes?: unknown;
}

export interface MeshClient {
  readonly state: ClientState;
  list(): Promise<PeersFrame>;
  send(request: SendRequest): Promise<ReceiptFrame>;
  announce(request: AnnounceRequest): Promise<AcceptedFrame>;
  /** Reserves room for `bytes`, uploads it, and returns the reference. */
  attach(bytes: Uint8Array<ArrayBuffer>): Promise<Attachment>;
  /** Downloads a referenced payload, verifying it against its own address. */
  fetchAttachment(
    digest: string,
    options?: { readonly maxBytes?: number },
  ): Promise<Uint8Array<ArrayBuffer>>;
  /** The byte length of a referenced payload, or `null` when it is absent. */
  lengthOf(digest: string): Promise<number | null>;
}

export interface MeshToolResult {
  content: Array<{ type: "text"; text: string }>;
  details: Record<string, unknown>;
}

/** The exact frame values for one inbound delivery, kept out of the model's context. */
export interface InboundDetails {
  readonly id: string;
  readonly from: string;
  readonly project: string;
  readonly task: string;
  readonly body: string;
  readonly reply_to?: string;
  /** The reference as received. No payload byte is ever recorded. */
  readonly attachment?: string;
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
  /** The reference this message carried, when it carried one. */
  readonly attachment?: string;
}

/**
 * The exact values of one outbound announcement and the acceptance that
 * acknowledged it.
 *
 * Two counts rather than a status, because that is what the relay answered
 * with: an announcement has no single outcome to record.
 */
export interface AnnouncedDetails {
  readonly id: string;
  readonly project: string;
  readonly task: string;
  readonly body: string;
  readonly delivered: number;
  readonly shed: number;
  readonly reply_to?: string;
  /** The reference this announcement carried, when it carried one. */
  readonly attachment?: string;
}

/**
 * What one validated inbound delivery contributes to the session.
 *
 * `entryType` travels with the rest because the class decides it, and the
 * caller that persists the entry should not have to re-derive the class from
 * the frame it no longer holds.
 */
export interface InboundInjection {
  /** Rendered provenance and quoted body, delivered to the session as a user prompt. */
  readonly text: string;
  /** The unmodified frame values, persisted as a session entry rather than sent to the model. */
  readonly details: InboundDetails;
  /** The session-entry type for this delivery's class. */
  readonly entryType: typeof INBOUND_MESSAGE_TYPE | typeof INBOUND_NOTICE_TYPE;
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
 * An interface rather than the client alone, because most of the actions reach
 * past the connection: `join` replaces it, `send` and `announce` each persist a
 * record beside it, and the attachment paths touch the filesystem. Keeping those
 * behind named members is what lets the tool's behaviour — including its refusal
 * to register a non-interactive session, and every attachment rule — be
 * exercised without a socket and without a real file.
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
  /** The room the live client joined, for recording an outbound frame. */
  readonly room: RoomId | null;
  join(parameters: JoinParameters): Promise<JoinOutcome>;
  recordSend(details: OutboundDetails): void;
  recordAnnounce(details: AnnouncedDetails): void;
  /**
   * Reads a local file the caller named, for attaching.
   *
   * Behind the host because the failure is the interesting case: an unreadable
   * or absent path must be refused before any frame is written, and asserting
   * that costs a real unreadable file otherwise.
   */
  readAttachment(path: string): Promise<Uint8Array<ArrayBuffer>>;
  /**
   * Writes a fetched payload to a file this extension owns, returning its path.
   *
   * The name is derived from `digest` alone. A filename supplied by a remote
   * peer would become a path component on this machine; no frame carries one,
   * and none is accepted if a future one does.
   */
  saveAttachment(digest: string, bytes: Uint8Array<ArrayBuffer>): Promise<string>;
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

/**
 * Names a refused reservation, and never a message sent without its attachment.
 *
 * A caller that attached a file asked for one thing: the message *and* the
 * material. Sending the body alone would report success for a request that was
 * not performed, and the recipient would read prose referring to material that
 * never arrived.
 */
function attachmentFailure(action: "send" | "announce", error: unknown): MeshToolResult {
  if (error instanceof RequestFailed && error.reason === "refused") {
    // The recovery differs by which bound refused. A payload over the
    // per-payload maximum will never fit, so waiting is the wrong advice; a full
    // room or store is capacity, which does free up.
    const recovery =
      error.status === "payload_too_large"
        ? "The payload is over the per-payload maximum, so waiting will not help: split it, " +
          "send a smaller part, or describe it in the body instead."
        : "This is capacity rather than a size limit: the room's payloads are removed when " +
          "its last peer leaves and each payload expires on its own, so retrying later or " +
          "attaching something smaller can succeed.";
    return result(
      `OMP Relay refused to hold the attachment (${error.status ?? "refused"}), so no ${action} ` +
        `was performed and nothing reached the room. ${recovery}`,
      {
        action,
        status: "refused",
        reason: "attachment_refused",
        ...(error.status === null ? {} : { refusal: error.status }),
      },
    );
  }
  if (error instanceof RequestFailed && error.reason === "unsupported") {
    return result(
      `This relay does not implement attachments, so nothing was ${action === "send" ? "sent" : "announced"}. ` +
        `Include the material in the message body, or split it, and say why.`,
      { action, status: "unavailable", reason: "attachments_unsupported" },
    );
  }
  return requestFailure(error);
}

/**
 * Reports that a referenced payload is gone, as expiry rather than as a fault.
 *
 * The distinction decides what a caller does next. A transfer that failed is
 * retried; a payload that is no longer held will not reappear, and the recovery
 * is to ask its sender to send it again.
 */
function expiredResult(reference: string): MeshToolResult {
  return result(
    `The relay no longer holds the payload at ${reference}. This is expiry rather than ` +
      "a failure: stored payloads have a limited lifetime and a room's payloads are " +
      "removed when its last peer leaves. Ask the sender to send it again; retrying the " +
      "fetch will not recover it.",
    { action: "fetch", status: "unavailable", reference, reason: "expired" },
  );
}

/**
 * How a sender is told what it must pass on: how long the payload has left.
 *
 * "about" belongs to each band rather than to the sentence, because a payload
 * with seconds left is stated as a bound rather than a rounded count -- and
 * "held for about less than a minute" is worse than the bug it replaces. That
 * band became reachable when `reserve` began granting an already-held payload
 * the life it has left; before that, every grant was the full lifetime.
 */
function expiryNote(seconds: number | undefined): string {
  if (seconds === undefined || seconds <= 0) return "";
  const hours = Math.floor(seconds / 3600);
  const minutes = Math.floor(seconds / 60);
  const stated =
    hours >= 1
      ? `about ${hours} hour${hours === 1 ? "" : "s"}`
      : seconds < 60
        ? "less than a minute"
        : `about ${minutes} minute${minutes === 1 ? "" : "s"}`;
  return ` The attachment is held for ${stated}; say so in the body, because a recipient reading later will find it gone.`;
}

function receiptResult(
  receipt: ReceiptFrame,
  attachment?: string,
  expiry?: number,
): MeshToolResult {
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
  const rendered =
    attachment === undefined
      ? text
      : `${text} It carries attachment reference ${attachment}, which the recipient fetches with ` +
        `action "fetch".${expiryNote(expiry)}`;
  return result(rendered, {
    action: "send",
    id: receipt.id,
    to: receipt.to,
    status: receipt.status,
    ...(attachment === undefined ? {} : { attachment }),
    ...(expiry === undefined ? {} : { expires_in: expiry }),
  });
}

/**
 * Renders an announcement's acceptance as its two counts.
 *
 * Worded as queueing rather than reading, for the reason a routed receipt is:
 * the relay placed the frame in each peer's queue and knows nothing about what
 * any of them did with it.
 *
 * Zero deliveries is a success. It says the room held nobody else, which is a
 * fact about the room rather than a failure of the request, and reporting it as
 * an error would send a caller to recover a connection that is fine.
 *
 * A shed count is not a delivery to retry blindly: the peer that shed it is a
 * peer that is not reading its socket, so an immediate resend adds to a queue
 * that is already full.
 */
function acceptedResult(
  accepted: AcceptedFrame,
  attachment?: string,
  expiry?: number,
): MeshToolResult {
  const { id, delivered, shed } = accepted;
  const peers = (count: number): string => (count === 1 ? "1 peer" : `${count} peers`);

  let text: string;
  if (delivered === 0 && shed === 0) {
    text = `Announcement ${id} reached nobody: no other peer is in this room. The room was empty, so this is not a failure.`;
  } else if (shed === 0) {
    text = `Announcement ${id} was queued for ${peers(delivered)}; this does not mean any of them has read it.`;
  } else {
    text = `Announcement ${id} was queued for ${peers(delivered)} and shed by ${peers(shed)} that is not reading its connection; a shed peer is not reading, so resending would add to a queue that is already full.`;
  }
  const rendered =
    attachment === undefined
      ? text
      : `${text} It carries attachment reference ${attachment}, which a recipient fetches with ` +
        `action "fetch".${expiryNote(expiry)}`;
  return result(rendered, {
    action: "announce",
    id,
    delivered,
    shed,
    ...(attachment === undefined ? {} : { attachment }),
    ...(expiry === undefined ? {} : { expires_in: expiry }),
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
  if (
    args.action !== "join" &&
    args.action !== "list" &&
    args.action !== "send" &&
    args.action !== "announce" &&
    args.action !== "fetch"
  ) {
    return validationFailure('action must be "join", "list", "send", "announce", or "fetch"');
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
    if (args.attach !== undefined && typeof args.attach !== "string") {
      return validationFailure("attach must be a string path when provided");
    }
  }

  if (args.action === "announce") {
    // Refused rather than ignored. A caller that supplied a target believed it
    // was addressing one peer; broadcasting silently instead would be worse
    // than a stated refusal, and there is no target to supply -- the absence of
    // a peer component is what addresses the room.
    if (args.to !== undefined) {
      return validationFailure(
        "announce takes no to: an announcement addresses the whole room, and naming a " +
          'peer is what action "send" is for',
      );
    }
    // The join-only selectors are refused for the same reason, one field over.
    // `project` and `task` name a room, so accepting them and announcing anyway
    // would broadcast into the room this session already holds -- the wrong
    // peers, silently, from a caller that named different ones. `as` names a
    // peer. Neither is a thing an announcement can carry, because `join` is the
    // only way to change the room or peer name of a live session.
    for (const field of ["project", "task", "as"] as const) {
      if (args[field] !== undefined) {
        return validationFailure(
          `announce takes no ${field}: an announcement goes to the room this session already ` +
            'joined, and changing the room or peer name of a live session is what action "join" is for',
        );
      }
    }
    if (typeof args.message !== "string") {
      return validationFailure("announce requires a string message");
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
    if (args.attach !== undefined && typeof args.attach !== "string") {
      return validationFailure("attach must be a string path when provided");
    }
  }

  if (args.action === "fetch") {
    // The fields that name a peer, a room, or a message have no meaning here,
    // and accepting one silently would let a caller believe it had scoped a
    // fetch that is scoped by the reference alone.
    for (const field of ["to", "message", "reply_to", "project", "task", "as", "attach"] as const) {
      if (args[field] !== undefined) {
        return validationFailure(
          `fetch takes no ${field}: a fetch is addressed by its reference alone, in the room ` +
            "this session already joined",
        );
      }
    }
    if (typeof args.reference !== "string") {
      return validationFailure("fetch requires a string reference");
    }
    // Checked here rather than left to the client, so a malformed reference is a
    // validation error naming the field rather than a request failure.
    const broken = digestProblem(args.reference);
    if (broken !== null) {
      return validationFailure(`reference ${describeDigestProblem(broken)}`);
    }
    if (args.max_bytes !== undefined) {
      if (typeof args.max_bytes !== "number" || !Number.isInteger(args.max_bytes) || args.max_bytes < 0) {
        return validationFailure("max_bytes must be a non-negative integer when provided");
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

    if (args.action === "fetch") {
      const reference = args.reference as string;
      const ceiling = args.max_bytes as number | undefined;

      // The ceiling is honoured with the length-only request, so a payload over
      // it costs no transfer. Asking first also distinguishes expiry from a
      // transfer fault, which a caller recovers from differently: an expired
      // payload is asked for again, a failed transfer is retried.
      if (ceiling !== undefined) {
        const length = await client.lengthOf(reference);
        if (length === null) {
          return expiredResult(reference);
        }
        if (length > ceiling) {
          return result(
            `The payload at ${reference} is ${length} bytes, over the ${ceiling}-byte ceiling; ` +
              "nothing was transferred and no file was written. Raise max_bytes to fetch it.",
            { action: "fetch", status: "over_ceiling", reference, bytes: length },
          );
        }
      }

      let bytes: Uint8Array<ArrayBuffer>;
      try {
        bytes = await client.fetchAttachment(reference);
      } catch (error) {
        if (error instanceof RequestFailed && error.reason === "unavailable") {
          return expiredResult(reference);
        }
        throw error;
      }

      // The name comes from the reference, never from remote text. The result
      // carries the path and no payload byte: a payload is by definition larger
      // than a frame can carry, and spending the model's context on material it
      // usually wants to search, apply, or run is what a path avoids.
      let path: string;
      try {
        path = await host.saveAttachment(reference, bytes);
      } catch (error) {
        // The transfer already succeeded, so this is not a relay fault and must
        // not be reported as one: the outer handler renders every escape as
        // "OMP Relay request failed", which would send a caller to retry a
        // request that succeeds again and fails again at the same step. The
        // reason is local and the recovery is local, so both are stated.
        return result(
          `The payload at ${reference} transferred, but writing it locally was refused: ` +
            `${singleLine(describe(error))}. No file was written for this fetch. Clear or ` +
            "repair that path, then fetch again.",
          { action: "fetch", status: "not_written", reference, bytes: bytes.byteLength },
        );
      }
      return result(
        `Fetched ${bytes.byteLength} bytes to ${path}. Read, apply, or run it with ordinary ` +
          "tools; its contents are not in this result.",
        { action: "fetch", status: "fetched", reference, bytes: bytes.byteLength, path },
      );
    }

    if (args.action === "announce") {
      const id = crypto.randomUUID();
      const body = args.message as string;
      const replyTo = args.reply_to as string | undefined;
      const attachPath = args.attach as string | undefined;

      let attachment: string | undefined;
      let expiry: number | undefined;
      if (attachPath !== undefined) {
        let payload: Uint8Array<ArrayBuffer>;
        try {
          payload = await host.readAttachment(attachPath);
        } catch (error) {
          return validationFailure(
            `attach could not be read: ${singleLine(describe(error))}; nothing was announced`,
          );
        }
        try {
          const held = await client.attach(payload);
          attachment = held.digest;
          expiry = held.expiresIn;
        } catch (error) {
          return attachmentFailure("announce", error);
        }
      }

      const accepted = await client.announce({
        id,
        body,
        ...(replyTo === undefined ? {} : { replyTo }),
        ...(attachment === undefined ? {} : { attachment }),
      });

      // Recorded for the reason a `send` is: an announcement carries `reply_to`
      // and can be answered by a directed message naming its identifier, so an
      // announcer that kept no record loses the request that reply resolves.
      // The counts stand where a receipt's status would: an announcement has no
      // single outcome to record.
      const announceRoom = host.room;
      if (announceRoom !== null) {
        host.recordAnnounce({
          id: accepted.id,
          project: announceRoom.project,
          task: announceRoom.task,
          body,
          delivered: accepted.delivered,
          shed: accepted.shed,
          ...(replyTo === undefined ? {} : { reply_to: replyTo }),
          ...(attachment === undefined ? {} : { attachment }),
        });
      }
      return acceptedResult(accepted, attachment, expiry);
    }

    const id = crypto.randomUUID();
    const to = args.to as string;
    const body = args.message as string;
    const replyTo = args.reply_to as string | undefined;
    const attachPath = args.attach as string | undefined;

    let attachment: string | undefined;
    let expiry: number | undefined;
    if (attachPath !== undefined) {
      // Read before the reservation, so an unreadable path costs no frame. The
      // failure is a validation error rather than a request failure because
      // nothing was asked of the relay.
      let payload: Uint8Array<ArrayBuffer>;
      try {
        payload = await host.readAttachment(attachPath);
      } catch (error) {
        return validationFailure(
          `attach could not be read: ${singleLine(describe(error))}; no message was sent`,
        );
      }
      try {
        const held = await client.attach(payload);
        attachment = held.digest;
        expiry = held.expiresIn;
      } catch (error) {
        return attachmentFailure("send", error);
      }
    }

    const receipt = await client.send({
      id,
      to,
      body,
      ...(replyTo === undefined ? {} : { replyTo }),
      ...(attachment === undefined ? {} : { attachment }),
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
        ...(attachment === undefined ? {} : { attachment }),
      });
    }
    return receiptResult(receipt, attachment, expiry);
  } catch (error) {
    return requestFailure(error);
  }
}

/**
 * Renders one inbound delivery for the session, and the frame values to persist.
 *
 * The rendered text is what the model and the operator see, so every value
 * interpolated into it is neutralized first: the provenance fields to a single
 * line each, the body to text without terminal control sequences. Provenance
 * occupies column zero and every body line is quoted, which is what makes the
 * boundary unambiguous — a body line can no longer be read as a provenance
 * header, whatever it contains.
 *
 * The first provenance line names the class, and states for an announcement
 * that it was addressed to the room. Without that the two are
 * indistinguishable in the transcript, and a session would answer the room
 * privately believing it had been asked.
 *
 * `purpose` is the exception, and only because of where it comes from: it is
 * accepted from the global file alone, so it is first-party operator text with
 * the same trust as a context file they wrote, and neutralizing it would mangle
 * prose nobody untrusted authored. It is rendered as a labelled block ahead of
 * the provenance header, so it is distinguishable from both the header and the
 * remote body rather than reading as part of either.
 */
export function buildInboundInjection(
  delivery: DeliveryFrame,
  room: RoomId,
  purpose: string | null = null,
): InboundInjection {
  const details: InboundDetails = {
    id: delivery.id,
    from: delivery.from,
    project: room.project,
    task: room.task,
    body: delivery.body,
    ...(delivery.reply_to === undefined ? {} : { reply_to: delivery.reply_to }),
    ...(delivery.attachment === undefined ? {} : { attachment: delivery.attachment }),
  };
  const announcement = delivery.type === "notice";
  const lines = [
    ...(purpose === null ? [] : [PURPOSE_HEADING, purpose, ""]),
    announcement
      ? `Room announcement from ${singleLine(delivery.from)}, addressed to everyone in this room`
      : `Remote message from ${singleLine(delivery.from)}`,
    `Project: ${singleLine(room.project)}`,
    `Task: ${singleLine(room.task)}`,
    `${announcement ? "Announcement" : "Message"} ID: ${singleLine(delivery.id)}`,
    ...(delivery.reply_to === undefined ? [] : [`Reply to: ${singleLine(delivery.reply_to)}`]),
    // Stated, never resolved. Nothing has been downloaded: fetching on arrival
    // would let a remote peer cause this machine to download arbitrary bytes at
    // a moment nothing local chose, and put them on the path toward the model's
    // context. Neutralized like every other remote value, and on its own
    // provenance line so a body line cannot forge one.
    ...(delivery.attachment === undefined
      ? []
      : [
          `Attachment available, not downloaded: ${singleLine(delivery.attachment)}`,
          'Fetch it deliberately with mesh action "fetch" if this work needs it.',
        ]),
    "",
    ...quotedBody(delivery.body),
  ];
  return {
    text: lines.join("\n"),
    details,
    entryType: announcement ? INBOUND_NOTICE_TYPE : INBOUND_MESSAGE_TYPE,
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
        onDelivery(delivery) {
          // Which delivery mode this frame takes, decided before the preamble
          // is claimed, because the preamble may ride only a delivery that
          // starts or steers a turn.
          //
          // A `message` always steers: `deliverAs: "steer"` queues on the
          // steering queue, and the runtime's drain gate resumes from *any*
          // transcript tail when that queue is non-empty
          // (agent-session.ts:6269-6275 in OMP 18.0.4), so it starts a turn on
          // an idle session and steers into a running one.
          //
          // A `notice` must not interrupt work in flight, so it defers with
          // `followUp` while the model is streaming. Idle, it steers -- there is
          // nothing to interrupt, and `followUp` would not start a turn at all:
          // a follow-up-only resume needs an `assistant`/`toolResult` tail
          // (agent-session.ts:6281-6288 in OMP 18.0.4), which a fresh session
          // does not have.
          //
          // The idle test can race a run that begins before the delivery lands,
          // in which case the notice becomes a steer into it. Accepted rather
          // than guarded: the window is narrow, and guarding it would need a
          // runtime primitive that tests idleness and delivers atomically.
          const deferred = delivery.type === "notice" && !ctx.isIdle();

          // A deferred notice reaches the model behind the run it waited for and
          // behind anything that steered into it, so a preamble riding it would
          // be read after the work it was meant to frame. It stays owed to the
          // next delivery that starts or steers a turn.
          const purpose = deferred ? null : pendingPurpose;
          if (purpose !== null) {
            pendingPurpose = null;
            purposeDelivered = true;
          }

          const injection = buildInboundInjection(delivery, config.room, purpose);
          // A session entry, which the runtime documents as state persistence
          // that never reaches the LLM. It is where the exact frame values live,
          // so the rendered text can be neutralized without losing them. The
          // type carries the class, so a later reader tells the two apart
          // without parsing rendered text.
          pi.appendEntry(injection.entryType, injection.details);
          // The call shape is load-bearing in all three of its parts.
          // Not a custom message: the runtime converts one to provider role
          // `developer` (compaction/messages.ts:194-211), ranking a remote peer
          // above the local operator. Not a bare `sendUserMessage`: with no
          // `deliverAs` it takes the prompt path, which auto-reads `@path` file
          // mentions out of the remote body (agent-session.ts:5811-5820 in OMP
          // 18.0.4) — and neither `expandPromptTemplates: false` nor the
          // `> ` body quoting stops that, because a preceding space already
          // satisfies the mention boundary. That applies to a notice exactly
          // as it does to a message,
          // which is why the idle branch here is `steer` and not an omitted
          // `deliverAs`.
          pi.sendUserMessage(injection.text, {
            deliverAs: deferred ? "followUp" : "steer",
          });
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
    const outcome = await resolveClient({
      env: process.env,
      cwd: ctx.cwd,
      parameters,
      agentDir: pi.pi.getAgentDir(),
    });
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
      .enum(["join", "list", "send", "announce", "fetch"])
      .describe(
        "Connect to a room, list connected peers, send a message to one peer, announce to every other peer in the room, or fetch a payload someone attached",
      ),
    project: pi.zod
      .string()
      .optional()
      .describe("join only: room project, overriding the project configuration file"),
    task: pi.zod
      .string()
      .optional()
      .describe("join only: room task, overriding the project configuration file"),
    as: pi.zod.string().optional().describe("join only: this session's peer name"),
    to: pi.zod
      .string()
      .optional()
      .describe("Peer name; required for send, and rejected for announce"),
    message: pi.zod
      .string()
      .optional()
      .describe("Message body; required for send and for announce"),
    reply_to: pi.zod.string().optional().describe("Message identifier being answered"),
    attach: pi.zod
      .string()
      .optional()
      .describe(
        "send and announce: path to a local file to attach, for material too large for a message body such as a diff, a log bundle, or a build artifact. The recipient is told a reference and fetches it deliberately.",
      ),
    reference: pi.zod
      .string()
      .optional()
      .describe("fetch only: the attachment reference a delivery reported"),
    max_bytes: pi.zod
      .number()
      .int()
      .nonnegative()
      .optional()
      .describe(
        "fetch only: decline a payload larger than this, reporting its size and transferring nothing",
      ),
  });

  pi.registerTool({
    name: "mesh",
    label: "OMP Relay Mesh",
    description:
      "Join a relay room, list the peers in it, send work to one peer, or announce something the whole room needs. Join first: nothing else works until this session has joined, and the join result reports the room it resolved, where each part of it came from, and who else is present. Send work to the peer that must do it, by name; announce information the room needs in order not to collide, with no target at all. A routed send result means the message was queued for the recipient, and an announcement's counts mean it was queued for that many peers; neither means anyone read, accepted, or answered it.",
    parameters,
    async execute(_toolCallId, args, _signal, _onUpdate, ctx) {
      const host: MeshHost = {
        interactive: ctx.mode === INTERACTIVE_MODE,
        client,
        room: live?.config.room ?? null,
        join: (request) => performJoin(ctx, request),
        recordSend: (details) => pi.appendEntry(OUTBOUND_MESSAGE_TYPE, details),
        recordAnnounce: (details) => pi.appendEntry(OUTBOUND_ANNOUNCE_TYPE, details),
        readAttachment: async (path) => new Uint8Array(await readFile(path)),
        saveAttachment: saveFetchedPayload,
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
    const global = await loadGlobalConfig(process.env, pi.pi.getAgentDir());
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
