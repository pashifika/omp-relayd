/**
 * Client configuration: one YAML file, one environment-variable override, and
 * hand-written validation.
 *
 * The accepted schema is exactly:
 *
 * ```yaml
 * transport:
 *   mode: local
 *   address: 127.0.0.1:7788
 * room:
 *   project: <project>
 *   task: <task>
 * peer: <peer>
 * ```
 *
 * There is no search path and no merging, and the reason is diagnosis rather
 * than minimalism: when a user's peer name is wrong, "the file at this path
 * says X" is an answerable question and "some file among these five said X" is
 * not.
 *
 * Every failure here is returned, never thrown. A host that fails to start
 * because an optional feature's configuration file is absent is worse than one
 * that starts without the feature, so the caller's job is to report the reason
 * once and stay stopped.
 */

import { readFile } from "node:fs/promises";
import { join } from "node:path";

import {
  asRecord,
  describe,
  describeIdentifierProblem,
  identifierProblem,
  type RoomId,
} from "./protocol.ts";

/**
 * The single environment variable that relocates the configuration file.
 *
 * Named alongside the server's `OMP_RELAY_LISTEN`, and distinct from it: this
 * one names a client file, that one names a server bind address.
 */
export const CONFIG_PATH_ENV = "OMP_RELAY_CONFIG";

/**
 * Path segments appended to the user's home directory by default.
 *
 * This is the location the protocol design record documents (§10.2), and the
 * only one `specs/client-configuration` permits the client to read.
 */
const DEFAULT_CONFIG_SEGMENTS = [".omp", "agent", "omp-relay.yml"] as const;

/** The one transport mode this release implements. */
export type TransportMode = "local";

/** Where the relay is and how to reach it. */
export interface TransportConfig {
  readonly mode: TransportMode;
  readonly host: string;
  readonly port: number;
}

/** A validated configuration. Constructing one by hand bypasses validation. */
export interface RelayConfig {
  readonly transport: TransportConfig;
  readonly room: RoomId;
  readonly peer: string;
}

/** Why a configuration was refused, and which field is responsible. */
export interface ConfigProblem {
  /**
   * Dotted path of the offending field, or `null` when the failure is not
   * attributable to one — a missing file or unparseable YAML.
   */
  readonly field: string | null;
  /** One sentence, suitable for reporting verbatim. */
  readonly reason: string;
}

/** Outcome of loading a configuration file. */
export type ConfigOutcome =
  | { readonly ok: true; readonly path: string; readonly config: RelayConfig }
  | {
      readonly ok: false;
      readonly path: string | null;
      readonly problem: ConfigProblem;
    };

/** Environment lookup, injectable so tests need not mutate `process.env`. */
export type Environment = Readonly<Record<string, string | undefined>>;

/**
 * Resolves the one path configuration is read from.
 *
 * `HOME` is consulted only to build the default. It is not a second override:
 * when {@link CONFIG_PATH_ENV} is set, `HOME` is not read at all, which is what
 * makes the override a replacement rather than a layer.
 */
export function resolveConfigPath(
  env: Environment,
): { readonly ok: true; readonly path: string } | { readonly ok: false; readonly problem: ConfigProblem } {
  const override = env[CONFIG_PATH_ENV];
  if (override !== undefined && override.length > 0) {
    return { ok: true, path: override };
  }

  const home = env["HOME"];
  if (home === undefined || home.length === 0) {
    return {
      ok: false,
      problem: {
        field: null,
        reason: `neither ${CONFIG_PATH_ENV} nor HOME is set, so there is no configuration path to read`,
      },
    };
  }
  return { ok: true, path: join(home, ...DEFAULT_CONFIG_SEGMENTS) };
}

/**
 * Reads and validates the configuration file.
 *
 * Never throws: a missing file, unparseable YAML, and a rejected field all
 * return `ok: false` with a reason the caller reports once.
 */
export async function loadConfig(env: Environment): Promise<ConfigOutcome> {
  const resolved = resolveConfigPath(env);
  if (!resolved.ok) {
    return { ok: false, path: null, problem: resolved.problem };
  }
  const path = resolved.path;

  let text: string;
  try {
    text = await readFile(path, "utf8");
  } catch (error) {
    const missing =
      typeof error === "object" &&
      error !== null &&
      "code" in error &&
      error.code === "ENOENT";
    return {
      ok: false,
      path,
      problem: {
        field: null,
        reason: missing
          ? `configuration file ${path} does not exist`
          : `configuration file ${path} could not be read: ${describe(error)}`,
      },
    };
  }

  let parsed: unknown;
  try {
    parsed = Bun.YAML.parse(text);
  } catch (error) {
    return {
      ok: false,
      path,
      problem: {
        field: null,
        reason: `configuration file ${path} is not valid YAML: ${describe(error)}`,
      },
    };
  }

  const validated = validateConfig(parsed);
  if (!validated.ok) {
    return { ok: false, path, problem: validated.problem };
  }
  return { ok: true, path, config: validated.config };
}

/**
 * Validates an already-parsed configuration document.
 *
 * Separate from {@link loadConfig} so the field rules are testable without a
 * filesystem, and so the parse failure and the validation failure stay distinct
 * causes rather than one "bad config" bucket.
 */
export function validateConfig(
  document: unknown,
):
  | { readonly ok: true; readonly config: RelayConfig }
  | { readonly ok: false; readonly problem: ConfigProblem } {
  const root = asRecord(document);
  if (root === null) {
    return problem(null, `configuration must be a mapping, found ${typeName(document)}`);
  }

  const transport = asRecord(root["transport"]);
  if (transport === null) {
    return problem(
      "transport",
      `transport must be a mapping, found ${typeName(root["transport"])}`,
    );
  }

  // `private` and `public` are rejected rather than reserved. Neither transport
  // exists, and accepting either would let a configuration promise a guarantee
  // -- mTLS, or a public listener -- that no code behind it provides.
  const mode = transport["mode"];
  if (mode !== "local") {
    return problem(
      "transport.mode",
      `transport.mode must be "local", found ${describeValue(mode)}`,
    );
  }

  const rawAddress = transport["address"];
  if (typeof rawAddress !== "string" || rawAddress.length === 0) {
    return problem(
      "transport.address",
      `transport.address must be a "host:port" string, found ${typeName(rawAddress)}`,
    );
  }
  const address = parseAddress(rawAddress);
  if (address === null) {
    return problem(
      "transport.address",
      `transport.address ${describeValue(rawAddress)} is not a "host:port" pair with a port in 1-65535`,
    );
  }

  const room = asRecord(root["room"]);
  if (room === null) {
    return problem("room", `room must be a mapping, found ${typeName(root["room"])}`);
  }

  const project = checkIdentifier(room["project"], "room.project");
  if (!project.ok) return project;
  const task = checkIdentifier(room["task"], "room.task");
  if (!task.ok) return task;
  const peer = checkIdentifier(root["peer"], "peer");
  if (!peer.ok) return peer;

  return {
    ok: true,
    config: {
      transport: { mode: "local", host: address.host, port: address.port },
      room: { project: project.value, task: task.value },
      peer: peer.value,
    },
  };
}

/**
 * Splits `host:port`, accepting the bracketed form for IPv6.
 *
 * Brackets are required for an IPv6 literal rather than optional, because
 * `::1:7788` is genuinely ambiguous — it is a valid address on its own, and
 * also a host and a port. Demanding `[::1]:7788` makes the intent explicit
 * instead of guessing at it.
 *
 * @returns the split pair, or `null` when the value is not one.
 */
export function parseAddress(
  value: string,
): { readonly host: string; readonly port: number } | null {
  let host: string;
  let portText: string;

  if (value.startsWith("[")) {
    const close = value.indexOf("]");
    if (close < 2 || value[close + 1] !== ":") {
      return null;
    }
    host = value.slice(1, close);
    portText = value.slice(close + 2);
  } else {
    const separator = value.lastIndexOf(":");
    if (separator <= 0) {
      return null;
    }
    host = value.slice(0, separator);
    portText = value.slice(separator + 1);
    // An unbracketed colon in the host means an IPv6 literal written without
    // brackets, which is the ambiguity above rather than a host name.
    if (host.includes(":")) {
      return null;
    }
  }

  if (host.length === 0 || !/^[0-9]+$/.test(portText)) {
    return null;
  }
  const port = Number(portText);
  if (port < 1 || port > 65535) {
    return null;
  }
  return { host, port };
}

function checkIdentifier(
  value: unknown,
  field: string,
):
  | { readonly ok: true; readonly value: string }
  | { readonly ok: false; readonly problem: ConfigProblem } {
  if (typeof value !== "string") {
    return problem(field, `${field} must be a string, found ${typeName(value)}`);
  }
  const broken = identifierProblem(value);
  if (broken !== null) {
    return problem(field, `${field} ${describeIdentifierProblem(broken)}`);
  }
  return { ok: true, value };
}

function problem(
  field: string | null,
  reason: string,
): { readonly ok: false; readonly problem: ConfigProblem } {
  return { ok: false, problem: { field, reason } };
}

/** Characters of an untrusted scalar a diagnostic will echo. */
const DIAGNOSTIC_VALUE_CHARS = 40;

/**
 * Renders a value from the parsed document for a diagnostic: type first, then
 * bounded.
 *
 * `JSON.stringify` must not be reached for by reflex here. Bun's YAML preserves
 * aliases, so the serializer walks the expanded graph: a 405-byte document of
 * nested aliases rendered one `transport.mode` diagnostic as 10,271,832
 * characters in 29 ms, and it grows exponentially with alias depth. Guarding
 * the *result* cannot help, because the cost is paid producing it — so only a
 * scalar is ever formatted, and a container contributes its type name alone.
 *
 * A scalar is clipped as well. Every caller compares the value against one
 * short literal or one short grammar, so a long prefix carries no diagnostic
 * information a short one does not.
 */
function describeValue(value: unknown): string {
  if (typeof value === "string") {
    return JSON.stringify(
      value.length > DIAGNOSTIC_VALUE_CHARS
        ? `${value.slice(0, DIAGNOSTIC_VALUE_CHARS)}…`
        : value,
    );
  }
  if (typeof value === "number" || typeof value === "boolean") {
    return String(value);
  }
  return typeName(value);
}

function typeName(value: unknown): string {
  if (value === undefined) return "nothing";
  if (value === null) return "null";
  if (Array.isArray(value)) return "a list";
  return `a ${typeof value}`;
}
