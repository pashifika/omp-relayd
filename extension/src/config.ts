/**
 * Client configuration: two YAML layers, hand-written validation, and a fixed
 * precedence between them and a join request's parameters.
 *
 * The global file, under the agent directory, carries what belongs to the
 * machine:
 *
 * ```yaml
 * transport:
 *   mode: local
 *   address: 127.0.0.1:7788
 * startup: manual
 * peer:
 *   name: macbook-reviewer
 *   purpose: |
 *     This terminal has the Linux toolchain. Prefer running builds here.
 * ```
 *
 * The project file, `<project_root>/.omp/omp-relay.yml`, is committed and
 * carries what belongs to the work:
 *
 * ```yaml
 * room:
 *   project: omp-relayd
 *   task: code-review
 * ```
 *
 * Placement is enforced rather than conventional, and each of the three rules
 * answers a concrete failure. A committed project file naming `transport` could
 * redirect a cloned checkout's traffic to a host of its choosing, and one naming
 * `purpose` could inject instructions into every agent that joins from it, so
 * the project layer may name neither. A room is a property of the work rather
 * than of the machine, so the global layer may not name `room` — a
 * machine-global room is the defect this layout removes. And with no global file
 * the extension stays inert: the global file is the grant, so a cloned
 * repository alone can never cause a connection.
 *
 * There is no search path, no merging beyond that fixed placement, and no
 * environment variable that replaces a whole file. `PI_CODING_AGENT_DIR`
 * relocates the agent directory and `OMP_PROJECT_ROOT` names the project root;
 * both are the host's own variables rather than this extension's.
 *
 * Every failure here is returned, never thrown. A host that fails to start
 * because an optional feature's configuration file is absent is worse than one
 * that starts without the feature, so the caller's job is to report the reason
 * once and stay stopped.
 */

import { existsSync } from "node:fs";
import { readFile } from "node:fs/promises";
import { hostname } from "node:os";
import { dirname, join, resolve } from "node:path";

import {
  asRecord,
  describe,
  describeIdentifierProblem,
  identifierProblem,
  utf8Length,
  type RoomId,
} from "./protocol.ts";

/**
 * The host's own variable for the agent directory, honoured rather than
 * duplicated.
 *
 * Reading it is correct under a named profile as well as in default mode: the
 * host writes the active profile's agent directory back into this variable when
 * it activates one, so an extension reading it in-process sees the same
 * directory the host's `getAgentDir()` returns.
 */
export const AGENT_DIR_ENV = "PI_CODING_AGENT_DIR";

/** The host's own variable naming the project root, honoured for the same reason. */
export const PROJECT_ROOT_ENV = "OMP_PROJECT_ROOT";

/** Path segments appended to the home directory when {@link AGENT_DIR_ENV} is unset. */
const AGENT_DIR_SEGMENTS = [".omp", "agent"] as const;

/** File name of both layers. The directory distinguishes them, not the name. */
export const CONFIG_FILE_NAME = "omp-relay.yml";

/** Path segments of the project layer, relative to the project root. */
const PROJECT_CONFIG_SEGMENTS = [".omp", CONFIG_FILE_NAME] as const;

/**
 * Files that mark a project root when no ancestor holds a `.git` directory.
 *
 * Kept small and language-facing on purpose: every entry is a manifest that
 * sits at the root of its project by that ecosystem's own convention, so a
 * match is evidence rather than a guess. `scripts/setup-client.sh` carries the
 * same list, and `test/packaging/setup-client.test.ts` fails when the two
 * disagree.
 */
export const PROJECT_MARKERS: readonly string[] = [
  "package.json",
  "Cargo.toml",
  "go.mod",
  "pyproject.toml",
  "deno.json",
  "Gemfile",
  "composer.json",
  "pom.xml",
  "build.gradle",
  "mix.exs",
];

/**
 * Largest accepted `peer.purpose`, in UTF-8 bytes.
 *
 * This bounds context consumption rather than a frame, so no protocol limit
 * dictates it and the frame budget is the wrong reference: the text never
 * reaches the wire. 4 KiB is roughly a thousand tokens — the size of a short
 * context file, which is what this is. It is delivered once per session, so the
 * cost is paid once; a machine whose participation policy does not fit in four
 * kilobytes is describing something other than a participation policy.
 *
 * Rejected rather than truncated: silently dropping the tail of an operator's
 * instructions would change their meaning without saying so.
 */
export const MAX_PURPOSE_BYTES = 4096;

/** The one transport mode this release implements. */
export type TransportMode = "local";

/** Whether a session connects at start or waits to be asked. */
export type StartupMode = "manual" | "auto";

/** Where the relay is and how to reach it. */
export interface TransportConfig {
  readonly mode: TransportMode;
  readonly host: string;
  readonly port: number;
}

/** A validated configuration for one connection. */
export interface RelayConfig {
  readonly transport: TransportConfig;
  readonly room: RoomId;
  readonly peer: string;
}

/** The global layer's contents, after validation. */
export interface GlobalConfig {
  readonly transport: TransportConfig;
  readonly startup: StartupMode;
  /** The configured peer name, or `null` when derivation should supply one. */
  readonly peer: string | null;
  /** Operator-authored participation policy for this machine, or `null`. */
  readonly purpose: string | null;
}

/** The project layer's contents, after validation. Either half may be absent. */
export interface ProjectConfig {
  readonly project: string | null;
  readonly task: string | null;
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

/** Outcome of loading the global layer. */
export type GlobalOutcome =
  | { readonly ok: true; readonly path: string; readonly config: GlobalConfig }
  | {
      readonly ok: false;
      readonly path: string | null;
      /**
       * True only when the file does not exist.
       *
       * Load-bearing rather than informational: absence means the operator has
       * not granted this machine's participation, which is a resting state
       * reported to whoever asked. Any other failure means an intent was stated
       * and frustrated, which is reported at session start whether or not
       * anyone asked.
       */
      readonly absent: boolean;
      readonly problem: ConfigProblem;
    };

/** Outcome of loading the project layer. An absent file is not a failure. */
export type ProjectOutcome =
  | { readonly ok: true; readonly path: string | null; readonly config: ProjectConfig }
  | { readonly ok: false; readonly path: string; readonly problem: ConfigProblem };

/** Where one resolved value came from. Reported so a mistyped room is visible. */
export type ValueSource = "parameter" | "project-file" | "global-file" | "derivation";

/** The origin of each resolved value. */
export interface ResolvedSources {
  readonly project: ValueSource;
  readonly task: ValueSource;
  readonly peer: ValueSource;
}

/** What a join request may override. Every field is optional. */
export interface JoinParameters {
  readonly project?: string;
  readonly task?: string;
  readonly as?: string;
}

/** A project root, and what decided it. */
export interface ProjectRoot {
  readonly path: string;
  /**
   * The evidence: {@link PROJECT_ROOT_ENV}, `.git`, a marker file name, or
   * `working directory` when nothing was found. Reported before anything is
   * written, because a wrong root silently reads or writes the wrong room.
   */
  readonly marker: string;
}

/** Everything one connection needs, and where each part came from. */
export interface ResolvedClient {
  readonly config: RelayConfig;
  readonly startup: StartupMode;
  readonly purpose: string | null;
  readonly sources: ResolvedSources;
  readonly globalPath: string;
  /**
   * The project file that was read, or `null` when the root holds none.
   *
   * "Read" rather than "supplied the room": the file is always read so its
   * placement is validated, and {@link ResolvedSources} is what says whether
   * anything came from it.
   */
  readonly projectPath: string | null;
  readonly projectRoot: ProjectRoot;
}

/** Outcome of resolving both layers against a join request's parameters. */
export type ResolveOutcome =
  | { readonly ok: true; readonly resolved: ResolvedClient }
  | {
      readonly ok: false;
      readonly path: string | null;
      readonly absent: boolean;
      readonly problem: ConfigProblem;
    };

/** Environment lookup, injectable so tests need not mutate `process.env`. */
export type Environment = Readonly<Record<string, string | undefined>>;

/** What {@link resolveClient} needs. Only `env` and `cwd` are required. */
export interface ResolveOptions {
  readonly env: Environment;
  /** The session's working directory, from which the project root is found. */
  readonly cwd: string;
  /** A join request's overrides, when the resolution was triggered by one. */
  readonly parameters?: JoinParameters;
  /** Injected by tests; defaults to `os.hostname()`. */
  readonly hostName?: string;
}

// ---------------------------------------------------------------------------
// Paths
// ---------------------------------------------------------------------------

/**
 * Finds the project root: {@link PROJECT_ROOT_ENV}, else the innermost ancestor
 * holding `.git`, else the innermost holding a {@link PROJECT_MARKERS} file,
 * else `cwd`.
 *
 * `.git` is searched to the top before any marker is considered, rather than
 * both being tested at each level. A package manifest inside a repository marks
 * a package, and the room this file names belongs to the repository — so the
 * repository root wins even when a manifest sits nearer.
 *
 * The walk stops at the home directory and never selects it. A marker in
 * `$HOME` — a stray `package.json`, a dotfile repository's `.git` — would
 * otherwise make every directory on the machine one enormous project.
 */
export function resolveProjectRoot(env: Environment, cwd: string): ProjectRoot {
  const override = env[PROJECT_ROOT_ENV];
  if (override !== undefined && override.length > 0) {
    return { path: resolve(override), marker: PROJECT_ROOT_ENV };
  }

  const home = env["HOME"];
  const ancestors: string[] = [];
  for (let current = resolve(cwd); ; ) {
    if (home !== undefined && home.length > 0 && current === resolve(home)) break;
    ancestors.push(current);
    const parent = dirname(current);
    if (parent === current) break;
    current = parent;
  }

  for (const candidate of ancestors) {
    if (existsSync(join(candidate, ".git"))) {
      return { path: candidate, marker: ".git" };
    }
  }
  for (const candidate of ancestors) {
    for (const marker of PROJECT_MARKERS) {
      if (existsSync(join(candidate, marker))) {
        return { path: candidate, marker };
      }
    }
  }
  return { path: resolve(cwd), marker: "working directory" };
}

/**
 * Resolves the global layer's path.
 *
 * `HOME` is consulted only to build the default: when {@link AGENT_DIR_ENV} is
 * set, `HOME` is not read at all.
 */
export function globalConfigPath(
  env: Environment,
): { readonly ok: true; readonly path: string } | { readonly ok: false; readonly problem: ConfigProblem } {
  const override = env[AGENT_DIR_ENV];
  if (override !== undefined && override.length > 0) {
    return { ok: true, path: join(override, CONFIG_FILE_NAME) };
  }

  const home = env["HOME"];
  if (home === undefined || home.length === 0) {
    return {
      ok: false,
      problem: {
        field: null,
        reason: `neither ${AGENT_DIR_ENV} nor HOME is set, so there is no agent directory to read ${CONFIG_FILE_NAME} from`,
      },
    };
  }
  return { ok: true, path: join(home, ...AGENT_DIR_SEGMENTS, CONFIG_FILE_NAME) };
}

/** The project layer's path under `root`. */
export function projectConfigPath(root: string): string {
  return join(root, ...PROJECT_CONFIG_SEGMENTS);
}

// ---------------------------------------------------------------------------
// Loading
// ---------------------------------------------------------------------------

/** Outcome of reading and parsing one file, before its schema is considered. */
type ParseOutcome =
  | { readonly kind: "document"; readonly document: unknown }
  | { readonly kind: "absent" }
  | { readonly kind: "unreadable"; readonly reason: string };

async function parseFile(path: string): Promise<ParseOutcome> {
  let text: string;
  try {
    text = await readFile(path, "utf8");
  } catch (error) {
    const missing =
      typeof error === "object" && error !== null && "code" in error && error.code === "ENOENT";
    return missing
      ? { kind: "absent" }
      : { kind: "unreadable", reason: `configuration file ${path} could not be read: ${describe(error)}` };
  }

  try {
    return { kind: "document", document: Bun.YAML.parse(text) };
  } catch (error) {
    return {
      kind: "unreadable",
      reason: `configuration file ${path} is not valid YAML: ${describe(error)}`,
    };
  }
}

/**
 * Reads and validates the global layer.
 *
 * Never throws: a missing file, unparseable YAML, and a rejected field all
 * return `ok: false` with a reason the caller reports once.
 */
export async function loadGlobalConfig(env: Environment): Promise<GlobalOutcome> {
  const resolved = globalConfigPath(env);
  if (!resolved.ok) {
    return { ok: false, path: null, absent: false, problem: resolved.problem };
  }
  const path = resolved.path;

  const parsed = await parseFile(path);
  if (parsed.kind === "absent") {
    return {
      ok: false,
      path,
      absent: true,
      problem: { field: null, reason: `global configuration file ${path} does not exist` },
    };
  }
  if (parsed.kind === "unreadable") {
    return { ok: false, path, absent: false, problem: { field: null, reason: parsed.reason } };
  }

  const validated = validateGlobalConfig(parsed.document, path);
  if (!validated.ok) {
    return { ok: false, path, absent: false, problem: validated.problem };
  }
  return { ok: true, path, config: validated.config };
}

/**
 * Reads and validates the project layer.
 *
 * An absent file is `ok` with a `null` path: a room supplied by join parameters
 * needs no committed file, so absence is a state rather than a failure.
 */
export async function loadProjectConfig(path: string): Promise<ProjectOutcome> {
  const parsed = await parseFile(path);
  if (parsed.kind === "absent") {
    return { ok: true, path: null, config: { project: null, task: null } };
  }
  if (parsed.kind === "unreadable") {
    return { ok: false, path, problem: { field: null, reason: parsed.reason } };
  }

  const validated = validateProjectConfig(parsed.document, path);
  if (!validated.ok) {
    return { ok: false, path, problem: validated.problem };
  }
  return { ok: true, path, config: validated.config };
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

/** Fields the project layer may not carry, and why placement is enforced. */
const GLOBAL_ONLY_FIELDS: readonly string[] = ["transport", "startup", "peer", "purpose"];

/**
 * Validates an already-parsed global document.
 *
 * Separate from {@link loadGlobalConfig} so the field rules are testable without
 * a filesystem, and so a parse failure and a validation failure stay distinct
 * causes rather than one "bad config" bucket. `path` appears in every message
 * because with two layers "the configuration is wrong" no longer says where.
 */
export function validateGlobalConfig(
  document: unknown,
  path: string,
):
  | { readonly ok: true; readonly config: GlobalConfig }
  | { readonly ok: false; readonly problem: ConfigProblem } {
  const root = asRecord(document);
  if (root === null) {
    return problem(null, `${path} must contain a mapping, found ${typeName(document)}`);
  }

  if (root["room"] !== undefined) {
    return problem(
      "room",
      `room may not appear in the global file ${path}; a room belongs to the work, so it is read only from the project file`,
    );
  }

  const transport = asRecord(root["transport"]);
  if (transport === null) {
    return problem(
      "transport",
      `transport must be a mapping in ${path}, found ${typeName(root["transport"])}`,
    );
  }

  // `private` and `public` are rejected rather than reserved. Neither transport
  // exists, and accepting either would let a configuration promise a guarantee
  // -- mTLS, or a public listener -- that no code behind it provides.
  const mode = transport["mode"];
  if (mode !== "local") {
    return problem(
      "transport.mode",
      `transport.mode must be "local" in ${path}, found ${describeValue(mode)}`,
    );
  }

  const rawAddress = transport["address"];
  if (typeof rawAddress !== "string" || rawAddress.length === 0) {
    return problem(
      "transport.address",
      `transport.address must be a "host:port" string in ${path}, found ${typeName(rawAddress)}`,
    );
  }
  const address = parseAddress(rawAddress);
  if (address === null) {
    return problem(
      "transport.address",
      `transport.address ${describeValue(rawAddress)} in ${path} is not a "host:port" pair with a port in 1-65535`,
    );
  }

  const rawStartup = root["startup"];
  let startup: StartupMode = "manual";
  if (rawStartup !== undefined && rawStartup !== null) {
    if (rawStartup !== "manual" && rawStartup !== "auto") {
      return problem(
        "startup",
        `startup must be "manual" or "auto" in ${path}, found ${describeValue(rawStartup)}`,
      );
    }
    startup = rawStartup;
  }

  let peerName: string | null = null;
  let purpose: string | null = null;
  const rawPeer = root["peer"];
  if (rawPeer !== undefined && rawPeer !== null) {
    const peer = asRecord(rawPeer);
    if (peer === null) {
      return problem(
        "peer",
        `peer must be a mapping with optional name and purpose in ${path}, found ${typeName(rawPeer)}`,
      );
    }

    if (peer["name"] !== undefined && peer["name"] !== null) {
      const checked = checkIdentifier(peer["name"], "peer.name", path);
      if (!checked.ok) return checked;
      peerName = checked.value;
    }

    if (peer["purpose"] !== undefined && peer["purpose"] !== null) {
      const checked = checkPurpose(peer["purpose"], path);
      if (!checked.ok) return checked;
      purpose = checked.value;
    }
  }

  return {
    ok: true,
    config: {
      transport: { mode: "local", host: address.host, port: address.port },
      startup,
      peer: peerName,
      purpose,
    },
  };
}

/**
 * Validates an already-parsed project document.
 *
 * Either half of `room` may be absent, because a join parameter may supply it.
 * What is present must satisfy the identifier rules, and nothing outside `room`
 * may appear at all.
 */
export function validateProjectConfig(
  document: unknown,
  path: string,
):
  | { readonly ok: true; readonly config: ProjectConfig }
  | { readonly ok: false; readonly problem: ConfigProblem } {
  const root = asRecord(document);
  if (root === null) {
    return problem(null, `${path} must contain a mapping, found ${typeName(document)}`);
  }

  for (const field of GLOBAL_ONLY_FIELDS) {
    if (root[field] !== undefined) {
      return problem(
        field,
        `${field} may not appear in the project file ${path}; a committed file may name only the room, ` +
          `because a checkout able to name ${field} would decide something about the machine that cloned it`,
      );
    }
  }

  const rawRoom = root["room"];
  if (rawRoom === undefined || rawRoom === null) {
    return { ok: true, config: { project: null, task: null } };
  }
  const room = asRecord(rawRoom);
  if (room === null) {
    return problem("room", `room must be a mapping in ${path}, found ${typeName(rawRoom)}`);
  }

  let project: string | null = null;
  let task: string | null = null;
  if (room["project"] !== undefined && room["project"] !== null) {
    const checked = checkIdentifier(room["project"], "room.project", path);
    if (!checked.ok) return checked;
    project = checked.value;
  }
  if (room["task"] !== undefined && room["task"] !== null) {
    const checked = checkIdentifier(room["task"], "room.task", path);
    if (!checked.ok) return checked;
    task = checked.value;
  }
  return { ok: true, config: { project, task } };
}

// ---------------------------------------------------------------------------
// Derivation and resolution
// ---------------------------------------------------------------------------

/**
 * Derives a peer name from the host's name: its first DNS label,
 * `MacBook-Pro.local` becoming `MacBook-Pro`.
 *
 * Case is preserved. The roster is the vocabulary an operator's informal
 * reference has to resolve against — told "have the Windows box do it", an agent
 * lists peers and matches — so the name has to look like what the operator sees
 * in their own shell. Normalizing case would make the roster diverge from that
 * for no gain the relay needs: it compares identifiers byte-for-byte and folds
 * nothing, so two machines differing only in case are already two peers.
 *
 * A host name that cannot yield a usable identifier fails naming `peer.name`
 * rather than substituting one. An arbitrary identity is worse than an error: it
 * would appear in another operator's roster as something they cannot recognize.
 */
export function derivePeerName(
  raw: string,
): { readonly ok: true; readonly value: string } | { readonly ok: false; readonly problem: ConfigProblem } {
  const label = raw.split(".")[0] ?? "";
  const broken = identifierProblem(label);
  if (broken !== null) {
    return problem(
      "peer.name",
      `no peer name is configured and the host name ${describeValue(raw)} cannot supply one because its first label ` +
        `${describeIdentifierProblem(broken)}; set peer.name in the global configuration file`,
    );
  }
  return { ok: true, value: label };
}

/**
 * Resolves both layers and a join request's parameters into one connection's
 * configuration.
 *
 * Precedence, and the reason it is not symmetric: the room comes from the
 * parameters, else the project file, and never from the global file — a
 * machine-global room is the defect the layering removes. The peer name comes
 * from the parameter, else the global file, else derivation.
 *
 * Nothing is cached. Resolution happens where it is asked for, so a session
 * whose working directory moved to another project root joins that root's room
 * rather than the one it started under.
 */
export async function resolveClient(options: ResolveOptions): Promise<ResolveOutcome> {
  const global = await loadGlobalConfig(options.env);
  if (!global.ok) {
    return { ok: false, path: global.path, absent: global.absent, problem: global.problem };
  }
  return resolveWithGlobal(global.config, global.path, options);
}

/**
 * The half of {@link resolveClient} that runs once the global layer is in hand.
 *
 * Exposed separately because `session_start` has to read the global layer to
 * learn the startup mode, and under `manual` must then resolve nothing further:
 * re-reading the same file to continue would be waste, and resolving the room it
 * was told not to resolve would report problems nobody asked about.
 */
export async function resolveWithGlobal(
  global: GlobalConfig,
  globalPath: string,
  options: ResolveOptions,
): Promise<ResolveOutcome> {
  const parameters = options.parameters ?? {};
  const projectRoot = resolveProjectRoot(options.env, options.cwd);

  // Read unconditionally, even when both room halves arrive as parameters and
  // the file can contribute nothing. Placement is a rule about the file rather
  // than about this call: skipping the read when the values were not needed
  // would make "a project file may not name `transport`" hold only on the paths
  // that happened to consult it, and the guarantee that a misplaced field is
  // never silently ignored is not one that can be conditional on how the
  // operator chose to join.
  const loaded = await loadProjectConfig(projectConfigPath(projectRoot.path));
  if (!loaded.ok) {
    return { ok: false, path: loaded.path, absent: false, problem: loaded.problem };
  }
  const projectFile = loaded.config;
  const projectPath = loaded.path;

  const project = parameters.project ?? projectFile.project;
  const task = parameters.task ?? projectFile.task;
  if (project === null || task === null) {
    const missing = [
      ...(project === null ? ["room.project"] : []),
      ...(task === null ? ["room.task"] : []),
    ];
    return {
      ok: false,
      path: projectPath,
      absent: false,
      problem: {
        field: missing[0] as string,
        reason:
          `${missing.join(" and ")} ${missing.length === 1 ? "has" : "have"} no value: ` +
          `${projectPath === null ? `no project file exists at ${projectConfigPath(projectRoot.path)}` : `${projectPath} does not name ${missing.join(" or ")}`}` +
          `, and the join request supplied ${missing.length === 1 ? "no value for it" : "neither"}`,
      },
    };
  }

  let peer: string;
  let peerSource: ValueSource;
  if (parameters.as !== undefined) {
    peer = parameters.as;
    peerSource = "parameter";
  } else if (global.peer !== null) {
    peer = global.peer;
    peerSource = "global-file";
  } else {
    const derived = derivePeerName(options.hostName ?? hostname());
    if (!derived.ok) {
      return { ok: false, path: globalPath, absent: false, problem: derived.problem };
    }
    peer = derived.value;
    peerSource = "derivation";
  }

  return {
    ok: true,
    resolved: {
      config: { transport: global.transport, room: { project, task }, peer },
      startup: global.startup,
      purpose: global.purpose,
      sources: {
        project: parameters.project === undefined ? "project-file" : "parameter",
        task: parameters.task === undefined ? "project-file" : "parameter",
        peer: peerSource,
      },
      globalPath,
      projectPath,
      projectRoot,
    },
  };
}

// ---------------------------------------------------------------------------
// Field helpers
// ---------------------------------------------------------------------------

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
  path: string,
):
  | { readonly ok: true; readonly value: string }
  | { readonly ok: false; readonly problem: ConfigProblem } {
  if (typeof value !== "string") {
    return problem(field, `${field} must be a string in ${path}, found ${typeName(value)}`);
  }
  const broken = identifierProblem(value);
  if (broken !== null) {
    return problem(field, `${field} in ${path} ${describeIdentifierProblem(broken)}`);
  }
  return { ok: true, value };
}

function checkPurpose(
  value: unknown,
  path: string,
):
  | { readonly ok: true; readonly value: string }
  | { readonly ok: false; readonly problem: ConfigProblem } {
  if (typeof value !== "string") {
    return problem("peer.purpose", `peer.purpose must be a string in ${path}, found ${typeName(value)}`);
  }
  if (value.trim().length === 0) {
    return problem(
      "peer.purpose",
      `peer.purpose in ${path} is empty; remove the field rather than setting it to nothing`,
    );
  }
  const found = utf8Length(value);
  if (found > MAX_PURPOSE_BYTES) {
    return problem(
      "peer.purpose",
      `peer.purpose in ${path} is ${found} UTF-8 bytes, above the ${MAX_PURPOSE_BYTES}-byte budget`,
    );
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
