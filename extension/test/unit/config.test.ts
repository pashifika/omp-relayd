/**
 * Two-layer configuration: project-root discovery, per-layer validation, fixed
 * field placement, host-name derivation, and the precedence between the layers
 * and a join request's parameters.
 *
 * The behavior under test is as much about what does *not* happen as what does:
 * a rejected configuration must leave the host running and must not produce a
 * connection attempt, so the last test here watches a real listener and asserts
 * nothing arrives.
 */

import { afterAll, describe, expect, test } from "bun:test";
import { createServer } from "node:net";
import { mkdtemp, mkdir, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { RelayClient } from "../../src/client.ts";
import {
  AGENT_DIR_ENV,
  CONFIG_FILE_NAME,
  derivePeerName,
  globalConfigPath,
  loadGlobalConfig,
  loadProjectConfig,
  MAX_PURPOSE_BYTES,
  parseAddress,
  PROJECT_MARKERS,
  PROJECT_ROOT_ENV,
  projectConfigPath,
  resolveClient,
  resolveProjectRoot,
  validateGlobalConfig,
  validateProjectConfig,
  type Environment,
} from "../../src/config.ts";

const GLOBAL = `
transport:
  mode: local
  address: 127.0.0.1:7788
peer:
  name: macbook-reviewer
`;

const PROJECT = `
room:
  project: omp-relayd
  task: layer-client-configuration
`;

const roots: string[] = [];

async function scratch(): Promise<string> {
  const root = await mkdtemp(join(tmpdir(), "omp-relay-config-"));
  roots.push(root);
  return root;
}

/** One scratch machine: an agent directory, a project root, and an env for both. */
interface Machine {
  readonly agentDir: string;
  readonly projectRoot: string;
  readonly globalPath: string;
  readonly projectPath: string;
  readonly env: Environment;
}

/**
 * Lays out both layers under scratch directories.
 *
 * `HOME` points at a third directory that holds neither file, so a test that
 * accidentally resolves through the default path fails rather than silently
 * reading nothing.
 */
async function machine(
  layers: { global?: string | null; project?: string | null } = {},
): Promise<Machine> {
  const agentDir = await scratch();
  const projectRoot = await scratch();
  const globalPath = join(agentDir, CONFIG_FILE_NAME);
  const projectPath = projectConfigPath(projectRoot);

  const globalBody = layers.global === undefined ? GLOBAL : layers.global;
  if (globalBody !== null) {
    await writeFile(globalPath, globalBody, "utf8");
  }
  const projectBody = layers.project === undefined ? PROJECT : layers.project;
  if (projectBody !== null) {
    await mkdir(join(projectRoot, ".omp"), { recursive: true });
    await writeFile(projectPath, projectBody, "utf8");
  }

  return {
    agentDir,
    projectRoot,
    globalPath,
    projectPath,
    env: {
      HOME: await scratch(),
      [AGENT_DIR_ENV]: agentDir,
      [PROJECT_ROOT_ENV]: projectRoot,
    },
  };
}

afterAll(() => {
  // Left in the OS temp directory deliberately: a failed run's inputs are
  // evidence, and the OS reclaims them.
  if (roots.length > 0) {
    console.log(`config fixtures written under ${roots.length} scratch directory(ies)`);
  }
});

describe("project-root discovery", () => {
  test("the environment variable wins outright", async () => {
    const root = await scratch();
    const resolved = resolveProjectRoot({ [PROJECT_ROOT_ENV]: root }, "/somewhere/else");
    expect(resolved).toEqual({ path: root, marker: PROJECT_ROOT_ENV });
  });

  test("the innermost ancestor holding .git is the root", async () => {
    const home = await scratch();
    const repo = join(home, "work", "widget");
    const deep = join(repo, "src", "parser");
    await mkdir(join(repo, ".git"), { recursive: true });
    await mkdir(deep, { recursive: true });

    const resolved = resolveProjectRoot({ HOME: home }, deep);
    expect(resolved).toEqual({ path: repo, marker: ".git" });
  });

  test(".git outranks a language marker that sits nearer the working directory", async () => {
    // A package manifest inside a repository marks a package. The room this
    // file names belongs to the repository, so the repository root wins even
    // though `package.json` is three levels closer.
    const home = await scratch();
    const repo = join(home, "work", "monorepo");
    const pkg = join(repo, "packages", "app");
    await mkdir(join(repo, ".git"), { recursive: true });
    await mkdir(pkg, { recursive: true });
    await writeFile(join(pkg, "package.json"), "{}\n", "utf8");

    const resolved = resolveProjectRoot({ HOME: home }, pkg);
    expect(resolved).toEqual({ path: repo, marker: ".git" });
    console.log(
      `with package.json at ${pkg} and .git at ${repo}, the resolved root is ${resolved.path} by ${resolved.marker}`,
    );
  });

  test.each([...PROJECT_MARKERS])("%s marks a root when no ancestor holds .git", async (marker) => {
    const home = await scratch();
    const project = join(home, "work", "thing");
    const deep = join(project, "a", "b");
    await mkdir(deep, { recursive: true });
    await writeFile(join(project, marker), "\n", "utf8");

    expect(resolveProjectRoot({ HOME: home }, deep)).toEqual({ path: project, marker });
  });

  test("the walk stops at the home directory and never selects it", async () => {
    // A dotfile repository or a stray manifest in `$HOME` would otherwise make
    // every directory on the machine one enormous project.
    const home = await scratch();
    const loose = join(home, "notes", "scratch");
    await mkdir(join(home, ".git"), { recursive: true });
    await writeFile(join(home, "package.json"), "{}\n", "utf8");
    await mkdir(loose, { recursive: true });

    const resolved = resolveProjectRoot({ HOME: home }, loose);
    expect(resolved).toEqual({ path: loose, marker: "working directory" });
    console.log(
      `with .git and package.json both directly in HOME (${home}), the resolved root is ${resolved.path} by "${resolved.marker}" -- HOME was not selected`,
    );
  });
});

describe("the global layer's path", () => {
  test("the agent-directory variable relocates it", () => {
    const resolved = globalConfigPath({ [AGENT_DIR_ENV]: "/opt/profiles/work/agent" });
    expect(resolved.ok).toBe(true);
    if (!resolved.ok) return;
    expect(resolved.path).toBe("/opt/profiles/work/agent/omp-relay.yml");
  });

  test("the default is one documented location under HOME", () => {
    const resolved = globalConfigPath({ HOME: "/home/dev" });
    expect(resolved.ok).toBe(true);
    if (!resolved.ok) return;
    expect(resolved.path).toBe("/home/dev/.omp/agent/omp-relay.yml");
  });

  test("with neither there is no path to read", async () => {
    const outcome = await loadGlobalConfig({});
    expect(outcome.ok).toBe(false);
    if (outcome.ok) return;
    expect(outcome.path).toBeNull();
    expect(outcome.problem.reason).toContain(AGENT_DIR_ENV);
  });
});

describe("the global layer's schema", () => {
  const parse = (body: string): unknown => Bun.YAML.parse(body);
  const check = (body: string) => validateGlobalConfig(parse(body), "/g/omp-relay.yml");

  test("a valid file yields the values the client will use", () => {
    const outcome = check(GLOBAL);
    expect(outcome.ok).toBe(true);
    if (!outcome.ok) return;
    expect(outcome.config).toEqual({
      transport: { mode: "local", host: "127.0.0.1", port: 7788 },
      startup: "manual",
      peer: "macbook-reviewer",
      purpose: null,
    });
  });

  test.each(["private", "public", "Local"])("transport.mode %p is refused", (mode) => {
    // `private` and `public` are refused rather than reserved: neither
    // transport exists, so accepting either would let configuration promise a
    // guarantee no code provides.
    const outcome = check(GLOBAL.replace("mode: local", `mode: ${mode}`));
    expect(outcome.ok).toBe(false);
    if (outcome.ok) return;
    expect(outcome.problem.field).toBe("transport.mode");
  });

  test.each([
    ["127.0.0.1", "no port at all"],
    ["127.0.0.1:", "an empty port"],
    ["127.0.0.1:http", "a non-numeric port"],
    ["127.0.0.1:0", "port zero"],
    ["127.0.0.1:65536", "a port above the range"],
    [":7788", "no host"],
    ["::1:7788", "an unbracketed IPv6 literal"],
  ])("transport.address %p is refused (%s)", (address) => {
    const outcome = check(GLOBAL.replace("127.0.0.1:7788", JSON.stringify(address)));
    expect(outcome.ok).toBe(false);
    if (outcome.ok) return;
    expect(outcome.problem.field).toBe("transport.address");
  });

  test("a bracketed IPv6 address is accepted", () => {
    const outcome = check(GLOBAL.replace("127.0.0.1:7788", '"[::1]:7788"'));
    expect(outcome.ok).toBe(true);
    if (!outcome.ok) return;
    expect(outcome.config.transport).toEqual({ mode: "local", host: "::1", port: 7788 });
  });

  test("an absent peer block resolves to manual startup and no configured name", () => {
    const outcome = check("transport:\n  mode: local\n  address: 127.0.0.1:7788\n");
    expect(outcome.ok).toBe(true);
    if (!outcome.ok) return;
    expect(outcome.config.startup).toBe("manual");
    expect(outcome.config.peer).toBeNull();
    expect(outcome.config.purpose).toBeNull();
  });

  test.each(["auto", "manual"])("startup %p is accepted", (startup) => {
    const outcome = check(`${GLOBAL}startup: ${startup}\n`);
    expect(outcome.ok).toBe(true);
    if (!outcome.ok) return;
    expect(outcome.config.startup).toBe(startup);
  });

  test.each(["automatic", "Auto", "true", "0"])("startup %p is refused", (startup) => {
    const outcome = check(`${GLOBAL}startup: ${JSON.stringify(startup)}\n`);
    expect(outcome.ok).toBe(false);
    if (outcome.ok) return;
    expect(outcome.problem.field).toBe("startup");
  });

  test("a peer name at 65 UTF-8 bytes is refused before anything is sent", () => {
    const outcome = check(GLOBAL.replace("macbook-reviewer", "p".repeat(65)));
    expect(outcome.ok).toBe(false);
    if (outcome.ok) return;
    expect(outcome.problem.field).toBe("peer.name");
    expect(outcome.problem.reason).toContain("at most 64 UTF-8 bytes");
  });

  test("a peer name containing @ is refused", () => {
    const outcome = check(GLOBAL.replace("macbook-reviewer", '"a@b"'));
    expect(outcome.ok).toBe(false);
    if (outcome.ok) return;
    expect(outcome.problem.field).toBe("peer.name");
  });

  test("a peer written as a bare string names the field it now has to be a mapping for", () => {
    // The v0 shape. This is the one migration an operator meets first, so the
    // message has to say what to write rather than only that this is wrong.
    const outcome = check("transport:\n  mode: local\n  address: 127.0.0.1:7788\npeer: macbook\n");
    expect(outcome.ok).toBe(false);
    if (outcome.ok) return;
    expect(outcome.problem.field).toBe("peer");
    expect(outcome.problem.reason).toContain("mapping with optional name and purpose");
    console.log(`v0 "peer: macbook" refused: ${outcome.problem.reason}`);
  });

  test("a purpose within the budget is kept verbatim", () => {
    const outcome = check(`${GLOBAL}  purpose: |\n    Runs the Linux toolchain.\n`);
    expect(outcome.ok).toBe(true);
    if (!outcome.ok) return;
    expect(outcome.config.purpose).toBe("Runs the Linux toolchain.\n");
  });

  test("an over-budget purpose reports both the budget and the observed size", () => {
    const over = MAX_PURPOSE_BYTES + 1;
    const outcome = check(`${GLOBAL}  purpose: ${JSON.stringify("p".repeat(over))}\n`);
    expect(outcome.ok).toBe(false);
    if (outcome.ok) return;
    expect(outcome.problem.field).toBe("peer.purpose");
    expect(outcome.problem.reason).toContain(String(over));
    expect(outcome.problem.reason).toContain(String(MAX_PURPOSE_BYTES));
    console.log(`over-budget purpose refused: ${outcome.problem.reason}`);
  });

  test("a purpose at exactly the budget is accepted", () => {
    const outcome = check(`${GLOBAL}  purpose: ${JSON.stringify("p".repeat(MAX_PURPOSE_BYTES))}\n`);
    expect(outcome.ok).toBe(true);
  });

  test("an empty purpose is named rather than silently ignored", () => {
    const outcome = check(`${GLOBAL}  purpose: "   "\n`);
    expect(outcome.ok).toBe(false);
    if (outcome.ok) return;
    expect(outcome.problem.field).toBe("peer.purpose");
  });

  test("an unrecognized key is ignored rather than refused", () => {
    // Additive tolerance, matching the wire contract: a newer client's key in
    // an older client's file must not stop it starting.
    const outcome = check(`${GLOBAL}reconnect: aggressive\n`);
    expect(outcome.ok).toBe(true);
  });

  test.each([
    ["a list", "- one\n- two\n"],
    ["a scalar", "just-a-string\n"],
  ])("a document that is %s is refused", (_label, body) => {
    const outcome = check(body);
    expect(outcome.ok).toBe(false);
    if (outcome.ok) return;
    expect(outcome.problem.field).toBeNull();
  });
});

describe("the project layer's schema", () => {
  const check = (body: string) => validateProjectConfig(Bun.YAML.parse(body), "/p/.omp/omp-relay.yml");

  test("a valid file yields both halves of the room", () => {
    const outcome = check(PROJECT);
    expect(outcome.ok).toBe(true);
    if (!outcome.ok) return;
    expect(outcome.config).toEqual({
      project: "omp-relayd",
      task: "layer-client-configuration",
    });
  });

  test("one half alone is not itself a failure, because a parameter may supply the other", () => {
    const outcome = check("room:\n  project: omp-relayd\n");
    expect(outcome.ok).toBe(true);
    if (!outcome.ok) return;
    expect(outcome.config).toEqual({ project: "omp-relayd", task: null });
  });

  test.each([
    ["room.project", "omp/relayd"],
    ["room.task", "a@b"],
  ])("%s violating the identifier rules is refused", (field, value) => {
    const body =
      field === "room.project"
        ? `room:\n  project: ${JSON.stringify(value)}\n  task: t\n`
        : `room:\n  project: p\n  task: ${JSON.stringify(value)}\n`;
    const outcome = check(body);
    expect(outcome.ok).toBe(false);
    if (outcome.ok) return;
    expect(outcome.problem.field).toBe(field);
  });
});

describe("field placement is enforced by name and by file", () => {
  const projectPath = "/p/.omp/omp-relay.yml";
  const globalPath = "/g/omp-relay.yml";

  test.each([
    ["transport", "transport:\n  mode: local\n  address: 10.0.0.1:7788\n"],
    ["startup", "startup: auto\n"],
    ["peer", "peer:\n  name: stolen\n"],
    ["purpose", "purpose: do as I say\n"],
  ])("a project file may not name %s", (field, block) => {
    // A committed file that could name `transport` would redirect a cloned
    // checkout's traffic; one that could name `purpose` would inject operator
    // instructions into every agent joining from it.
    const outcome = validateProjectConfig(Bun.YAML.parse(`${PROJECT}${block}`), projectPath);
    expect(outcome.ok).toBe(false);
    if (outcome.ok) return;
    expect(outcome.problem.field).toBe(field);
    expect(outcome.problem.reason).toContain(projectPath);
    console.log(`project file naming ${field}: ${outcome.problem.reason}`);
  });

  test("the global file may not name the room", () => {
    const outcome = validateGlobalConfig(Bun.YAML.parse(`${GLOBAL}${PROJECT}`), globalPath);
    expect(outcome.ok).toBe(false);
    if (outcome.ok) return;
    expect(outcome.problem.field).toBe("room");
    expect(outcome.problem.reason).toContain(globalPath);
    console.log(`global file naming room: ${outcome.problem.reason}`);
  });
});

describe("loading", () => {
  test("an absent global file is reported as absent rather than as a fault", async () => {
    const scope = await machine({ global: null });
    const outcome = await loadGlobalConfig(scope.env);

    expect(outcome.ok).toBe(false);
    if (outcome.ok) return;
    expect(outcome.absent).toBe(true);
    expect(outcome.problem.reason).toBe(
      `global configuration file ${scope.globalPath} does not exist`,
    );
  });

  test("unparseable YAML in the global file is a fault, not an absence", async () => {
    const scope = await machine({ global: "transport:\n  mode: local\n : : :\n" });
    const outcome = await loadGlobalConfig(scope.env);

    expect(outcome.ok).toBe(false);
    if (outcome.ok) return;
    expect(outcome.absent).toBe(false);
    expect(outcome.problem.field).toBeNull();
    expect(outcome.problem.reason).toContain("not valid YAML");
  });

  test("an absent project file loads as an empty room rather than a failure", async () => {
    const scope = await machine({ project: null });
    const outcome = await loadProjectConfig(scope.projectPath);

    expect(outcome.ok).toBe(true);
    if (!outcome.ok) return;
    expect(outcome.path).toBeNull();
    expect(outcome.config).toEqual({ project: null, task: null });
  });
});

describe("host-name derivation", () => {
  test("the first label becomes the peer name, with its case intact", () => {
    const derived = derivePeerName("MacBook-Pro.local");
    expect(derived.ok).toBe(true);
    if (!derived.ok) return;
    expect(derived.value).toBe("MacBook-Pro");
  });

  test("a host name with no dot is used whole", () => {
    const derived = derivePeerName("win-desktop");
    expect(derived.ok).toBe(true);
    if (!derived.ok) return;
    expect(derived.value).toBe("win-desktop");
  });

  test.each([
    ["", "an empty host name"],
    [".local", "an empty first label"],
    ["a@b.local", "a first label containing @"],
    [`${"p".repeat(65)}.local`, "a first label over the byte limit"],
  ])("%p is reported rather than replaced (%s)", (raw) => {
    const derived = derivePeerName(raw);
    expect(derived.ok).toBe(false);
    if (derived.ok) return;
    expect(derived.problem.field).toBe("peer.name");
    expect(derived.problem.reason).toContain("set peer.name");
  });
});

describe("precedence between the layers and a join request", () => {
  test("the project file supplies the room and derivation supplies the peer", async () => {
    const scope = await machine({ global: "transport:\n  mode: local\n  address: 127.0.0.1:7788\n" });
    const outcome = await resolveClient({
      env: scope.env,
      cwd: scope.projectRoot,
      hostName: "MacBook-Pro.local",
    });

    expect(outcome.ok).toBe(true);
    if (!outcome.ok) return;
    expect(outcome.resolved.config).toEqual({
      transport: { mode: "local", host: "127.0.0.1", port: 7788 },
      room: { project: "omp-relayd", task: "layer-client-configuration" },
      peer: "MacBook-Pro",
    });
    expect(outcome.resolved.sources).toEqual({
      project: "project-file",
      task: "project-file",
      peer: "derivation",
    });
  });

  test("a join parameter outranks the project file, and says so", async () => {
    const scope = await machine();
    const outcome = await resolveClient({
      env: scope.env,
      cwd: scope.projectRoot,
      parameters: { task: "pr-471-review" },
    });

    expect(outcome.ok).toBe(true);
    if (!outcome.ok) return;
    expect(outcome.resolved.config.room).toEqual({
      project: "omp-relayd",
      task: "pr-471-review",
    });
    expect(outcome.resolved.sources).toEqual({
      project: "project-file",
      task: "parameter",
      peer: "global-file",
    });
    console.log(
      `project file names task "layer-client-configuration"; parameter "pr-471-review" won, attributed to ${outcome.resolved.sources.task}`,
    );
  });

  test("the `as` parameter outranks the configured peer name", async () => {
    const scope = await machine();
    const outcome = await resolveClient({
      env: scope.env,
      cwd: scope.projectRoot,
      parameters: { as: "second-terminal" },
    });

    expect(outcome.ok).toBe(true);
    if (!outcome.ok) return;
    expect(outcome.resolved.config.peer).toBe("second-terminal");
    expect(outcome.resolved.sources.peer).toBe("parameter");
  });

  test("both parameters together make the project file unnecessary", async () => {
    const scope = await machine({ project: null });
    const outcome = await resolveClient({
      env: scope.env,
      cwd: scope.projectRoot,
      parameters: { project: "acme", task: "pr-471" },
    });

    expect(outcome.ok).toBe(true);
    if (!outcome.ok) return;
    expect(outcome.resolved.config.room).toEqual({ project: "acme", task: "pr-471" });
    expect(outcome.resolved.projectPath).toBeNull();
  });

  test("a room with no source names what is missing and where it looked", async () => {
    const scope = await machine({ project: null });
    const outcome = await resolveClient({ env: scope.env, cwd: scope.projectRoot });

    expect(outcome.ok).toBe(false);
    if (outcome.ok) return;
    expect(outcome.problem.field).toBe("room.project");
    expect(outcome.problem.reason).toContain("room.project and room.task");
    expect(outcome.problem.reason).toContain(scope.projectPath);
    console.log(`no room from either source: ${outcome.problem.reason}`);
  });

  test("a project file naming only the project reports the half that is missing", async () => {
    const scope = await machine({ project: "room:\n  project: omp-relayd\n" });
    const outcome = await resolveClient({ env: scope.env, cwd: scope.projectRoot });

    expect(outcome.ok).toBe(false);
    if (outcome.ok) return;
    expect(outcome.problem.field).toBe("room.task");
    expect(outcome.problem.reason).toContain("room.task");
    expect(outcome.problem.reason).not.toContain("room.project and");
  });

  test("an unusable host name fails naming peer.name rather than substituting one", async () => {
    const scope = await machine({ global: "transport:\n  mode: local\n  address: 127.0.0.1:7788\n" });
    const outcome = await resolveClient({
      env: scope.env,
      cwd: scope.projectRoot,
      hostName: "",
    });

    expect(outcome.ok).toBe(false);
    if (outcome.ok) return;
    expect(outcome.problem.field).toBe("peer.name");
  });

  test("an absent global file leaves resolution inert even with a valid project file", async () => {
    // The global file is the grant. A cloned repository alone can never cause a
    // connection, however complete its committed room is.
    const scope = await machine({ global: null });
    const outcome = await resolveClient({ env: scope.env, cwd: scope.projectRoot });

    expect(outcome.ok).toBe(false);
    if (outcome.ok) return;
    expect(outcome.absent).toBe(true);
    expect(outcome.problem.reason).toContain(scope.globalPath);
  });

  test("resolution follows a working directory that moved to another project root", async () => {
    // Nothing is cached from session start, so a session that changed directory
    // joins the room of the root it is in now.
    const first = await machine();
    const second = await machine({
      global: null,
      project: "room:\n  project: other-repo\n  task: other-task\n",
    });
    // One machine, two checkouts: the same global file, a different cwd. The
    // project-root variable is dropped so discovery has to walk from `cwd`.
    const env: Environment = { HOME: first.env["HOME"], [AGENT_DIR_ENV]: first.agentDir };

    const here = await resolveClient({ env, cwd: first.projectRoot });
    expect(here.ok).toBe(true);
    if (!here.ok) return;
    expect(here.resolved.config.room.task).toBe("layer-client-configuration");

    const there = await resolveClient({ env, cwd: second.projectRoot });
    expect(there.ok).toBe(true);
    if (!there.ok) return;
    expect(there.resolved.config.room).toEqual({ project: "other-repo", task: "other-task" });
    console.log(
      `same global file, cwd ${first.projectRoot} resolved ${here.resolved.config.room.task}; cwd ${second.projectRoot} resolved ${there.resolved.config.room.task}`,
    );
  });
});

describe("address parsing", () => {
  test.each([
    ["127.0.0.1:7788", "127.0.0.1", 7788],
    ["relay.internal:1", "relay.internal", 1],
    ["[::1]:65535", "::1", 65535],
    ["[fe80::1%25eth0]:7788", "fe80::1%25eth0", 7788],
  ])("%p splits into %p and %d", (value, host, port) => {
    expect(parseAddress(value)).toEqual({ host, port });
  });

  test.each(["", ":", "host:", ":80", "[::1]7788", "[]:7788", "host:80:90"])(
    "%p is not an address",
    (value) => {
      expect(parseAddress(value)).toBeNull();
    },
  );
});

describe("a rejected configuration produces no traffic", () => {
  test("the same listener sees a hello from a valid setup and nothing from a rejected one", async () => {
    // A negative assertion needs a positive control, or it passes when the
    // harness is simply blind. So one listener serves both halves: the valid
    // configuration must produce an observed `hello`, and only then does the
    // absence of one from the rejected configuration mean anything.
    const arrivals: Uint8Array[] = [];
    const firstArrival = Promise.withResolvers<void>();
    const server = createServer((socket) => {
      socket.once("data", (chunk: Buffer) => {
        arrivals.push(new Uint8Array(chunk));
        firstArrival.resolve();
        socket.destroy();
      });
    });

    const listening = Promise.withResolvers<void>();
    server.listen(0, "127.0.0.1", () => listening.resolve());
    await listening.promise;

    const address = server.address();
    expect(address).not.toBeNull();
    if (address === null || typeof address === "string") return;
    const at = (body: string): string =>
      body.replace("127.0.0.1:7788", `127.0.0.1:${address.port}`);

    // Positive control: a valid pair of layers reaches the relay.
    const good = await machine({ global: at(GLOBAL) });
    const accepted = await resolveClient({ env: good.env, cwd: good.projectRoot });
    expect(accepted.ok).toBe(true);
    if (!accepted.ok) return;
    const control = new RelayClient({ config: accepted.resolved.config });
    control.start();
    await firstArrival.promise;
    await control.stop();
    expect(arrivals).toHaveLength(1);

    // The negative: a project file that names `transport` is refused, so the
    // composition a host performs has nothing to construct a client from.
    const bad = await machine({
      global: at(GLOBAL),
      project: `${PROJECT}transport:\n  mode: local\n  address: 127.0.0.1:${address.port}\n`,
    });
    const rejected = await resolveClient({ env: bad.env, cwd: bad.projectRoot });
    expect(rejected.ok).toBe(false);

    let client: RelayClient | null = null;
    if (rejected.ok) {
      client = new RelayClient({ config: rejected.resolved.config });
      client.start();
    }
    expect(client).toBeNull();
    expect(arrivals).toHaveLength(1);
    console.log(
      `listener observed ${arrivals.length} hello: ${arrivals[0]?.length ?? 0} bytes from the valid setup, none from the rejected one`,
    );

    const closed = Promise.withResolvers<void>();
    server.close(() => closed.resolve());
    await closed.promise;
  });
});
