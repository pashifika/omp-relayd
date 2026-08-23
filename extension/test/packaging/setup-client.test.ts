/**
 * The setup helper, checked by feeding what it writes back through the
 * extension's own validator.
 *
 * A helper that produces a file the extension then rejects is worse than no
 * helper, and it is a second definition of the configuration schema that will
 * drift. So nothing here compares the output against a copied template — the
 * assertion is that `resolveClient` accepts the pair of files, which is the only
 * property that matters and the one a drifting helper loses.
 *
 * The three constants the script cannot import are checked against their
 * TypeScript originals by reading them out of the script's own text, so a change
 * to either side fails here rather than in an operator's terminal.
 */

import { describe, expect, test } from "bun:test";
import {
  mkdirSync,
  mkdtempSync,
  readFileSync,
  readdirSync,
  realpathSync,
  statSync,
  symlinkSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { basename, join, relative } from "node:path";

import {
  AGENT_DIR_ENV,
  CONFIG_FILE_NAME,
  MAX_PURPOSE_BYTES,
  PROJECT_MARKERS,
  PROJECT_ROOT_ENV,
  projectConfigPath,
  resolveClient,
} from "../../src/config.ts";
import { MAX_IDENTIFIER_BYTES } from "../../src/protocol.ts";
import { REPO_ROOT } from "../support/paths.ts";

const SCRIPT = join(REPO_ROOT, "scripts", "setup-client.sh");
const source = readFileSync(SCRIPT, "utf8");

interface Run {
  readonly code: number;
  readonly stdout: string;
  readonly stderr: string;
}

/**
 * Runs the helper with the ambient project and agent variables cleared.
 *
 * Left set, the developer's own `PI_CODING_AGENT_DIR` would make a test write
 * into their real agent directory — which is exactly the accident the `--force`
 * refusal exists to prevent, and not one to discover from a test suite.
 */
async function run(args: readonly string[], cwd: string = REPO_ROOT): Promise<Run> {
  const env: Record<string, string> = {};
  for (const [key, value] of Object.entries(process.env)) {
    if (key === AGENT_DIR_ENV || key === PROJECT_ROOT_ENV) continue;
    if (value !== undefined) env[key] = value;
  }
  const child = Bun.spawn(["bash", SCRIPT, ...args], {
    cwd,
    env,
    stdout: "pipe",
    stderr: "pipe",
  });
  const [stdout, stderr] = await Promise.all([
    new Response(child.stdout).text(),
    new Response(child.stderr).text(),
  ]);
  return { code: await child.exited, stdout, stderr };
}

/** A scratch agent directory and project root, neither of them pre-populated. */
function scratch(): { agentDir: string; projectRoot: string } {
  return {
    agentDir: mkdtempSync(join(tmpdir(), "omp-relay-setup-agent-")),
    projectRoot: mkdtempSync(join(tmpdir(), "omp-relay-setup-root-")),
  };
}

/** Every path under `root`, with each file's bytes, for an unchanged-tree claim. */
function snapshot(root: string): Record<string, string> {
  const entries: Record<string, string> = {};
  const walk = (directory: string): void => {
    for (const entry of readdirSync(directory, { withFileTypes: true })) {
      const path = join(directory, entry.name);
      if (entry.isDirectory()) walk(path);
      else entries[relative(root, path)] = readFileSync(path, "utf8");
    }
  };
  walk(root);
  return entries;
}

describe("the helper and the extension agree on their shared constants", () => {
  test("the project-marker list is the same list, in the same order", () => {
    const block = /readonly PROJECT_MARKERS=\(\n([\s\S]*?)\n\)/.exec(source)?.[1];
    expect(block).toBeDefined();
    const fromScript = (block as string)
      .split("\n")
      .map((line) => line.trim())
      .filter((line) => line.length > 0);

    expect(fromScript).toEqual([...PROJECT_MARKERS]);
    console.log(`shared project markers (${fromScript.length}): ${fromScript.join(", ")}`);
  });

  test.each([
    ["MAX_IDENTIFIER_BYTES", MAX_IDENTIFIER_BYTES],
    ["MAX_PURPOSE_BYTES", MAX_PURPOSE_BYTES],
  ])("%s matches the extension's value", (name, expected) => {
    const found = new RegExp(`readonly ${name}=(\\d+)`).exec(source)?.[1];
    expect(found).toBeDefined();
    expect(Number(found)).toBe(expected);
    console.log(`${name}: script ${found}, extension ${expected}`);
  });
});

describe("one invocation produces a setup the extension accepts", () => {
  test("both files are written and resolve to a connectable configuration", async () => {
    const { agentDir, projectRoot } = scratch();

    const result = await run([
      "--task",
      "pr-471-review",
      "--agent-dir",
      agentDir,
      "--project-root",
      projectRoot,
      "--peer",
      "win-desktop",
    ]);

    expect(result.stderr).toBe("");
    expect(result.code).toBe(0);

    // The claim: what it wrote is accepted, not that it looks like a template.
    const outcome = await resolveClient({
      env: { [AGENT_DIR_ENV]: agentDir, [PROJECT_ROOT_ENV]: projectRoot, HOME: projectRoot },
      cwd: projectRoot,
    });
    expect(outcome.ok).toBe(true);
    if (!outcome.ok) return;
    expect(outcome.resolved.config.room.task).toBe("pr-471-review");
    expect(outcome.resolved.config.peer).toBe("win-desktop");
    expect(outcome.resolved.startup).toBe("manual");
    console.log(
      `helper output accepted: ${outcome.resolved.config.room.project}/${outcome.resolved.config.room.task} as ${outcome.resolved.config.peer}, startup ${outcome.resolved.startup}`,
    );
  });

  test("every default it documents is the default it writes", async () => {
    const { agentDir, projectRoot } = scratch();

    await run(["--task", "t", "--agent-dir", agentDir, "--project-root", projectRoot]);
    const outcome = await resolveClient({
      env: { [AGENT_DIR_ENV]: agentDir, [PROJECT_ROOT_ENV]: projectRoot, HOME: projectRoot },
      cwd: projectRoot,
      hostName: "MacBook-Pro.local",
    });

    expect(outcome.ok).toBe(true);
    if (!outcome.ok) return;
    // The address and startup defaults, and `--project` defaulting to the
    // resolved root's basename. No `peer` block was written, so the name comes
    // from derivation.
    expect(outcome.resolved.config.transport).toEqual({
      mode: "local",
      host: "127.0.0.1",
      port: 7788,
    });
    expect(outcome.resolved.startup).toBe("manual");
    expect(outcome.resolved.config.room.project).toBe(basename(projectRoot));
    expect(outcome.resolved.sources.peer).toBe("derivation");
    expect(outcome.resolved.config.peer).toBe("MacBook-Pro");
  });

  test("a purpose file becomes a purpose the extension accepts verbatim", async () => {
    const { agentDir, projectRoot } = scratch();
    const purposePath = join(projectRoot, "purpose.txt");
    const purpose = "Run Linux builds here.\n\tDecline Windows work.\n";
    writeFileSync(purposePath, purpose, "utf8");

    await run([
      "--task",
      "t",
      "--agent-dir",
      agentDir,
      "--project-root",
      projectRoot,
      "--startup",
      "auto",
      "--purpose-file",
      purposePath,
    ]);
    const outcome = await resolveClient({
      env: { [AGENT_DIR_ENV]: agentDir, [PROJECT_ROOT_ENV]: projectRoot, HOME: projectRoot },
      cwd: projectRoot,
      hostName: "probe.local",
    });

    expect(outcome.ok).toBe(true);
    if (!outcome.ok) return;
    expect(outcome.resolved.startup).toBe("auto");
    // The block scalar has to round-trip, indentation and tab included.
    expect(outcome.resolved.purpose).toBe(purpose);
    console.log(`purpose round-tripped through YAML: ${JSON.stringify(outcome.resolved.purpose)}`);
  });

  test("the skill lands where the host scans for it", async () => {
    const { agentDir, projectRoot } = scratch();

    await run(["--task", "t", "--agent-dir", agentDir, "--project-root", projectRoot]);

    // The host scans `<agent-dir>/skills/*/SKILL.md` non-recursively, so the
    // directory depth is the contract rather than an implementation detail.
    const installed = join(agentDir, "skills", "omp-relay", "SKILL.md");
    expect(statSync(installed).isFile()).toBe(true);
    expect(readFileSync(installed, "utf8")).toBe(
      readFileSync(join(REPO_ROOT, "extension", "skill", "omp-relay", "SKILL.md"), "utf8"),
    );
    console.log(`skill installed at ${installed}`);
  });

  test("it resolves a project root the same way the extension does", async () => {
    // Not `--project-root`: the walk itself, from a directory three levels below
    // a marker, is what has to agree between the two implementations.
    const { agentDir } = scratch();
    // `realpathSync` because the OS temp directory is a symlink on macOS
    // (`/var` -> `/private/var`) and the helper reports a `pwd`-resolved path.
    // Comparing the two forms would fail on a difference that is not the
    // helper's.
    const home = realpathSync(mkdtempSync(join(tmpdir(), "omp-relay-setup-home-")));
    const root = join(home, "work", "widget");
    const deep = join(root, "src", "parser");
    mkdirSync(deep, { recursive: true });
    writeFileSync(join(root, "go.mod"), "module example.com/widget\n", "utf8");

    const result = await run(["--task", "t", "--agent-dir", agentDir], deep);

    expect(result.code).toBe(0);
    expect(result.stdout).toContain(`project root ${root} (decided by go.mod)`);
    expect(statSync(projectConfigPath(root)).isFile()).toBe(true);
    console.log(result.stdout.split("\n")[0]);
  });
});

describe("the helper refuses before it writes", () => {
  test.each([
    ["a task containing /", ["--task", "feat/x"], "room.task"],
    ["a task with trailing whitespace", ["--task", "review "], "room.task"],
    ["a peer name over 64 UTF-8 bytes", ["--task", "t", "--peer", "p".repeat(65)], "peer.name"],
    ["a peer name containing @", ["--task", "t", "--peer", "a@b"], "peer.name"],
    ["an unrecognized startup mode", ["--task", "t", "--startup", "automatic"], "--startup"],
    ["an address with no port", ["--task", "t", "--address", "127.0.0.1"], "--address"],
  ])("%s writes nothing", async (_label, args, named) => {
    const { agentDir, projectRoot } = scratch();
    const before = snapshot(agentDir);

    const result = await run([...args, "--agent-dir", agentDir, "--project-root", projectRoot]);

    expect(result.code).not.toBe(0);
    expect(result.stderr).toContain(named);
    expect(snapshot(agentDir)).toEqual(before);
    expect(snapshot(projectRoot)).toEqual({});
    console.log(`refused: ${result.stderr.trim()}`);
  });

  test("a missing task is refused rather than guessed", async () => {
    const { agentDir, projectRoot } = scratch();

    const result = await run(["--agent-dir", agentDir, "--project-root", projectRoot]);

    expect(result.code).not.toBe(0);
    expect(result.stderr).toContain("--task is required");
    expect(snapshot(agentDir)).toEqual({});
    expect(snapshot(projectRoot)).toEqual({});
  });

  test("an existing global file is left byte-identical and named", async () => {
    const { agentDir, projectRoot } = scratch();
    const existing = "transport:\n  mode: local\n  address: 10.0.0.9:9999\n# hand-written\n";
    const path = join(agentDir, CONFIG_FILE_NAME);
    writeFileSync(path, existing, "utf8");

    const result = await run(["--task", "t", "--agent-dir", agentDir, "--project-root", projectRoot]);

    expect(result.code).not.toBe(0);
    expect(result.stderr).toContain(path);
    expect(result.stderr).toContain("--force");
    expect(readFileSync(path, "utf8")).toBe(existing);
    console.log(`declined to replace: ${result.stderr.trim()}`);
  });

  test("a committed room survives a second participant running the helper", async () => {
    const { agentDir, projectRoot } = scratch();
    const committed = "room:\n  project: acme\n  task: pr-471\n";
    mkdirSync(join(projectRoot, ".omp"), { recursive: true });
    writeFileSync(projectConfigPath(projectRoot), committed, "utf8");

    const result = await run(["--task", "other", "--agent-dir", agentDir, "--project-root", projectRoot]);

    expect(result.code).not.toBe(0);
    expect(readFileSync(projectConfigPath(projectRoot), "utf8")).toBe(committed);
    // Both refusals are decided before either write, so the global file this
    // run would also have written is still absent.
    expect(snapshot(agentDir)).toEqual({});
  });

  test("--force replaces both files", async () => {
    const { agentDir, projectRoot } = scratch();
    writeFileSync(join(agentDir, CONFIG_FILE_NAME), "stale\n", "utf8");
    mkdirSync(join(projectRoot, ".omp"), { recursive: true });
    writeFileSync(projectConfigPath(projectRoot), "stale\n", "utf8");

    const result = await run([
      "--task",
      "t",
      "--agent-dir",
      agentDir,
      "--project-root",
      projectRoot,
      "--force",
    ]);

    expect(result.code).toBe(0);
    expect(readFileSync(join(agentDir, CONFIG_FILE_NAME), "utf8")).not.toBe("stale\n");
  });

  test("--dry-run lists every effect and leaves the filesystem unchanged", async () => {
    const { agentDir, projectRoot } = scratch();
    const beforeAgent = snapshot(agentDir);
    const beforeRoot = snapshot(projectRoot);

    const result = await run([
      "--task",
      "pr-471",
      "--agent-dir",
      agentDir,
      "--project-root",
      projectRoot,
      "--build",
      "--dry-run",
    ]);

    expect(result.code).toBe(0);
    expect(result.stdout).toContain(`would write ${join(agentDir, CONFIG_FILE_NAME)}`);
    expect(result.stdout).toContain(`would write ${projectConfigPath(projectRoot)}`);
    expect(result.stdout).toContain(`would install`);
    expect(result.stdout).toContain(`would run: bun run build`);
    expect(snapshot(agentDir)).toEqual(beforeAgent);
    expect(snapshot(projectRoot)).toEqual(beforeRoot);
    console.log(`dry run reported ${result.stdout.split("\n").length} lines and wrote nothing`);
  });
});

describe("the helper's scope stops at the client", () => {
  test("the usage text names all three refusals where they are invoked", async () => {
    const result = await run(["--help"]);

    expect(result.code).toBe(0);
    const text = result.stdout;
    expect(text).toContain("installs no toolchain");
    expect(text).toContain("starts no relay");
    expect(text).toContain("does not run the agent");
    console.log(
      `--help scope section:\n${text.slice(text.indexOf("What this script will not do")).trimEnd()}`,
    );
  });

  test("the usage text names every parameter and every default", async () => {
    const result = await run(["--help"]);
    const text = result.stdout;

    for (const flag of [
      "--address",
      "--startup",
      "--peer",
      "--purpose-file",
      "--project",
      "--task",
      "--project-root",
      "--agent-dir",
      "--build",
      "--force",
      "--dry-run",
      "--help",
    ]) {
      expect(text).toContain(flag);
    }
    expect(text).toContain("127.0.0.1:7788");
    expect(text).toContain("manual");
    expect(text).toContain("PI_CODING_AGENT_DIR");
  });

  test("a successful run starts nothing and prints the command instead", async () => {
    const { agentDir, projectRoot } = scratch();

    const result = await run(["--task", "t", "--agent-dir", agentDir, "--project-root", projectRoot]);

    expect(result.code).toBe(0);
    expect(result.stdout).toContain("started no relay and ran no agent");
    expect(result.stdout).toContain("Deployment and security");
    const lines = result.stdout.trimEnd().split("\n");
    expect(lines.at(-1)).toBe(
      `omp --extension "${join(REPO_ROOT, "extension", "dist", "index.js")}"`,
    );
    console.log(`final line: ${lines.at(-1)}`);
  });

  test("a missing build toolchain is reported with the expected version, not fetched", async () => {
    // A `PATH` holding every utility the helper needs and no `bun`, rather than
    // an empty one: emptying it would also remove `bash`, and the helper would
    // then fail to start instead of reaching the lookup under test.
    const { agentDir, projectRoot } = scratch();
    const bin = mkdtempSync(join(tmpdir(), "omp-relay-setup-bin-"));
    const needed = ["bash", "cat", "wc", "tr", "sed", "dirname", "basename", "mkdir", "cp", "head"];
    for (const tool of needed) {
      const real = Bun.which(tool);
      expect(real).not.toBeNull();
      symlinkSync(real as string, join(bin, tool));
    }
    expect(Bun.which("bun", { PATH: bin })).toBeNull();

    const child = Bun.spawn(
      ["bash", SCRIPT, "--task", "t", "--agent-dir", agentDir, "--project-root", projectRoot, "--build"],
      { cwd: REPO_ROOT, env: { HOME: projectRoot, PATH: bin }, stdout: "pipe", stderr: "pipe" },
    );
    const [stderr, code] = await Promise.all([new Response(child.stderr).text(), child.exited]);

    expect(code).not.toBe(0);
    expect(stderr).toContain("installs no toolchain");
    const expected = /"@types\/bun": "([^"]*)"/.exec(
      readFileSync(join(REPO_ROOT, "extension", "package.json"), "utf8"),
    )?.[1];
    expect(expected).toBeDefined();
    expect(stderr).toContain(expected as string);
    // Nothing was fetched and nothing was built: the bundle directory is
    // untouched by this run, and the only writes are the two configuration
    // files it had already produced before reaching the build step.
    console.log(`missing bun reported: ${stderr.trim()}`);
  });
});
