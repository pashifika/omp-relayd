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
  existsSync,
  lstatSync,
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
import { basename, dirname, join, relative } from "node:path";

import type { ResolvedClient } from "../../src/config.ts";
import {
  AGENT_DIR_ENV,
  CONFIG_FILE_NAME,
  MAX_PURPOSE_BYTES,
  PROJECT_MARKERS,
  PROJECT_ROOT_ENV,
  parseAddress,
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

/**
 * Every value below is an identifier the rules accept, and every one of them
 * also means something to YAML: `#` opens a comment, `: ` ends a key, a leading
 * `!` is a tag, and a newline ends the scalar and starts whatever the text says
 * next. Nothing here asserts on the bytes of the file — the assertion is that
 * `resolveClient` hands back the value that went in, because a file the
 * extension misreads is the helper's defect and not the operator's.
 */
describe("a value YAML would reinterpret is written so YAML does not", () => {
  async function resolveWhatItWrote(args: readonly string[]): Promise<ResolvedClient> {
    const { agentDir, projectRoot } = scratch();
    const result = await run([...args, "--agent-dir", agentDir, "--project-root", projectRoot]);
    expect(result.stderr).toBe("");
    expect(result.code).toBe(0);

    const outcome = await resolveClient({
      env: { [AGENT_DIR_ENV]: agentDir, [PROJECT_ROOT_ENV]: projectRoot, HOME: projectRoot },
      cwd: projectRoot,
      hostName: "probe.local",
    });
    if (!outcome.ok) {
      throw new Error(
        `the helper reported success and the extension rejected what it wrote: ${outcome.problem.field ?? "file"}: ${outcome.problem.reason}`,
      );
    }
    return outcome.resolved;
  }

  test.each([
    ["a leading # is part of the task, not a comment", "#471"],
    ["a colon and space is part of the task, not a mapping", "triage: parser errors"],
    ["a leading ! is part of the task, not a tag", "!urgent"],
    ["a quote and a backslash stay literal", 'say "hi" \\ then go'],
  ])("%s", async (_label, task) => {
    const resolved = await resolveWhatItWrote(["--task", task]);

    expect(resolved.config.room.task).toBe(task);
    console.log(`task ${JSON.stringify(task)} resolved as ${JSON.stringify(resolved.config.room.task)}`);
  });

  test("a # in a peer name does not truncate it", async () => {
    const resolved = await resolveWhatItWrote(["--task", "t", "--peer", "desk #2"]);

    expect(resolved.config.peer).toBe("desk #2");
    expect(resolved.sources.peer).toBe("global-file");
  });

  test("a newline in a task stays in the task and composes no second field", async () => {
    // The identifier rules permit an inner newline, so this is a value the
    // helper has to carry rather than one it may refuse. Unquoted it ends the
    // scalar, and a `peer` block is a placement the project file may not hold
    // at all — the room would be lost to an error about a field the operator
    // never wrote.
    const task = "review\npeer:\n  name: repository-controlled";

    const resolved = await resolveWhatItWrote(["--task", task]);

    expect(resolved.config.room.task).toBe(task);
    expect(resolved.sources.peer).toBe("derivation");
    expect(resolved.config.peer).toBe("probe");
    console.log(`a newline stayed inside the task: ${JSON.stringify(resolved.config.room.task)}`);
  });

  test("the bracketed IPv6 form the README documents is the address that resolves", async () => {
    const resolved = await resolveWhatItWrote(["--task", "t", "--address", "[::1]:7788"]);

    expect(resolved.config.transport).toEqual({ mode: "local", host: "::1", port: 7788 });
  });
});

/**
 * The helper's grammar and its budget, checked against the extension's rather
 * than against hand-written expectations. `parseAddress` and
 * `MAX_PURPOSE_BYTES` are the authority; the only property worth asserting is
 * that both sides reach the same verdict on the same input, since the helper
 * refusing what the extension accepts is as much a defect as the reverse.
 */
describe("the helper's verdict is the extension's verdict", () => {
  test.each([
    "127.0.0.1:7788",
    "[::1]:7788",
    "localhost:1",
    "host:80:90",
    ":7788",
    "::1:7788",
    "127.0.0.1:0",
    "127.0.0.1:65536",
    "127.0.0.1",
  ])("--address %s", async (address) => {
    const { agentDir, projectRoot } = scratch();
    const parsed = parseAddress(address);

    const result = await run([
      "--task",
      "t",
      "--address",
      address,
      "--agent-dir",
      agentDir,
      "--project-root",
      projectRoot,
    ]);

    if (parsed === null) {
      expect(result.code).not.toBe(0);
      expect(result.stderr).toContain("--address");
      expect(snapshot(agentDir)).toEqual({});
      expect(snapshot(projectRoot)).toEqual({});
      console.log(`both refuse ${address}: ${result.stderr.trim()}`);
      return;
    }

    expect(result.code).toBe(0);
    const outcome = await resolveClient({
      env: { [AGENT_DIR_ENV]: agentDir, [PROJECT_ROOT_ENV]: projectRoot, HOME: projectRoot },
      cwd: projectRoot,
      hostName: "probe.local",
    });
    expect(outcome.ok).toBe(true);
    if (!outcome.ok) return;
    expect(outcome.resolved.config.transport).toEqual({
      mode: "local",
      host: parsed.host,
      port: parsed.port,
    });
    console.log(`both accept ${address} as host ${parsed.host} port ${parsed.port}`);
  });

  test.each([MAX_PURPOSE_BYTES - 1, MAX_PURPOSE_BYTES, MAX_PURPOSE_BYTES + 1])(
    "a %d-byte purpose file with no final newline",
    async (size) => {
      // What the extension measures is the scalar YAML parses, and the literal
      // block puts back exactly one of the trailing newlines the helper's
      // `cat` strips. So a file of N bytes becomes N+1 bytes of purpose, and
      // the largest file that fits the budget is one byte below it.
      const accepted = size + 1 <= MAX_PURPOSE_BYTES;
      const { agentDir, projectRoot } = scratch();
      const purposePath = join(projectRoot, "purpose.txt");
      writeFileSync(purposePath, "p".repeat(size), "utf8");

      const result = await run([
        "--task",
        "t",
        "--startup",
        "auto",
        "--purpose-file",
        purposePath,
        "--agent-dir",
        agentDir,
        "--project-root",
        projectRoot,
      ]);

      if (!accepted) {
        expect(result.code).not.toBe(0);
        expect(result.stderr).toContain("--purpose-file");
        expect(snapshot(agentDir)).toEqual({});
        expect(existsSync(projectConfigPath(projectRoot))).toBe(false);
        console.log(`refused a ${size}-byte purpose file: ${result.stderr.trim()}`);
        return;
      }

      expect(result.code).toBe(0);
      const outcome = await resolveClient({
        env: { [AGENT_DIR_ENV]: agentDir, [PROJECT_ROOT_ENV]: projectRoot, HOME: projectRoot },
        cwd: projectRoot,
        hostName: "probe.local",
      });
      expect(outcome.ok).toBe(true);
      if (!outcome.ok) return;
      const bytes = new TextEncoder().encode(outcome.resolved.purpose ?? "").length;
      expect(bytes).toBe(MAX_PURPOSE_BYTES);
      console.log(`a ${size}-byte file became a ${bytes}-byte purpose, exactly the budget`);
    },
  );
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

  // `[ -e ]` follows a link and cannot see a dangling one, while `mkdir -p` and
  // `>` both write through whatever a link points at. A repository can track
  // `.omp` or the project file as a symlink, so these are the cloned-checkout
  // cases where refusing before writing has to mean lstat.
  test("a symlinked project file is refused and its target is left alone", async () => {
    const { agentDir, projectRoot } = scratch();
    const victim = join(projectRoot, "victim.txt");
    writeFileSync(victim, "ORIGINAL USER DATA\n", "utf8");
    mkdirSync(join(projectRoot, ".omp"), { recursive: true });
    symlinkSync("../victim.txt", projectConfigPath(projectRoot));

    // With `--force`, which is the advice the overwrite refusal gives and the
    // one path that would otherwise truncate the target.
    const result = await run([
      "--task",
      "t",
      "--agent-dir",
      agentDir,
      "--project-root",
      projectRoot,
      "--force",
    ]);

    expect(result.code).not.toBe(0);
    expect(result.stderr).toContain(projectConfigPath(projectRoot));
    expect(result.stderr).toContain("symbolic link");
    expect(lstatSync(projectConfigPath(projectRoot)).isSymbolicLink()).toBe(true);
    expect(readFileSync(victim, "utf8")).toBe("ORIGINAL USER DATA\n");
    expect(snapshot(agentDir)).toEqual({});
    console.log(`refused: ${result.stderr.trim()}`);
  });

  test("a dangling project-file symlink is refused rather than created through", async () => {
    const { agentDir, projectRoot } = scratch();
    mkdirSync(join(projectRoot, ".omp"), { recursive: true });
    symlinkSync(join(projectRoot, "nowhere.yml"), projectConfigPath(projectRoot));

    const result = await run(["--task", "t", "--agent-dir", agentDir, "--project-root", projectRoot]);

    expect(result.code).not.toBe(0);
    expect(result.stderr).toContain("symbolic link");
    expect(existsSync(join(projectRoot, "nowhere.yml"))).toBe(false);
    expect(snapshot(agentDir)).toEqual({});
  });

  test("a symlinked .omp directory is refused rather than followed out of the checkout", async () => {
    const { agentDir, projectRoot } = scratch();
    const outside = mkdtempSync(join(tmpdir(), "omp-relay-setup-outside-"));
    symlinkSync(outside, join(projectRoot, ".omp"));

    const result = await run(["--task", "t", "--agent-dir", agentDir, "--project-root", projectRoot]);

    expect(result.code).not.toBe(0);
    expect(result.stderr).toContain(join(projectRoot, ".omp"));
    expect(snapshot(outside)).toEqual({});
    expect(snapshot(agentDir)).toEqual({});
    console.log(`refused before writing outside the checkout: ${result.stderr.trim()}`);
  });

  // `mkdir -p` and `>` discover a wrong type only when they reach it, and by
  // then the earlier writes have happened: with `<project-root>/.omp` a regular
  // file, the global file was already replaced when the project parent failed.
  // Every path the helper creates or writes has a knowable type beforehand, so
  // the wrong one is refused by name beside the symlink cases above.
  test.each([
    ["the agent directory", "agent", "", "file", "is not a directory"],
    ["the global file", "agent", CONFIG_FILE_NAME, "directory", "is not a regular file"],
    ["the skills directory", "agent", "skills", "file", "is not a directory"],
    ["the skill target", "agent", join("skills", "omp-relay"), "file", "is not a directory"],
    ["the project's .omp", "project", ".omp", "file", "is not a directory"],
    [
      "the project file",
      "project",
      join(".omp", CONFIG_FILE_NAME),
      "directory",
      "is not a regular file",
    ],
  ])("%s of the wrong type is refused with nothing written", async (_label, owner, rel, kind, wording) => {
    // The agent directory is nested one level down so that the case where it is
    // itself a regular file still has a directory to snapshot.
    const agentParent = mkdtempSync(join(tmpdir(), "omp-relay-setup-agent-"));
    const projectRoot = mkdtempSync(join(tmpdir(), "omp-relay-setup-root-"));
    const agentDir = join(agentParent, "agent");
    const blocked = join(owner === "agent" ? agentDir : projectRoot, rel);
    mkdirSync(dirname(blocked), { recursive: true });
    if (kind === "directory") mkdirSync(blocked);
    else writeFileSync(blocked, "not what this writes\n", "utf8");
    const beforeAgent = snapshot(agentParent);
    const beforeProject = snapshot(projectRoot);

    const result = await run(["--task", "t", "--agent-dir", agentDir, "--project-root", projectRoot]);

    expect(result.code).not.toBe(0);
    // Before the message, because the message is not the property: this is the
    // sequence where round 1 replaced the global file and then failed.
    expect(snapshot(agentParent)).toEqual(beforeAgent);
    expect(snapshot(projectRoot)).toEqual(beforeProject);
    expect(result.stderr).toContain(blocked);
    expect(result.stderr).toContain(wording);
    console.log(`refused: ${result.stderr.trim()}`);
  });
});

/**
 * The two-machine procedure runs the identical command from a checkout of the
 * same repository on both ends, so the second machine always meets the room the
 * first one committed. That file is the repository's own statement of the room:
 * the helper keeps it, says so, and goes on to install the machine-local half.
 * Refusing there leaves the second machine with no global file and no skill,
 * which is everything running the helper on it was for.
 *
 * It is kept whatever it names and however it is written. Which room a file
 * names is a question for the YAML parser the extension has and this shell
 * script does not: a hand-written `project: shared` and the quoted scalar the
 * helper composes are the same room, so any text comparison refuses the
 * ordinary committed file — the one a person wrote and committed, which is the
 * whole point of putting the room there. So the room comes from the kept file
 * and this run's --project and --task decide nothing, and the report says so.
 */
describe("a second checkout of the same repository can still set itself up", () => {
  const ROOM = ["--task", "two-machine-check", "--project", "shared"];

  /** The room machine B's extension resolves, with the committed file in place. */
  async function roomOn(agentDir: string, projectRoot: string): Promise<ResolvedClient> {
    const outcome = await resolveClient({
      env: { [AGENT_DIR_ENV]: agentDir, [PROJECT_ROOT_ENV]: projectRoot, HOME: projectRoot },
      cwd: projectRoot,
      hostName: "machine-b.local",
    });
    if (!outcome.ok) {
      throw new Error(
        `the helper set machine B up and the extension rejected the result: ${outcome.problem.field ?? "file"}: ${outcome.problem.reason}`,
      );
    }
    return outcome.resolved;
  }

  /** A project file committed by a person, rather than composed by the helper. */
  function commit(projectRoot: string, text: string): string {
    mkdirSync(join(projectRoot, ".omp"), { recursive: true });
    writeFileSync(projectConfigPath(projectRoot), text, "utf8");
    return text;
  }

  /** The lines of a report that speak about the project file. */
  function keptReport(stdout: string): string {
    return stdout
      .split("\n")
      .filter((line) => line.includes("kept ") || line.trimStart().startsWith("setup-client:   |"))
      .join("\n");
  }

  test("the committed room is kept and the machine-local half still lands", async () => {
    const { projectRoot } = scratch();
    const first = mkdtempSync(join(tmpdir(), "omp-relay-setup-agent-a-"));
    const second = mkdtempSync(join(tmpdir(), "omp-relay-setup-agent-b-"));
    const shared = [...ROOM, "--project-root", projectRoot];

    expect((await run([...shared, "--agent-dir", first])).code).toBe(0);
    const committed = readFileSync(projectConfigPath(projectRoot), "utf8");

    // The report has to name the outcome the real run will reach, or the mode
    // that exists to be read before acting says the wrong thing.
    const rehearsal = await run([...shared, "--agent-dir", second, "--dry-run"]);
    expect(rehearsal.code).toBe(0);
    expect(rehearsal.stdout).toContain(`would keep ${projectConfigPath(projectRoot)}`);
    expect(rehearsal.stdout).toContain("| room:");

    const machineB = await run([...shared, "--agent-dir", second]);

    expect(machineB.stderr).toBe("");
    expect(machineB.code).toBe(0);
    expect(readFileSync(projectConfigPath(projectRoot), "utf8")).toBe(committed);
    expect(machineB.stdout).toContain(`kept ${projectConfigPath(projectRoot)}`);
    expect(statSync(join(second, CONFIG_FILE_NAME)).isFile()).toBe(true);
    expect(statSync(join(second, "skills", "omp-relay", "SKILL.md")).isFile()).toBe(true);

    const resolved = await roomOn(second, projectRoot);
    expect(resolved.config.room.project).toBe("shared");
    expect(resolved.config.room.task).toBe("two-machine-check");
    expect(resolved.config.peer).toBe("machine-b");
    console.log(
      `machine B joined ${resolved.config.room.project}/${resolved.config.room.task} as ${resolved.config.peer}, keeping the committed file`,
    );
  });

  // None of these is the byte string the helper composes, and every one of them
  // names the room this run asked for. The first is decisive: the file is meant
  // to be committed by a person, a person writes `project: shared`, and the
  // helper always writes `project: "shared"`, so a comparing helper refuses the
  // normal case. The rest are what a colleague's checkout and editor do to it.
  test.each([
    ["plain unquoted scalars, as a person writes them", "room:\n  project: shared\n  task: two-machine-check\n"],
    [
      "CRLF line endings, as a Windows checkout commits them",
      "room:\r\n  project: shared\r\n  task: two-machine-check\r\n",
    ],
    ["a trailing blank line", 'room:\n  project: "shared"\n  task: "two-machine-check"\n\n'],
    ["leading indentation", "  room:\n    project: shared\n    task: two-machine-check\n"],
    ["an interior blank line", "room:\n\n  project: shared\n  task: two-machine-check\n"],
    [
      "a comment",
      "# the room for this pairing session; both machines read it\nroom:\n  project: shared\n  task: two-machine-check\n",
    ],
  ])("a committed file written with %s is kept and machine B set up around it", async (label, text) => {
    const { projectRoot } = scratch();
    const second = mkdtempSync(join(tmpdir(), "omp-relay-setup-agent-b-"));
    const committed = commit(projectRoot, text);

    const machineB = await run([...ROOM, "--project-root", projectRoot, "--agent-dir", second]);

    expect(machineB.stderr).toBe("");
    expect(machineB.code).toBe(0);
    expect(machineB.stdout).toContain(`kept ${projectConfigPath(projectRoot)}`);
    // Kept means untouched, down to the line endings it arrived with.
    expect(readFileSync(projectConfigPath(projectRoot), "utf8")).toBe(committed);
    expect(statSync(join(second, CONFIG_FILE_NAME)).isFile()).toBe(true);
    expect(statSync(join(second, "skills", "omp-relay", "SKILL.md")).isFile()).toBe(true);

    // The kept file is what decides the room, so the extension reading it is
    // the only thing that settles whether keeping it was right.
    const resolved = await roomOn(second, projectRoot);
    expect(resolved.config.room.project).toBe("shared");
    expect(resolved.config.room.task).toBe("two-machine-check");
    console.log(`machine B kept a file with ${label} and still installed its own half`);
  });

  test("a committed room the flags disagree with is kept, and the report names the room that applies", async () => {
    const { projectRoot } = scratch();
    const second = mkdtempSync(join(tmpdir(), "omp-relay-setup-agent-b-"));
    const committed = commit(projectRoot, "room:\n  project: shared\n  task: someone-elses-topic\n");

    const machineB = await run([...ROOM, "--project-root", projectRoot, "--agent-dir", second]);

    expect(machineB.stderr).toBe("");
    expect(machineB.code).toBe(0);
    expect(readFileSync(projectConfigPath(projectRoot), "utf8")).toBe(committed);

    // An operator who passed --task two-machine-check and joined
    // someone-elses-topic has to see that from this output alone.
    expect(machineB.stdout).toContain(`kept ${projectConfigPath(projectRoot)}`);
    expect(machineB.stdout).toContain("someone-elses-topic");

    expect(statSync(join(second, CONFIG_FILE_NAME)).isFile()).toBe(true);
    expect(statSync(join(second, "skills", "omp-relay", "SKILL.md")).isFile()).toBe(true);

    const resolved = await roomOn(second, projectRoot);
    expect(resolved.config.room.project).toBe("shared");
    expect(resolved.config.room.task).toBe("someone-elses-topic");
    console.log(`asked for two-machine-check, told which room applies:\n${keptReport(machineB.stdout)}`);
  });

  test("--force replaces a committed room with the one this run names", async () => {
    const { projectRoot } = scratch();
    const second = mkdtempSync(join(tmpdir(), "omp-relay-setup-agent-b-"));
    const committed = commit(projectRoot, "room:\n  project: shared\n  task: someone-elses-topic\n");

    const forced = await run([
      ...ROOM,
      "--project-root",
      projectRoot,
      "--agent-dir",
      second,
      "--force",
    ]);

    expect(forced.stderr).toBe("");
    expect(forced.code).toBe(0);
    expect(forced.stdout).toContain(`wrote ${projectConfigPath(projectRoot)}`);
    expect(readFileSync(projectConfigPath(projectRoot), "utf8")).not.toBe(committed);

    const resolved = await roomOn(second, projectRoot);
    expect(resolved.config.room.task).toBe("two-machine-check");
    console.log(`--force moved the room to ${resolved.config.room.project}/${resolved.config.room.task}`);
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

  test("a missing build toolchain is reported with the expected version and installs nothing", async () => {
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
    const beforeAgent = snapshot(agentDir);
    const beforeRoot = snapshot(projectRoot);

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
    // "installs nothing" is a claim about the filesystem, not about the
    // message. Both trees are as they were and the skill destination was never
    // created, which only holds while the lookup precedes every write.
    expect(snapshot(agentDir)).toEqual(beforeAgent);
    expect(snapshot(projectRoot)).toEqual(beforeRoot);
    expect(existsSync(join(agentDir, "skills"))).toBe(false);
    console.log(`missing bun reported, nothing installed: ${stderr.trim()}`);
  });
});
