/**
 * The documented path end to end: run the setup helper, load the shipped
 * bundle, and join a real relay.
 *
 * Every layer below this one is checked in isolation somewhere — the helper's
 * output against the validator in `test/packaging`, the join contract against a
 * scripted relay in `test/unit`. None of that proves an operator who follows the
 * README reaches a working session, because each of those tests supplies by hand
 * what the previous step was supposed to produce. Here nothing is supplied: the
 * helper writes the files, the loader loads the committed artifact, and the real
 * relay answers the handshake.
 *
 * Lives in the integration suite because it needs the relay binary, and so it
 * runs in the one CI job holding both toolchains.
 */

import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { copyFile, mkdir, mkdtemp, readFile, writeFile } from "node:fs/promises";
import { existsSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

import type { ExtensionContext } from "@oh-my-pi/pi-coding-agent";
import { loadExtensions } from "@oh-my-pi/pi-coding-agent/extensibility/extensions/loader";
import { getAgentDir, setAgentDir } from "@oh-my-pi/pi-utils";

import { AGENT_DIR_ENV, PROJECT_ROOT_ENV, projectConfigPath } from "../../src/config.ts";
import { PACKAGE_ROOT, REPO_ROOT } from "../support/paths.ts";
import {
  RELAY_SETUP_TIMEOUT_MS,
  startRelay,
  type RelayProcess,
} from "../support/relay-process.ts";
import { Signal } from "../support/signal.ts";


const BUNDLE = resolve(PACKAGE_ROOT, "dist", "index.js");
const HELPER = join(REPO_ROOT, "scripts", "setup-client.sh");

let relay: RelayProcess;
const previousEnv: Record<string, string | undefined> = {};
const previousAgentDir = getAgentDir();

beforeAll(async () => {
  previousEnv[AGENT_DIR_ENV] = process.env[AGENT_DIR_ENV];
  previousEnv[PROJECT_ROOT_ENV] = process.env[PROJECT_ROOT_ENV];
  relay = await startRelay();
}, RELAY_SETUP_TIMEOUT_MS);

afterAll(async () => {
  setAgentDir(previousAgentDir);
  for (const [key, value] of Object.entries(previousEnv)) {
    if (value === undefined) delete process.env[key];
    else process.env[key] = value;
  }
  await relay?.stop();
});

interface RuntimeObservation {
  readonly userMessages: Array<{ content: unknown; options: unknown }>;
  readonly entries: Array<{ customType: string; data: unknown }>;
  readonly injected: Signal<void>;
}

/** A context complete enough for join, announce, and inbound notice delivery. */
function context(
  cwd: string,
  notifications: string[],
  observation?: RuntimeObservation,
): ExtensionContext {
  return {
    mode: "tui",
    cwd,
    isIdle: () => true,
    ui: {
      notify(message: string) {
        notifications.push(message);
      },
    },
    setTimeout: (callback: () => void, ms?: number) => setTimeout(callback, ms),
    setInterval: (callback: () => void, ms?: number) => setInterval(callback, ms),
    clearTimer: (handle: unknown) => {
      clearTimeout(handle as Parameters<typeof clearTimeout>[0]);
      clearInterval(handle as Parameters<typeof clearInterval>[0]);
    },
    sendUserMessage(content: unknown, options: unknown) {
      observation?.userMessages.push({ content, options });
      observation?.injected.fire(undefined);
    },
    appendEntry(customType: string, data: unknown) {
      observation?.entries.push({ customType, data });
    },
  } as unknown as ExtensionContext;
}

describe("the documented setup reaches a joined session", () => {
  test("helper output, committed bundle, and real relay agree", async () => {
    // The committed bundle, not the source: it is what the README tells an
    // operator to load after a clone. CI separately proves it matches the
    // source beside it; locally, run `bun run build` if this is stale.
    expect(existsSync(BUNDLE)).toBe(true);

    const agentDir = await mkdtemp(join(tmpdir(), "omp-relay-e2e-agent-"));
    const projectRoot = await mkdtemp(join(tmpdir(), "omp-relay-e2e-root-"));

    const helper = Bun.spawn(
      [
        "bash",
        HELPER,
        "--task",
        "end-to-end-check",
        "--project",
        "omp-relayd",
        "--address",
        `127.0.0.1:${relay.port}`,
        "--agent-dir",
        agentDir,
        "--project-root",
        projectRoot,
        "--peer",
        "e2e-terminal",
      ],
      { cwd: REPO_ROOT, stdout: "pipe", stderr: "pipe" },
    );
    const [helperOut, helperErr, helperCode] = await Promise.all([
      new Response(helper.stdout).text(),
      new Response(helper.stderr).text(),
      helper.exited,
    ]);
    expect(helperErr).toBe("");
    expect(helperCode).toBe(0);
    console.log(`helper:\n${helperOut.trimEnd()}`);

    // The two files exist and hold what the operator will read back.
    console.log(
      `global file:\n${(await readFile(join(agentDir, "omp-relay.yml"), "utf8")).trimEnd()}`,
    );
    console.log(
      `project file:\n${(await readFile(projectConfigPath(projectRoot), "utf8")).trimEnd()}`,
    );

    // The documented deployment: one JavaScript file in an otherwise empty
    // directory, registered by explicit path.
    const deployment = await mkdtemp(join(tmpdir(), "omp-relay-e2e-ext-"));
    const extensionPath = join(deployment, "index.js");
    await copyFile(BUNDLE, extensionPath);
    setAgentDir(agentDir);
    const loaded = await loadExtensions([extensionPath], deployment);
    expect(loaded.errors).toEqual([]);
    expect(loaded.extensions).toHaveLength(1);

    const extension = loaded.extensions[0];
    expect(extension).toBeDefined();
    if (extension === undefined) return;

    process.env[AGENT_DIR_ENV] = agentDir;
    process.env[PROJECT_ROOT_ENV] = projectRoot;
    const notifications: string[] = [];
    const ctx = context(projectRoot, notifications);

    try {
      // `manual` is what the helper wrote, so session start must connect to
      // nothing and say nothing.
      for (const handler of extension.handlers.get("session_start") ?? []) {
        await handler({ type: "session_start" }, ctx);
      }
      expect(notifications).toEqual([]);

      const mesh = extension.tools.get("mesh");
      expect(mesh).toBeDefined();
      if (mesh === undefined) return;

      const joined = await mesh.definition.execute(
        "verify-1",
        { action: "join" },
        undefined,
        undefined,
        ctx,
      );
      const details = joined.details as Record<string, unknown>;

      expect(details["action"]).toBe("join");
      expect(details["status"]).toBeUndefined();
      expect(details["project"]).toBe("omp-relayd");
      expect(details["task"]).toBe("end-to-end-check");
      expect(details["peer"]).toBe("e2e-terminal");
      // The relay's own answer, so the roster is evidence that the handshake
      // completed rather than that the client believes it did.
      expect(details["peers"]).toEqual(["e2e-terminal"]);
      // Both room halves come from the committed project file: this join
      // passed no parameters, and `--project` was the helper's flag, not the
      // join's. That distinction is exactly what the sources exist to make.
      expect(details["sources"]).toEqual({
        project: "project-file",
        task: "project-file",
        peer: "global-file",
      });
      console.log(
        `join result:\n${(joined.content as Array<{ text: string }>)[0]?.text}\n` +
          `details: ${JSON.stringify(details)}`,
      );

      const listed = await mesh.definition.execute(
        "verify-2",
        { action: "list" },
        undefined,
        undefined,
        ctx,
      );
      expect((listed.details as Record<string, unknown>)["peers"]).toEqual(["e2e-terminal"]);
    } finally {
      for (const handler of extension.handlers.get("session_shutdown") ?? []) {
        await handler({ type: "session_shutdown" }, ctx);
      }
    }
  }, RELAY_SETUP_TIMEOUT_MS);

  test("three loaded extension sessions announce through the real relay", async () => {
    const agentDir = await mkdtemp(join(tmpdir(), "omp-relay-three-agent-"));
    const projectRoot = await mkdtemp(join(tmpdir(), "omp-relay-three-root-"));
    await mkdir(join(projectRoot, ".omp"), { recursive: true });
    await writeFile(
      join(agentDir, "omp-relay.yml"),
      [
        "transport:",
        "  mode: local",
        `  address: "127.0.0.1:${relay.port}"`,
        "startup: manual",
        "peer:",
        '  name: "default-peer"',
        "",
      ].join("\n"),
      "utf8",
    );
    await writeFile(
      projectConfigPath(projectRoot),
      ["room:", "  project: omp-relayd", "  task: three-extension-sessions", ""].join("\n"),
      "utf8",
    );

    process.env[AGENT_DIR_ENV] = agentDir;
    process.env[PROJECT_ROOT_ENV] = projectRoot;
    setAgentDir(agentDir);

    // Three independent runtime instances from three copies of the committed
    // bundle. The copied paths defeat module-instance sharing; all three still
    // execute the artifact the README tells an operator to load.
    const sessions = await Promise.all(
      ["author", "first", "second"].map(async (name) => {
        const deployment = await mkdtemp(join(tmpdir(), `omp-relay-three-${name}-`));
        const extensionPath = join(deployment, "index.js");
        await copyFile(BUNDLE, extensionPath);
        const loaded = await loadExtensions([extensionPath], deployment);
        expect(loaded.errors).toEqual([]);
        const extension = loaded.extensions[0];
        if (extension === undefined) throw new Error(`no extension loaded for ${name}`);

        const observation: RuntimeObservation = {
          userMessages: [],
          entries: [],
          injected: new Signal<void>(),
        };
        // `loadExtensions` deliberately installs throwing action stubs until a
        // host binds its runtime. Bind only the two actions this delivery path
        // uses; registration and tool execution remain the real bundle.
        loaded.runtime.sendUserMessage = (content, options) => {
          observation.userMessages.push({ content, options });
          observation.injected.fire(undefined);
        };
        loaded.runtime.appendEntry = (customType, data) => {
          observation.entries.push({ customType, data });
        };
        const ctx = context(projectRoot, [], observation);
        for (const handler of extension.handlers.get("session_start") ?? []) {
          await handler({ type: "session_start" }, ctx);
        }
        const mesh = extension.tools.get("mesh");
        if (mesh === undefined) throw new Error(`mesh not registered for ${name}`);
        const joined = await mesh.definition.execute(
          `join-${name}`,
          { action: "join", as: `three-${name}` },
          undefined,
          undefined,
          ctx,
        );
        expect((joined.details as Record<string, unknown>)["status"]).toBeUndefined();
        return { name, extension, mesh, ctx, observation };
      }),
    );

    try {
      const author = sessions[0];
      const first = sessions[1];
      const second = sessions[2];
      if (author === undefined || first === undefined || second === undefined) {
        throw new Error("three extension sessions were not created");
      }

      const announced = await author.mesh.definition.execute(
        "announce-1",
        {
          action: "announce",
          message: "The schema landed; do not emit the numeric form.",
          reply_to: "decision-7",
        },
        undefined,
        undefined,
        author.ctx,
      );
      await Promise.all([
        first.observation.injected.until(1),
        second.observation.injected.until(1),
      ]);

      const details = announced.details as Record<string, unknown>;
      expect(details["delivered"]).toBe(2);
      expect(details["shed"]).toBe(0);
      expect(author.observation.userMessages).toEqual([]);
      for (const recipient of [first, second]) {
        expect(recipient.observation.userMessages).toHaveLength(1);
        expect(recipient.observation.userMessages[0]?.options).toEqual({
          deliverAs: "steer",
        });
        expect(String(recipient.observation.userMessages[0]?.content)).toContain(
          "Room announcement from three-author, addressed to everyone in this room",
        );
        expect(recipient.observation.entries).toEqual([
          {
            customType: "io.github.pashifika.omp-relay.notice",
            data: {
              id: details["id"],
              from: "three-author",
              project: "omp-relayd",
              task: "three-extension-sessions",
              body: "The schema landed; do not emit the numeric form.",
              reply_to: "decision-7",
            },
          },
        ]);
      }
      console.log(
        `three extension sessions: delivered=${details["delivered"]}, shed=${details["shed"]}; recipient injections=${first.observation.userMessages.length}+${second.observation.userMessages.length}; author injections=${author.observation.userMessages.length}`,
      );
    } finally {
      for (const session of sessions) {
        for (const handler of session.extension.handlers.get("session_shutdown") ?? []) {
          await handler({ type: "session_shutdown" }, session.ctx);
        }
      }
    }
  }, RELAY_SETUP_TIMEOUT_MS);
});
