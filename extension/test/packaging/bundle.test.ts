import { expect, test } from "bun:test";
import { copyFile, mkdtemp, readdir } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

import { loadExtensions } from "@oh-my-pi/pi-coding-agent/extensibility/extensions/loader";

test("the standalone bundle registers mesh through the OMP loader", async () => {
  const directory = await mkdtemp(join(tmpdir(), "omp-relay-bundle-"));
  const extensionPath = join(directory, "index.js");
  await copyFile(resolve("dist/index.js"), extensionPath);

  expect(await readdir(directory)).toEqual(["index.js"]);

  const loaded = await loadExtensions([extensionPath], directory);
  expect(loaded.errors).toEqual([]);
  expect(loaded.extensions).toHaveLength(1);
  expect([...loaded.extensions[0]!.tools.keys()]).toEqual(["mesh"]);
  expect([...loaded.extensions[0]!.handlers.keys()].sort()).toEqual([
    "message_start",
    "session_shutdown",
    "session_start",
  ]);
});
