/**
 * Configuration loading, validation, and path resolution.
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
  CONFIG_PATH_ENV,
  loadConfig,
  parseAddress,
  resolveConfigPath,
  validateConfig,
  type Environment,
} from "../../src/config.ts";

const VALID = `
transport:
  mode: local
  address: 127.0.0.1:7788
room:
  project: omp-relayd
  task: implement-relay-client-library
peer: macbook-reviewer
`;

const roots: string[] = [];

async function scratch(): Promise<string> {
  const root = await mkdtemp(join(tmpdir(), "omp-relay-config-"));
  roots.push(root);
  return root;
}

/** Writes `body` to a scratch file and returns an env pointing the loader at it. */
async function configuredWith(body: string): Promise<Environment> {
  const path = join(await scratch(), "client.yaml");
  await writeFile(path, body, "utf8");
  return { [CONFIG_PATH_ENV]: path };
}

afterAll(() => {
  // Left in the OS temp directory deliberately: a failed run's inputs are
  // evidence, and the OS reclaims them.
  if (roots.length > 0) {
    console.log(`config fixtures written under ${roots.length} scratch directory(ies)`);
  }
});

describe("path resolution", () => {
  test("the default path is one documented location under HOME", () => {
    const resolved = resolveConfigPath({ HOME: "/home/dev" });
    expect(resolved.ok).toBe(true);
    if (!resolved.ok) return;
    expect(resolved.path).toBe("/home/dev/.config/omp-relay/client.yaml");
  });

  test("the override replaces the default entirely rather than layering on it", async () => {
    // Both a HOME default and an override exist, and each names a *different*
    // valid configuration. Reading the override's values proves there was no
    // merge and no fallback.
    const home = await scratch();
    await mkdir(join(home, ".config", "omp-relay"), { recursive: true });
    await writeFile(
      join(home, ".config", "omp-relay", "client.yaml"),
      VALID.replace("macbook-reviewer", "from-the-default-path"),
      "utf8",
    );

    const overridePath = join(await scratch(), "elsewhere.yaml");
    await writeFile(
      overridePath,
      VALID.replace("macbook-reviewer", "from-the-override"),
      "utf8",
    );

    const outcome = await loadConfig({ HOME: home, [CONFIG_PATH_ENV]: overridePath });
    expect(outcome.ok).toBe(true);
    if (!outcome.ok) return;
    expect(outcome.path).toBe(overridePath);
    expect(outcome.config.peer).toBe("from-the-override");
  });

  test("with neither the override nor HOME there is no path to read", async () => {
    const outcome = await loadConfig({});
    expect(outcome.ok).toBe(false);
    if (outcome.ok) return;
    expect(outcome.path).toBeNull();
    expect(outcome.problem.reason).toContain(CONFIG_PATH_ENV);
  });
});

describe("loading", () => {
  test("a valid file yields the values the client will use", async () => {
    const outcome = await loadConfig(await configuredWith(VALID));
    expect(outcome.ok).toBe(true);
    if (!outcome.ok) return;
    expect(outcome.config).toEqual({
      transport: { mode: "local", host: "127.0.0.1", port: 7788 },
      room: { project: "omp-relayd", task: "implement-relay-client-library" },
      peer: "macbook-reviewer",
    });
  });

  test("a missing file stops the client without throwing", async () => {
    const path = join(await scratch(), "absent.yaml");
    const outcome = await loadConfig({ [CONFIG_PATH_ENV]: path });

    expect(outcome.ok).toBe(false);
    if (outcome.ok) return;
    expect(outcome.problem.reason).toBe(`configuration file ${path} does not exist`);
  });

  test("unparseable YAML stops the client without throwing", async () => {
    const env = await configuredWith("transport:\n  mode: local\n : : :\n");
    const outcome = await loadConfig(env);

    expect(outcome.ok).toBe(false);
    if (outcome.ok) return;
    expect(outcome.problem.field).toBeNull();
    expect(outcome.problem.reason).toContain("not valid YAML");
  });
});

describe("validation names the offending field", () => {
  const parse = (body: string): unknown => Bun.YAML.parse(body);

  test.each([
    ["private", "transport.mode"],
    ["public", "transport.mode"],
    ["Local", "transport.mode"],
  ])("transport.mode %p is refused", (mode, field) => {
    // `private` and `public` are refused rather than reserved: neither
    // transport exists, so accepting either would let configuration promise a
    // guarantee no code provides.
    const outcome = validateConfig(parse(VALID.replace("mode: local", `mode: ${mode}`)));
    expect(outcome.ok).toBe(false);
    if (outcome.ok) return;
    expect(outcome.problem.field).toBe(field);
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
    const outcome = validateConfig(
      parse(VALID.replace("127.0.0.1:7788", JSON.stringify(address))),
    );
    expect(outcome.ok).toBe(false);
    if (outcome.ok) return;
    expect(outcome.problem.field).toBe("transport.address");
  });

  test("a bracketed IPv6 address is accepted", () => {
    const outcome = validateConfig(
      parse(VALID.replace("127.0.0.1:7788", '"[::1]:7788"')),
    );
    expect(outcome.ok).toBe(true);
    if (!outcome.ok) return;
    expect(outcome.config.transport).toEqual({ mode: "local", host: "::1", port: 7788 });
  });

  test("a peer at 65 UTF-8 bytes is refused before anything is sent", () => {
    const outcome = validateConfig(
      parse(VALID.replace("macbook-reviewer", "p".repeat(65))),
    );
    expect(outcome.ok).toBe(false);
    if (outcome.ok) return;
    expect(outcome.problem.field).toBe("peer");
    expect(outcome.problem.reason).toContain("at most 64 UTF-8 bytes");
  });

  test("a peer containing @ is refused", () => {
    const outcome = validateConfig(parse(VALID.replace("macbook-reviewer", '"a@b"')));
    expect(outcome.ok).toBe(false);
    if (outcome.ok) return;
    expect(outcome.problem.field).toBe("peer");
  });

  test.each([
    ["room.project", "  project: omp-relayd\n"],
    ["room.task", "  task: implement-relay-client-library\n"],
  ])("a missing %s is refused", (field, line) => {
    const outcome = validateConfig(parse(VALID.replace(line, "")));
    expect(outcome.ok).toBe(false);
    if (outcome.ok) return;
    expect(outcome.problem.field).toBe(field);
  });

  test("a room component containing / is refused", () => {
    const outcome = validateConfig(parse(VALID.replace("omp-relayd", '"omp/relayd"')));
    expect(outcome.ok).toBe(false);
    if (outcome.ok) return;
    expect(outcome.problem.field).toBe("room.project");
  });

  test.each([
    ["a list", "- one\n- two\n"],
    ["a scalar", "just-a-string\n"],
  ])("a document that is %s is refused", (_label, body) => {
    const outcome = validateConfig(parse(body));
    expect(outcome.ok).toBe(false);
    if (outcome.ok) return;
    expect(outcome.problem.field).toBeNull();
  });

  test("an unrecognized key is ignored rather than refused", () => {
    // Additive tolerance, matching the wire contract: a newer client's key in
    // an older client's file must not stop it starting.
    const outcome = validateConfig(parse(`${VALID}reconnect: aggressive\n`));
    expect(outcome.ok).toBe(true);
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
  test("the same listener sees a hello from a valid file and nothing from a rejected one", async () => {
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

    // Positive control: a valid file reaches the relay.
    const accepted = await loadConfig(await configuredWith(at(VALID)));
    expect(accepted.ok).toBe(true);
    if (!accepted.ok) return;
    const control = new RelayClient({ config: accepted.config });
    control.start();
    await firstArrival.promise;
    await control.stop();
    expect(arrivals).toHaveLength(1);

    // The negative: a rejected file yields no config, so the composition a host
    // performs has nothing to construct a client from.
    const rejected = await loadConfig(
      await configuredWith(at(VALID).replace("mode: local", "mode: private")),
    );
    expect(rejected.ok).toBe(false);

    let client: RelayClient | null = null;
    if (rejected.ok) {
      client = new RelayClient({ config: rejected.config });
      client.start();
    }
    expect(client).toBeNull();
    expect(arrivals).toHaveLength(1);
    console.log(
      `listener observed ${arrivals.length} hello: ${arrivals[0]?.length ?? 0} bytes from the valid configuration, none from the rejected one`,
    );

    const closed = Promise.withResolvers<void>();
    server.close(() => closed.resolve());
    await closed.promise;
  });
});
