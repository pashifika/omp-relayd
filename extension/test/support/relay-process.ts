/**
 * Builds and runs the real relay binary for the integration suite.
 *
 * Testing against the binary rather than a hand-written mock is the point of the
 * integration tests: a mock would agree with whatever the TypeScript side
 * believes about the protocol, and that agreement is precisely the assumption
 * under test.
 *
 * The bound port is learned from the relay's own startup log rather than chosen
 * by the test. Binding port 0 and reading back what the OS assigned cannot race
 * another process, where picking a port and hoping can — and the relay's
 * obligation to name the address it actually bound is what makes that possible,
 * so `relay-operations` is load-bearing here rather than decorative.
 */

import { SERVER_ROOT } from "./paths.ts";

/**
 * How long to wait for the startup line before failing.
 *
 * A real timer, deliberately: this bounds a real process performing real I/O,
 * and there is no clock to fake. It is a *bound*, not a sleep — the wait ends as
 * soon as the log line appears, so the normal path pays milliseconds. A fixed
 * sleep would both slow every run and mask a slow start as a pass.
 */
const STARTUP_TIMEOUT_MS = 10_000;

/** How long to wait for a terminated relay to exit before escalating. */
const SHUTDOWN_TIMEOUT_MS = 5_000;

/** Matches the relay's `relay listening local_addr=<addr>` startup event. */
const LISTENING = /relay listening\s+local_addr=(\S+)/;

let building: Promise<string> | null = null;

/**
 * Builds the relay once per test run and returns the binary path.
 *
 * Memoized rather than guarded by a file check: `cargo build` is already a no-op
 * when nothing changed, so the only thing worth avoiding is several test files
 * invoking cargo concurrently and contending on its lock.
 */
export function buildRelay(): Promise<string> {
  building ??= (async (): Promise<string> => {
    const build = Bun.spawn(
      ["cargo", "build", "--locked", "--release", "--bin", "omp-relayd"],
      { cwd: SERVER_ROOT, stdout: "pipe", stderr: "pipe" },
    );
    const [stdout, stderr, code] = await Promise.all([
      new Response(build.stdout).text(),
      new Response(build.stderr).text(),
      build.exited,
    ]);
    if (code !== 0) {
      throw new Error(
        `cargo build --release failed with exit code ${code}.\n${stderr}${stdout}`,
      );
    }
    return `${SERVER_ROOT}/target/release/omp-relayd`;
  })();
  return building;
}

export interface RelayProcess {
  /** The address the relay reported binding, parsed from its startup log. */
  readonly host: string;
  readonly port: number;
  /** Every log line the relay has emitted, for a failure message. */
  readonly log: readonly string[];
  /** Terminates the relay and waits for it to exit. */
  stop(): Promise<void>;
}

/**
 * Starts the relay and waits until it reports the address it bound.
 *
 * @param port the port to bind, or 0 to let the OS choose. Passing a port
 *   explicitly exists for the reconnect test, which has to restart the relay
 *   where the client is still trying to reach it.
 */
export async function startRelay(port = 0): Promise<RelayProcess> {
  const binary = await buildRelay();
  const relay = Bun.spawn([binary, `127.0.0.1:${port}`], {
    cwd: SERVER_ROOT,
    stdout: "pipe",
    stderr: "pipe",
    env: { ...process.env, RUST_LOG: "info" },
  });

  const log: string[] = [];
  const bound = Promise.withResolvers<{ host: string; port: number }>();

  /** Drains one stream into `log`, resolving `bound` when the address appears. */
  const drain = async (stream: ReadableStream<Uint8Array>): Promise<void> => {
    const decoder = new TextDecoder();
    let pending = "";
    for await (const chunk of stream) {
      pending += decoder.decode(chunk, { stream: true });
      const lines = pending.split("\n");
      pending = lines.pop() ?? "";
      for (const line of lines) {
        log.push(line);
        const match = LISTENING.exec(line);
        const address = match?.[1];
        if (address === undefined) continue;
        const separator = address.lastIndexOf(":");
        bound.resolve({
          host: address.slice(0, separator),
          port: Number(address.slice(separator + 1)),
        });
      }
    }
  };

  // Both streams are drained, and not only for the log: an undrained pipe fills
  // and then blocks the child on write.
  void drain(relay.stdout).catch(() => {});
  void drain(relay.stderr).catch(() => {});

  const exitedEarly = relay.exited.then(
    (code) =>
      `the relay exited with code ${code} before reporting a bound address.\n${log.join("\n")}`,
  );
  const timedOut = Bun.sleep(STARTUP_TIMEOUT_MS).then(
    () =>
      `the relay did not report a bound address within ${STARTUP_TIMEOUT_MS} ms.\n${log.join("\n")}`,
  );

  const outcome = await Promise.race([bound.promise, exitedEarly, timedOut]);
  if (typeof outcome === "string") {
    relay.kill("SIGKILL");
    throw new Error(outcome);
  }

  return {
    host: outcome.host,
    port: outcome.port,
    log,
    stop: async (): Promise<void> => {
      if (relay.killed) {
        await relay.exited;
        return;
      }
      relay.kill("SIGTERM");
      const escalate = Bun.sleep(SHUTDOWN_TIMEOUT_MS).then(() => "timeout" as const);
      const settled = await Promise.race([relay.exited, escalate]);
      if (settled === "timeout") {
        relay.kill("SIGKILL");
        await relay.exited;
        throw new Error(
          `the relay ignored SIGTERM for ${SHUTDOWN_TIMEOUT_MS} ms and had to be killed.\n${log.join("\n")}`,
        );
      }
    },
  };
}
