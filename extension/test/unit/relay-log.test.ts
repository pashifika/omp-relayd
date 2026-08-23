/**
 * Parsing the relay's startup log line.
 *
 * This is test-support code with its own test, which is unusual and earned. The
 * integration suite learns the relay's port from this line, so a parser that
 * silently fails to match reports "the relay did not report a bound address"
 * about a relay that reported one — and that is not a hypothetical: the plain
 * form matched locally while the colored form CI produces did not, and the CI
 * job failed for a reason that had nothing to do with the code under test.
 */

import { describe, expect, test } from "bun:test";

import { parseListeningAddress, stripAnsi } from "../support/relay-process.ts";

/** The line as the relay writes it to a pipe with coloring disabled. */
const PLAIN =
  "2026-08-23T02:15:51.773019Z  INFO omp_relayd: relay listening local_addr=127.0.0.1:59477";

/**
 * The same event as the relay writes it under CI.
 *
 * Copied from a real failing job rather than composed here, escape sequence for
 * escape sequence: a hand-written approximation would be a guess at the input
 * that broke, which is the one thing this test must not be.
 */
const COLORED =
  "\u001B[2m2026-08-23T02:25:17.531886Z\u001B[0m \u001B[32m INFO\u001B[0m " +
  "\u001B[2momp_relayd\u001B[0m\u001B[2m:\u001B[0m relay listening " +
  "\u001B[3mlocal_addr\u001B[0m\u001B[2m=\u001B[0m127.0.0.1:45549";

describe("startup line parsing", () => {
  test("the plain form yields the bound host and port", () => {
    expect(parseListeningAddress(PLAIN)).toEqual({ host: "127.0.0.1", port: 59477 });
  });

  test("the colored form CI produces yields the same shape", () => {
    expect(parseListeningAddress(COLORED)).toEqual({ host: "127.0.0.1", port: 45549 });
    console.log(`colored line parsed to ${JSON.stringify(parseListeningAddress(COLORED))}`);
  });

  test("stripping leaves the line readable for a failure message", () => {
    const stripped = stripAnsi(COLORED);
    expect(stripped).toContain("relay listening local_addr=127.0.0.1:45549");
    expect(stripped).not.toContain("\u001B");
  });

  test("an IPv6 bind address keeps its host intact", () => {
    expect(
      parseListeningAddress("  INFO omp_relayd: relay listening local_addr=[::1]:7788"),
    ).toEqual({ host: "[::1]", port: 7788 });
  });

  test.each([
    ["another log event", "  INFO omp_relayd: termination signal received"],
    ["the bind-failure event", "  ERROR omp_relayd: could not bind listener listen=x"],
    ["a port that is not a number", "relay listening local_addr=127.0.0.1:http"],
    ["a port out of range", "relay listening local_addr=127.0.0.1:70000"],
    ["no port at all", "relay listening local_addr=127.0.0.1"],
  ])("%s is not a bound address", (_label, line) => {
    expect(parseListeningAddress(line)).toBeNull();
  });
});
