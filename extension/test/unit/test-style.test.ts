/**
 * The machine-checked half of the table convention in `AGENTS.md`, in two parts.
 *
 * The **shape** check reads the test tree and refuses a table whose rows are
 * positional, or whose title names its cases by rendering one of their values
 * when this scan cannot see the row to be a single value.
 *
 * The **name** check reads the names the runner actually printed. It exists
 * because the property the convention is *for* — every case names itself — is
 * invisible to any text scan: two cases can be perfectly shaped and still report
 * under one name. That defect shipped on this branch (twelve duplicate names in
 * `protocol.test.ts`) and no shape rule could have caught it.
 *
 * Every case below that refuses a shape corresponds to a hole a review found in
 * an earlier version of the scanner, recorded in the change's `evidence/`. They
 * are here so a later simplification cannot reopen one silently.
 */

import { describe, expect, test } from "bun:test";
import { Glob } from "bun";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import {
  duplicateNames,
  report,
  scanTestTree,
  unsubstitutedNames,
  violations,
  type Violation,
} from "../support/test-style.ts";

const TREE = join(import.meta.dir, "..");

describe("the test tree conforms to the table convention", () => {
  const sites = scanTestTree(TREE);

  test("every parameterized table names its cases rather than numbering columns", () => {
    const offenders = violations(sites);
    expect(offenders.length, `\n${report(offenders)}`).toBe(0);
    // A count, not a bare pass: a scan that stopped matching would report zero
    // offenders too, and this is what tells the two apart.
    console.log(`${sites.length} each sites inspected, ${offenders.length} non-conforming`);
  });

  test("the scan reaches every test file and classifies what it finds", () => {
    // Three controls, because a detector can be blind in three ways: reading no
    // files, reading them and recognizing no tables, or recognizing tables but
    // resolving none of their rows. The last one is not hypothetical — an
    // earlier version classified all 47 sites as `expression` or `scalar`, so
    // the positional rule had no applicable site and passed vacuously.
    expect(sites.length).toBeGreaterThan(30);
    expect(sites.filter((site) => site.fieldTitle).length).toBeGreaterThan(20);
    expect(sites.filter((site) => site.rows === "object").length).toBeGreaterThan(20);
    const files = new Set(sites.map((site) => site.file));
    const shapes = new Map<string, number>();
    for (const site of sites) shapes.set(site.rows, (shapes.get(site.rows) ?? 0) + 1);
    console.log(
      `${sites.length} sites across ${files.size} files; shapes ${JSON.stringify([...shapes])}`,
    );
  });

  test("the scan's file patterns are the ones the runner collects", () => {
    // `bun test` collects `*.test.*`, `*_test.*`, `*.spec.*`, and `*_spec.*`. A
    // scan of `*.test.ts` alone left three of the four spellings ungated, so a
    // whole file could escape by its name.
    const root = mkdtempSync(join(tmpdir(), "omp-relay-names-"));
    try {
      const table = 'test.each([["a", 1], ["b", 2]])("%s is %d", () => {});\n';
      for (const name of ["a.test.ts", "b_test.ts", "c.spec.ts", "d_spec.ts"]) {
        writeFileSync(join(root, name), table, "utf8");
      }
      expect(violations(scanTestTree(root))).toHaveLength(4);
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });
});

describe("the shape check refuses what it exists to refuse", () => {
  /** Writes `source` as a test file in a scratch tree and scans only that. */
  function scanSource(source: string): readonly Violation[] {
    const root = mkdtempSync(join(tmpdir(), "omp-relay-test-style-"));
    try {
      writeFileSync(join(root, "sample.test.ts"), source, "utf8");
      return violations(scanTestTree(root));
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  }

  // Assembled rather than written out, because a literal positional table in
  // this file would be found by the scan of the real tree above.
  const POSITIONAL = `test.each([\n  ["a", 1],\n  ["b", 2],\n])("%s is %d", (name, value) => {});\n`;

  const refused = [
    {
      scenario: "an inline multi-column positional table is refused, naming its width",
      source: POSITIONAL,
      reason: /positional table of 2 columns/,
    },
    {
      scenario: "a positional table hoisted into a const is refused, which is the convention's own form",
      source: `const cases = [["a", 1], ["b", 2]] as const;\ntest.each(cases)("$scenario", (n, v) => {});\n`,
      reason: /positional table of 2 columns/,
    },
    {
      scenario: "a positional table behind a type argument list is refused",
      source: `test.each<[string, number]>([["a", 1], ["b", 2]])("%s is %d", (n, v) => {});\n`,
      reason: /positional table of 2 columns/,
    },
    {
      scenario: "a positional table whose rows carry leading comments is refused",
      source: `test.each([\n  // first\n  ["a", 1],\n  // second\n  ["b", 2],\n])("%s is %d", (n, v) => {});\n`,
      reason: /positional table of 2 columns/,
    },
    {
      scenario: "a table whose first row is narrow is judged by its widest row",
      source: `test.each([["only"], ["a", 1, true]])("$scenario", (a, b, c) => {});\n`,
      reason: /positional table of 3 columns/,
    },
    {
      scenario: "a chain broken across lines by a formatter is still attributed",
      source: `test\n  .each([\n    ["a", 1],\n  ])("%s is %d", (n, v) => {});\n`,
      reason: /positional table of 2 columns/,
    },
    {
      scenario: "a table holding a regex with an unbalanced bracket is still read",
      source: `test.each([\n  ["a", /\\(/],\n  ["b", /x/],\n])("%s is %d", (n, r) => {});\n`,
      reason: /positional table of 2 columns/,
    },
    {
      scenario: "a regex holding a backtick does not blind the rest of the file",
      source: `test("a", () => { expect(s).toMatch(/[\`]/); });\n${POSITIONAL}`,
      reason: /positional table of 2 columns/,
    },
    {
      scenario: "a fixed title over named-object rows is refused, because N cases share one name",
      source: `test.each([{ a: 1 }, { a: 2 }])("a fixed title", ({ a }) => {});\n`,
      reason: /object rows under the fixed title/,
    },
    {
      scenario: "a printf title over rows this scan cannot see is refused when it renders two values",
      source: `test.each([...ROWS])("%s is %d", (n, v) => {});\n`,
      reason: /unknown rows under the 2-placeholder title/,
    },
    {
      scenario: "a printf title over a mapped table is refused, because a map says nothing about its rows",
      source: `test.each(FIXTURES.map((f) => [f.name, f]))("%s is committed", (n, f) => {});\n`,
      reason: /expression rows under the printf title/,
    },
    {
      scenario: "an unterminated template literal is reported rather than swallowing the file",
      source: `const broken = \`never closed;\n${POSITIONAL}`,
      reason: /could not read a template literal that never closes/,
    },
  ];

  test.each(refused)("$scenario", ({ source, reason }) => {
    const found = scanSource(source);
    expect(found.length).toBeGreaterThan(0);
    expect(found.map((violation) => violation.reason).join("\n")).toMatch(reason);
    console.log(`refused: ${report(found).trim()}`);
  });

  const accepted = [
    {
      scenario: "a named-object table titled by its scenario is accepted",
      source: `test.each([{ scenario: "x", value: 1 }])("$scenario", ({ value }) => {});\n`,
    },
    {
      scenario: "the same table hoisted into a const is accepted",
      source: `const cases = [{ scenario: "x", value: 1 }];\ntest.each(cases)("$scenario", ({ value }) => {});\n`,
    },
    {
      scenario: "a single-column scalar table keeps its printf title",
      source: `test.each(["auto", "manual"])("startup %p is accepted", (mode) => {});\n`,
    },
    {
      scenario: "a scalar table reached by a resolvable spread keeps its printf title",
      source: `const MARKERS = ["a", "b"];\ntest.each([...MARKERS])("%s marks a root", (marker) => {});\n`,
    },
    {
      scenario: "an opaque one-column table keeps a single-placeholder title",
      source: `test.each([MAX - 1, MAX, MAX + 1])("a %d-byte file", (size) => {});\n`,
    },
    {
      scenario: "a mapped table titled by a case field is accepted",
      source: `test.each(F.map((f) => ({ scenario: f.name, f })))("$scenario", ({ f }) => {});\n`,
    },
    {
      scenario: "a describe.each is held to the same rules",
      source: `describe.each([{ scenario: "x" }])("$scenario", () => {});\n`,
    },
    {
      scenario: "an each inside a string literal is not a call site",
      source: `test("prose", () => {\n  expect(text).toContain('test.each([["a", 1]])');\n});\n`,
    },
    {
      scenario: "a commented-out positional table is not a call site",
      source: `// test.each([["a", 1]])("%s", () => {});\ntest("real", () => {});\n`,
    },
    {
      scenario: "a division is not a regex literal",
      source: `const half = total / 2 / 1;\ntest.each([{ scenario: "x" }])("$scenario", () => {});\n`,
    },
  ];

  test.each(accepted)("$scenario", ({ source }) => {
    expect(scanSource(source)).toEqual([]);
  });
});

describe("every case reports under a name of its own", () => {
  /**
   * The unit tree, minus this file.
   *
   * Explicit paths rather than a directory, because a child run of the whole
   * directory would include this file and spawn its own child without end.
   */
  const under = [...new Glob("*.test.ts").scanSync(join(TREE, "unit"))]
    .filter((name) => name !== "test-style.test.ts")
    .map((name) => join("test", "unit", name))
    .sort();

  test("no two cases in one file share a name, and no title keeps a placeholder", async () => {
    const outfile = join(mkdtempSync(join(tmpdir(), "omp-relay-junit-")), "report.xml");
    const run = Bun.spawnSync({
      cmd: ["bun", "test", ...under, "--reporter=junit", `--reporter-outfile=${outfile}`],
      cwd: join(TREE, ".."),
      stdout: "pipe",
      stderr: "pipe",
    });
    const junit = readFileSync(outfile, "utf8");
    rmSync(join(outfile, ".."), { recursive: true, force: true });

    const duplicates = duplicateNames(junit);
    const unsubstituted = unsubstitutedNames(junit);
    const total = [...junit.matchAll(/<testcase\b/g)].length;

    // The child run must have produced a report, or the two assertions below
    // are vacuous. A non-zero exit is fine: a failing test still reports a name.
    expect(total).toBeGreaterThan(200);
    expect(
      duplicates.length,
      `\n${duplicates.map((d) => `  ${d.file}: ${JSON.stringify(d.name)} reported ${d.count} times`).join("\n")}`,
    ).toBe(0);
    expect(unsubstituted.length, `\n${unsubstituted.join("\n")}`).toBe(0);
    console.log(
      `${total} reported cases across ${under.length} files: ${duplicates.length} duplicate names, ${unsubstituted.length} unsubstituted titles (exit ${run.exitCode})`,
    );
  }, 60_000);

  test("the duplicate and placeholder readers find what they are looking for", () => {
    // The control on the assertions above, which pass when a report is empty
    // exactly as they pass when it is clean.
    const junit = [
      '<testcase classname="a.test.ts" name="shared"></testcase>',
      '<testcase classname="a.test.ts" name="shared"></testcase>',
      '<testcase classname="b.test.ts" name="shared"></testcase>',
      '<testcase classname="a.test.ts" name="a 5-byte file"></testcase>',
      '<testcase classname="a.test.ts" name="a %d-byte file"></testcase>',
      '<testcase classname="a.test.ts" name="100%% done"></testcase>',
    ].join("\n");

    expect(duplicateNames(junit)).toEqual([{ file: "a.test.ts", name: "shared", count: 2 }]);
    expect(unsubstitutedNames(junit)).toEqual(["a %d-byte file"]);
  });
});
