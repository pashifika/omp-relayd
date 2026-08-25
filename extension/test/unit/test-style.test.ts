/**
 * The machine-checked half of the table convention in `AGENTS.md`.
 *
 * Two rules, both about what a reader of one row can tell: a row that is an
 * array of two or more values is positional, so its meaning is its position;
 * and a printf-titled table names a case by rendering one of its values, which
 * is honest only when the case *is* that one value.
 *
 * What no check here judges: whether a table's cases belong together, and
 * whether its expectations are columns rather than recomputed in the body. Both
 * are review concerns, stated in `AGENTS.md` as such.
 */

import { describe, expect, test } from "bun:test";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { report, scanTestTree, violations, type Violation } from "../support/test-style.ts";

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

  test("the scan reaches every test file and finds tables in them", () => {
    // The control on the assertion above. Both halves are needed: a detector
    // that read no files, and one that read them and recognized nothing, both
    // report a clean tree.
    expect(sites.length).toBeGreaterThan(30);
    const named = sites.filter((site) => site.fieldTitle);
    expect(named.length).toBeGreaterThan(20);
    const files = new Set(sites.map((site) => site.file));
    console.log(
      `${sites.length} sites across ${files.size} files; ${named.length} titled by a case field`,
    );
  });
});

describe("the check refuses the shapes it exists to refuse", () => {
  /** Writes `source` as a test file in a scratch tree and scans only that. */
  function scanSource(source: string): readonly Violation[] {
    const root = mkdtempSync(join(tmpdir(), "omp-relay-test-style-"));
    writeFileSync(join(root, "sample.test.ts"), source, "utf8");
    return violations(scanTestTree(root));
  }

  // Assembled rather than written out, because a literal positional table in
  // this file would be found by the scan of the real tree above.
  const POSITIONAL = `test.each([\n  ["a", 1],\n  ["b", 2],\n])("%s is %d", (name, value) => {});\n`;
  const PRINTF_OBJECTS = `test.each([\n  { scenario: "x", value: 1 },\n])("%s", ({ value }) => {});\n`;
  const PRINTF_EXPRESSION = `test.each(cases)("%s is refused", ({ value }) => {});\n`;

  const refused = [
    {
      scenario: "a multi-column positional table is refused, naming its width",
      source: POSITIONAL,
      reason: /positional table of 2 columns/,
    },
    {
      scenario: "a printf title over named-object rows is refused",
      source: PRINTF_OBJECTS,
      reason: /rows are object/,
    },
    {
      scenario: "a printf title over rows of unprovable width is refused",
      source: PRINTF_EXPRESSION,
      reason: /rows are expression/,
    },
  ];

  test.each(refused)("$scenario", ({ source, reason }) => {
    const found = scanSource(source);
    expect(found).toHaveLength(1);
    expect(found[0]?.reason).toMatch(reason);
    expect(report(found)).toContain("sample.test.ts:1");
    console.log(`refused: ${report(found).trim()}`);
  });

  const accepted = [
    {
      scenario: "a named-object table titled by its scenario is accepted",
      source: `test.each([\n  { scenario: "x", value: 1 },\n])("$scenario", ({ value }) => {});\n`,
    },
    {
      scenario: "a single-column scalar table keeps its printf title",
      source: `test.each(["auto", "manual"])("startup %p is accepted", (mode) => {});\n`,
    },
    {
      scenario: "a scalar table built by a spread keeps its printf title",
      source: `test.each([...MARKERS])("%s marks a root", (marker) => {});\n`,
    },
    {
      scenario: "an expression table titled by a case field is accepted",
      source: `test.each(cases)("$scenario", ({ value }) => {});\n`,
    },
    {
      scenario: "a describe.each is held to the same rules",
      source: `describe.each([\n  { scenario: "x" },\n])("$scenario", () => {});\n`,
    },
    {
      scenario: "an each inside a string literal is not a call site",
      source: `test("prose", () => {\n  expect(text).toContain('test.each([["a", 1]])');\n});\n`,
    },
    {
      scenario: "a commented-out positional table is not a call site",
      source: `// test.each([["a", 1]])("%s", () => {});\ntest("real", () => {});\n`,
    },
  ];

  test.each(accepted)("$scenario", ({ source }) => {
    expect(scanSource(source)).toEqual([]);
  });
});
