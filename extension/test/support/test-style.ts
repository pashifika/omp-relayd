/**
 * Reads the test tree and reports how each parameterized table is written.
 *
 * The convention in `AGENTS.md` is that a multi-column case table is a
 * `readonly` array of objects with a `scenario` field, titled `"$scenario"`, so
 * every case names itself in the runner's output and no value is reached by
 * counting columns. This module is the machine-checked half of that: it finds
 * every `each` site and classifies the shape of its rows and its title.
 *
 * What it deliberately does not judge: whether a table's cases belong together,
 * and whether its expectations are columns rather than recomputed in the body.
 * Both are review concerns, and a check that appeared to cover them would be
 * worse than one whose boundary is written down.
 *
 * A source scan rather than a parse: `ts-morph` or the compiler API would be
 * exact, and would be a dependency and a full parse of the test tree for a rule
 * about the first two tokens after `each(`. The scanner below balances brackets
 * and skips strings and comments, which is what a regex cannot do and what
 * classifying a row shape requires.
 */

import { Glob } from "bun";
import { readFileSync } from "node:fs";
import { join } from "node:path";

/** How the rows of one `each` table are written. */
export type RowShape =
  /** `[{ scenario: … }, …]` — a named-field table. */
  | "object"
  /** `[["a", "b"], …]` — a positional tuple table. */
  | "positional"
  /** `["a", "b"]` — one column, where the value is the whole case. */
  | "scalar"
  /** `cases`, `X.map(…)` — not an array literal, so the shape is unknown here. */
  | "expression";

/** One `test.each` / `describe.each` / `it.each` call site. */
export interface EachSite {
  /** Path as written in a report: relative to the scanned root. */
  readonly file: string;
  /** 1-indexed line of the `each` token. */
  readonly line: number;
  /** `test`, `it`, or `describe`, with any `.only`/`.skip`/`.failing` dropped. */
  readonly kind: string;
  readonly rows: RowShape;
  /** Columns in the first row, for a positional table; otherwise `undefined`. */
  readonly columns?: number;
  /** The title literal, or `undefined` when it is not a plain literal. */
  readonly title?: string;
  /** Whether the title carries a printf placeholder such as `%s` or `%p`. */
  readonly printfTitle: boolean;
  /** Whether the title interpolates a case field, such as `$scenario`. */
  readonly fieldTitle: boolean;
}

/** A site that does not conform, with the reason it does not. */
export interface Violation {
  readonly site: EachSite;
  readonly reason: string;
}

const PLACEHOLDER = /%[sdifjop#]/;
const FIELD_TITLE = /\$[A-Za-z_$][\w$]*/;

/**
 * Positions of every `each` call, with strings and comments skipped so a
 * `.each(` inside a string literal is not mistaken for a call.
 */
function scan(source: string): EachSite[] {
  const sites: EachSite[] = [];
  let at = 0;
  while (at < source.length) {
    const skipped = skipTrivia(source, at);
    if (skipped > at) {
      at = skipped;
      continue;
    }
    if (source.startsWith(".each(", at)) {
      const site = read(source, at);
      if (site !== undefined) sites.push(site);
      at += ".each(".length;
      continue;
    }
    at += 1;
  }
  return sites;
}

/**
 * Advances past a comment or a quoted string starting at `at`, or returns `at`
 * unchanged when it starts neither.
 */
function skipTrivia(source: string, at: number): number {
  if (source.startsWith("//", at)) {
    const end = source.indexOf("\n", at);
    return end === -1 ? source.length : end;
  }
  if (source.startsWith("/*", at)) {
    const end = source.indexOf("*/", at + 2);
    return end === -1 ? source.length : end + 2;
  }
  const quote = source[at];
  if (quote !== '"' && quote !== "'" && quote !== "`") return at;
  let cursor = at + 1;
  while (cursor < source.length) {
    const char = source[cursor];
    if (char === "\\") {
      cursor += 2;
      continue;
    }
    if (char === quote) return cursor + 1;
    // An unterminated single- or double-quoted string cannot cross a line; a
    // template literal can, so only the quoted forms stop at a newline.
    if (char === "\n" && quote !== "`") return cursor;
    cursor += 1;
  }
  return source.length;
}

/** Reads one `each` site whose `.each(` begins at `at`. */
function read(source: string, at: number): EachSite | undefined {
  const kind = callee(source, at);
  if (kind === undefined) return undefined;

  const open = at + ".each".length;
  const close = matching(source, open, "(", ")");
  if (close === undefined) return undefined;
  const argument = source.slice(open + 1, close).trim();

  const line = source.slice(0, at).split("\n").length;
  const { rows, columns } = classify(argument);
  const title = titleOf(source, close + 1);
  const stripped = title?.replaceAll("%%", "") ?? "";

  return {
    file: "",
    line,
    kind,
    rows,
    ...(columns === undefined ? {} : { columns }),
    ...(title === undefined ? {} : { title }),
    printfTitle: title !== undefined && PLACEHOLDER.test(stripped),
    fieldTitle: title !== undefined && FIELD_TITLE.test(stripped),
  };
}

/** The `test`, `it`, or `describe` that owns the `.each(` ending at `at`. */
function callee(source: string, at: number): string | undefined {
  const before = source.slice(Math.max(0, at - 40), at);
  const match = /\b(test|it|describe)(?:\.(?:only|skip|failing|todo|if|each))*$/.exec(before);
  return match?.[1];
}

/** Index of the delimiter matching the `open` at `from`, skipping trivia. */
function matching(
  source: string,
  from: number,
  open: string,
  close: string,
): number | undefined {
  let depth = 0;
  let at = from;
  while (at < source.length) {
    const skipped = skipTrivia(source, at);
    if (skipped > at) {
      at = skipped;
      continue;
    }
    const char = source[at];
    if (char === open) depth += 1;
    else if (char === close) {
      depth -= 1;
      if (depth === 0) return at;
    }
    at += 1;
  }
  return undefined;
}

/** Classifies the rows of an `each` argument. */
function classify(argument: string): { rows: RowShape; columns?: number } {
  if (!argument.startsWith("[")) return { rows: "expression" };
  const end = matching(argument, 0, "[", "]");
  if (end === undefined) return { rows: "expression" };
  const elements = split(argument.slice(1, end));
  if (elements.some((element) => element.startsWith("["))) {
    const first = elements.find((element) => element.startsWith("["))!;
    const inner = matching(first, 0, "[", "]");
    const columns = inner === undefined ? 1 : split(first.slice(1, inner)).length;
    return { rows: "positional", columns };
  }
  if (elements.some((element) => element.startsWith("{"))) return { rows: "object" };
  return { rows: "scalar" };
}

/** Splits an array literal's body at its top-level commas. */
function split(body: string): string[] {
  const parts: string[] = [];
  let depth = 0;
  let start = 0;
  let at = 0;
  while (at < body.length) {
    const skipped = skipTrivia(body, at);
    if (skipped > at) {
      at = skipped;
      continue;
    }
    const char = body[at]!;
    if ("([{".includes(char)) depth += 1;
    else if (")]}".includes(char)) depth -= 1;
    else if (char === "," && depth === 0) {
      parts.push(body.slice(start, at).trim());
      start = at + 1;
    }
    at += 1;
  }
  const last = body.slice(start).trim();
  if (last.length > 0) parts.push(last);
  return parts.filter((part) => part.length > 0);
}

/** The title literal of the call that follows an `each` argument list. */
function titleOf(source: string, from: number): string | undefined {
  let at = from;
  while (at < source.length && /\s/.test(source[at]!)) at += 1;
  if (source[at] !== "(") return undefined;
  at += 1;
  while (at < source.length && /\s/.test(source[at]!)) at += 1;
  const quote = source[at];
  if (quote !== '"' && quote !== "'" && quote !== "`") return undefined;
  const end = skipTrivia(source, at);
  return source.slice(at + 1, end - 1);
}

/** Every `each` site under `root`, in file and line order. */
export function scanTestTree(root: string): readonly EachSite[] {
  const files = [...new Glob("**/*.test.ts").scanSync(root)].sort();
  return files.flatMap((file) => {
    // Read synchronously so the check stays a plain assertion rather than an
    // async one; the whole tree is a few hundred kilobytes.
    const source = readFileSync(join(root, file), "utf8");
    return scan(source).map((site) => ({ ...site, file }));
  });
}

/**
 * The sites that do not conform.
 *
 * Two rules, both about what a reader of one row can tell:
 *
 * 1. A row that is an array of two or more values is a positional table: the
 *    meaning of each value is its position, so the reader counts columns.
 * 2. A printf-titled table names a case by rendering one of its values. That is
 *    honest only when the case *is* that one value — a scalar array literal.
 *    Anywhere else the title stands in for the behavior instead of stating it,
 *    including when the rows come from an expression whose shape is not visible
 *    here: unprovable is treated as unconforming rather than waved through.
 */
export function violations(sites: readonly EachSite[]): readonly Violation[] {
  const found: Violation[] = [];
  for (const site of sites) {
    if (site.rows === "positional" && (site.columns ?? 1) > 1) {
      found.push({
        site,
        reason:
          `a positional table of ${site.columns} columns: give each case a named ` +
          `field and title the run "$scenario"`,
      });
      continue;
    }
    if (site.printfTitle && site.rows !== "scalar") {
      found.push({
        site,
        reason:
          `a printf-titled table whose rows are ${site.rows}: a placeholder names a ` +
          `case by one of its values, which only a single-column scalar table is`,
      });
    }
  }
  return found;
}

/** One line per violation, as a failure message prints it. */
export function report(found: readonly Violation[]): string {
  return found
    .map(({ site, reason }) => `  ${site.file}:${site.line} (${site.kind}.each): ${reason}`)
    .join("\n");
}
