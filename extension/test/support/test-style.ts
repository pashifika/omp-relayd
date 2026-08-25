/**
 * The machine-checked half of the table convention in `AGENTS.md`.
 *
 * Two rules, both about what a reader of one row can tell: a row that is an
 * array of two or more values is positional, so its meaning is its position;
 * and a table whose rows are not single scalars must title itself from a case
 * field, because a printf placeholder names a case by rendering one of its
 * values and only a one-column table *is* that value.
 *
 * Written as a source scan rather than a parse: `ts-morph` or the compiler API
 * would be exact, and would be a dependency plus a parse of the whole test tree
 * for a rule about the first two tokens after `each(`. The cost of that choice
 * is everything below — bracket balancing, string and comment skipping, regex
 * literals, and same-file `const` resolution — so it is worth knowing why each
 * one is here rather than assuming it is incidental complexity.
 *
 * Three principles keep the approximation honest:
 *
 * 1. **The convention hoists its tables.** `AGENTS.md` shows the table as a
 *    named `const`, so classifying only inline literals would leave the rule
 *    with no applicable site in the tree it guards. {@link resolve} follows a
 *    bare identifier back to its same-file initializer.
 * 2. **Unreadable is unconforming.** Anywhere this scanner cannot establish a
 *    shape — an argument it cannot balance, a file whose quoting desynchronizes
 *    it — it reports a violation naming what it could not read. A checker that
 *    goes quiet when confused is worse than no checker, because its silence is
 *    indistinguishable from a clean tree.
 * 3. **Every gap found is pinned by a case.** Each blind spot listed in
 *    `evidence/review.md` has a case in `test/unit/test-style.test.ts`, so a
 *    later simplification cannot reopen one silently.
 *
 * What no rule here judges: whether a table's cases belong together, and
 * whether its expectations are columns rather than recomputed in the body. Both
 * are review concerns, stated in `AGENTS.md` as such. Nor does it judge case
 * *names*: two cases sharing a name is invisible to any text scan, and is the
 * runner-name gate's job ({@link duplicateNames}).
 */

import { Glob } from "bun";
import { readFileSync } from "node:fs";
import { join } from "node:path";

/** How the rows of one `each` table are written. */
export type RowShape =
  /** `[{ scenario: … }, …]` — a named-field table. */
  | "object"
  /** `[["a", 1], …]` — a positional tuple table. */
  | "positional"
  /** `["a", "b"]` — one column, where the value is the whole case. */
  | "scalar"
  /** `X.map(…)` — a value whose rows this scan cannot see. */
  | "expression"
  /** An array whose elements are neither literals nor resolvable. */
  | "unknown"
  /** An argument, title, or file this scan could not read at all. */
  | "unreadable";

/** One `test.each` / `describe.each` / `it.each` call site. */
export interface EachSite {
  /** Path as written in a report: relative to the scanned root. */
  readonly file: string;
  /** 1-indexed line of the `each` token. */
  readonly line: number;
  /** `test`, `it`, or `describe`, with any `.only`/`.skip`/`.failing` dropped. */
  readonly kind: string;
  readonly rows: RowShape;
  /** Columns in the *widest* row of a positional table; otherwise `undefined`. */
  readonly columns?: number;
  /** The title literal, or `undefined` when it is not a plain literal. */
  readonly title?: string;
  /** Whether the title carries a printf placeholder such as `%s` or `%p`. */
  readonly printfTitle: boolean;
  /** Whether the title interpolates a case field, such as `$scenario`. */
  readonly fieldTitle: boolean;
  /** Set when `rows` is `"unreadable"`: what could not be read. */
  readonly note?: string;
}

/** A site that does not conform, with the reason it does not. */
export interface Violation {
  readonly site: EachSite;
  readonly reason: string;
}

/** A test name that more than one case in the same file reports. */
export interface DuplicateName {
  readonly file: string;
  readonly name: string;
  readonly count: number;
}

const PLACEHOLDER = /%[sdifjop#]/;
const FIELD_TITLE = /\$[A-Za-z_$][\w$]*/;

/**
 * Bun's own test-file patterns, so a file the runner collects cannot escape the
 * scan by its name. `bun test` picks up `*.test.*`, `*_test.*`, `*.spec.*`, and
 * `*_spec.*`; a scan of `**‍/*.test.ts` alone left three of the four ungated.
 */
const TEST_FILES = "**/*{.,_}{test,spec}.{ts,tsx,js,jsx,mts,cts}";

/** Characters after which a `/` opens a regex literal rather than dividing. */
const BEFORE_REGEX = new Set(["(", ",", "=", ":", "[", "!", "&", "|", "?", "{", "}", ";", "+", "*", "%", "^", "~", "<", ">", "\n"]);

/** Everything one file's scan found, including whether it stayed in sync. */
interface FileScan {
  readonly sites: EachSite[];
  /** Set when quoting or bracketing desynchronized the walk. */
  readonly desync?: string;
}

/**
 * Advances past a comment, a quoted string, or a regex literal starting at
 * `at`; returns `at` unchanged when it starts none of them.
 *
 * A regex literal has to be recognized, not just balanced: `/[`]/` holds a lone
 * backtick, and treating it as a template literal made the walk consume the
 * rest of the file — every `each` site after it vanished, and the check reported
 * a clean tree. `outcome.past` is `-1` when a string or template ran to the end
 * of the source without closing, which is the desync a caller must fail on.
 */
function skipTrivia(source: string, at: number): { past: number; unterminated?: string } {
  if (source.startsWith("//", at)) {
    const end = source.indexOf("\n", at);
    return { past: end === -1 ? source.length : end };
  }
  if (source.startsWith("/*", at)) {
    const end = source.indexOf("*/", at + 2);
    return { past: end === -1 ? source.length : end + 2 };
  }
  if (source[at] === "/" && opensRegex(source, at)) {
    const end = endOfRegex(source, at);
    if (end === undefined) return { past: at + 1 };
    return { past: end };
  }
  const quote = source[at];
  if (quote !== '"' && quote !== "'" && quote !== "`") return { past: at };
  let cursor = at + 1;
  while (cursor < source.length) {
    const char = source[cursor];
    if (char === "\\") {
      cursor += 2;
      continue;
    }
    if (char === quote) return { past: cursor + 1 };
    // An unterminated single- or double-quoted string cannot cross a line; a
    // template literal can, so only the quoted forms stop at a newline.
    if (char === "\n" && quote !== "`") return { past: cursor };
    cursor += 1;
  }
  return { past: source.length, unterminated: quote === "`" ? "a template literal" : "a string" };
}

/** Whether the `/` at `at` opens a regex literal rather than dividing. */
function opensRegex(source: string, at: number): boolean {
  let back = at - 1;
  while (back >= 0 && (source[back] === " " || source[back] === "\t")) back -= 1;
  if (back < 0) return true;
  const previous = source[back]!;
  if (BEFORE_REGEX.has(previous)) return true;
  // `return /re/`, `typeof /re/`, `case /re/`: a keyword, not a value.
  return /\b(return|typeof|case|in|of|new|delete|void|do|else|yield|await)$/.test(
    source.slice(Math.max(0, back - 9), back + 1),
  );
}

/** End of the regex literal opening at `at`, past its flags. */
function endOfRegex(source: string, at: number): number | undefined {
  let cursor = at + 1;
  let inClass = false;
  while (cursor < source.length) {
    const char = source[cursor];
    if (char === "\\") {
      cursor += 2;
      continue;
    }
    if (char === "\n") return undefined;
    if (char === "[") inClass = true;
    else if (char === "]") inClass = false;
    else if (char === "/" && !inClass) {
      cursor += 1;
      while (cursor < source.length && /[a-z]/.test(source[cursor]!)) cursor += 1;
      return cursor;
    }
    cursor += 1;
  }
  return undefined;
}

/** Every `each` call in one file, with the shape and title of each. */
function scan(source: string): FileScan {
  const sites: EachSite[] = [];
  const declarations = declaredArrays(source);
  let at = 0;
  while (at < source.length) {
    const trivia = skipTrivia(source, at);
    if (trivia.unterminated !== undefined) {
      return { sites, desync: trivia.unterminated };
    }
    if (trivia.past > at) {
      at = trivia.past;
      continue;
    }
    if (source.startsWith(".each", at)) {
      const site = read(source, at, declarations);
      if (site !== undefined) sites.push(site);
      at += ".each".length;
      continue;
    }
    at += 1;
  }
  return { sites };
}

/**
 * Same-file `const NAME = [ … ]` initializers, by name.
 *
 * The convention's own example hoists the table into a named `const`, so
 * without this every conforming table classifies as `"expression"` and the
 * positional rule has no site to apply to — the rule would be vacuously
 * satisfied by the very tree it guards.
 */
function declaredArrays(source: string): Record<string, string> {
  const found: Record<string, string> = {};
  const declaration = /\bconst\s+([A-Za-z_$][\w$]*)\s*(?::[^=]*)?=\s*(?=\[)/g;
  for (const match of source.matchAll(declaration)) {
    const open = match.index + match[0].length;
    const close = matching(source, open, "[", "]");
    if (close === undefined) continue;
    found[match[1]!] = source.slice(open, close + 1);
  }
  return found;
}

/** Reads one `each` site whose `.each` begins at `at`. */
function read(
  source: string,
  at: number,
  declarations: Record<string, string>,
): EachSite | undefined {
  const kind = callee(source, at);
  if (kind === undefined) return undefined;

  // `test.each<[string, number]>([…])`: a type argument list sits between the
  // token and its call, and skipping it is what keeps the typed spelling of the
  // forbidden shape visible.
  let open = at + ".each".length;
  while (open < source.length && /\s/.test(source[open]!)) open += 1;
  if (source[open] === "<") {
    const shut = matching(source, open, "<", ">");
    if (shut === undefined) {
      return unreadable(source, at, kind, "a type argument list that does not close");
    }
    open = shut + 1;
    while (open < source.length && /\s/.test(source[open]!)) open += 1;
  }
  if (source[open] !== "(") return undefined;

  const close = matching(source, open, "(", ")");
  if (close === undefined) {
    return unreadable(source, at, kind, "an argument list that does not close");
  }
  const argument = source.slice(open + 1, close).trim();
  const { rows, columns, note } = classify(argument, declarations);
  const title = titleOf(source, close + 1);
  const stripped = title?.replaceAll("%%", "") ?? "";

  return {
    file: "",
    line: lineOf(source, at),
    kind,
    rows,
    ...(columns === undefined ? {} : { columns }),
    ...(title === undefined ? {} : { title }),
    ...(note === undefined ? {} : { note }),
    printfTitle: title !== undefined && PLACEHOLDER.test(stripped),
    fieldTitle: title !== undefined && FIELD_TITLE.test(stripped),
  };
}

/** A site whose shape could not be established, which is itself a finding. */
function unreadable(source: string, at: number, kind: string, note: string): EachSite {
  return {
    file: "",
    line: lineOf(source, at),
    kind,
    rows: "unreadable",
    note,
    printfTitle: false,
    fieldTitle: false,
  };
}

/** 1-indexed line holding the offset `at`. */
function lineOf(source: string, at: number): number {
  let line = 1;
  for (let cursor = 0; cursor < at; cursor += 1) if (source[cursor] === "\n") line += 1;
  return line;
}

/**
 * The `test`, `it`, or `describe` that owns the `.each` at `at`.
 *
 * Whitespace between the token and the property is skipped, so a chain broken
 * across lines by a formatter is still attributed rather than dropped.
 */
function callee(source: string, at: number): string | undefined {
  let back = at;
  while (back > 0 && /\s/.test(source[back - 1]!)) back -= 1;
  const before = source.slice(Math.max(0, back - 48), back);
  const match = /\b(test|it|describe)((?:\s*\.\s*(?:only|skip|skipIf|todo|todoIf|failing|if|each))*)$/.exec(
    before,
  );
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
    const trivia = skipTrivia(source, at);
    if (trivia.unterminated !== undefined) return undefined;
    if (trivia.past > at) {
      at = trivia.past;
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
function classify(
  argument: string,
  declarations: Record<string, string>,
): { rows: RowShape; columns?: number; note?: string } {
  const resolved = resolve(argument, declarations);
  if (resolved === undefined) return { rows: "expression" };
  if (!resolved.startsWith("[")) return { rows: "expression" };

  const end = matching(resolved, 0, "[", "]");
  if (end === undefined) {
    return { rows: "unreadable", note: "an array literal that does not close" };
  }

  const elements = split(resolved.slice(1, end)).map((element) =>
    stripLeadingTrivia(element),
  );
  if (elements.length === 0) return { rows: "scalar" };

  const spread = elements
    .filter((element) => element.startsWith("..."))
    .map((element) => resolve(element.slice(3).trim(), declarations));
  const rowTexts = [
    ...elements.filter((element) => !element.startsWith("...")),
    ...spread.flatMap((text) =>
      text === undefined || !text.startsWith("[")
        ? []
        : split(text.slice(1, matching(text, 0, "[", "]") ?? text.length - 1)).map(
            stripLeadingTrivia,
          ),
    ),
  ];
  const unresolvedSpread = spread.some((text) => text === undefined || !text.startsWith("["));

  // Widest row, not the first: a table whose first row is narrow and whose
  // later rows are wide is still positional, and counting only the first let it
  // through as a one-column table.
  const arrayRows = rowTexts.filter((element) => element.startsWith("["));
  if (arrayRows.length > 0) {
    const widths = arrayRows.map((row) => {
      const inner = matching(row, 0, "[", "]");
      return inner === undefined ? 1 : split(row.slice(1, inner)).length;
    });
    return { rows: "positional", columns: Math.max(...widths) };
  }
  if (rowTexts.some((element) => element.startsWith("{"))) return { rows: "object" };
  if (unresolvedSpread) {
    return { rows: "unknown", note: "a spread this scan cannot resolve" };
  }
  if (rowTexts.every((element) => isScalar(element))) return { rows: "scalar" };
  return { rows: "unknown", note: "rows that are neither literals nor resolvable" };
}

/**
 * Follows a bare identifier to its same-file `const` initializer.
 *
 * Only a bare name: `X.map(…)` produces rows whose shape is the callback's
 * return value, which this scan does not read, so it stays an expression and is
 * held to the field-title rule instead.
 */
function resolve(argument: string, declarations: Record<string, string>): string | undefined {
  const trimmed = argument.trim();
  if (/^[A-Za-z_$][\w$]*$/.test(trimmed)) return declarations[trimmed];
  return trimmed;
}

/** Whether a row is a single value rather than a tuple or an object. */
function isScalar(element: string): boolean {
  if (element.length === 0) return false;
  const head = element[0]!;
  if (head === "[" || head === "{") return false;
  return /^["'`\-+\d]/.test(head) || /^(true|false|null|undefined)\b/.test(element);
}

/** Drops comments and whitespace before a row's first real token. */
function stripLeadingTrivia(element: string): string {
  let at = 0;
  for (;;) {
    while (at < element.length && /\s/.test(element[at]!)) at += 1;
    if (element.startsWith("//", at)) {
      const end = element.indexOf("\n", at);
      if (end === -1) return "";
      at = end + 1;
      continue;
    }
    if (element.startsWith("/*", at)) {
      const end = element.indexOf("*/", at + 2);
      if (end === -1) return "";
      at = end + 2;
      continue;
    }
    return element.slice(at);
  }
}

/** Splits an array literal's body at its top-level commas. */
function split(body: string): string[] {
  const parts: string[] = [];
  let depth = 0;
  let start = 0;
  let at = 0;
  while (at < body.length) {
    const trivia = skipTrivia(body, at);
    if (trivia.unterminated !== undefined) break;
    if (trivia.past > at) {
      at = trivia.past;
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
  const trivia = skipTrivia(source, at);
  if (trivia.unterminated !== undefined) return undefined;
  return source.slice(at + 1, trivia.past - 1);
}

/** Every `each` site under `root`, in file and line order. */
export function scanTestTree(root: string): readonly EachSite[] {
  const files = [...new Glob(TEST_FILES).scanSync(root)].sort();
  return files.flatMap((file) => {
    // Read synchronously so the check stays a plain assertion rather than an
    // async one; the whole tree is a few hundred kilobytes.
    const source = readFileSync(join(root, file), "utf8");
    const { sites, desync } = scan(source);
    const own = sites.map((site) => ({ ...site, file }));
    if (desync === undefined) return own;
    // A walk that lost sync would otherwise report every site after the
    // desync as absent, which reads exactly like a clean file.
    return [
      ...own,
      {
        file,
        line: 1,
        kind: "file",
        rows: "unreadable" as const,
        note: `${desync} that never closes, so the scan of this file stopped early`,
        printfTitle: false,
        fieldTitle: false,
      },
    ];
  });
}

/**
 * The sites that do not conform.
 *
 * Four rules, each about something a reader of one row cannot otherwise tell.
 *
 * 1. A row that is an array of two or more values is positional: its meaning is
 *    its position, so the reader counts columns.
 * 2. `object` and `expression` rows must take their title from a case field. An
 *    object rendered by `%s` names nothing, and a `.map` tells this scan nothing
 *    at all about its rows, so a table built that way has to name its own cases.
 * 3. `unknown` rows — an array literal whose elements are opaque — are judged by
 *    how many placeholders the title carries. One placeholder renders one value
 *    per case, which is exactly the single-column contract, and `[...MARKERS]`
 *    or `[MAX - 1, MAX, MAX + 1]` is a legitimate scalar table this scan cannot
 *    prove. Two or more placeholders mean the row must be a tuple, so the
 *    positional rule applies even though the elements cannot be read.
 * 4. Anything this scan could not read at all is reported rather than passed,
 *    because silence and conformance must not look alike.
 *
 * The residual gap, stated rather than papered over: an opaque array of tuples
 * under a *single*-placeholder title passes rule 3, and only review will catch
 * it. Closing it would cost every legitimate site in rule 3's own example.
 */
export function violations(sites: readonly EachSite[]): readonly Violation[] {
  const found: Violation[] = [];
  for (const site of sites) {
    if (site.rows === "unreadable") {
      found.push({
        site,
        reason: `this check could not read ${site.note ?? "the site"} — unreadable is treated as unconforming, so fix the source or teach the scanner`,
      });
      continue;
    }
    if (site.rows === "positional" && (site.columns ?? 1) > 1) {
      found.push({
        site,
        reason:
          `a positional table of ${site.columns} columns: give each case a named ` +
          `field and title the run "$scenario"`,
      });
      continue;
    }
    if (site.rows === "scalar" || site.fieldTitle) continue;
    const placeholders = countPlaceholders(site.title);
    if (site.rows === "unknown" && placeholders === 1) continue;
    const titled =
      site.title === undefined
        ? "a title this check cannot read"
        : placeholders > 1
          ? `the ${placeholders}-placeholder title ${JSON.stringify(site.title)}`
          : site.printfTitle
            ? `the printf title ${JSON.stringify(site.title)}`
            : `the fixed title ${JSON.stringify(site.title)}`;
    found.push({
      site,
      reason:
        `${site.rows} rows under ${titled}: only a table this check can see to be ` +
        `single-column may name a case by rendering it — title this run "$scenario"`,
    });
  }
  return found;
}

/** How many printf placeholders a title carries, `%%` excluded. */
function countPlaceholders(title: string | undefined): number {
  if (title === undefined) return 0;
  return [...title.replaceAll("%%", "").matchAll(/%[sdifjop#]/g)].length;
}

/** One line per violation, as a failure message prints it. */
export function report(found: readonly Violation[]): string {
  return found
    .map(({ site, reason }) => `  ${site.file}:${site.line} (${site.kind}.each): ${reason}`)
    .join("\n");
}

/**
 * Test names that more than one case in the same file reports.
 *
 * The one property no text scan can check: two cases can be perfectly shaped
 * and still report under one name, and then a failure identifies neither. Read
 * from the names the runner actually printed, so hoisting, generics, `.map`,
 * and every other spelling are irrelevant. Scoped per file, because two files
 * are free to name a test the same thing.
 */
export function duplicateNames(junit: string): readonly DuplicateName[] {
  const counts: Record<string, number> = {};
  const cases = junit.matchAll(/<testcase\b([^>]*)>/g);
  for (const [, attributes] of cases) {
    const file = /\bclassname="([^"]*)"/.exec(attributes ?? "")?.[1] ?? "";
    const name = /\bname="([^"]*)"/.exec(attributes ?? "")?.[1] ?? "";
    const key = `${file}\u0000${name}`;
    counts[key] = (counts[key] ?? 0) + 1;
  }
  return Object.entries(counts)
    .filter(([, count]) => count > 1)
    .map(([key, count]) => {
      const [file = "", name = ""] = key.split("\u0000");
      return { file, name, count };
    })
    .sort((a, b) => a.file.localeCompare(b.file) || a.name.localeCompare(b.name));
}

/**
 * Test names carrying a printf placeholder the runner never substituted.
 *
 * A title with more placeholders than the case has columns renders literally,
 * so the name states `%d` where a value belongs.
 */
export function unsubstitutedNames(junit: string): readonly string[] {
  const names = [...junit.matchAll(/<testcase\b[^>]*\bname="([^"]*)"/g)].map(
    (match) => match[1] ?? "",
  );
  return names.filter((name) => PLACEHOLDER.test(name.replaceAll("%%", "")));
}
