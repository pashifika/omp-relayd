/**
 * The shipped `omp-relay` skill, checked against the capability it implements.
 *
 * `relay-collaboration-workflow` is normative prose: it requires the skill to
 * *state* things, and no runtime assertion can prove a model followed them. The
 * design records that split deliberately — the enforceable half is the `join`
 * result contract and the guards, covered elsewhere; this file covers the
 * inspectable half.
 *
 * So this is an inspection, not a verdict. Every obligation prints the sentence
 * of the skill that satisfies it, because a bare green tick would hide both
 * failure modes at once: a requirement whose matcher is too loose to mean
 * anything, and a requirement whose matcher happens to catch unrelated text.
 * A reader of the output can re-derive the conclusion instead of trusting it.
 *
 * The frontmatter check mirrors the host's own validator
 * (`discovery/agent-plugin-format.ts`), so a skill the host would refuse to load
 * fails here rather than after an operator has installed it.
 */

import { describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { basename, dirname, join } from "node:path";

import { REPO_ROOT } from "../support/paths.ts";

const SKILL_DIR = join(REPO_ROOT, "extension", "skill", "omp-relay");
const SKILL_PATH = join(SKILL_DIR, "SKILL.md");

/** The six fields the Agent Skills frontmatter schema is closed to. */
const SKILL_FIELDS: Record<string, true> = {
  name: true,
  description: true,
  license: true,
  compatibility: true,
  metadata: true,
  "allowed-tools": true,
};

const source = readFileSync(SKILL_PATH, "utf8");

/** Splits the leading `---` block from the body, without a YAML dependency. */
function split(text: string): { frontmatter: Record<string, unknown>; body: string } {
  const match = /^---\n([\s\S]*?)\n---\n([\s\S]*)$/.exec(text);
  if (match === null) {
    throw new Error(`${SKILL_PATH} has no leading frontmatter block`);
  }
  const parsed: unknown = Bun.YAML.parse(match[1] as string);
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
    throw new Error(`${SKILL_PATH} frontmatter is not a mapping`);
  }
  return { frontmatter: parsed as Record<string, unknown>, body: match[2] as string };
}

const { frontmatter, body } = split(source);

/**
 * One obligation of `relay-collaboration-workflow`, and the pattern that finds
 * the text carrying it.
 *
 * Patterns are deliberately anchored on the distinguishing words of each
 * obligation rather than on a heading, so moving a section does not silently
 * turn a real check into a check of the table of contents.
 */
interface Obligation {
  /** The requirement heading in `specs/relay-collaboration-workflow/spec.md`. */
  readonly requirement: string;
  /** The specific thing the skill must state. */
  readonly states: string;
  readonly pattern: RegExp;
}

const OBLIGATIONS: readonly Obligation[] = [
  {
    requirement: "The skill joins before it addresses anyone",
    states: "join is the first call, before listing or sending",
    pattern: /`mesh\(action: "join"\)` is the first call\.[^\n]*\n[^\n]*/,
  },
  {
    requirement: "The skill joins before it addresses anyone",
    states: "the common two-component selector maps to task and as, with no project",
    pattern:
      /\| `<task>\/<peer>` \|[^|]+\| `mesh\(action: "join", task: "<task>", as: "<peer>"\)` \|/,
  },
  {
    requirement: "The skill joins before it addresses anyone",
    states: "the three-component selector maps all three to join parameters",
    pattern:
      /\| `<project>\/<task>\/<peer>` \|[^|]+\| `mesh\(action: "join", project: "<project>", task: "<task>", as: "<peer>"\)` \|/,
  },
  {
    requirement: "The skill joins before it addresses anyone",
    states: "the two-component form leaves the project to the file or the root's directory name",
    pattern:
      /The two-component form passes no `project` at all\.[\s\S]{0,200}?directory name when no file names\n?one/,
  },
  {
    requirement: "The skill joins before it addresses anyone",
    states: "only two or three non-empty components are a selector, and nothing is guessed",
    pattern:
      /\*\*Exactly two or three non-empty components are\n?a selector\.\*\*[\s\S]{0,200}?invented component\./,
  },
  {
    requirement: "The skill joins before it addresses anyone",
    states: "a selector names this session's local peer, not a recipient",
    pattern:
      /A selector is not an address\.[\s\S]{0,400}?peer component of a selector\./,
  },
  {
    requirement: "The skill joins before it addresses anyone",
    states: "no selector means every value resolves from configuration and derivation",
    pattern: /Omit `project`, `task`, and `as` entirely and let all three resolve\./,
  },
  {
    requirement: "The skill joins before it addresses anyone",
    states: "a selector or a derived project is reported with the room, local peer, and sources",
    pattern:
      /\*\*Report the resolved room, this session's peer name, and each source whenever\nthe operator supplied a selector or the project came from `derivation`\.\*\*/,
  },
  {
    requirement: "The skill joins before it addresses anyone",
    states: "a derived project makes two differently renamed clones two rooms",
    pattern:
      /`omp-relayd\/two-machine-check` and `relayd\/two-machine-check` are two rooms/,
  },
  {
    requirement: "The skill joins before it addresses anyone",
    states: "a project-file room with no selector needs no confirmation",
    pattern:
      /When the room came entirely from the project file and no selector was supplied,\nno confirmation is needed[^.]*\. Proceed\./,
  },
  {
    requirement: "An informal peer reference is resolved against the roster",
    states: "an informal reference is not a peer name",
    pattern: /is not a peer name\. Peer names are what the roster reports[^\n]*/,
  },
  {
    requirement: "An informal peer reference is resolved against the roster",
    states: "the roster is consulted before sending",
    pattern: /\*\*list, then match, then send\.\*\*/,
  },
  {
    requirement: "An informal peer reference is resolved against the roster",
    states: "any identifier-valid string is accepted, so an unmatched name looks offline",
    pattern:
      /accepts \*any\* string that satisfies the identifier rules as a target\.[\s\S]{0,220}?consult it first\./,
  },
  {
    requirement: "An informal peer reference is resolved against the roster",
    states: "an unmatched reference is raised rather than sent",
    pattern: /\*\*No match\*\* →[\s\S]{0,160}?guess a spelling\./,
  },
  {
    requirement: "An informal peer reference is resolved against the roster",
    states: "an ambiguous reference is raised rather than guessed",
    pattern: /\*\*More than one consistent match\*\* →[\s\S]{0,120}?Do not choose\./,
  },
  {
    requirement: "A session alone in its room stops instead of sending",
    states: "a lone session stops, names the room, and asks about the other end",
    pattern: /\*\*stop and tell the operator\*\*\.[\s\S]{0,180}?rooms actually match\./,
  },
  {
    requirement: "A session alone in its room stops instead of sending",
    states: "an offline receipt conflates three causes",
    pattern: /conflates three\ndifferent situations:\n\n(?:- [^\n]+\n){3}/,
  },
  {
    requirement: "A briefing sent to a remote agent is self-contained",
    states: "the recipient shares no context with the sender",
    pattern: /shares no context with this one\.[\s\S]{0,200}?unanswerable\./,
  },
  {
    requirement: "A briefing sent to a remote agent is self-contained",
    states: "a briefing carries repository and revision, steps, artifact, and criterion",
    pattern:
      /1\. \*\*Repository and revision\*\*[\s\S]*?4\. \*\*Acceptance criterion\*\*[\s\S]{0,120}?is done\./,
  },
  {
    requirement: "A briefing sent to a remote agent is self-contained",
    states: "the recipient is an autonomous agent, so a briefing is a request",
    pattern: /autonomous agent with its own operator, not a subprocess\. A\nbriefing is a request\./,
  },
  {
    requirement: "A briefing sent to a remote agent is self-contained",
    states: "a reply carries the received identifier as `reply_to`",
    pattern: /set `reply_to` to the identifier of the\nmessage you are answering\./,
  },
  {
    requirement: "The skill states what a receipt does and does not mean",
    states: "routed means the relay queued the frame",
    pattern: /A `routed` receipt means \*\*[^*]+\*\*\./,
  },
  {
    requirement: "The skill states what a receipt does and does not mean",
    states: "routed does not mean read, accepted, or completed",
    pattern: /It does not mean the peer read it, accepted it[^\n]*/,
  },
  {
    requirement: "The skill states what a receipt does and does not mean",
    states: "a reply arrives later as its own directed inbound message",
    pattern: /A reply arrives later as its own directed inbound message, which starts or\nsteers a turn\./,
  },
  {
    requirement: "The skill states what a receipt does and does not mean",
    states: "the session does not wait for a reply",
    pattern: /\*\*Do not wait for a reply\.\*\*[^\n]*\n[^\n]*/,
  },
  {
    requirement: "The skill chooses between announcing and addressing one peer",
    states: "the two classes are chosen by the content rather than roster size",
    pattern: /two delivery classes, chosen by what the content is — not by how many\npeers happen to be present:/,
  },
  {
    requirement: "The skill chooses between announcing and addressing one peer",
    states: "directed work names one peer and shared information is announced once",
    pattern: /Use `send` for work one peer must do\.[\s\S]{0,300}?Use `announce` once for information every peer needs in order not to collide:/,
  },
  {
    requirement: "The skill chooses between announcing and addressing one peer",
    states: "the peer and room address forms are distinct and neither is on the wire",
    pattern: /`<project>\/<task>@<peer>`[\s\S]{0,260}?`<project>\/<task>`[\s\S]{0,260}?Neither combined address is written on the wire:/,
  },
  {
    requirement: "The skill chooses between announcing and addressing one peer",
    states: "no peer name is reserved to mean everyone",
    pattern: /No peer name is reserved\nto mean "everyone"; an announcement carries no target field at all\./,
  },
  {
    requirement: "The skill chooses between announcing and addressing one peer",
    states: "an announcement excludes its author and is not awaited as confirmation",
    pattern: /An announcement never reaches its author\.[\s\S]{0,180}?do not wait to see the notice itself/,
  },
  {
    requirement: "The skill states what a receipt does and does not mean",
    states: "accepted carries delivered and shed queue counts rather than a status",
    pattern: /An announcement's `accepted` reply carries counts instead of one status:[\s\S]{0,220}?`delivered`[\s\S]{0,160}?`shed`/,
  },
  {
    requirement: "The skill states what a receipt does and does not mean",
    states: "shed means not reading and must not be retried blindly",
    pattern: /A shed count is not an invitation to retry blindly\.[\s\S]{0,150}?queue is already full\./,
  },
  {
    requirement: "The skill states what a receipt does and does not mean",
    states: "zero deliveries is an empty room rather than an error",
    pattern: /Zero deliveries and\nzero shed means the room held nobody else; it is an empty-room observation, not\nan error\./,
  },
  {
    requirement: "The skill states what a receipt does and does not mean",
    states: "an inbound announcement starts idle and waits during a run",
    pattern: /When this\nsession is idle, it starts a turn[\s\S]{0,160}?waits for that run to finish rather than aborting or steering it\./,
  },
  {
    requirement: "The skill sends large work as an attachment rather than as prose",
    states: "an oversized payload travels as an attachment rather than in the body",
    pattern: /A message body is capped\.[\s\S]{0,220}?all wrong:/,
  },
  {
    requirement: "The skill sends large work as an attachment rather than as prose",
    states: "truncating, splitting, and naming a local path are each refused by name",
    pattern:
      /\*\*Do not truncate it\.\*\*[\s\S]*?\*\*Do not split it across messages\.\*\*[\s\S]*?\*\*Do not name a local path\.\*\*/,
  },
  {
    requirement: "The skill sends large work as an attachment rather than as prose",
    states: "the recipient may be on another machine, so a path is not a shared reference",
    pattern:
      /may be on a different machine, so\n\s*a path is not a shared reference even when both sessions are working on the\n\s*same repository/,
  },
  {
    requirement: "The skill sends large work as an attachment rather than as prose",
    states: "the body explains and the attachment carries",
    pattern:
      /\*\*The body explains; the attachment carries\.\*\*[\s\S]{0,320}?before\*? ?deciding whether to transfer\n?anything\./,
  },
  {
    requirement: "The skill treats a reference as expiring and a fetch as deliberate",
    states: "the sender is told the lifetime and states it in the body",
    pattern:
      /The result of a successful attachment\nstates how long the relay will hold it\. Say so in the body/,
  },
  {
    requirement: "The skill treats a reference as expiring and a fetch as deliberate",
    states: "an unavailable payload is expiry, and the response is a resend rather than a retry",
    pattern:
      /\*\*expiry, not\nfailure\*\*\.[\s\S]{0,200}?Ask the sender to send it again\./,
  },
  {
    requirement: "The skill treats a reference as expiring and a fetch as deliberate",
    states: "an inbound attachment has not been downloaded and fetching is explicit",
    pattern:
      /\*\*An inbound attachment has not been downloaded\.\*\*[\s\S]{0,220}?explicit act:/,
  },
  {
    requirement: "The skill treats a reference as expiring and a fetch as deliberate",
    states: "the session decides whether it needs the payload, and uses the ceiling when unsure",
    pattern:
      /Decide whether you need it before fetching\.[\s\S]{0,280}?pass a ceiling — the size is reported and nothing\nis transferred when it is exceeded/,
  },
  {
    requirement: "The skill treats a reference as expiring and a fetch as deliberate",
    states: "a fetch yields a path used with ordinary tools, not content in the result",
    pattern:
      /A fetch yields \*\*a file path, not content\*\*\.[\s\S]{0,260}?that is what a path exists to avoid\./,
  },
  {
    requirement: "The skill states what a refused reservation means",
    states: "the three bounds are named with the different response each calls for",
    pattern:
      /\| `payload_too_large` \|[\s\S]*?\| `room_full` \|[\s\S]*?\| `store_full` \|[^\n]*\n/,
  },
  {
    requirement: "The skill states what a refused reservation means",
    states: "a refusal sent nothing, so resending after addressing the bound is not a duplicate",
    pattern:
      /\*\*nothing was sent\*\*\.[\s\S]{0,260}?sending again is correct rather than a duplicate\./,
  },
];

/** One-line rendering of a matched excerpt, so a table row stays a row. */
function excerpt(text: string, limit = 150): string {
  const flat = text.replace(/\s+/gu, " ").trim();
  return flat.length > limit ? `${flat.slice(0, limit)}…` : flat;
}

describe("the shipped omp-relay skill", () => {
  test("the host would load it: closed frontmatter, matching name, usable description", () => {
    const unexpected = Object.keys(frontmatter).filter((key) => SKILL_FIELDS[key] !== true);
    expect(unexpected).toEqual([]);

    // The host requires `name` to equal the containing directory, and the
    // native provider is registered with `requireDescription: true`.
    expect(frontmatter["name"]).toBe(basename(SKILL_DIR));
    expect(dirname(SKILL_PATH)).toBe(SKILL_DIR);

    const description = frontmatter["description"];
    expect(typeof description).toBe("string");
    const text = description as string;
    expect(text.trim().length).toBeGreaterThan(0);
    expect(text.length).toBeLessThanOrEqual(1024);

    console.log(
      `frontmatter fields: ${Object.keys(frontmatter).join(", ")}; ` +
        `name=${JSON.stringify(frontmatter["name"])} matches directory ${JSON.stringify(basename(SKILL_DIR))}; ` +
        `description ${text.length} of 1024 characters`,
    );
  });

  test("every relay-collaboration-workflow obligation is carried by observable text", () => {
    const missing: string[] = [];
    const observed: string[] = [];

    for (const obligation of OBLIGATIONS) {
      const match = obligation.pattern.exec(body);
      if (match === null) {
        missing.push(`${obligation.requirement} — ${obligation.states}`);
        continue;
      }
      observed.push(`  ${obligation.states}\n    ${excerpt(match[0])}`);
    }

    // Printed before the assertion so the evidence survives a failure.
    console.log(
      `skill obligations observed (${observed.length} of ${OBLIGATIONS.length}):\n${observed.join("\n")}`,
    );
    expect(missing).toEqual([]);
  });

  test("the obligations cover every requirement the capability states", () => {
    // Guards the table itself. Losing a row here would quietly shrink the
    // inspection to whatever remained, and the test above would still pass.
    const requirements = [...new Set(OBLIGATIONS.map((entry) => entry.requirement))];
    expect(requirements).toHaveLength(9);
    console.log(`requirements represented in the table:\n${requirements.map((r) => `  ${r}`).join("\n")}`);
  });
});
