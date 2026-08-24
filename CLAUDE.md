# Repository Guidelines

## Authority and scope

`rasen/docs/omp-relay-messagepack-protocol-design.md` is the tracked protocol
baseline and the source of truth for the wire format, framing, message set,
configuration shape, and transport scope. `rasen/specs/` holds the accepted
capability specifications; a requirement there outranks any description in a
change proposal. Treat the rules in this file as the default project guidance.

When implementation changes a normative decision, update the protocol design
record or the affected specification in the same change rather than leaving the
two to disagree.

## Product definition

OMP Relay is a cross-terminal peer relay for OMP extension clients. The relay
server is a Rust daemon in `server/`; the TypeScript client library and the OMP
extension live in `extension/` and run under Bun.

Fixed decisions:

- **MessagePack on the wire**, encoded as maps with string field names, never as
  positional tuples. Rust uses `rmp_serde::to_vec_named` or `with_struct_map()`.
- **YAML for human-edited configuration only.** Never YAML on the wire.
- **No second production codec.** JSON is permitted only in tests, logs, and
  diagnostic tooling.
- Frames are a `u32` big-endian payload length followed by that many bytes.
- The initial release supports `local` transport only: plain TCP, no TLS, no
  authentication, trusted host or trusted private LAN. `private` transport with
  mTLS is a later release.
- The server routes frames and holds nothing else in memory: no message history,
  no queue for an absent peer, no replay after a reconnect.
- **The one exception is the payload store, and it is not persistence.** A
  payload too large for a message body is uploaded by reference and held as a
  bounded, room-scoped set of content-addressed temporary files, reachable over
  HTTP on the same listener as the frames. Every payload is removed when its room
  empties, when its time to live elapses, or at startup, and nothing survives a
  restart. The ceilings and the lifetime are code constants; the configuration
  surface stays at two values.

Out of scope: public Internet deployment, and any integration with OMP's
built-in `hub` or internal IRC implementation.

## Two repositories in one working tree

This tree contains two independent Git repositories, and conflating them is the
easiest mistake to make here.

- The outer repository tracks code and delivery: `.github/`, `server/`,
  `extension/`, `Dockerfile`, `compose.yml`, `test-fixtures/`.
- `rasen/` is a **separate repository with its own remote**, holding planning
  artifacts: `changes/`, `specs/`, `docs/`.

Never stage `rasen/` from the outer repository — it must stay untracked there.
Commit planning work with `git -C rasen`. Code and planning changes are separate
commits in separate repositories, and neither belongs in the other's history.

## Git workflow

`main` is protected by `.github/rulesets/main.json`: direct pushes, force
pushes, and branch deletion are all rejected.

Before implementation begins, create a short-lived topic branch from `main`.
Never commit implementation directly to `main`; the forge will refuse the push,
so discovering this late only wastes the work of rewinding.

Name the branch `<type>/<short-slug>` using the Conventional Commits type that
will dominate the change — `feat/relay-handshake`, `fix/frame-length-guard`,
`ci/rust-job`. Land it through a pull request. **The merge commit is the only
method the ruleset permits**, so there is no choice to make and no squash to
prefer; unresolved review threads block the merge. Write commit messages as
Conventional Commits. The agent chooses coherent commit boundaries.

That restriction is what makes a dependent pull request cheap. CI runs on every
pull request, including one whose base is another topic branch, so a chain may be
opened as a chain — `#1` onto `main`, `#2` onto `#1`'s branch, `#3` onto `#2`'s —
and each diff shows only its own change. Merge bottom-up; the forge retargets
each child as its parent lands. A squash would rewrite the commits the children
still carry, re-inflating every downstream diff and forcing a rebase per merge,
which is precisely why the method is pinned rather than recommended.

`strict_required_status_checks_policy` makes every child `BEHIND` as its parent
lands, so each one is updated and re-tested before it can merge. That cost is
the point rather than a defect: this tree holds two languages whose wire
contract is enforced only by the fixtures under `test-fixtures/protocol-v1/`,
and two pull requests that are each green alone can still break that contract
together. Do not relax the policy to make a stack cheaper.

## CI conventions

Branch protection requires exactly **one** status check, named `ci`, produced by
the gate job in `.github/workflows/ci.yml` that aggregates `needs.*.result` under
`if: always()`. Runtime job names never appear in `.github/rulesets/main.json`.

- Adding, removing, or restructuring a runtime job means editing the gate's
  `needs` and nothing else. Never add a job name to the ruleset.
- The gate job's `name` is coupled by string to the ruleset and nothing verifies
  the two agree. Renaming it blocks every merge with no failing job to point at.
- Bottom-up retargeting depends on `delete_branch_on_merge`, a repository
  setting that lives outside this repository — the committed ruleset cannot
  express it and nothing verifies it. With it off, the forge retargets nothing:
  each child keeps pointing at a merged branch and needs a manual retarget, a
  branch update, and a fresh `ci` run. It is on; if the workflow above ever
  stops matching what the forge does, check it first.
- **A merge queue is rejected, not merely unused.** It would remove the
  strict-policy branch updates a stack pays for, but it changes how `ci`
  reports — and that is the one coupling above which nothing verifies. Buying
  back a few minutes per stack by disturbing the check whose silent failure
  blocks every merge is the wrong trade.
- **Every `uses:` is pinned to a full 40-character commit SHA with a trailing
  version comment.** The `hygiene` job enforces both halves and fails naming the
  offending reference. A tag is mutable by its publisher; a bare SHA is
  unreviewable.
- Declare `permissions: contents: read`, check out with
  `persist-credentials: false`, and keep a concurrency group that cancels a
  superseded run.
- Never use `paths` filters on a job the gate depends on: a filtered-out job
  reports `skipped`, and the gate treats a skip as a failure.
- A language job belongs to the change that introduces its toolchain, because a
  job whose manifest does not exist yet cannot pass.

The ruleset is committed JSON and is authoritative. Change protection by editing
that file and reimporting it, never through the web interface. It targets
`~DEFAULT_BRANCH`, but `ci.yml` names `main` literally — if the default branch is
ever renamed, update the workflow triggers in the same commit or `ci` silently
stops reporting.

Nothing enforces that agreement, and a web-interface edit leaves the file
describing protection the forge no longer applies — worse than an out-of-date
file, because the documented remedy is to reimport it, which would silently
revert the live change. Diff the two before trusting either, sorting keys so
field order is not mistaken for drift:

```bash
gh api "repos/$OWNER/$REPO/rulesets/$ID" |
  jq -S '[.rules[] | {(.type): .parameters}] | add' > /tmp/live.json
jq -S '[.rules[] | {(.type): .parameters}] | add' .github/rulesets/main.json |
  diff - /tmp/live.json && echo "no drift"
```

## Testing and verification

Add deterministic tests for changed behavior. Cover the protocol at its
boundaries: frame length limits, partial and coalesced reads, unknown fields,
field-order independence across both languages, disconnect and reconnect,
heartbeat timeout, and correlation of replies to requests. The cross-language
fixtures under `test-fixtures/protocol-v1/` are the contract between the Rust
and TypeScript implementations; both directions must decode the other's output.

A verification that exists only as a report is a claim without a method. Retain
what was run, what it produced, and the revision it ran against, so a reader can
re-derive the conclusion instead of trusting it. A check asserting a structural
claim prints the values it observed rather than a bare verdict, because a bare
pass hides a mis-specified assertion in either direction.

State which checks were not run and why, so an absence reads as a stated
boundary rather than a gap. Never describe a check as passing unless it was
actually executed.
