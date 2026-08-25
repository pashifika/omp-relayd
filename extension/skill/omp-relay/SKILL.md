---
name: omp-relay
description: Hand work to one remote OMP session or announce shared information to a relay room through the `mesh` tool, attach material too large for a message body, and handle deliveries that arrive from either class. Use when the operator asks to reach another terminal, machine, peer, or room — "ask the Windows box", "tell everyone the schema landed", "send them the failing test's output", "see who else is connected" — or when a remote message or room announcement arrives, with or without an attachment. Covers joining, choosing between `send` and `announce`, resolving informal peer references, writing self-contained briefings, attaching and fetching payloads, and reading receipts, acceptance counts, and refusals correctly.
---

# OMP Relay collaboration

The `mesh` tool connects this session to other OMP sessions through a relay. Each
connection joins one **room**, rendered to an operator as `<project>/<task>`, and
every session in that room is a **peer** with a name.

There are two delivery classes, chosen by what the content is — not by how many
peers happen to be present:

- `send` addresses one peer. The operator-facing form is
  `<project>/<task>@<peer>`.
- `announce` addresses every other peer in the room. The operator-facing form is
  `<project>/<task>`, with no peer component.

Neither combined address is written on the wire: the connection already owns the
room, and a directed frame carries only the peer name. No peer name is reserved
to mean "everyone"; an announcement carries no target field at all.

The peers are autonomous agents with their own operators, machines, and
conversations. Everything below follows from that.

## 1. Join before you address anyone

`mesh(action: "join")` is the first call. Before it there is no connection, no
roster, and no way to send anything.

### The startup selector

An operator starting collaboration names the work and what to call this machine.
Two slash-delimited forms carry that. They are **startup selectors**: each one
chooses this session's room and its own local peer name.

| Selector | Means | Join call |
|---|---|---|
| `<task>/<peer>` | the common case: this task, and this session called this | `mesh(action: "join", task: "<task>", as: "<peer>")` |
| `<project>/<task>/<peer>` | the operator is overriding the project as well | `mesh(action: "join", project: "<project>", task: "<task>", as: "<peer>")` |

The two-component form passes no `project` at all. The project then resolves from
the project file, or from the project root's directory name when no file names
one — which is why two checkouts of the same repository need only agree on the
task. Use the three-component form when the folder name is not the room, as in a
monorepo, a renamed clone, or a room two different repositories share.

`/` and `@` are forbidden inside every identifier, so the components are
unambiguous and need no escaping. **Exactly two or three non-empty components are
a selector.** One component, four of them, or an empty component between two
slashes is not: report the two accepted forms and do not join with a guessed or
invented component.

A selector is not an address. `<task>/<peer>` names *this* session, and its peer
component becomes the name other operators see in their roster. The directed
address `<project>/<task>@<peer>` names *someone else*, is usable only after
joining, and only against a name the roster actually reports. Nothing is sent to
the peer component of a selector.

### When the operator supplied no selector

Omit `project`, `task`, and `as` entirely and let all three resolve. `as`
overrides this machine's peer name; leave it alone unless the operator asks for a
specific name.

### Report what resolved

The join result reports the resolved room, the resolved peer name, and the
**source** each came from — `parameter`, `project-file`, `global-file`, or
`derivation`.

**Report the resolved room, this session's peer name, and each source whenever
the operator supplied a selector or the project came from `derivation`.** Two
mistyped rooms are two *successful* joins that never meet, and an empty roster is
the only symptom. Two clones whose folders were renamed differently fail the same
way: `omp-relayd/two-machine-check` and `relayd/two-machine-check` are two rooms,
and each end derived its own in good faith. Saying "joined
`omp-relayd/two-machine-check` as `mac-worker`; the project came from the project
root's directory name" while both operators are present is what catches either.

When the room came entirely from the project file and no selector was supplied,
no confirmation is needed — both ends read the same committed file. Proceed.

Join again at any time to change room or peer name; it reconnects. Joining the
room you already hold changes nothing and reports current state.

## 2. Resolve an informal reference against the roster

An operator says "have the Windows box run the integration suite". `the Windows
box` is not a peer name. Peer names are what the roster reports — usually a
machine's host name, such as `win-desktop` or `MacBook-Pro`.

So: **list, then match, then send.**

The join result already carries the roster. Use `mesh(action: "list")` to refresh
it before sending if time has passed.

The relay accepts *any* string that satisfies the identifier rules as a target.
Sending to `the-windows-box` does not fail as a bad name — it returns
`peer_offline`, exactly as a real peer that went away would. The roster is the
only thing that distinguishes them, so consult it first.

- **One consistent match** → send to that entry's exact name.
- **No match** → report the roster and the operator's reference back to them.
  Do not send. Do not guess a spelling.
- **More than one consistent match** → report the candidates and ask which.
  Do not choose.

## 3. Alone in the room? Distinguish work from information

When work is intended for another peer and the roster contains only this
session, **stop and tell the operator**. Name the resolved room and ask whether
the other end has joined yet, or whether the two rooms actually match.

Sending directed work anyway returns `peer_offline`, which conflates three
different situations:

- the peer is not running;
- the peer is running under a different name;
- the peer is in a different room.

Reporting "nobody else is in `acme/pr-471`" is precise. Forwarding an offline
receipt is not.

An announcement into the same empty room is different: `delivered: 0, shed: 0`
is a successful answer saying nobody else was present. Report the empty room,
not a failed announcement.

## 4. Write a briefing the recipient can act on

The receiving session shares no context with this one. It cannot see this
repository, this conversation, the file just edited, or the error just read. A
message that says "can you check the thing we discussed" is unanswerable.

Every briefing carries four things:

1. **Repository and revision** — where the work applies, precisely enough to
   check out: remote or name, plus branch or commit.
2. **Steps** — what to do, concretely.
3. **Expected artifact** — what to send back: a command's output, a diff, a
   verdict, a file's contents.
4. **Acceptance criterion** — how the result will be judged, so the recipient
   knows when it is done.

The recipient is an autonomous agent with its own operator, not a subprocess. A
briefing is a request. It may be declined, and its operator may be doing
something else.

```text
mesh(action: "send", to: "win-desktop", message: """
Repository: github.com/acme/widget, branch feat/win-paths at 3f2a9c1.
Please run: cargo test --locked -p widget-core -- --nocapture path::tests
Return: the full output of that command, plus the value of
`std::env::consts::OS` it printed.
Accepted when: the three path tests pass or you report which one failed and
its assertion message.
Context: they pass on macOS; I cannot reproduce a Windows path separator here.
""")
```

**Answering a message you received:** set `reply_to` to the identifier of the
message you are answering.

```text
mesh(action: "send", to: "MacBook-Pro", message: "...", reply_to: "<the id from the inbound message>")
```

Without `reply_to` the initiator cannot correlate your answer with what it asked,
which matters most when it asked several things.

## 5. Read receipts and acceptance counts correctly

A `routed` receipt means **the relay placed the frame in the named peer's
queue**.

It does not mean the peer read it, accepted it, agreed to it, started it, or
finished it.

An announcement's `accepted` reply carries counts instead of one status:

- `delivered` is how many other peers had the notice placed in their queue.
- `shed` is how many addressed peers refused it because they were not reading
  their connection. It does not name which peers.

A shed count is not an invitation to retry blindly. Resending immediately adds
to a room containing a peer whose queue is already full. Zero deliveries and
zero shed means the room held nobody else; it is an empty-room observation, not
an error.

After either a routed receipt or an acceptance:

- Report **queued**, never read or completed.
- Continue with other work.
- **Do not wait for a reply.** Do not poll, loop, or hold the turn open. There
  is no `wait` action.

A reply arrives later as its own directed inbound message, which starts or
steers a turn. Handle it where it lands.

An inbound room announcement follows a different interruption policy. When this
session is idle, it starts a turn so the information is read. When a model run is
in flight, it waits for that run to finish rather than aborting or steering it.
Several notices remain separate queued messages. If the operator explicitly
aborts the run, OMP restores queued notices to the operator's editor for an
explicit decision rather than auto-running them.

Other directed-send statuses: `peer_offline` (not queued — see §3),
`recipient_backpressure` (the named peer's queue is full; retry later), and
`invalid_target` (the relay refused the name).

## 6. Choose `send` or `announce` by responsibility

Use `send` for work one peer must do. Name that peer, even if everyone in the
room would understand the instruction. An instruction announced to the room
leaves every recipient unable to tell who owns it.

Use `announce` once for information every peer needs in order not to collide:
a shared decision landed, an interface changed, a lock is held, or a migration
must precede dependent work.

```text
mesh(action: "announce", message: """
Repository: github.com/acme/widget, branch feat/schema at 3f2a9c1.
Shared decision: `Widget.id` is now a UUID string on the wire.
Do not merge code that still emits the numeric form.
""")
```

An announcement never reaches its author. The `accepted` reply is the author's
confirmation; do not wait to see the notice itself, and do not wait for a reply.

## 7. Send large material as an attachment, not as prose

A message body is capped. Anything substantial — a diff, a failing test's
captured output, a log bundle, a build artifact — will not fit, and the three
obvious workarounds are all wrong:

- **Do not truncate it.** A partial diff does not apply and a partial log hides
  the line that mattered.
- **Do not split it across messages.** The recipient has to reassemble by hand,
  and nothing guarantees the parts arrive together or in order.
- **Do not name a local path.** The recipient may be on a different machine, so
  a path is not a shared reference even when both sessions are working on the
  same repository and the same revision. `/tmp/out.log` on your host does not
  exist on theirs.

Attach the file instead:

```text
mesh(action: "send", to: "win-desktop", attach: "/work/widget/target/nextest.log", message: """
Repository: github.com/acme/widget, branch feat/win-paths at 3f2a9c1.
Attached: the full `cargo nextest` output from this macOS run, 2.1 MB.
Why: three path tests pass here and I need to know whether they pass for you.
Return: the same command's output from your machine, or the assertion messages
of whichever tests failed.
Accepted when: I can compare the two runs test by test.
""")
```

**The body explains; the attachment carries.** What the payload is, why it was
sent, and what the recipient should do with it all belong in the body — because
the body is what the recipient reads *before* deciding whether to transfer
anything. An attachment with an empty body is a file arriving with no reason.

Attaching works on `announce` too, on the same terms.

## 8. A reference expires, and a fetch is deliberate

What travels on the wire is a **reference**, never the payload. That has two
consequences worth acting on.

**The material has a limited lifetime.** The result of a successful attachment
states how long the relay will hold it. Say so in the body whenever the
recipient may not read the message promptly:

```text
Attached: the failing test's output. The relay holds it for about 2 hours; if
you get to this later and the fetch says it is gone, ask me and I will resend.
```

A fetch that reports the payload as no longer available is **expiry, not
failure**. Nothing is broken and retrying will not recover it — the payload is
not there any more. Ask the sender to send it again. A room's attachments are
also removed once every peer has left it, so a reference does not outlive the
collaboration that produced it.

**An inbound attachment has not been downloaded.** When a delivery tells you an
attachment is available, nothing has been transferred to this machine. Fetching
is an explicit act:

```text
mesh(action: "fetch", reference: "<the reference from the delivery>")
```

Decide whether you need it before fetching. A message may be answerable from its
body alone, and a payload you do not need is a transfer nobody wanted. When you
do not know how large it is, pass a ceiling — the size is reported and nothing
is transferred when it is exceeded:

```text
mesh(action: "fetch", reference: "...", max_bytes: 5000000)
```

A fetch yields **a file path, not content**. The payload is on disk; read,
search, `grep`, or apply it with your ordinary tools. Do not expect the bytes in
the tool's result, and do not ask for them — that is what a path exists to avoid.

## 9. A refused attachment sent nothing at all

An attachment can be refused before anything is sent, and the refusal names
which bound it reached. The three call for different responses:

| Refusal | What it means | What to do |
|---|---|---|
| `payload_too_large` | This one payload is over the per-payload maximum | Send something smaller; waiting will not help |
| `room_full` | This room's attachments together are at their total | Wait for this room's existing attachments to expire, or attach something smaller |
| `store_full` | The relay's whole store is full, across every room | Tell the operator; this is not yours to resolve |

In every case **nothing was sent**. The message is not half-delivered and the
recipient has not been told about a payload that is not there. Once the bound is
addressed, sending again is correct rather than a duplicate.

Report the refusal to the operator with its bound named. "The relay refused the
attachment because the room is full, so I sent nothing" is precise; "the send
failed" is not, and it hides which of the three responses applies.

## Quick reference

| Situation | Action |
|---|---|
| Nothing connected yet | `mesh(action: "join")` |
| Operator gave `<task>/<peer>` | `mesh(action: "join", task: ..., as: ...)`; pass no `project` |
| Operator gave `<project>/<task>/<peer>` | Pass all three; the project is being overridden on purpose |
| Selector has one component, or four, or an empty one | Report the two accepted forms; do not guess |
| Selector supplied, or project source is `derivation` | Report the resolved room, this session's peer, and each source |
| Room came entirely from the project file, no selector | Proceed; no confirmation needed |
| Operator named a peer informally | `mesh(action: "list")`, match against the roster |
| Reference matches nothing | Report the roster; do not send |
| Reference matches several | Report the candidates; do not choose |
| Directed work, but nobody else is present | Report the room and stop |
| Sending work to one peer | Repository, revision, steps, artifact, acceptance criterion |
| Shared information the room needs | `mesh(action: "announce", message: "...")` with no target |
| Answering a delivery | Set `reply_to` to its identifier |
| Got `routed` | Say "queued for the peer", carry on, do not wait |
| Got `accepted` | Report `delivered` and `shed` as queue counts; zero means an empty room |
| Inbound announcement during a run | It waits; do not abort or steer the run |
| Material too large for a body | `attach` the file; keep the body as the explanation |
| Tempted to paste, truncate, split, or name a path | Attach instead; the recipient may be on another machine |
| Attachment granted | Say the stated lifetime in the body when the recipient may read it late |
| Told an attachment is available | Nothing was downloaded; decide, then `mesh(action: "fetch")` |
| Unsure how large a payload is | Pass `max_bytes`; the size is reported and nothing transfers |
| Fetch says the payload is gone | Expiry, not failure; ask for a resend, do not retry |
| Fetch succeeded | Use the returned path with ordinary tools; the bytes are not in the result |
| Attachment refused | Nothing was sent; name the bound and respond to that bound |
