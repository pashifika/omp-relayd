---
name: omp-relay
description: Hand work to one remote OMP session or announce shared information to a relay room through the `mesh` tool, and handle deliveries that arrive from either class. Use when the operator asks to reach another terminal, machine, peer, or room — "ask the Windows box", "tell everyone the schema landed", "see who else is connected" — or when a remote message or room announcement arrives. Covers joining, choosing between `send` and `announce`, resolving informal peer references, writing self-contained briefings, and reading receipts and acceptance counts correctly.
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

- When the operator named a room, pass it: `mesh(action: "join", project: "...", task: "...")`.
- When they did not, omit both and the room resolves from the project file at
  `<project_root>/.omp/omp-relay.yml`.
- `as` overrides this machine's peer name. Leave it alone unless the operator
  asks for a specific name.

The join result reports the resolved room, the resolved peer name, and the
**source** each came from — `parameter`, `project-file`, `global-file`, or
`derivation`.

**When the room came from a join parameter, report the resolved room and its
source back to the operator.** Two mistyped rooms are two *successful* joins that
never meet, and an empty roster is the only symptom. Saying "joined
`acme/pr-471`, from the room you gave me" while both operators are present is
what catches the typo. When the room came from the project file, no confirmation
is needed — both ends read the same committed file.

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

## Quick reference

| Situation | Action |
|---|---|
| Nothing connected yet | `mesh(action: "join")` |
| Room came from a parameter | Report the resolved room and its source to the operator |
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
