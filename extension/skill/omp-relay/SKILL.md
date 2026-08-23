---
name: omp-relay
description: Hand work to an OMP session on another machine through the OMP Relay `mesh` tool, and handle work that arrives from one. Use when the operator asks to reach another terminal, machine, or peer — "ask the Windows box", "send this to the other laptop", "see who else is connected" — or when a remote message arrives and you must answer it. Covers joining a room, resolving an informal peer reference against the roster, writing a briefing the recipient can act on without context, and reading a receipt correctly.
---

# OMP Relay collaboration

The `mesh` tool connects this session to other OMP sessions through a relay. Each
connection joins one **room**, addressed as `<project>/<task>`, and every session
in that room is a **peer** with a name.

A room is an address space, not a channel. There is no room-wide utterance: every
`send` names exactly one target.

The peer on the other end is another autonomous agent with its own operator, its
own machine, and no knowledge of this conversation. Everything below follows from
that.

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

## 3. Alone in the room? Stop

When the roster contains only this session, **stop and tell the operator**. Name
the resolved room and ask whether the other end has joined yet, or whether the
two rooms actually match.

Sending anyway returns `peer_offline`, which conflates three different
situations:

- the peer is not running;
- the peer is running under a different name;
- the peer is in a different room.

Reporting "nobody else is in `acme/pr-471`" is precise. Forwarding an offline
receipt is not.

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

## 5. Read the receipt correctly

A `routed` receipt means **the relay placed the frame in the recipient's queue**.

It does not mean the recipient read it, accepted it, agreed to it, started it, or
finished it.

So after a `routed` receipt:

- Report to the operator that the request was **queued** — never that the task is
  done or under way.
- Continue with other work.
- **Do not wait for a reply.** Do not poll, do not loop, do not hold the turn
  open. There is no `wait` action and adding a sleep gains nothing.

A reply arrives later as its own inbound message, which starts or interrupts a
turn on its own. Handle it where it lands, whatever you were doing at the time.

Other statuses: `peer_offline` (not queued — see §3), `recipient_backpressure`
(the recipient's queue is full; retry later), `invalid_target` (the relay refused
the name).

## 6. Reaching several peers is repeated sending

There is no broadcast. To reach three peers, send three messages and report all
three receipts.

Before doing that, decide what the message actually is:

- **Work directed at one peer** — send it to that peer only. Do not copy it to
  the room.
- **Information several peers genuinely need** — send it to each in turn.

Every delivered message starts or interrupts a turn on the receiving side. A
human reading an IRC channel skips what does not concern them; an agent does not
get that choice. Broadcasting costs one interrupted turn per recipient, so the
cost of an ambient announcement is proportional to the room.

## Quick reference

| Situation | Action |
|---|---|
| Nothing connected yet | `mesh(action: "join")` |
| Room came from a parameter | Report the resolved room and its source to the operator |
| Operator named a peer informally | `mesh(action: "list")`, match against the roster |
| Reference matches nothing | Report the roster; do not send |
| Reference matches several | Report the candidates; do not choose |
| Roster holds only this session | Report the room and stop |
| Sending work | Repository, revision, steps, artifact, acceptance criterion |
| Answering a message | Set `reply_to` to its identifier |
| Got `routed` | Say "queued", carry on, do not wait |
| Several recipients | One `send` each, report every receipt |
