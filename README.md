# OMP Relay

Route work between named Oh My Pi sessions through a small self-hosted relay.

OMP Relay lets two OMP terminals hand work to each other without sharing a process or terminal. The Rust daemon routes MessagePack frames in memory; the bundled OMP extension exposes one `mesh` tool, ships the `omp-relay` skill that tells a session how to use it, and turns inbound messages into follow-up turns.

## Features

- **Named peers:** List and address OMP sessions by name within one project/task room. A peer name defaults to your machine's own name, so two machines differ without configuring anything.
- **Deliberate participation:** A session joins when you ask it to. A machine kept as a dedicated participant can connect at start instead.
- **Layered configuration:** What belongs to the machine lives in your agent directory; the room lives in a committed file, so two checkouts of one repository meet without either operator configuring anything.
- **Non-interrupting delivery:** Inbound work waits behind a streaming turn and starts a new turn when the receiving session is idle.
- **Resilient sessions:** Relay outages, reconnects, invalid configuration, and extension callback failures do not end the host session.
- **Standalone extension:** `extension/dist/index.js` includes its MessagePack dependency and loads without a neighboring `node_modules` directory.
- **Bounded relay:** Per-peer queues and frame limits bound memory; the relay stores no messages and replays no history.

## Contents

- [Requirements](#requirements)
- [Quick start](#quick-start)
- [Deployment and security](#deployment-and-security)
- [Configuration](#configuration)
- [Startup modes](#startup-modes)
- [Manual setup](#manual-setup)
- [Register the extension](#register-the-extension)
- [Use the `mesh` tool](#use-the-mesh-tool)
- [Verify two machines](#verify-two-machines)
- [Protocol](#protocol)
- [Development](#development)
- [License](#license)

## Requirements

For normal use:

- Docker with Compose v2
- Oh My Pi `18.0.1`

To rebuild or test the extension, install Bun `1.3.14`. To build the relay outside Docker, install Rust `1.85.0` or later.

## Quick start

Clone the repository and start the relay. Compose publishes port `7788` on loopback by default.

```bash
git clone https://github.com/pashifika/omp-relayd.git
cd omp-relayd
docker compose up -d --build
```

Write both configuration files and install the collaboration skill in one command. `--task` names the topic the two ends will meet on, and is the one value the helper never guesses:

```bash
scripts/setup-client.sh --task code-review
```

It reports the project root it resolved and the marker that decided it, writes `~/.omp/agent/omp-relay.yml` and `<project-root>/.omp/omp-relay.yml`, installs the `omp-relay` skill to `~/.omp/agent/skills/omp-relay/`, and prints the command to start OMP. Run it with `--dry-run` first to see every file it would touch.

Its scope stops at the client: it installs no toolchain, starts no relay, and does not run the agent. Run `scripts/setup-client.sh --help` for every parameter and default, or see [Manual setup](#manual-setup) to do the same work by hand.

Start OMP from the repository root with the bundled extension:

```bash
omp --extension "$PWD/extension/dist/index.js"
```

Then ask OMP to join: `Use the omp-relay skill to join and list the peers.` The join result reports the room it resolved, where each part of it came from, and who else is present.

## Deployment and security

The initial transport is unauthenticated, unencrypted TCP. Run it only on a trusted host or a trusted private LAN. Do not expose port `7788` to the public Internet.

The default Compose deployment is reachable only from the same host:

```bash
docker compose up -d --build
docker compose logs relay
```

To expose the relay to a trusted private LAN, opt in with one specific private address:

```bash
OMP_RELAY_BIND=192.168.1.10 docker compose up -d --build
```

`OMP_RELAY_BIND=0.0.0.0` publishes the unauthenticated port on every interface and is not a supported deployment. The container runs read-only, drops all Linux capabilities, and persists no relay state.

Stop the relay with:

```bash
docker compose down
```

## Configuration

Configuration is read from two files, and each field is accepted in exactly one of them. A field in the wrong file is rejected by name rather than ignored.

| File | Carries | Committed? |
|---|---|---|
| `~/.omp/agent/omp-relay.yml` | `transport`, `startup`, `peer` | No. It is your machine's. |
| `<project-root>/.omp/omp-relay.yml` | `room` | Yes. Both ends read the same one. |

The split follows how long each value lives. A relay address and a peer name describe one machine; a room describes one piece of work, and two checkouts of a repository should agree on it without either operator configuring anything.

Three rules follow from that, and validation enforces all three:

- A project file may not name `transport`, `startup`, `peer`, or `purpose`. It is committed, so a checkout able to name `transport` could redirect your session's traffic, and one able to name `purpose` could inject instructions into every agent that joins from it.
- The global file may not name `room`.
- With no global file, the extension stays inert. The global file is the grant: cloning a repository that carries a project file never causes a connection.

### The global file

```bash
mkdir -p ~/.omp/agent
cat > ~/.omp/agent/omp-relay.yml <<'GLOBAL'
transport:
  mode: local
  address: 127.0.0.1:7788
startup: manual
peer:
  # Optional. Omitted, the peer name is the first label of this host's name,
  # so `MacBook-Pro.local` becomes `MacBook-Pro`.
  name: macbook-reviewer
  # Optional. Read by the agent on this machine, and never sent anywhere.
  purpose: |
    This terminal has the macOS toolchain and the signing keys.
    Prefer running release builds here; decline Linux-only work.
GLOBAL
```

| Field | Type | Accepted value |
|---|---|---|
| `transport.mode` | string | Exactly `local` |
| `transport.address` | string | `host:port`, with port `1`–`65535`; bracket IPv6 addresses |
| `startup` | string | `manual` or `auto`. Optional; defaults to `manual` |
| `peer.name` | string | Optional. Non-empty, at most 64 UTF-8 bytes, no `/` or `@`, no leading or trailing whitespace. Defaults to the first label of the host name |
| `peer.purpose` | string | Optional. At most 4096 UTF-8 bytes of your own instructions to the agent on this machine |

`peer.purpose` never leaves the machine that owns it. Under `manual` it comes back in the join result; under `auto` it rides the session's first inbound message. It is accepted only from the global file, which is what keeps a committed file from instructing everyone who clones it.

### The project file

```bash
mkdir -p .omp
cat > .omp/omp-relay.yml <<'PROJECT'
room:
  project: omp-relayd
  task: code-review
PROJECT
```

| Field | Type | Accepted value |
|---|---|---|
| `room.project` | string | Non-empty, at most 64 UTF-8 bytes, no `/` or `@`, no leading or trailing whitespace |
| `room.task` | string | Same rules as `room.project` |

The project root is found by honouring `OMP_PROJECT_ROOT`, otherwise walking up from the working directory for a `.git` directory, otherwise for a language manifest such as `package.json` or `Cargo.toml`. The walk stops below your home directory and never selects it.

This file is optional. A join may carry the room instead:

```text
Use mesh with action join, project acme and task pr-471.
```

That is the path for a repository you cannot commit to. Because two mistyped rooms are two *successful* joins that never meet, the join result always reports which source each value came from — tell the other operator what yours resolved to.

### Environment variables

| Variable | Default | Purpose |
|---|---|---|
| `PI_CODING_AGENT_DIR` | `~/.omp/agent` | The agent directory holding the global file and installed skills. OMP's own variable; a named profile sets it for you |
| `OMP_PROJECT_ROOT` | discovered | Names the project root instead of discovering it |
| `OMP_RELAY_BIND` | `127.0.0.1` | Host address published by Compose |
| `RUST_LOG` | `info` | Relay log filter |

A missing global file leaves the extension inert and silent. A file that exists and does not validate is reported once — at session start when it is the global file, and in the join result when the join is what triggered the read. Either way the client stays stopped and the OMP session remains usable.

## Startup modes

| Mode | Connects | Choose it when |
|---|---|---|
| `manual` (default) | On `mesh(action: "join")` | This machine sometimes collaborates. Nothing is on the relay until you ask, so the roster holds only sessions that meant to be there |
| `auto` | At session start | This machine is a dedicated participant that exists to answer requests. Re-arming it every session would be busywork |

`manual` is the default for two reasons. A roster filled with every incidentally-open terminal in a repository is useless at the moment it matters — deciding which peer has the environment you need. And joining through the `omp-relay` skill makes acquiring the tool and acquiring the protocol for using it one event, instead of leaving an agent holding a tool it does not know how to read.

Under `auto`, set `peer.purpose`: no operator is present at connect time, so the purpose is the only thing telling the session what its machine is for.

## Manual setup

Everything `scripts/setup-client.sh` does, by hand. Do this if you would rather not run a script, or to understand what the script produced.

1. Write the global file. See [The global file](#the-global-file) above for the heredoc and the field table.
2. Write the project file at `<project-root>/.omp/omp-relay.yml`. See [The project file](#the-project-file). Commit it if the room should be shared.
3. Install the collaboration skill. The extension registers the `mesh` tool; the skill is what tells a session how to use it, so an installation without it is incomplete:

   ```bash
   mkdir -p ~/.omp/agent/skills
   cp -R extension/skill/omp-relay ~/.omp/agent/skills/
   ```

   OMP scans `~/.omp/agent/skills/*/SKILL.md` — one level, no deeper.

4. Build the bundle, only if you changed `extension/src/`. The committed `extension/dist/index.js` is what a fresh clone loads:

   ```bash
   cd extension && bun install --frozen-lockfile && bun run build
   ```

5. Start the agent with the extension path, as in [Register the extension](#register-the-extension).

## Register the extension

Registration uses an explicit OMP extension path:

```bash
omp --extension "$PWD/extension/dist/index.js"
```

The repository does not use `package.json#omp.extensions`. An explicit path matches the tested deployment: one JavaScript file copied into an otherwise empty directory.

Rebuild the bundle after changing `extension/src/`:

```bash
cd extension
bun install --frozen-lockfile
bun run build
```

The build produces the single ESM file `extension/dist/index.js`. It embeds `@msgpack/msgpack`; OMP supplies the runtime API and resolves no third-party package beside the bundle.

## Use the `mesh` tool

`mesh` has three actions:

| Action | Arguments | Result |
|---|---|---|
| `join` | optional `project`, `task`, `as` | Resolved room and peer name, the source of each, the current roster, and this machine's purpose under `manual` |
| `list` | none | Connected peer names in the joined room |
| `send` | `to`, `message`, optional `reply_to` | Relay receipt status and generated message identifier |

Nothing works before a join. `join` is also how a live session changes room or peer name: a room is fixed for a connection's lifetime, so joining elsewhere reconnects, and joining the room you already hold changes nothing.

Prompt examples:

```text
Use the omp-relay skill to join and tell me who is in the room.
```

```text
Use mesh with action send to win-desktop. Send: Review the parser error paths and reply with findings.
```

A `routed` receipt means the relay queued the message for the recipient. It does not mean the recipient read, accepted, or answered it. Other results distinguish an offline peer, a full recipient queue, and an invalid target. The `omp-relay` skill carries the rest of the workflow: resolving an informal reference against the roster, stopping when nobody else is in the room, and writing a briefing the far end can act on without shared context.

## Verify two machines

A single machine cannot demonstrate two peers meeting while peer names derive from host names: both sessions would derive the same name, and the relay gives a name to whichever connection registered last. So this procedure uses two machines, A and B, on one trusted network.

On the machine running the relay, publish it to the LAN address the other machine can reach:

```bash
OMP_RELAY_BIND=192.168.1.10 docker compose up -d --build
```

On each machine, from a checkout of the same repository:

```bash
scripts/setup-client.sh --task two-machine-check --address 192.168.1.10:7788
omp --extension "$PWD/extension/dist/index.js"
```

Both ends resolve the same room from the same committed project file, and each derives its own peer name from its own host name. Then:

1. In both sessions: `Use the omp-relay skill to join.` Each join result should report the same `project/task` and a roster naming both machines. If a roster names only one, the two rooms do not match — compare what each result reported and the source it came from.
2. In A: `Send win-desktop a request to report its OS and Bun version.` A reports `routed` and a message identifier.
3. B starts a turn carrying the sender, project, task, identifier, and body.
4. In B: reply to A, setting `reply_to` to A's identifier. A receives the reply carrying that reference.

To rehearse the flow on one machine, give each session a distinct name explicitly — `--peer alpha` on one and `--peer beta` on the other, each with its own `--agent-dir`. That exercises routing but not derivation, which is the part only two hosts can show.

## Protocol

[PROTOCOL.md](PROTOCOL.md) is the implementer-facing protocol v1 reference. It covers framing, MessagePack representation, every frame, receipt status, error code, and resource limit.

The accepted planning specifications under `rasen/specs/` are normative. If this rendering conflicts with an accepted specification, the specification wins and this document must be corrected.

## Development

Run the checks from their component directories:

```bash
cd extension
bun install --frozen-lockfile
bun run typecheck
bun run test:unit
bun run test:packaging
bun run test:integration
```

```bash
cd server
cargo fmt --all -- --check
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked
```

Cross-language fixtures live under `test-fixtures/protocol-v1/`. Both implementations decode fixtures produced by the other language.

## License

[MIT](LICENSE)
