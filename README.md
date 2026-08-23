# OMP Relay

Route work between named Oh My Pi sessions through a small self-hosted relay.

OMP Relay lets two OMP terminals hand work to each other without sharing a process or terminal. The Rust daemon routes MessagePack frames in memory; the bundled OMP extension exposes one `mesh` tool and turns inbound messages into follow-up turns.

## Features

- **Named peers:** List and address OMP sessions by name within one project/task room.
- **Non-interrupting delivery:** Inbound work waits behind a streaming turn and starts a new turn when the receiving session is idle.
- **Resilient sessions:** Relay outages, reconnects, invalid configuration, and extension callback failures do not end the host session.
- **Standalone extension:** `extension/dist/index.js` includes its MessagePack dependency and loads without a neighboring `node_modules` directory.
- **Bounded relay:** Per-peer queues and frame limits bound memory; the relay stores no messages and replays no history.

## Contents

- [Requirements](#requirements)
- [Quick start](#quick-start)
- [Deployment and security](#deployment-and-security)
- [Configuration](#configuration)
- [Register the extension](#register-the-extension)
- [Use the `mesh` tool](#use-the-mesh-tool)
- [Verify two terminals](#verify-two-terminals)
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

Create the default extension configuration:

```bash
mkdir -p ~/.omp/agent
cat > ~/.omp/agent/omp-relay.yml <<'YAML'
transport:
  mode: local
  address: 127.0.0.1:7788
room:
  project: omp-relayd
  task: code-review
peer: macbook-reviewer
YAML
```

Start OMP from the repository root with the bundled extension:

```bash
omp --extension "$PWD/extension/dist/index.js"
```

Ask OMP to use `mesh` with action `list`. A ready connection reports at least `macbook-reviewer`.

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

The extension reads one YAML file when a top-level interactive session starts. The default path is `~/.omp/agent/omp-relay.yml`. `OMP_RELAY_CONFIG` replaces that path completely; the extension does not merge or search configuration files.

The Quick start example is a complete configuration accepted verbatim. Its fields are:

| Field | Type | Accepted value |
|---|---|---|
| `transport.mode` | string | Exactly `local` |
| `transport.address` | string | `host:port`, with port `1`–`65535`; bracket IPv6 addresses |
| `room.project` | string | Non-empty, at most 64 UTF-8 bytes, no `/` or `@`, no leading or trailing whitespace |
| `room.task` | string | Same identifier rules as `room.project` |
| `peer` | string | Same identifier rules as `room.project`; unique within the room while connected |

Use a different file for a second peer:

```bash
OMP_RELAY_CONFIG=/tmp/omp-relay-beta.yml \
  omp --extension "$PWD/extension/dist/index.js"
```

Relevant environment variables:

| Variable | Default | Purpose |
|---|---|---|
| `OMP_RELAY_CONFIG` | `~/.omp/agent/omp-relay.yml` | Replaces the extension configuration path |
| `OMP_RELAY_BIND` | `127.0.0.1` | Host address published by Compose |
| `RUST_LOG` | `info` | Relay log filter |

A missing file, invalid YAML, unsupported mode, bad address, or invalid identifier is reported once. The client remains stopped and the OMP session remains usable.

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

`mesh` has two actions:

| Action | Arguments | Result |
|---|---|---|
| `list` | none | Connected peer names in the configured room |
| `send` | `to`, `message`, optional `reply_to` | Relay receipt status and generated message identifier |

Prompt examples:

```text
Use mesh with action list and report the connected peers.
```

```text
Use mesh with action send to terminal-beta. Send: Review the parser error paths and reply with findings.
```

A `routed` receipt means the relay queued the message for the recipient. It does not mean the recipient read, accepted, or answered it. Other results distinguish an offline peer, a full recipient queue, and an invalid target.

## Verify two terminals

From the repository root, start the relay and create two accepted configurations:

```bash
docker compose up -d --build
cat > /tmp/omp-relay-alpha.yml <<'YAML'
transport:
  mode: local
  address: 127.0.0.1:7788
room:
  project: omp-relayd
  task: two-terminal-check
peer: terminal-alpha
YAML
cat > /tmp/omp-relay-beta.yml <<'YAML'
transport:
  mode: local
  address: 127.0.0.1:7788
room:
  project: omp-relayd
  task: two-terminal-check
peer: terminal-beta
YAML
```

Open terminal A in the repository root:

```bash
OMP_RELAY_CONFIG=/tmp/omp-relay-alpha.yml \
  omp --extension "$PWD/extension/dist/index.js"
```

Open terminal B in the repository root:

```bash
OMP_RELAY_CONFIG=/tmp/omp-relay-beta.yml \
  omp --extension "$PWD/extension/dist/index.js"
```

Then verify the observable flow:

1. In both terminals, ask `Use mesh with action list.` Each side should report `terminal-alpha` and `terminal-beta`.
2. In A, ask `Use mesh with action send to terminal-beta. Send: Reply with your peer name.` A should report `routed` and a message identifier.
3. B should start a follow-up turn containing the sender, project, task, identifier, and body.
4. In B, send a reply to `terminal-alpha` and set `reply_to` to A's identifier. A should receive the reply with that reference.

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
