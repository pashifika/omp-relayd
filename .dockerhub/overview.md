# OMP Relay

A small self-hosted relay that routes work between named Oh My Pi sessions. Two OMP terminals hand tasks to each other without sharing a process or a terminal: this daemon routes MessagePack frames between named peers in memory, and the repository's OMP extension turns an inbound message into a follow-up turn on the receiving side.

Source, protocol reference and client setup: **[github.com/pashifika/omp-relayd](https://github.com/pashifika/omp-relayd)**

## Read this before you run it

The transport is **plain TCP with no authentication and no encryption**. Anyone who can reach the port can join a room, list peers, and send messages as any name they choose.

Run it on a trusted host, or on a trusted private network. Do not publish the port to the public Internet. There is no public mode, and adding one is not a configuration option — mutual TLS is a later release.

The image reflects that stance: it publishes port `7788` inside the container only, runs as an unprivileged `relay` user, reads no configuration file, writes nothing, and keeps no history. Restricting who can reach it is the published port's job.

## Run it

The safe minimum binds the published port to loopback, so reaching the relay requires being on the host:

```sh
docker run -d --name omp-relay \
  -p 127.0.0.1:7788:7788 \
  --read-only --cap-drop ALL \
  --security-opt no-new-privileges:true \
  {{IMAGE}}:{{VERSION}}
```

To reach it from a trusted private network, publish it on one specific private address — never `0.0.0.0`:

```sh
docker run -d --name omp-relay -p 192.168.1.10:7788:7788 \
  --read-only --cap-drop ALL --security-opt no-new-privileges:true \
  {{IMAGE}}:{{VERSION}}
```

The repository ships a `compose.yml` that does the same with the hardening already applied, and a `scripts/setup-client.sh` that writes both client configuration files and installs the collaboration skill.

## Configuration

The relay takes an address, and nothing else. There is no configuration file.

| Setting | Default | Meaning |
| --- | --- | --- |
| `OMP_RELAY_LISTEN` | `0.0.0.0:7788` in this image | Address the process binds inside the container |
| `RUST_LOG` | `info` | Log filter directives |
| `[ADDRESS]` argument, or `--bind ADDRESS` | — | Overrides `OMP_RELAY_LISTEN` when given |

`0.0.0.0` inside the container is deliberate: a container listening only on its own loopback would be unreachable even from its host, so exposure is decided by how you publish the port.

`docker run --rm {{IMAGE}}:{{VERSION}} --help` prints the same summary, and `--version` prints the build's version.

Logs are structured and carry room, peer, frame type, message identifier and payload size — never message bodies.

## Tags

| Tag | Points at |
| --- | --- |
| `{{VERSION}}` | A specific release. A published version tag is never moved. |
| `sha-<short>` | The exact commit the image was built from. Quote this when reporting a problem. |
| `latest` | The most recent release. |

Prefer a version tag in anything you deploy.

## Architectures

`linux/amd64` and `linux/arm64`, each compiled natively for its own architecture and published under one manifest list, so a plain `docker pull` selects the right one without naming a platform.

## Where these images come from

Every image is built and pushed by the repository's `Publish` workflow from a manual dispatch on `main`, on GitHub's own runners. The `sha-` tag names the commit, and the workflow run that produced a digest is discoverable in the repository's run history. No image here is built or pushed from a workstation.

## License

MIT. See [LICENSE](https://github.com/pashifika/omp-relayd/blob/main/LICENSE).
