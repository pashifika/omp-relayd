# syntax=docker/dockerfile:1

# The builder is pinned to the exact toolchain `.github/workflows/ci.yml` uses,
# so an image is built by the compiler that tested the code.
FROM rust:1.97.1-slim-bookworm AS build

WORKDIR /usr/src/omp-relayd

# Dependencies resolve from the manifest alone, so this layer is reused for
# every source-only change. The placeholder targets exist because cargo
# validates the manifest's target paths before it will fetch anything.
COPY server/Cargo.toml server/Cargo.lock ./
RUN mkdir src \
    && printf '' > src/lib.rs \
    && printf 'fn main() {}\n' > src/main.rs \
    && cargo fetch --locked

COPY server/src ./src
# `--locked` so the image cannot silently resolve a dependency differently from
# the version CI tested.
RUN cargo build --release --locked --bin omp-relayd

# Bookworm at both ends: the binary links this image's glibc. A rolling runtime
# tag is deliberate, so security updates arrive without a Dockerfile edit, while
# the builder above stays pinned for reproducibility.
FROM debian:bookworm-slim AS runtime

# Unprivileged, with no home and no shell: the relay needs no privileges and
# reads no configuration file.
#
# It does write, though. The payload store lives under `std::env::temp_dir()`,
# so the container needs a writable `/tmp`. The image layer provides one by
# default; a `--read-only` deployment must supply it as a mount, or the relay
# exits at startup unable to open the store. No `VOLUME` is declared for it:
# that would create an anonymous volume outliving the container to hold
# payloads whose whole contract is that they do not persist. `compose.yml`
# mounts a sized tmpfs instead, and `.dockerhub/overview.md` carries the same
# flag on its `docker run` examples.
RUN useradd --system --no-create-home --shell /usr/sbin/nologin relay

COPY --from=build /usr/src/omp-relayd/target/release/omp-relayd /usr/local/bin/omp-relayd

USER relay
EXPOSE 7788

# All interfaces inside the container: restricting exposure is the published
# port's job, and a container listening only on loopback would be unreachable
# even from its own host.
ENV OMP_RELAY_LISTEN=0.0.0.0:7788
ENV RUST_LOG=info

# No init shim. The relay installs its own SIGTERM handler, so it terminates
# correctly as PID 1, where a process relying on a default handler would not.
ENTRYPOINT ["/usr/local/bin/omp-relayd"]
