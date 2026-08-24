# Contributing

This file holds the procedures only the maintainer can perform, because they need
credentials or permissions nobody else holds. Running the checks and building the
image from the working tree need neither, so they stay in
[README.md](README.md#development).

What the code and the delivery pipeline must satisfy lives in
[AGENTS.md](AGENTS.md) — branch naming, Conventional Commits, the two
repositories in this tree, the single-status-check CI contract, and the pinning
rules for workflow actions. That file is the authority and this one does not
restate it, because two copies of a rule are two rules.

## Publishing the Docker Hub description

The Docker Hub repository's description is content in this repository, under
`.dockerhub/`, rather than text typed into a web form:

```bash
scripts/sync-dockerhub-description.sh                          # validate and show
scripts/sync-dockerhub-description.sh --raw overview | pbcopy  # then paste
```

It substitutes the image name and version from `compose.yml` and
`server/Cargo.toml`, so the page cannot advertise a version this repository does
not deploy, and it refuses a short description over Docker Hub's 100-character
limit rather than letting the registry truncate it.

The last step is a paste because Docker Hub requires it: its repository endpoint
rejects a personal access token by design, answering `token issued from personal
access token`, and accepts only the account password — which means two-factor
authentication switched off
([hub-feedback#1927](https://github.com/docker/hub-feedback/issues/1927)). A
`--publish` mode exists for an account that has made that trade. The default mode
needs no credential at all.

## Releasing a version

Publishing a version tag that already exists is skipped rather than repeated, so
a release starts by moving the version. Two files must agree, and the publish
workflow fails naming both values when they do not:

1. Set the new version in `server/Cargo.toml`.
2. Run any resolving cargo command in `server/` — `cargo check` is enough — so
   `server/Cargo.lock` records it, and commit the lock file. CI builds with
   `--locked`, so a stale lock fails the gate rather than being quietly updated
   there.
3. Set the same version in `compose.yml`'s `image:` tag.
4. Land all three through a pull request. `main` is protected and the `ci` check
   must pass.

Then publish, and update where the version is visible:

5. Dispatch the `Publish` workflow with its inputs left alone. That builds both
   architectures and touches the registry not at all. It is worth doing first
   because the run takes about a minute and a mistaken tag is permanent.
6. Dispatch again with `dry_run` unchecked. That publishes the new version tag
   and a `sha-` tag naming the commit, and moves `latest`.
7. Confirm the release against what the registry now holds, following
   [Confirming a release](#confirming-a-release). A green publish run says the
   workflow succeeded, not that the published image serves the release.
8. Re-paste the Docker Hub description with
   `scripts/sync-dockerhub-description.sh --raw overview`. The body names the
   version through a placeholder, so the rendered text changes with the bump
   while the published page does not until it is pasted.

Most of this is enforced rather than remembered: an already-published version is
skipped, and a disagreement between `server/Cargo.toml` and `compose.yml` fails
the run printing both values. Three steps are not. The lock file, which CI
consumes with `--locked` but cannot write. The description paste, which has no
API that accepts a token. And the confirmation, which by construction runs
against the registry rather than inside the run that populated it.

Two boundaries, so neither reads as an omission. `extension/package.json` carries
its own version and nothing couples it to the image; bump it when the extension
changes, not when the relay does. And a previous version tag stays published and
keeps resolving to its digest — a release adds a tag rather than replacing one,
which is what lets a deployment pin a version and stay there.

## Confirming a release

A green publish run proves the workflow succeeded. It does not prove that the
image the registry now hands an operator serves the release: an image that
starts on both architectures under one manifest list satisfies every check
`publish.yml` performs and can still refuse the frame the release was cut for.

Start by reading what the registry holds. No credential is needed — the
repository is public and this endpoint answers an anonymous token.

```bash
IMAGE=pashifika/omp-relayd
VERSION=$(awk -F: '/^[[:space:]]*image:/ { print $NF; exit }' compose.yml)
COMMIT=sha-$(git rev-parse --short=7 origin/main)

docker buildx imagetools inspect "$IMAGE:$VERSION"

for tag in "$VERSION" "$COMMIT" latest; do
  printf '%-16s %s\n' "$tag" \
    "$(docker buildx imagetools inspect --format '{{.Manifest.Digest}}' "$IMAGE:$tag")"
done
```

The index must name exactly `linux/amd64` and `linux/arm64`, and the three tags
must report one digest. Read the previous version's tag as well: it must still
report the digest it reported before, because a release adds a tag rather than
replacing one.

Then run the digest, never the tag:

```bash
DIGEST=$(docker buildx imagetools inspect --format '{{.Manifest.Digest}}' "$IMAGE:$VERSION")

docker run -d --name omp-relay-confirm \
  -p 127.0.0.1:17788:7788 \
  --read-only --cap-drop ALL \
  --security-opt no-new-privileges:true \
  --tmpfs /tmp:rw,noexec,nosuid,size=320m,mode=1777 \
  "$IMAGE@$DIGEST"
```

`compose.yml` names `pashifika/omp-relayd:<version>`, so any earlier
`docker compose up --build` in this checkout has already claimed that exact tag
locally — and `docker run "$IMAGE:$VERSION"` then runs the working tree instead
of the registry. Silently: the names match and only the image id differs. That
is the shadowing `relay-operations` documents, met from the other side, and it
is the one way a confirmation can report success while confirming nothing. A
digest reference cannot resolve to anything but the content that hashes to it.

The invocation above is the registry page's own hardened command run verbatim,
so the page is under test alongside the image. Dropping the `--tmpfs` reproduces
the startup error that page quotes, which is worth doing once to see that the
quote is still accurate.

Last, exercise the frames the release added. Drive `attach()` and `announce()`
against `127.0.0.1:17788` through the in-tree client: a relay predating
attachments answers `reserve` with `unsupported_frame`, which the client reports
as `reason=unsupported` rather than as a fault, so a stale image is
distinguishable from a broken one. The previous release's evidence file carries
the script that did this, verbatim, with the output it produced.

That script is deliberately not under `scripts/`. `extension/tsconfig.json`
type-checks `src/**` and `test/**` only, so a `.ts` file there would sit outside
the gate and rot with nothing failing; one re-read from the last release's
evidence is re-read on purpose.

Record what was run, what it produced, and the digest it ran against. A
`dry_run` rehearsal exports nothing, so it exercises neither the manifest-list
step nor the tag move — a green rehearsal confirms neither.
