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
7. Re-paste the Docker Hub description with
   `scripts/sync-dockerhub-description.sh --raw overview`. The body names the
   version through a placeholder, so the rendered text changes with the bump
   while the published page does not until it is pasted.

Most of this is enforced rather than remembered: an already-published version is
skipped, and a disagreement between `server/Cargo.toml` and `compose.yml` fails
the run printing both values. The two steps nothing enforces are the lock file,
which CI consumes with `--locked` but cannot write, and the description paste,
which has no API that accepts a token.

Two boundaries, so neither reads as an omission. `extension/package.json` carries
its own version and nothing couples it to the image; bump it when the extension
changes, not when the relay does. And a previous version tag stays published and
keeps resolving to its digest — a release adds a tag rather than replacing one,
which is what lets a deployment pin a version and stay there.
