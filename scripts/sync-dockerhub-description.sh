#!/usr/bin/env bash
#
# Publishes `.dockerhub/` to the Docker Hub repository's two description fields.
#
# Local by design, not a workflow. The image is an artifact whose digest has to
# be traceable to a commit, which is why publishing it is a recorded run on a
# protected branch. A description is prose: nothing depends on it matching a
# particular commit, and handing CI a second reason to hold a namespace-wide
# token would widen that token's blast radius for a cosmetic field.
#
# The body lives in `.dockerhub/` and is tracked, so what is published is
# reviewable as a diff rather than being whatever someone last typed into a web
# form. The image name and version are substituted from the same two files the
# publish workflow reads, so the page cannot claim a version the repository does
# not deploy.

set -euo pipefail

readonly REPO_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
readonly BODY_DIR="${REPO_ROOT}/.dockerhub"
readonly SHORT_FILE="${BODY_DIR}/short-description.txt"
readonly OVERVIEW_FILE="${BODY_DIR}/overview.md"
readonly COMPOSE_FILE="${REPO_ROOT}/compose.yml"
readonly MANIFEST_FILE="${REPO_ROOT}/server/Cargo.toml"

# Docker Hub truncates a longer short description rather than rejecting it, so
# the limit is enforced here where the failure is visible.
readonly MAX_SHORT_CHARS=100

readonly HUB_API="https://hub.docker.com/v2"

usage() {
  cat <<USAGE
sync-dockerhub-description.sh — publish .dockerhub/ to the Docker Hub repository

Usage:
  scripts/sync-dockerhub-description.sh [--dry-run] [--token-file <path>]

Options:
  --dry-run            Render and validate everything, contact nothing, and print
                       the payload's shape. Use this first.
  --token-file <path>  Read the Docker Hub personal access token from a file
                       instead of \$DOCKERHUB_TOKEN. The file's contents are
                       trimmed of a trailing newline and nothing else.
  -h, --help           Print this message

Credential:
  A Docker Hub personal access token with Read & Write access, in
  \$DOCKERHUB_TOKEN or in --token-file. The token is passed on standard input to
  \`jq\` and never appears in a command argument or in this script's output.

What it deliberately does not do:
  It does not create the Docker Hub repository. A description for a repository
  that does not exist is a typo, not a bootstrap step.

  It does not read the Docker CLI credential store. The credential is chosen per
  run, so what authenticates is what you passed rather than whatever a previous
  \`docker login\` left in a keychain.

  It does not check that the prose is true. It checks that what the API reports
  back is byte-for-byte what was sent.
USAGE
}

die() {
  printf 'sync-dockerhub-description.sh: %s\n' "$1" >&2
  exit 1
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || die "$1 is required and was not found on PATH"
}

# The version the repository deploys, read from the `[package]` table alone so a
# dependency's version can never be mistaken for the crate's own. Same reader as
# `.github/workflows/publish.yml`.
read_crate_version() {
  awk '
    /^\[/ { in_package = ($0 == "[package]"); next }
    in_package && /^[[:space:]]*version[[:space:]]*=/ {
      if (match($0, /"[^"]*"/)) { print substr($0, RSTART + 1, RLENGTH - 2); exit }
    }
  ' "$MANIFEST_FILE"
}

# The image reference the deployment names, so this page cannot advertise an
# image the repository does not deploy.
read_compose_image_ref() {
  awk '
    /^[[:space:]]*image:[[:space:]]*/ {
      line = $0
      sub(/^[[:space:]]*image:[[:space:]]*/, "", line)
      gsub(/^["'"'"']|["'"'"']$/, "", line)
      print line
      exit
    }
  ' "$COMPOSE_FILE"
}

render() {
  # Substitution rather than a literal version in the body: a literal would rot
  # silently on the next release, and a check for staleness only reports a
  # problem that this avoids having.
  sed -e "s|{{IMAGE}}|${IMAGE}|g" -e "s|{{VERSION}}|${VERSION}|g" "$1"
}

main() {
  local dry_run=false
  local token_file=""

  while [ "$#" -gt 0 ]; do
    case "$1" in
      --dry-run) dry_run=true; shift ;;
      --token-file)
        [ "$#" -ge 2 ] || die "--token-file needs a path"
        token_file="$2"; shift 2 ;;
      -h|--help) usage; exit 0 ;;
      *) usage >&2; die "unrecognised argument: $1" ;;
    esac
  done

  require_command awk
  require_command curl
  require_command jq

  [ -f "$SHORT_FILE" ] || die "missing ${SHORT_FILE}"
  [ -f "$OVERVIEW_FILE" ] || die "missing ${OVERVIEW_FILE}"
  [ -f "$COMPOSE_FILE" ] || die "missing ${COMPOSE_FILE}"
  [ -f "$MANIFEST_FILE" ] || die "missing ${MANIFEST_FILE}"

  VERSION="$(read_crate_version)"
  [ -n "$VERSION" ] || die "no version in the [package] table of ${MANIFEST_FILE}"

  local compose_ref
  compose_ref="$(read_compose_image_ref)"
  [ -n "$compose_ref" ] || die "no image reference in ${COMPOSE_FILE}"

  IMAGE="${compose_ref%:*}"
  local compose_tag="${compose_ref##*:}"

  # Printed before the verdict, so a mis-specified comparison is visible either
  # way rather than hidden behind a bare pass.
  printf 'server/Cargo.toml [package] version : %s\n' "$VERSION"
  printf 'compose.yml image reference         : %s\n' "$compose_ref"
  printf 'target repository                   : %s\n' "$IMAGE"

  [ "$compose_tag" = "$VERSION" ] \
    || die "compose.yml pins tag '${compose_tag}' but the crate version is '${VERSION}'"

  local short overview
  short="$(render "$SHORT_FILE" | awk 'NR == 1 { print; exit }')"
  overview="$(render "$OVERVIEW_FILE")"

  [ -n "$short" ] || die "${SHORT_FILE} is empty"
  [ -n "$overview" ] || die "${OVERVIEW_FILE} is empty"

  # An unsubstituted placeholder would be published verbatim, so a typo in a
  # placeholder name fails here rather than appearing on the page.
  case "${short}${overview}" in
    *'{{'*) die "an unsubstituted {{placeholder}} remains after rendering" ;;
  esac

  local short_chars
  short_chars="$(printf '%s' "$short" | awk '{ print length($0) }')"
  printf 'short description                   : %s chars (limit %s)\n' \
    "$short_chars" "$MAX_SHORT_CHARS"
  printf 'overview                            : %s bytes\n' \
    "$(printf '%s' "$overview" | wc -c | tr -d ' ')"

  [ "$short_chars" -le "$MAX_SHORT_CHARS" ] \
    || die "the short description is ${short_chars} chars, over the ${MAX_SHORT_CHARS} limit"

  local payload
  payload="$(jq -n --arg d "$short" --arg f "$overview" \
    '{description: $d, full_description: $f}')"

  if [ "$dry_run" = true ]; then
    printf '\ndry run: nothing was sent. The request would be:\n'
    printf '  PATCH %s/repositories/%s/\n' "$HUB_API" "$IMAGE"
    printf '  payload keys: %s\n' "$(printf '%s' "$payload" | jq -r 'keys | join(", ")')"
    printf '  description: %s\n' "$short"
    printf '\nrendered overview:\n'
    printf '%s\n' "$overview" | sed 's/^/  /'
    exit 0
  fi

  local token
  if [ -n "$token_file" ]; then
    [ -f "$token_file" ] || die "missing token file ${token_file}"
    token="$(awk 'NR == 1 { print; exit }' "$token_file")"
  else
    token="${DOCKERHUB_TOKEN:-}"
  fi
  [ -n "$token" ] || die "no token: set \$DOCKERHUB_TOKEN or pass --token-file"

  # Built with jq so the token is never interpolated into a shell word, and read
  # by curl from a pipe so it is never a command argument.
  local jwt
  jwt="$(jq -n --arg u "${IMAGE%%/*}" --arg p "$token" '{username: $u, password: $p}' \
    | curl -fsS -H 'Content-Type: application/json' --data-binary @- \
        "${HUB_API}/users/login/" \
    | jq -r '.token // empty')"
  [ -n "$jwt" ] || die "authentication returned no token; check the credential and its access"

  printf '\nauthenticated as %s\n' "${IMAGE%%/*}"

  local response
  response="$(printf '%s' "$payload" \
    | curl -fsS -X PATCH \
        -H "Authorization: JWT ${jwt}" \
        -H 'Content-Type: application/json' \
        --data-binary @- \
        "${HUB_API}/repositories/${IMAGE}/")"

  # Read back rather than trusting the write: the API answers with the stored
  # record, so comparing it against what was sent is the whole verification.
  local stored_short stored_overview
  stored_short="$(printf '%s' "$response" | jq -r '.description // ""')"
  stored_overview="$(printf '%s' "$response" | jq -r '.full_description // ""')"

  printf 'stored description                  : %s\n' "$stored_short"
  printf 'stored overview                     : %s bytes\n' \
    "$(printf '%s' "$stored_overview" | wc -c | tr -d ' ')"

  [ "$stored_short" = "$short" ] \
    || die "the stored description differs from what was sent"
  [ "$stored_overview" = "$overview" ] \
    || die "the stored overview differs from what was sent"

  printf '\nsynced https://hub.docker.com/r/%s\n' "$IMAGE"
}

main "$@"
