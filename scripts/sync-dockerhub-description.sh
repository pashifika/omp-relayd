#!/usr/bin/env bash
#
# Renders `.dockerhub/` into the two fields Docker Hub shows for a repository,
# and optionally publishes them.
#
# Local by design, not a workflow. The image is an artifact whose digest has to
# be traceable to a commit, which is why publishing it is a recorded run on a
# protected branch. A description is prose: nothing depends on it matching a
# particular commit.
#
# Rendering, not publishing, is the default mode, and that is forced by Docker
# Hub rather than chosen. `PATCH /v2/repositories/{namespace}/{repository}/`
# rejects a JWT issued from a personal access token, deliberately:
#
#   {"message": "token issued from personal access token"}
#
#   "This is intentional. Personal access tokens (for now) are only meant to
#    access hub registry (docker push|pull) primarily for CI use-case. We
#    deliberately do not allow all API access. Otherwise it defeats the purpose
#    of having 2FA if everything can be accessed via token without second
#    factor."
#     -- docker/hub-feedback#1927, and still so in #2438 (2025)
#
# So the endpoint wants the account password, which means two-factor
# authentication switched off. Trading an account's second factor for a prose
# field is a bad trade, so `--publish` exists but is opt-in and unrecommended,
# and the default mode needs no credential at all.
#
# Re-test the token path if docker/roadmap#115 is ever reopened and delivered.
#
# What this does buy, with no credential: one source of truth for the text, the
# image name and version substituted from the files the publish workflow reads,
# a refusal instead of registry-side truncation, and a body reviewable as a diff
# rather than typed into a web form.

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
sync-dockerhub-description.sh — render .dockerhub/, and optionally publish it

Usage:
  scripts/sync-dockerhub-description.sh
  scripts/sync-dockerhub-description.sh --raw short|overview
  scripts/sync-dockerhub-description.sh --publish [--password-file <path>]

Modes:
  (default)            Validate and print both fields with the values they were
                       rendered from. Contacts nothing.
  --raw short          Print only the short description, for piping to a
                       clipboard: ... --raw short | pbcopy
  --raw overview       Print only the overview body, likewise.
  --publish            Send both fields to Docker Hub. Read the note below first.

Options:
  --password-file <p>  Read the account password from a file rather than from
                       \$DOCKERHUB_PASSWORD. Only meaningful with --publish.
  -h, --help           Print this message

Publishing, and why it is not the default:
  Docker Hub's repository endpoint rejects a personal access token by design and
  answers 403 with "token issued from personal access token". It accepts only the
  account password, so --publish also requires two-factor authentication to be
  off. See docker/hub-feedback#1927 and #2438.

  If that trade is not one you want to make -- and it should not be, for a prose
  field -- paste instead:

    scripts/sync-dockerhub-description.sh --raw short | pbcopy
    scripts/sync-dockerhub-description.sh --raw overview | pbcopy

  then https://hub.docker.com/repository/docker/<namespace>/<repository>/general

What it deliberately does not do:
  It does not create the Docker Hub repository. A description for a repository
  that does not exist is a typo, not a bootstrap step.

  It does not read the Docker CLI credential store, which holds a token that this
  endpoint refuses anyway.

  It does not check that the prose is true. With --publish it checks that what the
  API reports back is byte-for-byte what was sent.
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

# Substitution rather than a literal version in the body: a literal would rot
# silently on the next release, and a staleness check only reports a problem this
# avoids having.
render() {
  sed -e "s|{{IMAGE}}|${IMAGE}|g" -e "s|{{VERSION}}|${VERSION}|g" "$1"
}

# Splits a curl invocation that appended its status code on the last line, so a
# failure can print the body the API sent rather than only a number.
http_status() { printf '%s' "$1" | awk 'END { print $0 }'; }
http_body() { printf '%s' "${1%$'\n'*}"; }

resolve() {
  [ -f "$SHORT_FILE" ] || die "missing ${SHORT_FILE}"
  [ -f "$OVERVIEW_FILE" ] || die "missing ${OVERVIEW_FILE}"
  [ -f "$COMPOSE_FILE" ] || die "missing ${COMPOSE_FILE}"
  [ -f "$MANIFEST_FILE" ] || die "missing ${MANIFEST_FILE}"

  VERSION="$(read_crate_version)"
  [ -n "$VERSION" ] || die "no version in the [package] table of ${MANIFEST_FILE}"

  COMPOSE_REF="$(read_compose_image_ref)"
  [ -n "$COMPOSE_REF" ] || die "no image reference in ${COMPOSE_FILE}"

  IMAGE="${COMPOSE_REF%:*}"
  local compose_tag="${COMPOSE_REF##*:}"
  [ "$compose_tag" = "$VERSION" ] \
    || die "compose.yml pins tag '${compose_tag}' but the crate version is '${VERSION}'"

  SHORT="$(render "$SHORT_FILE" | awk 'NR == 1 { print; exit }')"
  OVERVIEW="$(render "$OVERVIEW_FILE")"

  [ -n "$SHORT" ] || die "${SHORT_FILE} is empty"
  [ -n "$OVERVIEW" ] || die "${OVERVIEW_FILE} is empty"

  # An unsubstituted placeholder would be published verbatim, so a typo in a
  # placeholder name fails here rather than appearing on the page.
  case "${SHORT}${OVERVIEW}" in
    *'{{'*) die "an unsubstituted {{placeholder}} remains after rendering" ;;
  esac

  SHORT_CHARS="$(printf '%s' "$SHORT" | awk '{ print length($0) }')"
  [ "$SHORT_CHARS" -le "$MAX_SHORT_CHARS" ] \
    || die "the short description is ${SHORT_CHARS} chars, over the ${MAX_SHORT_CHARS} limit"
}

report() {
  # Printed before any verdict, so a mis-specified comparison is visible either
  # way rather than hidden behind a bare pass.
  printf 'server/Cargo.toml [package] version : %s\n' "$VERSION"
  printf 'compose.yml image reference         : %s\n' "$COMPOSE_REF"
  printf 'target repository                   : %s\n' "$IMAGE"
  printf 'short description                   : %s chars (limit %s)\n' \
    "$SHORT_CHARS" "$MAX_SHORT_CHARS"
  printf 'overview                            : %s bytes\n' \
    "$(printf '%s' "$OVERVIEW" | wc -c | tr -d ' ')"
}

publish() {
  local password_file="$1"
  local password

  if [ -n "$password_file" ]; then
    [ -f "$password_file" ] || die "missing password file ${password_file}"
    password="$(awk 'NR == 1 { print; exit }' "$password_file")"
  else
    password="${DOCKERHUB_PASSWORD:-}"
  fi

  if [ -z "$password" ]; then
    printf '%s\n' \
      "no password: set \$DOCKERHUB_PASSWORD or pass --password-file." \
      "A personal access token will not work here: Docker Hub answers 403 with" \
      "\"token issued from personal access token\" by design. See --help, and" \
      "prefer --raw with a paste if you would rather keep two-factor auth on." >&2
    exit 1
  fi

  local namespace="${IMAGE%%/*}"
  local login_response login_status
  # Built with jq so the credential is never interpolated into a shell word, and
  # read by curl from a pipe so it is never a command argument.
  login_response="$(jq -n --arg u "$namespace" --arg p "$password" \
    '{username: $u, password: $p}' \
    | curl -sS -H 'Content-Type: application/json' --data-binary @- \
        -w '\n%{http_code}' "${HUB_API}/users/login/")"
  login_status="$(http_status "$login_response")"
  local login_body
  login_body="$(http_body "$login_response")"

  if [ "$login_status" != "200" ]; then
    printf 'authentication returned HTTP %s:\n' "$login_status" >&2
    printf '%s\n' "$login_body" >&2
    exit 1
  fi

  if [ -n "$(printf '%s' "$login_body" | jq -r '.login_2fa_token // empty')" ]; then
    die "the account has two-factor authentication on, which this endpoint cannot satisfy; use --raw and paste"
  fi

  local jwt
  jwt="$(printf '%s' "$login_body" | jq -r '.token // empty')"
  [ -n "$jwt" ] || die "authentication succeeded but returned no token"

  printf '\nauthenticated as %s\n' "$namespace"

  local payload response status body
  payload="$(jq -n --arg d "$SHORT" --arg f "$OVERVIEW" \
    '{description: $d, full_description: $f}')"

  response="$(printf '%s' "$payload" \
    | curl -sS -X PATCH \
        -H "Authorization: JWT ${jwt}" \
        -H 'Content-Type: application/json' \
        --data-binary @- \
        -w '\n%{http_code}' "${HUB_API}/repositories/${IMAGE}/")"
  status="$(http_status "$response")"
  body="$(http_body "$response")"

  if [ "$status" != "200" ]; then
    printf 'the repository update returned HTTP %s:\n' "$status" >&2
    printf '%s\n' "$body" >&2
    case "$body" in
      *'personal access token'*)
        printf '%s\n' \
          "" \
          "That is Docker Hub refusing a personal access token on this endpoint by" \
          "design, not a fault here. Only the account password is accepted, which" \
          "means two-factor authentication off. See docker/hub-feedback#1927." \
          "Prefer --raw and a paste." >&2
        ;;
    esac
    exit 1
  fi

  # Read back rather than trusting the write: the API answers with the stored
  # record, so comparing it against what was sent is the whole verification.
  local stored_short stored_overview
  stored_short="$(printf '%s' "$body" | jq -r '.description // ""')"
  stored_overview="$(printf '%s' "$body" | jq -r '.full_description // ""')"

  printf 'stored description                  : %s\n' "$stored_short"
  printf 'stored overview                     : %s bytes\n' \
    "$(printf '%s' "$stored_overview" | wc -c | tr -d ' ')"

  [ "$stored_short" = "$SHORT" ] || die "the stored description differs from what was sent"
  [ "$stored_overview" = "$OVERVIEW" ] || die "the stored overview differs from what was sent"

  printf '\nsynced https://hub.docker.com/r/%s\n' "$IMAGE"
}

main() {
  local mode="report"
  local raw_field=""
  local password_file=""

  while [ "$#" -gt 0 ]; do
    case "$1" in
      --raw)
        [ "$#" -ge 2 ] || die "--raw needs short or overview"
        case "$2" in
          short|overview) raw_field="$2" ;;
          *) die "--raw takes short or overview, not '$2'" ;;
        esac
        mode="raw"; shift 2 ;;
      --publish) mode="publish"; shift ;;
      --password-file)
        [ "$#" -ge 2 ] || die "--password-file needs a path"
        password_file="$2"; shift 2 ;;
      -h|--help) usage; exit 0 ;;
      *) usage >&2; die "unrecognised argument: $1" ;;
    esac
  done

  require_command awk
  require_command curl
  require_command jq

  resolve

  case "$mode" in
    raw)
      # Only the body, so the caller can pipe it somewhere without trimming.
      if [ "$raw_field" = "short" ]; then
        printf '%s\n' "$SHORT"
      else
        printf '%s\n' "$OVERVIEW"
      fi
      ;;
    report)
      report
      printf '\nnothing was sent. To publish this text, either paste it:\n'
      printf '  scripts/sync-dockerhub-description.sh --raw short | pbcopy\n'
      printf '  scripts/sync-dockerhub-description.sh --raw overview | pbcopy\n'
      printf '  https://hub.docker.com/repository/docker/%s/general\n' "$IMAGE"
      printf 'or read the --publish note in --help first.\n'
      ;;
    publish)
      report
      publish "$password_file"
      ;;
  esac
}

main "$@"
