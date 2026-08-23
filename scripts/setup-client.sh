#!/usr/bin/env bash
#
# Writes the two OMP Relay client configuration files and installs the
# collaboration skill.
#
# This exists because the two-layer configuration made manual setup tedious: one
# heredoc became two files in two locations plus a skill installation. It is a
# script rather than a Makefile because what it does is validation and
# parameterised imperative effects, not a dependency graph over file targets —
# it has to measure identifiers in UTF-8 bytes and refuse before writing
# anything, which is a few readable lines of shell and unreadable in Make.
#
# Its scope stops at the client, and the three refusals in the usage text are
# part of the contract rather than omissions.
#
# The identifier rules, the purpose budget, and the project-marker list are
# duplicated from `extension/src/config.ts` because this script must not depend
# on a built bundle. `extension/test/packaging/setup-client.test.ts` fails when a
# duplicate drifts, and asserts that what this writes is accepted by the
# extension's own validator rather than that it matches a template.

set -euo pipefail

# --- Constants duplicated from the extension, guarded by the packaging tests ---

# `protocol.ts` MAX_IDENTIFIER_BYTES.
readonly MAX_IDENTIFIER_BYTES=64
# `config.ts` MAX_PURPOSE_BYTES.
readonly MAX_PURPOSE_BYTES=4096
# `config.ts` PROJECT_MARKERS, in the same order.
readonly PROJECT_MARKERS=(
  package.json
  Cargo.toml
  go.mod
  pyproject.toml
  deno.json
  Gemfile
  composer.json
  pom.xml
  build.gradle
  mix.exs
)

readonly DEFAULT_ADDRESS="127.0.0.1:7788"
readonly DEFAULT_STARTUP="manual"
readonly CONFIG_FILE_NAME="omp-relay.yml"
readonly SKILL_NAME="omp-relay"

readonly REPO_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"

usage() {
  cat <<USAGE
setup-client.sh — write the OMP Relay client configuration and install its skill

Usage:
  scripts/setup-client.sh --task <task> [options]

Required:
  --task <task>            Room task. Required and never inferred: a topic has no
                           defensible default, and guessing one from the branch is
                           the derivation the room's placement exists to avoid.

Options and their defaults:
  --project <project>      Room project.        Default: basename of the resolved project root
  --address <host:port>    Relay address.       Default: ${DEFAULT_ADDRESS}
  --startup <manual|auto>  Startup mode.        Default: ${DEFAULT_STARTUP}
  --peer <name>            This machine's peer name.
                           Default: omitted, so the extension derives it from the
                           first label of the host name.
  --purpose-file <path>    File whose contents become peer.purpose, at most
                           ${MAX_PURPOSE_BYTES} UTF-8 bytes.  Default: omitted
  --project-root <path>    Project root to write the project file under.
                           Default: OMP_PROJECT_ROOT, else the innermost ancestor
                           of the working directory holding .git, else one holding
                           a language marker, else the working directory
  --agent-dir <path>       Agent directory holding the global file and skills.
                           Default: \${PI_CODING_AGENT_DIR:-\$HOME/.omp/agent}
  --build                  Run 'bun run build' in extension/ before finishing
  --force                  Replace an existing configuration file
  --dry-run                Report every action and perform none
  --help                   Print this text

Files written:
  <agent-dir>/${CONFIG_FILE_NAME}                 transport, startup, peer
  <project-root>/.omp/${CONFIG_FILE_NAME}         room
  <agent-dir>/skills/${SKILL_NAME}/          the collaboration skill (always refreshed)

Neither configuration file is replaced without --force: the global file may hold
a purpose you wrote, and the project file may hold a room a colleague committed.

What this script will not do:
  * It installs no toolchain. A missing bun is reported with the version this
    project expects; nothing is fetched.
  * It starts no relay, through Compose or otherwise. Where a relay runs is a
    deployment decision; see "Deployment and security" in README.md.
  * It does not run the agent. The command is printed instead, because it is
    worth reading before it executes.
USAGE
}

fail() {
  printf 'setup-client: %s\n' "$1" >&2
  exit 1
}

# UTF-8 byte length, which is the unit every protocol budget counts. Measured
# with wc rather than ${#var}, whose unit depends on the locale.
byte_length() {
  printf '%s' "$1" | wc -c | tr -d ' '
}

# The `wire-protocol` identifier rules, refused by name and by value so the
# operator sees which of their inputs was rejected.
validate_identifier() {
  local label="$1" value="$2" bytes

  [ -n "$value" ] || fail "$label is empty; it must be a non-empty identifier"
  case "$value" in
    */*) fail "$label \"$value\" contains '/', which is reserved so that <project>/<task>@<peer> stays unambiguous" ;;
    *@*) fail "$label \"$value\" contains '@', which is reserved so that <project>/<task>@<peer> stays unambiguous" ;;
  esac
  case "$value" in
    [[:space:]]* | *[[:space:]]) fail "$label \"$value\" has leading or trailing whitespace" ;;
  esac
  bytes="$(byte_length "$value")"
  if [ "$bytes" -gt "$MAX_IDENTIFIER_BYTES" ]; then
    fail "$label \"$value\" is $bytes UTF-8 bytes, above the $MAX_IDENTIFIER_BYTES-byte limit"
  fi
}

# Mirrors `resolveProjectRoot` in `extension/src/config.ts`: the environment
# variable, then every ancestor for .git, then every ancestor for a language
# marker, stopping at the home directory and never selecting it.
resolve_project_root() {
  local start="$1" current candidate marker home
  home="${HOME:-}"

  if [ -n "${OMP_PROJECT_ROOT:-}" ]; then
    printf '%s\t%s\n' "$(cd -- "$OMP_PROJECT_ROOT" 2>/dev/null && pwd || printf '%s' "$OMP_PROJECT_ROOT")" "OMP_PROJECT_ROOT"
    return
  fi

  local ancestors=()
  current="$(cd -- "$start" && pwd)"
  while :; do
    if [ -n "$home" ] && [ "$current" = "$(cd -- "$home" 2>/dev/null && pwd || printf '%s' "$home")" ]; then
      break
    fi
    ancestors+=("$current")
    if [ "$current" = "/" ]; then break; fi
    current="$(dirname -- "$current")"
  done

  for candidate in "${ancestors[@]+"${ancestors[@]}"}"; do
    if [ -e "$candidate/.git" ]; then
      printf '%s\t%s\n' "$candidate" ".git"
      return
    fi
  done
  for candidate in "${ancestors[@]+"${ancestors[@]}"}"; do
    for marker in "${PROJECT_MARKERS[@]}"; do
      if [ -e "$candidate/$marker" ]; then
        printf '%s\t%s\n' "$candidate" "$marker"
        return
      fi
    done
  done
  printf '%s\t%s\n' "$(cd -- "$start" && pwd)" "working directory"
}

# --- Arguments ---------------------------------------------------------------

address="$DEFAULT_ADDRESS"
startup="$DEFAULT_STARTUP"
peer=""
purpose_file=""
project=""
task=""
project_root_flag=""
agent_dir_flag=""
do_build=0
force=0
dry_run=0

require_value() {
  [ "$2" -gt 0 ] || fail "$1 requires a value"
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --help | -h)
      usage
      exit 0
      ;;
    --address) require_value "$1" "$(($# - 1))" && address="$2" && shift 2 ;;
    --startup) require_value "$1" "$(($# - 1))" && startup="$2" && shift 2 ;;
    --peer) require_value "$1" "$(($# - 1))" && peer="$2" && shift 2 ;;
    --purpose-file) require_value "$1" "$(($# - 1))" && purpose_file="$2" && shift 2 ;;
    --project) require_value "$1" "$(($# - 1))" && project="$2" && shift 2 ;;
    --task) require_value "$1" "$(($# - 1))" && task="$2" && shift 2 ;;
    --project-root) require_value "$1" "$(($# - 1))" && project_root_flag="$2" && shift 2 ;;
    --agent-dir) require_value "$1" "$(($# - 1))" && agent_dir_flag="$2" && shift 2 ;;
    --build)
      do_build=1
      shift
      ;;
    --force)
      force=1
      shift
      ;;
    --dry-run)
      dry_run=1
      shift
      ;;
    *) fail "unrecognized argument \"$1\"; run with --help for usage" ;;
  esac
done

[ -n "$task" ] || fail "--task is required; a topic has no defensible default and is never inferred"

case "$startup" in
  manual | auto) ;;
  *) fail "--startup must be \"manual\" or \"auto\", not \"$startup\"" ;;
esac

case "$address" in
  *:*) ;;
  *) fail "--address \"$address\" is not a host:port pair" ;;
esac
address_port="${address##*:}"
case "$address_port" in
  '' | *[!0-9]*) fail "--address \"$address\" has no numeric port" ;;
esac
if [ "$address_port" -lt 1 ] || [ "$address_port" -gt 65535 ]; then
  fail "--address \"$address\" has port $address_port, outside 1-65535"
fi

# --- Resolution, reported before anything is written -------------------------

if [ -n "$project_root_flag" ]; then
  [ -d "$project_root_flag" ] || fail "--project-root \"$project_root_flag\" is not a directory"
  project_root="$(cd -- "$project_root_flag" && pwd)"
  project_root_marker="--project-root"
else
  IFS=$'\t' read -r project_root project_root_marker <<<"$(resolve_project_root "$PWD")"
fi

if [ -n "$agent_dir_flag" ]; then
  agent_dir="$agent_dir_flag"
elif [ -n "${PI_CODING_AGENT_DIR:-}" ]; then
  agent_dir="$PI_CODING_AGENT_DIR"
else
  [ -n "${HOME:-}" ] || fail "neither --agent-dir, PI_CODING_AGENT_DIR, nor HOME is set"
  agent_dir="$HOME/.omp/agent"
fi

[ -n "$project" ] || project="$(basename -- "$project_root")"

global_path="$agent_dir/$CONFIG_FILE_NAME"
project_path="$project_root/.omp/$CONFIG_FILE_NAME"
skill_source="$REPO_ROOT/extension/skill/$SKILL_NAME"
skill_target="$agent_dir/skills/$SKILL_NAME"

printf 'setup-client: project root %s (decided by %s)\n' "$project_root" "$project_root_marker"
printf 'setup-client: agent directory %s\n' "$agent_dir"

# --- Validation, before any write --------------------------------------------

validate_identifier "room.project" "$project"
validate_identifier "room.task" "$task"
[ -z "$peer" ] || validate_identifier "peer.name" "$peer"

purpose=""
if [ -n "$purpose_file" ]; then
  [ -f "$purpose_file" ] || fail "--purpose-file \"$purpose_file\" does not exist"
  purpose="$(cat -- "$purpose_file")"
  [ -n "$(printf '%s' "$purpose" | tr -d '[:space:]')" ] ||
    fail "--purpose-file \"$purpose_file\" is empty; omit the flag rather than writing nothing"
  purpose_bytes="$(byte_length "$purpose")"
  if [ "$purpose_bytes" -gt "$MAX_PURPOSE_BYTES" ]; then
    fail "--purpose-file \"$purpose_file\" is $purpose_bytes UTF-8 bytes, above the $MAX_PURPOSE_BYTES-byte budget"
  fi
fi

[ -d "$skill_source" ] || fail "the skill source $skill_source is missing from this checkout"

# Both refusals are decided before either file is written, so a run that would
# have declined the second file does not leave the first one replaced.
if [ "$force" -eq 0 ]; then
  for existing in "$global_path" "$project_path"; do
    if [ -e "$existing" ]; then
      fail "$existing already exists; pass --force to replace it, or move it aside"
    fi
  done
fi

# --- Composition --------------------------------------------------------------

compose_global() {
  printf 'transport:\n  mode: local\n  address: %s\n' "$address"
  printf 'startup: %s\n' "$startup"
  if [ -n "$peer" ] || [ -n "$purpose" ]; then
    printf 'peer:\n'
    [ -z "$peer" ] || printf '  name: %s\n' "$peer"
    if [ -n "$purpose" ]; then
      printf '  purpose: |\n'
      printf '%s\n' "$purpose" | sed 's/^/    /'
    fi
  fi
}

compose_project() {
  printf 'room:\n  project: %s\n  task: %s\n' "$project" "$task"
}

if [ "$dry_run" -eq 1 ]; then
  printf 'setup-client: dry run; nothing below is performed.\n'
  printf '  would write %s:\n' "$global_path"
  compose_global | sed 's/^/    | /'
  printf '  would write %s:\n' "$project_path"
  compose_project | sed 's/^/    | /'
  printf '  would install %s/ to %s/\n' "$skill_source" "$skill_target"
  if [ "$do_build" -eq 1 ]; then
    printf '  would run: bun run build (in %s/extension)\n' "$REPO_ROOT"
  fi
  printf '  would print the command that registers the extension\n'
  exit 0
fi

# --- Writing -------------------------------------------------------------------

mkdir -p -- "$agent_dir"
compose_global >"$global_path"
printf 'setup-client: wrote %s\n' "$global_path"

mkdir -p -- "$project_root/.omp"
compose_project >"$project_path"
printf 'setup-client: wrote %s\n' "$project_path"

# The skill is a shipped artifact rather than operator text, so it is refreshed
# rather than preserved: an installation carrying last release's workflow beside
# this release's tool is the failure this step exists to prevent.
mkdir -p -- "$skill_target"
cp -R -- "$skill_source/." "$skill_target/"
printf 'setup-client: installed the %s skill to %s\n' "$SKILL_NAME" "$skill_target"

# --- Optional build --------------------------------------------------------------

if [ "$do_build" -eq 1 ]; then
  expected_bun="$(sed -n 's/.*"@types\/bun": "\([^"]*\)".*/\1/p' "$REPO_ROOT/extension/package.json" | head -n1)"
  if ! command -v bun >/dev/null 2>&1; then
    fail "bun is not on PATH and this script installs no toolchain; install bun ${expected_bun:-see extension/package.json} and re-run with --build"
  fi
  printf 'setup-client: building the bundle with bun %s (this project expects %s)\n' \
    "$(bun --version)" "${expected_bun:-see extension/package.json}"
  (cd -- "$REPO_ROOT/extension" && bun run build)
fi

# --- What to do next -------------------------------------------------------------

cat <<NEXT
setup-client: done. This script started no relay and ran no agent.
setup-client: to start a relay, see "Deployment and security" in $REPO_ROOT/README.md.
setup-client: register the extension and start the agent with:

omp --extension "$REPO_ROOT/extension/dist/index.js"
NEXT
