#!/usr/bin/env bash
#
# Writes the two OMP Relay client configuration files and installs the
# extension and its collaboration skill.
#
# This exists because the two-layer configuration made manual setup tedious: one
# heredoc became two files in two locations plus two installations. It is a
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
readonly EXTENSION_NAME="omp-relay"

readonly REPO_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"

usage() {
  cat <<USAGE
setup-client.sh — write OMP Relay client configuration and install its extension and skill

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
  <agent-dir>/${CONFIG_FILE_NAME}                         transport, startup, peer
  <project-root>/.omp/${CONFIG_FILE_NAME}                 room
  <agent-dir>/extensions/${EXTENSION_NAME}/index.js       the OMP extension
  <agent-dir>/skills/${SKILL_NAME}/                       the collaboration skill

Existing global and project files are kept and named unless --force is passed.
The corresponding flags then decide nothing, while the extension and skill are
still refreshed.

What this script will not do:
  * It installs no toolchain. A missing bun is reported with the version this
    project expects; nothing is fetched.
  * It starts no relay, through Compose or otherwise. Where a relay runs is a
    deployment decision; see "Deployment and security" in README.md.
  * It does not run the agent.
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

# A double-quoted YAML scalar, because the identifier rules permit '#', ': ', a
# leading '!', and newlines, none of which survive an unquoted plain scalar:
# `task: #471` is a comment, `task: a: b` is not YAML at all, and a newline
# composes whatever the operator's value says next. Shell quoting quotes the
# shell, not the file this writes.
yaml_scalar() {
  local value="$1"
  value="${value//\\/\\\\}"
  value="${value//\"/\\\"}"
  value="${value//$'\r'/\\r}"
  value="${value//$'\n'/\\n}"
  printf '"%s"' "$value"
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

# Mirrors `parseAddress` in `extension/src/config.ts`, which asks more than a
# colon and a number: an IPv6 literal must be bracketed, the host may not be
# empty, and an unbracketed second colon is the ambiguity the brackets exist to
# remove. A weaker grammar here accepts `host:80:90` and `:7788` and writes a
# file the extension then rejects.
validate_address() {
  local value="$1" host port_text after digits

  case "$value" in
    '['*)
      case "$value" in
        *']:'*) ;;
        *) fail "--address \"$value\" opens a bracketed host but is not [host]:port" ;;
      esac
      host="${value#\[}"
      host="${host%%]*}"
      after="${value#*]}"
      port_text="${after#:}"
      [ -n "$host" ] || fail "--address \"$value\" has an empty bracketed host"
      ;;
    *)
      case "$value" in
        *:*) ;;
        *) fail "--address \"$value\" is not a host:port pair" ;;
      esac
      host="${value%:*}"
      port_text="${value##*:}"
      [ -n "$host" ] || fail "--address \"$value\" has an empty host"
      case "$host" in
        *:*) fail "--address \"$value\" has an unbracketed colon in the host; an IPv6 literal is written as [::1]:7788" ;;
      esac
      ;;
  esac

  case "$port_text" in
    '' | *[!0-9]*) fail "--address \"$value\" has no numeric port" ;;
  esac
  # Leading zeros are removed before comparing, because a long run of digits
  # overflows the shell's integers where the extension's Number() is merely out
  # of range, and both sides must reach the same verdict.
  digits="${port_text#"${port_text%%[!0]*}"}"
  if [ "${#digits}" -gt 5 ] || [ "${digits:-0}" -lt 1 ] || [ "${digits:-0}" -gt 65535 ]; then
    fail "--address \"$value\" has port $port_text, outside 1-65535"
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

validate_address "$address"

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
extension_source="$REPO_ROOT/extension/dist/index.js"
extension_target_dir="$agent_dir/extensions/$EXTENSION_NAME"
extension_target="$extension_target_dir/index.js"
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
  # Not the bytes of this variable: `cat` stripped the trailing newlines and the
  # literal block below puts exactly one back, so what YAML parses is this text
  # plus that newline. Measured here, a 4096-byte purpose passes and is then
  # emitted as 4097 bytes, which the extension rejects.
  purpose_bytes="$(($(byte_length "$purpose") + 1))"
  if [ "$purpose_bytes" -gt "$MAX_PURPOSE_BYTES" ]; then
    fail "--purpose-file \"$purpose_file\" becomes $purpose_bytes UTF-8 bytes of YAML, its text plus the newline the literal block keeps, above the $MAX_PURPOSE_BYTES-byte budget"
  fi
fi

[ -d "$skill_source" ] || fail "the skill source $skill_source is missing from this checkout"
if [ "$do_build" -eq 0 ]; then
  [ -f "$extension_source" ] || fail "the extension bundle $extension_source is missing; re-run with --build"
fi

# `[ -L ]` is lstat-based where `[ -e ]` is not: it sees a dangling link, and it
# is the only test that refuses before `mkdir -p` and `>` follow one. A
# repository can track `.omp` or the project file as a symlink, and following it
# writes outside the checkout or truncates whatever it points at, so every
# destination and every directory this creates is refused by name.
for linked in "$agent_dir" "$global_path" "$agent_dir/extensions" \
  "$extension_target_dir" "$extension_target" "$agent_dir/skills" "$skill_target" \
  "$project_root/.omp" "$project_path"; do
  if [ -L "$linked" ]; then
    fail "$linked is a symbolic link; this writes regular files into real directories, so move the link aside and re-run"
  fi
done

# The same preflight, for the other precondition `mkdir -p` and `>` discover
# only once they reach it: a regular-file `.omp` failed the second parent
# creation after the global file had already been replaced. Every type here is
# knowable before the first write, so each wrong one is refused by name.
for must_be_dir in "$agent_dir" "$agent_dir/extensions" "$extension_target_dir" \
  "$agent_dir/skills" "$skill_target" "$project_root/.omp"; do
  if [ -e "$must_be_dir" ] && [ ! -d "$must_be_dir" ]; then
    fail "$must_be_dir exists and is not a directory; this creates a directory there, so move it aside and re-run"
  fi
done
for must_be_file in "$global_path" "$extension_target" "$project_path"; do
  if [ -e "$must_be_file" ] && [ ! -f "$must_be_file" ]; then
    fail "$must_be_file exists and is not a regular file; this writes a regular file there, so move it aside and re-run"
  fi
done

# Before the first write rather than after it: the contract for a missing
# toolchain is that nothing is installed, and a check that runs once both files
# and the skill are in place cannot keep it.
if [ "$do_build" -eq 1 ]; then
  expected_bun="$(sed -n 's/.*"@types\/bun": "\([^"]*\)".*/\1/p' "$REPO_ROOT/extension/package.json" | head -n1)"
  command -v bun >/dev/null 2>&1 ||
    fail "bun is not on PATH and this script installs no toolchain; install bun ${expected_bun:-see extension/package.json} and re-run with --build"
fi

# --- Composition --------------------------------------------------------------

compose_global() {
  printf 'transport:\n  mode: local\n  address: %s\n' "$(yaml_scalar "$address")"
  printf 'startup: %s\n' "$startup"
  if [ -n "$peer" ] || [ -n "$purpose" ]; then
    printf 'peer:\n'
    [ -z "$peer" ] || printf '  name: %s\n' "$(yaml_scalar "$peer")"
    if [ -n "$purpose" ]; then
      printf '  purpose: |\n'
      printf '%s\n' "$purpose" | sed 's/^/    /'
    fi
  fi
}

compose_project() {
  printf 'room:\n  project: %s\n  task: %s\n' "$(yaml_scalar "$project")" "$(yaml_scalar "$task")"
}

# Existing operator configuration is preserved by default, but it must not
# prevent the shipped extension and skill from being refreshed. `--force`
# replaces both configuration files; otherwise each existing file is kept and
# reported, and only its absent counterpart is composed.
global_keep=0
project_keep=0
if [ "$force" -eq 0 ]; then
  [ ! -e "$global_path" ] || global_keep=1
  [ ! -e "$project_path" ] || project_keep=1
fi

if [ "$dry_run" -eq 1 ]; then
  printf 'setup-client: dry run; nothing below is performed.\n'
  if [ "$global_keep" -eq 1 ]; then
    printf '  would keep %s; transport, startup, and peer would remain from that file\n' "$global_path"
  else
    printf '  would write %s:\n' "$global_path"
    compose_global | sed 's/^/    | /'
  fi
  if [ "$project_keep" -eq 1 ]; then
    printf '  would keep %s; the room would come from that file, not from --project/--task:\n' "$project_path"
    sed 's/^/    | /' <"$project_path"
  else
    printf '  would write %s:\n' "$project_path"
    compose_project | sed 's/^/    | /'
  fi
  printf '  would install %s/ to %s/\n' "$skill_source" "$skill_target"
  if [ "$do_build" -eq 1 ]; then
    printf '  would run: bun run build (in %s/extension)\n' "$REPO_ROOT"
  fi
  printf '  would install the extension bundle %s to %s\n' "$extension_source" "$extension_target"
  exit 0
fi

# --- Writing -------------------------------------------------------------------

mkdir -p -- "$agent_dir"
if [ "$global_keep" -eq 1 ]; then
  printf 'setup-client: kept %s; transport, startup, and peer remain from that file\n' "$global_path"
else
  compose_global >"$global_path"
  printf 'setup-client: wrote %s\n' "$global_path"
fi

if [ "$project_keep" -eq 1 ]; then
  printf 'setup-client: kept %s; the room comes from that file, not from --project/--task:\n' "$project_path"
  sed 's/^/setup-client:   | /' <"$project_path"
else
  mkdir -p -- "$project_root/.omp"
  compose_project >"$project_path"
  printf 'setup-client: wrote %s\n' "$project_path"
fi

# The skill is a shipped artifact rather than operator text, so it is refreshed
# rather than preserved: an installation carrying last release's workflow beside
# this release's tool is the failure this step exists to prevent.
mkdir -p -- "$skill_target"
cp -R -- "$skill_source/." "$skill_target/"
printf 'setup-client: installed the %s skill to %s\n' "$SKILL_NAME" "$skill_target"

# --- Optional build --------------------------------------------------------------

if [ "$do_build" -eq 1 ]; then
  printf 'setup-client: building the bundle with bun %s (this project expects %s)\n' \
    "$(bun --version)" "${expected_bun:-see extension/package.json}"
  (cd -- "$REPO_ROOT/extension" && bun run build)
fi

# The host discovers `<agent-dir>/extensions/<name>/index.js` without a CLI
# flag. Copy after an optional build so `--build` installs the bundle it made.
[ -f "$extension_source" ] || fail "the build completed without producing $extension_source"
mkdir -p -- "$extension_target_dir"
cp -- "$extension_source" "$extension_target"
printf 'setup-client: installed the %s extension to %s\n' "$EXTENSION_NAME" "$extension_target"

# --- Completion -----------------------------------------------------------------

printf 'setup-client: installation complete.\n'
