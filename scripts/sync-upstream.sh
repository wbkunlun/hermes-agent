#!/usr/bin/env bash
# ============================================================================
# sync-upstream.sh — Merge upstream hermes-agent into the wbkunlun fork (SAFE)
# ============================================================================
#
# Brings a fork clone of NousResearch/hermes-agent up to latest upstream while
# PRESERVING fork-specific fixes:
#   - wecom: clawrelay streaming, 846609 subscription-death prevention/retry,
#     auth-failure zombie recovery, send_message registration
#   - wework platform adapter (fork-only, not in upstream)
#   - gateway: /sethome prompt disabled
#   - compression circuit-breaker, aux task timeout floor, acp edit fallback
#
# Why a rewrite
# ------------
# The previous version of this script was DANGEROUS:
#   1. It merged with `-X theirs` under a comment claiming "fallback to ours".
#      In `git merge <upstream>`, `-X theirs` resolves conflicts toward the
#      UPSTREAM side — silently deleting our wecom fixes. Never bring it back.
#   2. It rewrote pyproject.toml `version` to a date string. Upstream manages
#      `version` as semver (e.g. 0.19.0); our release tags (v2026.7.20-fork1)
#      are date-based OVERLAYS and must not overwrite the semver.
#   3. Its post-merge "wecom shim identity" check imported from
#      `gateway.platforms.wecom`, a path that no longer exists (wecom moved to
#      plugins/platforms/wecom/). The check false-failed. Replaced with real
#      fork regression tests + grep marker checks.
#   4. It auto-targeted `git tag -l 'v2026*' | head -1`, which after a sync
#      can pick a FORK tag (v...-forkN) instead of an upstream ref. Now
#      defaults to the explicit, unambiguous `upstream/main`.
#
# What this script deliberately does NOT do
# -----------------------------------------
#   - NO `-X theirs` / `-X ours`: conflicts surface for MANUAL resolution so
#     fork fixes are never silently clobbered. The script HALTS on conflict.
#   - NO pyproject.toml edit.
#   - NO push, NO tag, NO submodule bump, NO build. Those are irreversible /
#     outward-facing — the operator runs them after reviewing the local merge.
#
# Procedure (matches the verified 2026-07-21 v2026.7.20-fork1 sync)
# -----------------------------------------------------------------
#   1. Pre-flight: remotes present, working tree clean.
#   2. Fetch upstream.
#   3. Create a merge branch off origin/main (the deployment line).
#   4. git merge <ref> --no-edit  → conflicts HALT for manual resolution.
#   5. Guard against leftover conflict markers.
#   6. Run fork regression tests (wecom/streaming/wework/gateway + compressor/acp).
#   7. Run resync marker checks (grep fork-fix markers; any missing → HALT).
#   8. Print irreversible next-steps for the operator to run by hand.
#
# Usage
# -----
#   ./scripts/sync-upstream.sh                  # merge upstream/main (default)
#   ./scripts/sync-upstream.sh upstream/main    # explicit ref
#   ./scripts/sync-upstream.sh v0.19.0          # merge a specific upstream tag
#   ./scripts/sync-upstream.sh --dry-run
#   PYTHON=python3.11 ./scripts/sync-upstream.sh   # pick the interpreter
#
# Required remotes:  origin (fork, wbkunlun) + upstream (NousResearch)
# ============================================================================

set -euo pipefail

DRY_RUN=0
TARGET_REF="upstream/main"
PYTHON_BIN="${PYTHON:-.venv/bin/python}"

for arg in "$@"; do
    case "$arg" in
        --dry-run)        DRY_RUN=1 ;;
        --help|-h)        sed -n '2,60p' "$0"; exit 0 ;;
        --*)              echo "Unknown flag: $arg" >&2; exit 2 ;;
        *)                TARGET_REF="$arg" ;;
    esac
done

log()  { printf '\033[1;34m[sync]\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m[warn]\033[0m %s\n' "$*" >&2; }
fail() { printf '\033[1;31m[fatal]\033[0m %s\n' "$*" >&2; exit 1; }

run() {
    if [[ $DRY_RUN -eq 1 ]]; then printf '  would run: %s\n' "$*"; else "$@"; fi
}

# ── Pre-flight ───────────────────────────────────────────────────────────
log "pre-flight: remotes"
git remote get-url upstream >/dev/null 2>&1 \
    || fail "no 'upstream' remote — add: git remote add upstream https://github.com/NousResearch/hermes-agent.git"
git remote get-url origin   >/dev/null 2>&1 || fail "no 'origin' remote"

log "pre-flight: working tree clean"
[[ -z "$(git status --porcelain)" ]] || fail "working tree dirty — commit or stash before syncing"

# ── Fetch upstream ───────────────────────────────────────────────────────
log "fetching upstream"
run git fetch upstream --tags

log "target ref: $TARGET_REF"
git rev-parse --verify "$TARGET_REF^{commit}" >/dev/null 2>&1 \
    || fail "ref '$TARGET_REF' not found after fetch"

# ── Base = origin/main (the deployment line) ─────────────────────────────
BASE="origin/main"
git rev-parse --verify "$BASE^{commit}" >/dev/null 2>&1 || fail "no '$BASE' ref"
log "base (deployment line): $BASE -> $(git rev-parse --short "$BASE")"

# Sanity: refuse to "merge" a ref that is already an ancestor of BASE
# (i.e. nothing to bring in).
if git merge-base --is-ancestor "$TARGET_REF" "$BASE"; then
    fail "$TARGET_REF is already contained in $BASE — nothing to merge"
fi

# ── Create merge branch ──────────────────────────────────────────────────
MERGE_BRANCH="feat/upstream-merge"
if git show-ref --verify --quiet "refs/heads/$MERGE_BRANCH"; then
    warn "branch '$MERGE_BRANCH' exists — reusing. Delete it first for a clean run:"
    echo "    git branch -D $MERGE_BRANCH"
    run git checkout "$MERGE_BRANCH"
else
    run git checkout -b "$MERGE_BRANCH" "$BASE"
fi

# ── Merge (NO -X strategy — surface conflicts for manual resolution) ─────
log "merging $TARGET_REF (no auto-strategy; conflicts will HALT)"
if [[ $DRY_RUN -eq 1 ]]; then
    run git merge "$TARGET_REF" --no-edit
else
    if ! git merge "$TARGET_REF" --no-edit; then
        echo
        warn "merge has conflicts — resolve them MANUALLY, preserving fork fixes:"
        echo "    git diff --name-only --diff-filter=U      # list unmerged files"
        echo "    # edit each file, then:  git add <files> && git commit --no-edit"
        echo
        warn "resolution rule: KEEP fork fixes, ADOPT upstream elsewhere. Do NOT use -X theirs."
        warn "known conflict patterns so far:"
        echo "    agent/context_compressor.py: keep compression circuit-breaker wrapper,"
        echo "       adopt upstream's new _generate_summary(...) kwargs (e.g. memory_context=)."
        echo "    gateway/run.py: keep WeCom streaming progress_callback gate,"
        echo "       add upstream's new condition (e.g. 'or _live_status_adapter is not None')."
        echo
        fail "resolve the conflicts above, then re-run this script to continue (tests + markers)."
    fi
fi

# ── Conflict-marker guard ────────────────────────────────────────────────
# Conflict markers always sit at column 0 in real source; test fixtures that
# mention them do so inside string literals (indented/quoted), so scoping to
# non-test source at BOL avoids false positives.
log "guard: no leftover conflict markers in source"
if grep -rn --include='*.py' --include='*.yml' --include='*.yaml' --include='*.toml' \
        -E '^<<<<<<< |^>>>>>>> ' . 2>/dev/null \
        | grep -v -E '/tests/|/test_' ; then
    fail "leftover conflict markers in source — resolve before continuing"
fi

# ── Fork regression tests ────────────────────────────────────────────────
PY="$PYTHON_BIN"
if [[ ! -x "$PY" ]]; then
    if command -v python3 >/dev/null 2>&1; then PY="$(command -v python3)"; else PY="python"; fi
fi
log "running fork regression tests via $PY"
run "$PY" -m pytest \
    tests/tools/test_send_message_wecom.py \
    tests/gateway/test_wecom.py \
    tests/plugins/platforms/test_wework.py \
    tests/gateway/test_run_progress_topics.py \
    tests/gateway/test_status_command.py \
    tests/gateway/test_display_config.py \
    tests/agent/test_context_compressor.py \
    tests/acp/test_session_provenance.py \
    -p no:cacheprovider -q \
    || fail "fork regression tests FAILED — do NOT tag/push; review and fix"

# ── Resync marker checks (verify no fork fix was lost) ───────────────────
log "resync marker checks (any 'MISSING' = a fork fix was lost — re-apply before tagging)"
check_marker() {
    local needle="$1" file="$2" min="$3"
    local n=0
    if [[ -f "$file" ]]; then n=$(grep -c -- "$needle" "$file" 2>/dev/null || echo 0); fi
    if [[ "$n" -lt "$min" ]]; then
        warn "MISSING marker: '$needle' in $file (found $n, need >=$min)"
        return 1
    fi
    printf '  ok (%d>=%d):  %s :: %s\n' "$n" "$min" "$file" "$needle"
    return 0
}
MARKER_FAIL=0
check_marker "WeComStreamDelivery"                       gateway/run.py                          2 || MARKER_FAIL=1
check_marker "SUPPORTS_STREAM_FRAMES"                    plugins/platforms/wecom/adapter.py      1 || MARKER_FAIL=1
check_marker "WeCom websocket already closed before read" plugins/platforms/wecom/adapter.py     1 || MARKER_FAIL=1
check_marker "except BaseException"                      plugins/platforms/wecom/adapter.py      1 || MARKER_FAIL=1
check_marker "FORK(wbkunlun)"                            gateway/run.py                          1 || MARKER_FAIL=1
check_marker "_ws_live"                                  plugins/platforms/wecom/adapter.py      1 || MARKER_FAIL=1
[[ $MARKER_FAIL -eq 0 ]] || fail "one or more fork-fix markers MISSING — re-apply the lost fix(es) before tagging/pushing"

# ── Done (local only) ────────────────────────────────────────────────────
MERGE_SHA=$(git rev-parse --short HEAD)
cat <<NOTES
============================================================
upstream merge ready (LOCAL — nothing has been pushed)

  target ref    : $TARGET_REF
  merge branch  : $MERGE_BRANCH  ($MERGE_SHA)
  base          : $BASE

verify the merge locally, then run these IRREVERSIBLE steps by hand:

  # on the fork (wbkunlun/hermes-agent)
  git log --first-parent -8                                     # fork commits still intact?
  git diff --stat "$BASE..HEAD"                                 # what came in
  git tag v<YYYY.M.D>-forkN                                     # e.g. v2026.7.20-fork1
  git branch backup/main-before-v<YYYY.M.D>-forkN "$BASE"       # safety backup
  git push origin HEAD:main                                     # fast-forward origin/main
  git push origin v<YYYY.M.D>-forkN

  # then in wehermes
  git -C src/hermes-agent-submodule checkout "$MERGE_SHA"
  git add src/hermes-agent-submodule
  git commit -m "chore: bump hermes-agent submodule to v<YYYY.M.D>-forkN (...)"
  git push origin main                                          # triggers Unit Tests only
  git tag v<YYYY.M.D>-forkN && git push origin v<YYYY.M.D>-forkN   # tag triggers docker build

final verification (NOTE: the rtk proxy mangles 'git log --oneline' display —
use these authoritative commands instead):
  git ls-remote origin refs/heads/main                         # fork main tip
  git ls-remote wehermes refs/heads/main 2>/dev/null || true
  gh run list --limit 4                                         # GHA build + unit tests
  docker manifest inspect gzkunlun/hermes:v<YYYY.M.D>-forkN
============================================================
NOTES

log "done (local). Review the merge, then run the irreversible next-steps above."
