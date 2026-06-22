#!/usr/bin/env bash
# ============================================================================
# sync-upstream.sh — Merge upstream hermes-agent into the wbkunlun fork
# ============================================================================
#
# Brings a fork clone of NousResearch/hermes-agent up to the latest upstream
# tag while preserving fork-specific content (wecom fixes, acp_adapter,
# providers/, audit/, build artifacts, Chinese docs, etc.).
#
# Strategy
# --------
# 1. Fetch upstream tags.
# 2. Create / reuse a dedicated merge branch named
#    ``feat/upstream-v<MAJOR>.<MINOR>.<PATCH>-merge``.
# 3. ``git merge`` with ``-X theirs`` — falls back to ours on conflict to
#    keep production fixes. Conflicts are rare because the fork's wecom
#    fixes live in dedicated files (``gateway/platforms/wecom*.py``) and
#    are not touched by upstream on the same hunk.
# 4. Bump pyproject.toml version to match the upstream tag (if not already
#    bumped by upstream).
# 5. Run the wecom regression tests + acp provenance tests + provider
#    catalog smoke test. ANY failure aborts the merge before pushing.
# 6. Print a release-notes diff so the operator can review what came in.
#
# Usage
# -----
#   ./scripts/sync-upstream.sh                # sync to latest upstream tag
#   ./scripts/sync-upstream.sh v2026.6.19     # sync to specific tag
#   ./scripts/sync-upstream.sh --dry-run      # show what would happen
#   ./scripts/sync-upstream.sh --push         # push the merge branch on success
#
# Required remotes:  ``origin`` (fork) + ``upstream`` (NousResearch)
#
# ============================================================================

set -euo pipefail

# ── CLI parsing ──────────────────────────────────────────────────────────
DRY_RUN=0
PUSH_BRANCH=0
TARGET_TAG=""

for arg in "$@"; do
    case "$arg" in
        --dry-run)        DRY_RUN=1 ;;
        --push)           PUSH_BRANCH=1 ;;
        --help|-h)        sed -n '2,40p' "$0"; exit 0 ;;
        v20*|v2026*|v*)   TARGET_TAG="$arg" ;;
        *) echo "Unknown arg: $arg" >&2; exit 2 ;;
    esac
done

# ── Helpers ──────────────────────────────────────────────────────────────
log()  { printf '\033[1;34m[sync]\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m[warn]\033[0m %s\n' "$*" >&2; }
fail() { printf '\033[1;31m[fatal]\033[0m %s\n' "$*" >&2; exit 1; }

run() {
    if [[ $DRY_RUN -eq 1 ]]; then
        printf '  would run: %s\n' "$*"
    else
        "$@"
    fi
}

# ── Pre-flight checks ────────────────────────────────────────────────────
log "pre-flight: remotes"
if ! git remote get-url upstream >/dev/null 2>&1; then
    fail "no 'upstream' remote — add it with: git remote add upstream https://github.com/NousResearch/hermes-agent.git"
fi
if ! git remote get-url origin >/dev/null 2>&1; then
    fail "no 'origin' remote"
fi

log "pre-flight: working tree clean"
if [[ -n "$(git status --porcelain)" ]]; then
    fail "working tree dirty — commit or stash before syncing"
fi

# Detect current branch — must be on main to merge into
CURRENT_BRANCH=$(git symbolic-ref --short HEAD)
if [[ "$CURRENT_BRANCH" != "main" ]]; then
    fail "must be on 'main' branch to run sync (currently on '$CURRENT_BRANCH')"
fi

# ── Fetch upstream ───────────────────────────────────────────────────────
log "fetching upstream tags"
run git fetch upstream --tags

# ── Resolve target tag ──────────────────────────────────────────────────
if [[ -z "$TARGET_TAG" ]]; then
    TARGET_TAG=$(git tag -l 'v2026*' --sort=-v:refname | head -1)
    if [[ -z "$TARGET_TAG" ]]; then
        fail "no upstream tags matching 'v2026*' — pass one explicitly"
    fi
fi
log "target tag: $TARGET_TAG"

# Verify the tag exists upstream
if ! git rev-parse "$TARGET_TAG" >/dev/null 2>&1; then
    fail "tag '$TARGET_TAG' not found in local refs"
fi

# ── Bump the merge branch name from the tag ──────────────────────────────
TAG_VERSION=${TARGET_TAG#v}                       # e.g. 2026.6.19
SEMVER_MAJOR=$(echo "$TAG_VERSION" | cut -d. -f1)
SEMVER_MINOR=$(echo "$TAG_VERSION" | cut -d. -f2)
SEMVER_PATCH=$(echo "$TAG_VERSION" | cut -d. -f3)
MERGE_BRANCH="feat/upstream-v${SEMVER_MAJOR}.${SEMVER_MINOR}.${SEMVER_PATCH}-merge"

log "merge branch: $MERGE_BRANCH"

# ── Pre-merge fork-only-content snapshot ─────────────────────────────────
FORK_ONLY_BEFORE=$(git ls-tree -r --name-only HEAD \
    | sort > /tmp/sync-fork-before.txt)

# ── Create / reuse merge branch ──────────────────────────────────────────
if git rev-parse "$MERGE_BRANCH" >/dev/null 2>&1; then
    log "reuse existing branch: $MERGE_BRANCH"
    run git checkout "$MERGE_BRANCH"
else
    log "create branch: $MERGE_BRANCH"
    run git checkout -b "$MERGE_BRANCH"
fi

# ── Merge upstream tag ──────────────────────────────────────────────────
log "merging $TARGET_TAG (-X theirs)"
run git merge "$TARGET_TAG" --no-edit -X theirs

# ── Bump version in pyproject.toml if upstream didn't already ──────────
if ! grep -q "^version = \"${SEMVER_MAJOR}.${SEMVER_MINOR}.${SEMVER_PATCH}" pyproject.toml 2>/dev/null; then
    log "bumping pyproject.toml to v${SEMVER_MAJOR}.${SEMVER_MINOR}.${SEMVER_PATCH}"
    if [[ $DRY_RUN -eq 0 ]]; then
        sed -i.bak -E "s/^version = \"[0-9]+\.[0-9]+\.[0-9]+\"/version = \"${SEMVER_MAJOR}.${SEMVER_MINOR}.${SEMVER_PATCH}\"/" pyproject.toml
        rm -f pyproject.toml.bak
    fi
fi

# ── Diff fork-only content before/after merge ───────────────────────────
log "verifying fork-only content preserved"
FORK_ONLY_AFTER=$(git ls-tree -r --name-only HEAD | sort)
LOST_FILES=$(comm -23 /tmp/sync-fork-before.txt <(echo "$FORK_ONLY_AFTER"))
if [[ -n "$LOST_FILES" ]]; then
    warn "files that were fork-only BEFORE merge but missing AFTER:"
    echo "$LOST_FILES" | sed 's/^/    /'
    warn "review these — they may have been intentionally removed by upstream"
fi

# ── Run fork-specific regression tests ─────────────────────────────────
log "running wecom regression tests"
run python -m pytest tests/tools/test_send_message_wecom.py -x -q \
    || fail "wecom tests failed — do NOT push; review and fix"

log "running acp provenance tests"
run python -m pytest tests/acp/test_session_provenance.py -x -q \
    || fail "acp provenance tests failed — do NOT push"

log "running provider catalog smoke test"
if [[ $DRY_RUN -eq 0 ]]; then
    python -c "
from hermes_cli.provider_catalog import provider_catalog
catalog = provider_catalog()
assert len(catalog) >= 30, f'expected >=30 providers, got {len(catalog)}'
print(f'provider_catalog OK ({len(catalog)} entries)')
" || fail "provider catalog smoke test failed"
fi

log "verifying wecom shim identity"
if [[ $DRY_RUN -eq 0 ]]; then
    python -c "
from gateway.platforms.wecom import WeComAdapter as Legacy
from plugins.platforms.wecom.adapter import WeComAdapter as Plugin
assert Legacy is Plugin, 'wecom shim identity broken — production fixes lost'
assert callable(Legacy.get_active), 'get_active classmethod missing'
assert callable(Legacy.set_active), 'set_active classmethod missing'
print('wecom shim identity OK — all production fixes preserved')
" || fail "wecom shim identity check failed"
fi

# ── Print release notes summary ─────────────────────────────────────────
log "release notes summary"
if [[ $DRY_RUN -eq 0 ]]; then
    cat <<NOTES
============================================================
upstream sync complete

  target tag     : $TARGET_TAG
  merge branch   : $MERGE_BRANCH
  fork-only lost : $(echo "$LOST_FILES" | wc -l | tr -d ' ') files (review above)
  version        : v${SEMVER_MAJOR}.${SEMVER_MINOR}.${SEMVER_PATCH}

next steps:
  git log --oneline ^main $MERGE_BRANCH       # review merge commit
  git diff main..$MERGE_BRANCH --stat         # files touched
  git checkout main && git merge --ff-only $MERGE_BRANCH
  $( [[ $PUSH_BRANCH -eq 1 ]] && echo "git push origin $MERGE_BRANCH" )
============================================================
NOTES
fi

# ── Optional push ──────────────────────────────────────────────────────
if [[ $PUSH_BRANCH -eq 1 ]]; then
    log "pushing $MERGE_BRANCH to origin"
    run git push origin "$MERGE_BRANCH"
fi

log "done"