"""Every repo-bundled plugin manifest must parse through the real loader.

Regressions like an unescaped quote inside a double-quoted ``description``
(e.g. ``status:"duplicate"``) make ``parse_manifest_file`` return ``None``:
the plugin directory is silently skipped, its ``register()`` never runs, and
its hooks stop firing in production while unit tests stay green (they only
ever load synthetic manifests from ``tmp_path``). This suite closes that gap
by parsing the manifests actually shipped in the repo.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from hermes_cli.plugins_manifest import parse_manifest_file

REPO_PLUGINS_DIR = Path(__file__).resolve().parents[2] / "plugins"

_ALL_MANIFESTS = sorted(REPO_PLUGINS_DIR.glob("*/plugin.yaml"))

if not _ALL_MANIFESTS:  # pragma: no cover – guards against path drift
    raise RuntimeError(f"No plugin.yaml found under {REPO_PLUGINS_DIR}")


@pytest.mark.parametrize(
    "manifest_file",
    _ALL_MANIFESTS,
    ids=lambda p: p.parent.name,
)
def test_repo_plugin_manifest_parses(manifest_file: Path) -> None:
    """The loader must accept every bundled manifest (``None`` = skipped plugin)."""
    parsed = parse_manifest_file(manifest_file, manifest_file.parent, "builtin", "")
    assert parsed is not None, (
        f"{manifest_file} failed to parse — the loader would skip this plugin "
        "(hooks/tools silently dead)"
    )
    assert isinstance(parsed.name, str) and parsed.name.strip(), "name must be a non-empty string"
