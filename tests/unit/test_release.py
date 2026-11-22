"""Release consistency: version, changelog and release notes."""

import importlib.util
import tomllib
from pathlib import Path

import pytest

import streamsage

ROOT = Path(__file__).resolve().parents[2]
PROJECT = tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]


def _release_notes():  # type: ignore[no-untyped-def]
    spec = importlib.util.spec_from_file_location(
        "release_notes", ROOT / "scripts" / "release_notes.py"
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_package_version_comes_from_pyproject() -> None:
    assert streamsage.__version__ == PROJECT["version"]


def test_changelog_has_notes_for_the_current_version() -> None:
    notes = _release_notes().section(
        (ROOT / "CHANGELOG.md").read_text(), PROJECT["version"]
    )
    assert "Read-only by design" in notes
    assert "## [" not in notes  # stops before the next section
    assert "]: https://" not in notes  # excludes link references


def test_release_notes_fail_for_unknown_or_empty_versions() -> None:
    module = _release_notes()
    changelog = "## [Unreleased]\n\n## [1.0.0] - 2026-01-01\n\n- a\n"
    assert module.section(changelog, "1.0.0") == "- a\n"
    for version in ("9.9.9", "Unreleased"):
        with pytest.raises(SystemExit):
            module.section(changelog, version)
