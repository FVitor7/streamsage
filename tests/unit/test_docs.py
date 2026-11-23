"""Documentation and examples stay consistent with the code.

Checks that relative links and anchors resolve, that the example client
configurations are valid, and that every setting is documented while no
document mentions a setting that does not exist.
"""

import json
import re
from pathlib import Path

import pytest

from streamsage.infrastructure.config.settings import Settings

ROOT = Path(__file__).resolve().parents[2]
SKIPPED_DIRS = {".venv", ".git", "node_modules", ".pytest_cache", "dist"}
MARKDOWN = sorted(
    p for p in ROOT.rglob("*.md") if not SKIPPED_DIRS & set(p.relative_to(ROOT).parts)
)
CODE_BLOCK = re.compile(r"```.*?```", re.S)
SETTINGS = frozenset(f"STREAMSAGE_{name.upper()}" for name in Settings.model_fields)
# Mentioned in docs, but not settings: a shell variable in an example.
NOT_SETTINGS = frozenset({"STREAMSAGE_PROD_PASSWORD"})


def _anchors(markdown: str) -> set[str]:
    """Heading anchors, following GitHub's slug rules."""
    anchors: set[str] = set()
    seen: dict[str, int] = {}
    for heading in re.findall(r"^#{1,6} (.+)$", CODE_BLOCK.sub("", markdown), re.M):
        text = re.sub(r"[`*]", "", heading.strip().lower())
        slug = re.sub(r"[^\w\- ]", "", text).replace(" ", "-")
        count = seen.get(slug, 0)
        seen[slug] = count + 1
        anchors.add(slug if count == 0 else f"{slug}-{count}")
    return anchors


def _links(path: Path) -> list[str]:
    body = CODE_BLOCK.sub("", path.read_text())
    return re.findall(r"\]\(([^)\s]+)\)", body)


def test_markdown_files_are_found() -> None:
    names = {p.relative_to(ROOT).as_posix() for p in MARKDOWN}
    assert {"README.md", "docs/configuration.md", "SECURITY.md"} <= names


@pytest.mark.parametrize("path", MARKDOWN, ids=lambda p: p.relative_to(ROOT).as_posix())
def test_relative_links_and_anchors_resolve(path: Path) -> None:
    broken = []
    for target in _links(path):
        if target.startswith(("http://", "https://", "mailto:")):
            continue
        file_part, _, anchor = target.partition("#")
        destination = (path.parent / file_part).resolve() if file_part else path
        if not destination.exists():
            broken.append(f"{target} (missing file)")
        elif (
            anchor
            and destination.suffix == ".md"
            and anchor not in _anchors(destination.read_text())
        ):
            broken.append(f"{target} (missing anchor)")
    assert broken == []


def test_anchor_slugs_follow_github_rules() -> None:
    markdown = "# Big `title`\n## Q&A: what?\n## Q&A: what?\n```\n# not a heading\n```"
    assert _anchors(markdown) == {"big-title", "qa-what", "qa-what-1"}


@pytest.mark.parametrize(
    "path", sorted((ROOT / "examples").glob("*.json")), ids=lambda p: p.name
)
def test_example_client_configs_are_valid(path: Path) -> None:
    servers = json.loads(path.read_text())["mcpServers"]
    assert servers
    for server in servers.values():
        assert isinstance(server["command"], str) and server["command"]
        assert all(isinstance(arg, str) for arg in server.get("args", []))
        assert set(server.get("env", {})) <= SETTINGS


def test_every_setting_is_documented() -> None:
    configuration = (ROOT / "docs" / "configuration.md").read_text()
    env_example = (ROOT / ".env.example").read_text()
    assert sorted(s for s in SETTINGS if f"`{s}`" not in configuration) == []
    assert sorted(s for s in SETTINGS if f"{s}=" not in env_example) == []


def test_documents_only_mention_existing_settings() -> None:
    sources = [*MARKDOWN, ROOT / ".env.example", *(ROOT / "examples").glob("*.json")]
    mentioned = {
        name
        for path in sources
        for name in re.findall(r"STREAMSAGE_[A-Z0-9_]+(?![A-Z0-9_*])", path.read_text())
    }
    assert sorted(mentioned - SETTINGS - NOT_SETTINGS) == []
