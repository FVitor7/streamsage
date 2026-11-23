"""The package smoke test (scripts/smoke_test_package.py) stays accurate."""

import importlib.util
from pathlib import Path
from types import ModuleType

import pytest
from mcp import Client

from tests.unit.presentation.test_mcp_server import build

ROOT = Path(__file__).resolve().parents[2]


def _smoke() -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        "smoke_test_package", ROOT / "scripts" / "smoke_test_package.py"
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.anyio
async def test_smoke_test_expects_exactly_the_served_tools_and_prompts() -> None:
    smoke = _smoke()
    async with Client(build()) as client:
        tools = {t.name for t in (await client.list_tools()).tools}
        prompts = {p.name for p in (await client.list_prompts()).prompts}
    assert tools == smoke.EXPECTED_TOOLS
    assert prompts == smoke.EXPECTED_PROMPTS


def test_smoke_test_rejects_a_missing_file(tmp_path: Path) -> None:
    smoke = _smoke()
    assert smoke.main([str(tmp_path / "streamsage-0.0.0-py3-none-any.whl")]) == 1


def test_smoke_test_rejects_a_file_of_another_version(tmp_path: Path) -> None:
    smoke = _smoke()
    stale = tmp_path / "streamsage-0.0.1-py3-none-any.whl"
    stale.write_bytes(b"")
    with pytest.raises(smoke.SmokeTestError, match="is not version"):
        smoke.smoke_test(stale, "3.12")
