"""Print the CHANGELOG.md section for a version (used by the release workflow).

Usage: python scripts/release_notes.py 1.0.0
Exits with an error if the version has no section, so a release cannot be
published without notes.
"""

import re
import sys
from pathlib import Path


def section(changelog: str, version: str) -> str:
    pattern = rf"^## \[{re.escape(version)}\][^\n]*\n(.*?)(?=^## \[|^\[[^\]]+\]: |\Z)"
    match = re.search(pattern, changelog, flags=re.MULTILINE | re.DOTALL)
    if match is None or not match.group(1).strip():
        raise SystemExit(f"CHANGELOG.md has no notes for version {version}")
    return match.group(1).strip() + "\n"


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("usage: release_notes.py VERSION")
    root = Path(__file__).resolve().parents[1]
    sys.stdout.write(section((root / "CHANGELOG.md").read_text(), sys.argv[1]))
