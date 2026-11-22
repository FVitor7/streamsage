# Examples

| File | Use |
| --- | --- |
| [`claude_desktop.release.json`](claude_desktop.release.json) | Claude Desktop running the installed release package (`uv tool install …`) |
| [`claude_desktop.source.json`](claude_desktop.source.json) | Claude Desktop running a source checkout with `uv run` (development) |

Replace the placeholder paths with absolute paths (`which streamsage`,
`which uv`). Desktop apps often do not inherit your shell's `PATH`. Never
put real credentials in files you commit; see
[docs/configuration.md](../docs/configuration.md) and
[docs/clients.md](../docs/clients.md).
