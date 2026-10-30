#!/bin/sh
# PostToolUse (Edit|Write): ruff-format the edited file if it is Python. --force-exclude makes ruff honor the
# pyproject excludes even for an explicit path, so legacy files (and the user's WIP in them) are never reformatted.
f=$(python3 -c 'import json, sys; print(json.load(sys.stdin).get("tool_input", {}).get("file_path", ""))')
case "$f" in
  *.py) ;;
  *) exit 0 ;;
esac
cd "${CLAUDE_PROJECT_DIR:-.}" || exit 0
uv run --quiet ruff format --force-exclude "$f"
