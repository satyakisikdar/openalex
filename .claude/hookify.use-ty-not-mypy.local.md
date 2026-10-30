---
name: use-ty-not-mypy
enabled: true
event: bash
action: warn
pattern: \bmypy\b
---
This project type-checks with ty, not mypy (user decision). Run `uv run ty check` (part of `make check`).
