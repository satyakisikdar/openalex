---
name: no-git-add-all
enabled: true
event: bash
action: block
pattern: git\s+add\s+(-A\b|--all\b|-u\b|--update\b|\.(\s|$))
---
Stage explicit paths only (`.claude/docs/git.md`): the user often has uncommitted work in `.idea/`, notebooks, and
`flatten_openalex_files.py`, and data files must never be committed.
