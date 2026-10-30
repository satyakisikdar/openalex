---
name: no-verify
enabled: true
event: bash
action: block
pattern: --no-verify
---
`--no-verify` is forbidden (CLAUDE.md): the pre-commit hook runs `make check`. Fix the failure instead.
