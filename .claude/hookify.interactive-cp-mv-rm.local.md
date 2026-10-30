---
name: interactive-cp-mv-rm
enabled: true
event: bash
action: warn
pattern: (^|[;&|(`'"]\s*)(cp|mv|rm)\s+(?!-[a-zA-Z]*f)
---
`cp`, `mv`, and `rm` are aliased to `-iv` (interactive) on both the Mac and Yoda, so they prompt and then do
nothing in a non-interactive shell. On 2026-10-29 this blocked a file restore. Use `command cp ...` (or `\cp`,
or `-f`) when overwriting is intended; see `.claude/docs/dev.md`.
