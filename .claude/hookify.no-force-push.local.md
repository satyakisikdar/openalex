---
name: no-force-push
enabled: true
event: bash
action: block
pattern: git\s+push\b.*(--force(?!-with-lease)|\s-f\b)
---
Force-pushing is forbidden (`.claude/docs/git.md`). Main is never rewritten; use `--force-with-lease` only on
your own unmerged branch. To fix a pushed mistake, add a follow-up commit.
