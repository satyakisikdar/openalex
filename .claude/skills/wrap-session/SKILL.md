---
name: wrap-session
description: End-of-session routine for this repo (overrides the user-level wrap-session) - push, verify with gh, update CLAUDE.md and progress.md, check Yoda jobs. No dated README sections.
disable-model-invocation: true
---

# wrap-session (openalex)

1. **Commit and push.** `git status --short`: stage explicit paths only, never data (`data/`, `*.parquet`,
   `*.csv.gz`, logs) or secrets; scan `git diff --cached --stat`. Commit on a branch, `git merge --ff-only` into
   main, delete the branch, `git push`. Then confirm with `gh`:
   `gh api "repos/{owner}/{repo}/commits/main" -q .sha` must equal `git rev-parse HEAD`, and `git branch -a`
   shows only main.
2. **CLAUDE.md:** edit only for new gotchas, paths, ops knowledge, or behavior changes, or to remove stale lines.
   No timestamps or session summaries.
3. **README.md:** never add dated sections (project rule). Change it only when entry points, architecture, or key
   numbers change.
4. **progress.md:** add the session's major work under today's `## YYYY-MM-DD` heading (reuse it if it exists).
5. **Yoda:** `make sync`, then run `.claude/hooks/yoda-status.sh` and report running jobs and any log with START
   but no END. Long jobs must have their follow-up steps chained on Yoda, not in a local waiter.
6. **Report:** what was pushed, what is still running on Yoda, and what is next.
