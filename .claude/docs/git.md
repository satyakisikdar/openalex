# Git guide

- Commit as you go without asking: small atomic commits, imperative subject <= 72 chars, body says why.
  `make check` must pass (the hook enforces it).
- One branch per change: `feat/`, `fix/`, `docs/`, `refactor/`, `chore/`. Start from up-to-date main
  (`git pull --rebase`; if the tree has the user's WIP, `git fetch` and check ahead/behind instead of stashing).
- Merge with `git merge --ff-only` for linear history.
- After merging: delete local and remote branch, remove worktrees, `git worktree prune && git fetch --prune`.
  Only main remains between tasks.
- Stage explicit paths only; never `git add -A` / `-u` (the user often has uncommitted WIP in `.idea/`,
  notebooks and `flatten_openalex_files.py`).
- Never force-push main (`--force-with-lease` only on your own unmerged branch).
- Never commit data (`data/`, `*.parquet`, `*.csv.gz`), DB files, logs or secrets. Scan
  `git diff --cached --stat` before committing and pushing.
- Push when someone needs the change, and at session wrap-up.
