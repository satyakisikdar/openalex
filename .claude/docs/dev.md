# Dev guide

## Environment and tasks
- One env: `uv` (`pyproject.toml` + `uv.lock`). Python >= 3.13. Dev group: `pytest`, `ty`.
- Tasks via `Makefile`:
  - `make setup`: `uv sync` + `git config core.hooksPath .githooks` (local git config, run once per clone).
  - `make check`: `ruff check`, `ruff format --check`, `ty check`, `pytest`. Runs locally (no data touched).
  - `make sync`: rsync code to Yoda (see `data.md`).
- `.githooks/pre-commit` runs `make check`. Never `--no-verify`. Fix the failure instead.

## Code standards
- Type annotations on public signatures and anything non-obvious. No type-checker gymnastics; a targeted
  `# ty: ignore[rule]` with a reason is fine.
- Small, boring code. Reuse `src/utils.py` helpers (ID parsing, abstracts, manifests, pickles) before writing
  new ones. No new dependency for a few lines. No speculative abstractions.
- Right tool: pyarrow/pandas for frames, SQL (DuckDB) if set work grows, shell only for glue.
- Paths always via `pathlib.Path` (ruff `PTH`). No `os.path`, `glob.glob`, bare `open()`.
- Errors fail loudly with an actionable message, e.g.
  `raise SystemExit("merged ids missing; run flatten_merged_entries first")`.
  Never swallow exceptions (no `except: pass`, no `except Exception` silently returning a default).
- Notebooks in `notebooks/` are exploratory, excluded from lint. If a notebook is ever generated from source,
  build it with a make task, never hand-edit the output, commit source + output together, and get an
  independent review agent on new or changed teaching material before committing.

## Tests
- `tests/`: fast, side-effect-free, for logic with branches (parsers, ID converters, guards).
- Anything touching real snapshot data or a DB is a capped script run on Yoda, not a test.

## Shell gotchas (Mac and Yoda; both use zsh)
Hookify rules in `.claude/hookify.*.local.md` warn on or block several of these.
- **Aliases** (both machines): `cp='cp -iv'`, `mv='mv -iv'`, `rm='rm -iv'`, so they prompt, and in a
  non-interactive shell they do nothing. Use `command cp ...` or `\cp` (or `-f`) when overwriting is intended.
  Yoda also has `df='df -h'`, `du='du -h'`, `ln='ln -v'`; Mac has `ls='ls -G'`.
- **zsh does not word-split unquoted variables:** `F="a b"; md5 $F` passes one argument `a b`. Use an array
  (`F=(a b); md5 $F`) or `${=F}`.
- **zsh aborts the whole command on an unmatched glob** (`no matches found: requirements*.txt`), even inside
  `ls ... 2>/dev/null`. Quote the glob, use the `(N)` qualifier (`*.csv.gz(N)`), or use `find`.
- **`=word` expands to a command path** in zsh: `echo ===MAC` fails with `==MAC not found`. Quote words that
  start with `=`.
- **ssh runs remote commands in zsh too**, so the same rules apply inside `ssh yoda '...'`. Yoda's profile also
  prints terminal escape codes (`[?1000l`) into stdout, so don't byte-compare ssh output: compare `md5sum`
  hashes extracted with `grep -oE '^[0-9a-f]{32}'`.
- **Nested quotes over ssh break `python -c`.** Write the script to a file in the scratchpad and run
  `ssh yoda 'cd repo && uv run --no-sync python -' < script.py`.
- **`pgrep -f` matches the caller's own command line** when the pattern text appears in it. Use
  `ps -eo pid,cmd | grep '[a]udit_works'`.
- **Mac `grep` is ugrep:** empty alternatives like `(a|)` fail with `empty (sub)expression`.
- **`sed -i`:** Mac (BSD) needs `sed -i ''`; Yoda (GNU) takes `sed -i`.
- **Pipelines report the last command's status:** `make check | tail -1 && next` runs `next` even when the
  check fails. Use `set -o pipefail`.

## Legacy debt baseline
Legacy files are exempt from lint/format/types via `pyproject.toml` (`per-file-ignores`, `format.exclude`,
`tool.ty.overrides`). New files get no exemptions. When you clean a legacy file, remove it from those lists in
the same commit. Counts in `notes.md`.
