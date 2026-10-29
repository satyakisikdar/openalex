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

## Legacy debt baseline
Legacy files are exempt from lint/format/types via `pyproject.toml` (`per-file-ignores`, `format.exclude`,
`tool.ty.overrides`). New files get no exemptions. When you clean a legacy file, remove it from those lists in
the same commit. Counts in `notes.md`.
