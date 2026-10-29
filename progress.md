# Progress

## 2026-10-29
- `uv sync` on Yoda (Python 3.12.11 venv kept); jupyter, seaborn, brotli verified.
- Works Parquet writes are now crash-safe: `src/parquet_io.atomic_write_parquet` writes `<name>.partial-<pid>`,
  `Path.replace()`s onto the target, removes stale partials on re-run. Tests cover crash and re-run.
- All Parquet writes in `merge_all_skip_ids` and `filter_dataset.py` are atomic too; `merge_all_skip_ids` now
  writes for a single CSV and honours `overwrite`. Both scripts handle SIGTERM (`src/runtime.py`).

## 2026-10-28
- Set up `CLAUDE.md` + `.claude/docs/` guides (dev, data, git, notes) and this file.
- Tooling: `make check` (ruff, ty, pytest), `.githooks/pre-commit`, `make sync` (rsync to Yoda), first tests
  for ID/abstract parsers. Legacy code exempted via explicit baseline in `pyproject.toml`.
- Added `brotli` as a direct dependency; untracked `data/works_authors_small.parquet` (kept on disk).
- Aligned deps with Yoda (Python >=3.12, seaborn, requests, jupyterlab, ipykernel); first `make sync` done.
- Open: other bugs in `flatten_openalex_files.py` (see `.claude/docs/notes.md`).
