# Progress

## 2026-10-29
- Built authors (10 tables, 2.58B rows) and concepts in `oct-2026/` with capped DuckDB (`ORDER BY ALL`, UTC dates)
  streaming into a pool of 8 parallel brotli writers; about 7 minutes. Real write matches the dry run by row
  count and per-row hash in every table. Rows referencing deleted-entity placeholder IDs are now dropped
  everywhere (the deleted institution I4389424196 was a real raw row); small tables rebuilt, byte-identical
  to their dry run. `oct-2026/README.md` is generated and covers all 42 tables (2.6B rows, 12 GB).
- Raw Parquet snapshot download complete: 3,108 of 3,108 files match `manifest.json` (800.9 GB).
- Built the first optimized tables in `parquet-files/oct-2026/`: 31 tables (institutions, sources, topic
  hierarchy), 23.8M rows, 101 MB; dry run and real run byte-identical. Converter fixes found on real data:
  null Crossref Funder ID in `grid`, odd Wikipedia URL forms, exact-duplicate child rows.

- `uv sync` on Yoda (Python 3.12.11 venv kept); jupyter, seaborn, brotli verified.
- Works Parquet writes are now crash-safe: `src/parquet_io.atomic_write_parquet` writes `<name>.partial-<pid>`,
  `Path.replace()`s onto the target, removes stale partials on re-run. Tests cover crash and re-run.
- All Parquet writes in `merge_all_skip_ids` and `filter_dataset.py` are atomic too; `merge_all_skip_ids` now
  writes for a single CSV and honors `overwrite`. Both scripts handle SIGTERM (`src/runtime.py`).
- Fixed `filter_dataset.py` early-return and `idx=` bugs. `peek_parquet` works outside Jupyter; audited raw
  authors/institutions/sources Parquet against the old schema (new columns in `notes.md`, pending approval).
- Approved the oct-2026 schema for institutions, sources, and the topic hierarchy (`schema-oct-2026.md`) and
  built the converter (`preprocessing/optimize_parquet.py`, `src/optimize.py`); DuckDB added as a dev
  dependency and installed on Yoda. 24 tests pass. Not yet run on real data: next step is the dry run, then
  the real write to `oct-2026/` (user asked to hold all Parquet writes on 2026-10-29).

## 2026-10-28
- Set up `CLAUDE.md` + `.claude/docs/` guides (dev, data, git, notes) and this file.
- Tooling: `make check` (ruff, ty, pytest), `.githooks/pre-commit`, `make sync` (rsync to Yoda), first tests
  for ID/abstract parsers. Legacy code exempted via explicit baseline in `pyproject.toml`.
- Added `brotli` as a direct dependency; untracked `data/works_authors_small.parquet` (kept on disk).
- Aligned deps with Yoda (Python >=3.12, seaborn, requests, jupyterlab, ipykernel); first `make sync` done.
- Open: other bugs in `flatten_openalex_files.py` (see `.claude/docs/notes.md`).
