# Progress

## 2026-10-28
- Set up `CLAUDE.md` + `.claude/docs/` guides (dev, data, git, notes) and this file.
- Tooling: `make check` (ruff, ty, pytest), `.githooks/pre-commit`, `make sync` (rsync to Yoda), first tests
  for ID/abstract parsers. Legacy code exempted via explicit baseline in `pyproject.toml`.
- Added `brotli` as a direct dependency; untracked `data/works_authors_small.parquet` (kept on disk).
- Open: non-atomic Parquet writes and other bugs in `flatten_openalex_files.py` (see `.claude/docs/notes.md`).
