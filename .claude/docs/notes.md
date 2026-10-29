# Maintainer notes

## Known bugs in `preprocessing/flatten_openalex_files.py` (open)
- Works Parquet written before 2026-10-29 (pre `atomic_write_parquet`) is not validated: a file truncated by
  an old crash still exists and is still skipped. Check with `pq.read_metadata` if in doubt.
- Between a SIGKILL and the next re-run, a stale `*.parquet.partial-<pid>` sits in the output dir and breaks
  directory-level `pd.read_parquet(<dir>)` (pyarrow only skips `.`/`_` prefixes). The re-run removes it.
- `flatten_works_v3(threads>1)`: the 8-item positional `args` tuple doesn't match `process_work_json_v2`'s
  10 required params (`entry_count` missing, so later args shift; `publ_skip_ids`, `source_skip_ids`,
  `topic_info_d` missing). Only `threads=1` works.
- `__main__` is hand-edited per run (README step 5); the `exit(1)` after `flatten_sources()` is a run guard,
  but it exits non-zero on success.
- No START/END log markers yet (see `data.md`). SIGTERM is handled (`src.runtime.exit_on_sigterm`).

## Yoda env drift (resolved 2026-10-28)
- Local `pyproject.toml` now matches Yoda's env (Python >=3.12, seaborn, requests, jupyterlab, ipykernel,
  brotli); code synced and `uv sync` run on Yoda 2026-10-29 (3.12.11 venv kept).

## Other debt
- `src/utils.py:451` uses `display` (notebook-only name); `peek_parquet` fails outside Jupyter.
- `string_to_bool` calls `eval()` on input strings.
- `ruff` and `pandas-stubs` are in main deps; belong in the dev group.

## Lint/type baseline (2026-10-28)
- ruff (`E,F,PTH`, excl. `_old`): 549 findings, mostly `E501` (420) and `os.path.join` (56).
  Legacy `.py` files are exempted in `pyproject.toml`; `notebooks/` and `_old/` are excluded.
- ty: 46 diagnostics in `src/` (exempted via `tool.ty.overrides`).
