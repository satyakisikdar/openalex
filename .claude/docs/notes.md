# Maintainer notes

## Known bugs in `preprocessing/flatten_openalex_files.py` (open)
- `write_to_csv_and_parquet`: writes Parquet straight to the target and skips if the target exists. A crash
  mid-write leaves a truncated file that later runs skip forever. Fix: `.partial-<pid>` + `Path.replace()`.
- `merge_all_skip_ids`: only writes the combined parquet when `len(merged_dfs) > 1`; a kind with exactly one
  merged-ids CSV never gets its parquet (falls back to reading CSVs every time).
- `flatten_works_v3(threads>1)`: the 8-item positional `args` tuple doesn't match `process_work_json_v2`'s
  10 required params (`entry_count` missing, so later args shift; `publ_skip_ids`, `source_skip_ids`,
  `topic_info_d` missing). Only `threads=1` works.
- `flatten_merged_entries(overwrite=...)` ignores its argument (always passes `overwrite=False`).
- `__main__` has a stray `exit(1)` after `flatten_sources()` that stops before works.
- No START/END markers or SIGTERM handling yet (see `data.md`).

## Other debt
- `src/utils.py:451` uses `display` (notebook-only name); `peek_parquet` fails outside Jupyter.
- `src/utils.py` imports `requests`, which isn't a direct dependency in `pyproject.toml`.
- `string_to_bool` calls `eval()` on input strings.
- `data/works_authors_small.parquet` is tracked in git (violates no-data rule); `git rm --cached` if agreed.
- `ruff` and `pandas-stubs` are in main deps; belong in the dev group.

## Lint/type baseline (2026-10-28)
- ruff (`E,F,PTH`, excl. `_old`): 549 findings, mostly `E501` (420) and `os.path.join` (56).
  Legacy `.py` files are exempted in `pyproject.toml`; `notebooks/` and `_old/` are excluded.
- ty: 46 diagnostics in `src/` (exempted via `tool.ty.overrides`).
