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

## Raw Parquet vs old flattener schema (snapshot 2026-10-14; awaiting user approval)
Top-level columns are identical across every file of each table. "New" = no old column maps to it.
- **authors** (101,066,311 rows, 517 files). New: `raw_author_names`, `full_name`, `observed_orcids`
  (also `ids.observed_orcids`), `summary_stats{2yr_mean_citedness,h_index,i10_index}`, `affiliations`
  (list of institution + years), `topics`, `topic_share`, `sources` (list of source structs),
  `works_api_url`, `created_date`, `counts_by_year.oa_works_count`.
  Changed: `last_known_institution` (single) is now `last_known_institutions` (list).
  Gone: `ids.scopus/twitter/wikipedia/mag`, `hints`/`most_cited_work`.
- **institutions** (143,455 rows, 83 files). New: `lineage`, `is_super_system`, `type_id`, `image_url`,
  `image_thumbnail_url`, `roles`, `repositories`, `topics`, `topic_share`, `summary_stats`, `status`,
  `works_api_url`, `created_date`; `associated_institutions` now also carries `ror/display_name/
  country_code/type`. Gone: `ids.mag`. Old bug: flattener reads `display_name_acroynyms` (typo), so the
  old column was always empty; raw has `display_name_acronyms`.
- **sources** (268,629 rows, 136 files). New: `host_organization_lineage`, `summary_stats.h_index/i10_index`,
  `is_in_doaj_since_year`, `is_high_oa_rate`, `is_high_oa_rate_since_year`, `is_in_scielo`, `is_ojs`,
  `listed_in`, `is_preprint_repository`, `oa_flip_year`, `first_publication_year`, `last_publication_year`,
  `apc_prices`, `apc_usd`, `apc_usd_by_year`, `societies`, `alternate_titles`, `topics`, `topic_share`,
  `created_date`. Gone: `ids.fatcat`.
- Raw files sit in `updated_date=*/` dirs **and** have an `updated_date` column, so pyarrow hive-partition
  inference fails (`timestamp` vs `dictionary`). Read raw files individually (`pq.ParquetFile`) or with
  partitioning off.

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
