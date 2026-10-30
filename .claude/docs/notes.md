# Maintainer notes

## Planned
- **Possibly delete the raw Parquet snapshot** (`openalex-snapshot/data/parquet/`, 800.9 GB) to free space, but
  only after every condition below holds, and only on the user's explicit go-ahead (never automatic):
  1. Every entity is built in `oct-2026/`, works included, with dry run and real run matching.
  2. Every raw column is accounted for: kept, or dropped for a documented reason (derivable, duplicate, API URL,
     or all null) in `schema-oct-2026.md`. A column-by-column checklist per entity, not a spot check.
  3. Row counts reconcile per table against raw element counts, with every difference explained (bad years,
     duplicates, deleted IDs, xpac works).
  4. Nothing in `notes.md` still needs the raw files (open audits, the size comparison, re-runs after fixes).
  Caveat: the files can be re-downloaded from the public `s3://openalex` bucket, but it likely holds only the
  current snapshot (unverified; check OpenAlex docs or the bucket before deleting), so the 2026-10-14 version
  may not be fetchable again once a newer one is published.
- Size comparison: raw OpenAlex Parquet (`openalex-snapshot/data/parquet/<entity>/`, 800.9 GB total) vs our
  optimized tables (`parquet-files/oct-2026/`), per entity (e.g. raw `authors` 50.7 GB vs the sum of all
  `authors*` tables). Read both from the manifests (`content_length` in raw `manifest.json`, `bytes` in each
  `_manifest.json`); note what each side contains (we drop derivable and API-URL columns) so the ratio isn't
  misread. Authors done (see Benchmarks: authors); repeat for works once it is built, then all entities.

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

## Raw Parquet vs old flattener schema (snapshot 2026-10-14)
Decided 2026-10-29: new columns kept, except API URL columns (`works_api_url`, ...) which are dropped; list
columns split into child tables (so `last_known_institutions` -> `authors_last_known_institutions`); old
typos fixed (`display_name_acroynyms` -> `display_name_acronyms`); `is_high_oa_rate_since_year` int32.
`image_thumbnail_url` dropped too (derivable from `image_url`). Metadata layout (`_metadata`,
`_common_metadata`, `_manifest.json`) confirmed.
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
  country_code/type`. Gone: `ids.mag`. Old bug: flattener reads `display_name_acroynyms` (typo,
  `flatten_openalex_files.py:1355`), so the old column is the string `'null'` in all 114,881 rows of
  `csv-files/may-2025/institutions.csv.gz`; raw has `display_name_acronyms` (e.g. `['UMNG']`).
- **sources** (268,629 rows, 136 files). New: `host_organization_lineage`, `summary_stats.h_index/i10_index`,
  `is_in_doaj_since_year`, `is_high_oa_rate`, `is_high_oa_rate_since_year`, `is_in_scielo`, `is_ojs`,
  `listed_in`, `is_preprint_repository`, `oa_flip_year`, `first_publication_year`, `last_publication_year`,
  `apc_prices`, `apc_usd`, `apc_usd_by_year`, `societies`, `alternate_titles`, `topics`, `topic_share`,
  `created_date`. Gone: `ids.fatcat`.
- Raw files sit in `updated_date=*/` dirs **and** have an `updated_date` column, so pyarrow hive-partition
  inference fails (`timestamp` vs `dictionary`). Read raw files individually (`pq.ParquetFile`) or with
  partitioning off.

## Raw data quirks handled by the converter (snapshot 2026-10-14)
- `institutions.ids.grid` for I4405257899 (Association for Molecular Pathology) is `100011307`, a Crossref
  Funder ID, not a GRID ID: all-digit grid values are stored as null (logged).
- `institutions.ids.wikipedia`: 49 links use mobile hosts, language-variant paths (`/zh-hans/`, `/sr-el/`),
  or `?title=` forms; normalized to `lang:Title`. 8 are not Wikipedia (company sites, Commons, Wikivoyage,
  Fandom, a `wikipedi0` typo) and are stored as null.
- Exact-duplicate list elements (Bronovo Hospital lists itself twice in `lineage`; one institution has
  acronyms `['NGP', 'NGP']`) are dropped. `associated_institutions` repeats an ID with different
  relationships for 104 pairs (`parent` and `related`); those are distinct rows and kept.

## Benchmarks: optimized small tables (oct-2026, built 2026-10-29)
- 31 tables from institutions, sources, and the topic hierarchy: 23.8M rows, 101 MB, about 20 s end to end
  on Yoda (read, convert, write, three-reader check). Dry run and real run are byte-identical (md5).
- Defaults (5M rows per file, 1M-row row groups, brotli): every table is 1 file except `sources_topics` and
  `sources_topic_share` (2 files each). Largest: `sources_topic_share` 20.6 MB, `sources_topics` 13.7 MB,
  `sources_counts_by_year` 11.2 MB, `institutions_topic_share` 10.2 MB. Revisit sizes for authors and works.

## Benchmarks: funders (oct-2026, built 2026-10-30)
- 5 tables, 940,885 rows, 6.1 MB, one file each; whole build about 1 s (in-memory pyarrow path). Real write
  byte-identical to the dry run (15 files, md5).
- Raw quirks: 1 Wikidata value without scheme or `www.` (`wikidata.org/wiki/Q2217705`; the `wikidata` helper now
  accepts it), 22 repeated alternate titles, 387 counts-by-year entries outside 1500 to 2030.
- `role_id` (here and in `institutions_roles`) is not in `ID_COLUMNS`, so `drop_deleted` would not catch a
  deleted placeholder there; no placeholder occurs in this snapshot.

## Benchmarks: awards (oct-2026, built 2026-10-30)
- DuckDB streaming path (60 GB cap, 16 threads): 3 tables, 31,652,339 rows, 2.5 GB (`awards` 2.3 GB in 4 files,
  mostly descriptions), about 2 minutes. Real write byte-identical to the dry run (14 files, md5).
- The build log's "1,808 invalid ORCIDs stored as null" excludes the 5,000 empty strings (`nullif` handles
  those first); together they are the spec's 6,808.
- `primary_topic_id` comes from `topics[1].id` and `primary_topic_score` from `primary_topic.score`; the build
  fails if the two structs ever differ.

## Deleted and merged IDs (snapshot 2026-10-14)
- Placeholder IDs for deleted entities (`src.optimize.DELETED_IDS`): work 4285719527, author 5317838346,
  institution 4389424196, source 4317411217. Only the institution one occurs: it is a real row in raw
  institutions (dropped from 6 tables, 59 rows) and is referenced by 14 author affiliations and 1 last-known
  institution (dropped). The writers drop these rows from every table.
- `openalex-snapshot/data/merged_ids/*` directories exist but are empty. The Parquet snapshot ships one list:
  `works/deleted_ids.csv.gz` (71,528,592 work IDs with `deleted_date`), to apply when building works.

## Benchmarks: authors (oct-2026, built 2026-10-29)
- Capped DuckDB (60 GB, 16 threads) + pyarrow writers. Single writer: 19 min for concepts + authors (writing
  was the bottleneck: one thread at 100%, DuckDB idle). 8 parallel writers: about 7 min. Peak memory about
  14 GB; nothing spilled. Output 12.34 GB for the 10 authors tables.
- Codec test on a 5M-row authors file: brotli 122.9 MB (14.2 s write), zstd-9 130.0 MB (4.6 s), zstd-3
  141.0 MB (1.6 s), snappy 218.6 MB (1.1 s). Brotli kept by user decision.
- The streaming writer re-cuts batches so file i holds exactly rows [i*5M, (i+1)*5M); a dry run built with the
  older writer is compared by row count + `sum(hash(cols))` per table instead of file checksums.
- Size vs raw (from Parquet metadata and manifests): raw authors 50.62 GB (snappy; 100.49 GB uncompressed) vs
  optimized 12.34 GB (brotli), 24%. Each table as a share of its raw source columns: authors 52%,
  display_name_alternatives 65%, observed_orcids 58%, affiliations 19%, last_known_institutions 13%, topics 33%,
  topic_share 40%, concepts 53%, sources 7% (21.48 GB of nested source details -> 1.43 GB of IDs),
  counts_by_year 81%. Dropped raw columns: 4.44 GB (raw_author_names 2.55, ids 1.07, works_api_url 0.82).
  Savings come from dropping nested copies of other entities, numeric IDs, and brotli vs snappy.
- Sort order and row-group skipping (DuckDB, `authors_affiliations`, 263M rows, 263 row groups, warm cache):
  `author_id = x` 16 ms, `author_id between` 16 ms, `institution_id = Harvard` (not the sort key) 392 ms. Tables
  are fast only on their leading sort column; decide a second sort order for works lookup tables up front.

## Yoda env drift (resolved 2026-10-28)
- Local `pyproject.toml` now matches Yoda's env (Python >=3.12, seaborn, requests, jupyterlab, ipykernel,
  brotli); code synced and `uv sync` run on Yoda 2026-10-29 (3.12.11 venv kept).

## Other debt
- `string_to_bool` calls `eval()` on input strings.
- `ruff` and `pandas-stubs` are in main deps; belong in the dev group.

## Lint/type baseline (2026-10-28)
- ruff (`E,F,PTH`, excl. `_old`): 549 findings, mostly `E501` (420) and `os.path.join` (56).
  Legacy `.py` files are exempted in `pyproject.toml`; `notebooks/` and `_old/` are excluded.
- ty: 46 diagnostics in `src/` (exempted via `tool.ty.overrides`).
