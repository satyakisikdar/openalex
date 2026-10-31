# Progress

## 2026-10-30
- Works converter written (`write_works`, DuckDB) with every works decision approved (`schema-oct-2026.md`); two
  100-file runs on the final code match the audit formulas, pass 22 consistency checks, and are byte-identical.
  Types tightened from full-data cardinality (uint32 MAG and PMID; dict volume, issue, raw_type). Full dry run next.
- Sources rebuilt with `is_..._since_year` renamed to `in_doaj_since_year` and `high_oa_rate_since_year` (every
  `is_*`/`has_*` column is now bool); README now explains null handling in pandas. Real write matches the dry run.
- Works audits 3, 4, and 5 finished overnight and agree where they overlap (audit3's references anti-join hit
  the 60 GB cap 3 times; audit5 covers it). Decided: keep the 3,168,890 works with no year (null
  `publication_year`), drop self-references (1,059,673), dedupe MeSH and repeated authors; recorded in `parquet.md`.
- Funders audited (41,799 rows, `audit_funders.log`, `audit_funders2.log`); schema section proposed in
  `schema-oct-2026.md`. All 6 decisions approved; funder years kept up to 2030 (long-term grants).
- Built funders in `oct-2026/` (5 tables, 940,885 rows, 6.1 MB); real write byte-identical to the dry run;
  README now covers 47 tables.
- Converter is safe for columns over 2 GB (`_array`, `take`, sliced casts; proved with a 2.4 GB column on Yoda), and
  `dedupe` keeps first occurrences in input order. Regression rebuild of the 37 in-memory tables: 34 byte-identical;
  `funders_alternate_titles`, `institutions_display_name_acronyms`, and `institutions_lineage` have the same rows in a
  new within-key order (live copies not yet rebuilt).
- Rebuilt the 3 reordered tables live (institutions and funders rerun; all 51 files match the dry run).
- Awards audited and built in `oct-2026/` (DuckDB path): `awards` 17,927,707, `awards_investigators` 7,825,071,
  `awards_institutions` 5,899,561 rows, 2.5 GB, about 2 minutes; byte-identical to the dry run. `oct-2026/` now has
  50 tables (14 GB). Next: works (`works_awards` links to awards), abstracts last.
- Works section of `schema-oct-2026.md` not yet written: `audit_works6` (post-dedupe counts for authorships, MeSH,
  and concepts; nested copies vs built tables; funders) running on Yoda (`~/scratch/audit_works6.log`). Already
  known: `works_awards` needs only `award_id` (copied fields match `awards` in all 56,114,642 pairs);
  `corresponding_institution_ids` (32.0M works differ) and `authorships.countries` (1.66M differ) are not derivable.

## 2026-10-29
- Works schema proposal drafted (in chat, not yet in `schema-oct-2026.md`) from `audit_works.log`; two numbers
  in it were query artifacts: topic orphans (1.25B, really 0) and work-author duplicates (null author IDs
  collapsed). User decisions recorded in `.claude/docs/parquet.md` (non-xpac everywhere, `num_authors` =
  distinct author IDs, no `works_related_works`, funders first, abstracts last). Running overnight on Yoda,
  under `~/scratch/run_audits_supervised.sh` (reruns a failed audit up to 3 times):
  `audit_works3` (DuckDB anti-join orphans, duplicates, non-xpac row counts, derivability, name constancy),
  `audit_works4` (works with no year, self-citations), `audit_works5` (independent pyarrow check of
  missing, malformed, and dangling items; report `audit_works5_report.md`). Next: read the logs, build
  funders, finalize the works section.
- Works audit finished (31 queries + follow-up): xpac works are 137.8M of 464.8M (skipped); lineage expansion
  would grow authorships 647M -> 1.0B rows (1.55x) and is fully derivable (0 lineage differences), so it is
  dropped. Schema proposal not yet written. Added project hooks (ruff format, Yoda status at session start),
  skills (`yoda-run`, `entity-audit`, project `wrap-session`), and the `schema-coverage-verifier` agent.
- Works: raw schema checked (1,898 files, identical; differs from may-2025: `type_crossref` and grants gone,
  `awards`/`funders`/`study_designs`/`has_content` new, `ids` is a map, abstracts are a JSON string). Decided:
  keep skipping `is_xpac` works, no lineage expansion in `works_authorships`, `<entity>_id` names, `has_*` bool.
  Audit running on Yoda (`/home/ssikdar/scratch/audit_works*.log`); schema proposal next, nothing written.
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
