# Optimized Parquet guide (OpenAlex Parquet snapshot -> optimized tables)

Per-snapshot table specs: `schema-oct-2026.md` (institutions, sources, topic hierarchy; proposed).

OpenAlex now ships Parquet per table. We convert those into **optimized** Parquet. Everything below applies
to the optimized outputs. The JSONL flattener (`flatten_openalex_files.py`) remains the schema reference.

## Input (read-only)
- Raw snapshot: `/data/shared/OpenAlex/openalex-snapshot/data/parquet/<table>/` + `manifest.json`
  (22 tables: works, authors, sources, institutions, topics, ...). Download from the AWS bucket was in
  progress on 2026-10-29; don't treat a table as complete until it matches `manifest.json`.
- Never modify or write into the raw snapshot dir.

## Look before you write
- Peek with `src.utils.peek_parquet(path)` (file or directory): file count, row count from metadata, schema,
  5 example rows from the first fragment, without reading the whole table. Run it on Yoda.
- It currently calls `display()` (Jupyter-only) and fails in a plain script; switch to
  `print(sample_df.to_string())` when first needed.

## Output
- Location: `/data/shared/OpenAlex/processed-snapshots/parquet-files/<mon>-<yyyy>/` (lowercase 3-letter
  month, like existing `feb-2026`, `mar-2026`; this snapshot is `oct-2026`).
- Layout: **one top-level dir per table, flat**, holding only that table's Parquet files:
  `oct-2026/works/part-00000.parquet`, `oct-2026/works_authorships/...`, `oct-2026/authors_counts_by_year/...`.
  No per-entity nesting, no partition subdirs.
- **Metadata is stored with the data** (layout confirmed by user 2026-10-29):
  - per table dir: pyarrow `_metadata` + `_common_metadata` (`pq.write_metadata`; schema and row-group
    stats for fast planning) and `_manifest.json` (per file: name, rows, bytes, row groups; table totals,
    schema, source snapshot date, build time).
  - top level: `oct-2026/_manifest.json` summarizing every table.
  - Leading `_` keeps these out of pandas/pyarrow directory reads (they skip `_`/`.` names); in DuckDB read
    `'<table>/*.parquet'`. Never a bare `manifest.json` next to the data: that breaks directory reads.
- Must read cleanly in **pandas, pyarrow, and DuckDB**. Every new table gets a check that reads it with all
  three and compares schema and row count against the raw table. A table isn't done until that passes.
- Compression: brotli.
- No `key=value` partition dirs that repeat a column name (the raw snapshot's `updated_date=*/` + `updated_date`
  column breaks pyarrow's hive inference). Plain `part-NNNNN.parquet` files per table dir.
- Split each table into multiple files with sensible row groups so readers can parallelize. Starting point,
  to be benchmarked and recorded in `notes.md`: row groups of ~1M rows, files of a few hundred MB.
- Writes follow `data.md`: crash-safe (`src.parquet_io.atomic_write_parquet` or the same partial + replace
  pattern), re-runnable, deterministic (sort by ID before writing).

## Types
- Start from the old flattener's schema: `DTYPES` (`flatten_openalex_files.py:679`) and the per-table
  `pa.schema(...)` blocks in `write_to_csv_and_parquet` (~line 2227 onward).
- **Every column gets the tightest type its real range allows**, not just IDs. Examples: author count per
  work is small and never negative (`uint16`), citation counts are larger but never negative (`uint32`),
  flags `bool`, dates `date32`, timestamps `timestamp('ms')`, low-cardinality strings (type, country_code,
  ...) dictionary-encoded.
  - Pick types from observed min/max (DuckDB `min()/max()` over the raw table), with headroom for growth.
    Record the chosen type and observed range per column in `notes.md`.
  - Cast with overflow checking (pyarrow `cast(..., safe=True)`) so a value outside the range fails loudly
    instead of wrapping.
  - User-set exceptions: `sources.is_high_oa_rate_since_year` stays `int32`.
- **OpenAlex IDs: keep only the numeric part** (`https://openalex.org/W2741809807` -> `2741809807`), stored
  as `int64` (author IDs such as `5317838346` already exceed `uint32`). Applies to every ID column,
  including ID lists once split into their own tables (`works_referenced_works.referenced_work_id`). Reuse the `convert_openalex_id_to_int` /
  `convert_topic_id_to_int` logic from `src/utils.py`, but vectorized (pyarrow/DuckDB string ops), not
  per-row Python.
- **Other canonical IDs: drop the constant, redundant part so the original is trivial to rebuild**
  (`prefix + value`). Keep the variable part; store it as an integer when it is purely numeric, else a
  string. Expected shapes (verify each against `peek_parquet` output before relying on it):

  | ID | Raw example | Stored | Rebuild |
  |---|---|---|---|
  | DOI | `https://doi.org/10.1000/xyz` | `10.1000/xyz` (string) | `https://doi.org/` + value |
  | ORCID | `https://orcid.org/0000-0002-1825-0097` | `0000-0002-1825-0097` (string; last char can be `X`) | `https://orcid.org/` + value |
  | ROR | `https://ror.org/02mhbdp94` | `02mhbdp94` (string) | `https://ror.org/` + value |
  | PMID | `https://pubmed.ncbi.nlm.nih.gov/123` | `123` (`int64`) | prefix + value |
  | PMCID | `https://www.ncbi.nlm.nih.gov/pmc/articles/PMC123` | `123` (`int64`) | prefix + `PMC` + value |
  | Wikidata | `https://www.wikidata.org/wiki/Q123` | `123` (`int64`) | prefix + `Q` + value |

  - One prefix per column. Keep the column -> prefix map as one constant in code and copy it into this table.
  - A value that doesn't start with the expected prefix fails loudly (count and examples), never silently
    passes through.
  - Already-short IDs (MAG ints, ISSN `1234-567X`) stay as they are.

## Tables and columns
- **Every multivalued (list) column becomes its own table** `<parent>_<column>`, keyed by the parent ID,
  one row per element, struct fields as columns. Nested lists are exploded further (e.g. author
  affiliations: one row per author, institution, year). Old table names win where they exist
  (`authors_counts_by_year`, `institutions_associated_institutions`, `works_referenced_works`, ...).
  Optimized tables have no list columns.
- 1:1 structs (e.g. `summary_stats`) become columns of the parent table, except where the old flattener
  had a separate table for them (`<x>_ids`, `institutions_geo`); keep those tables.
- Drop API URL columns (`works_api_url` and similar) and columns trivially derived from another column
  (`image_thumbnail_url` = `image_url` + `&width=300`): never stored.

- **Large tables keep `publication_year`** (every works-derived table), so the most common filter needs no
  join.
- **`works_referenced_works`** carries `work_publication_year` and `referenced_work_publication_year`.
- Standard count names everywhere: `num_works`, `num_citations`, `num_oa_works`.
- Dates are `date32`; raw timestamps carry an update-job time of day, which is dropped (and counted in the log).

## Works: user directives and old semantics (from `process_work_json_v2` and `src.utils.parse_authorships`)
- ID columns are named `<entity>_id` (`work_id`, `author_id`, `institution_id`, `source_id`, `topic_id`,
  `referenced_work_id`, ...), never bare `id`.
- Every `has_*` column is `bool` (`has_fulltext`, `has_pdf`, `has_grobid_xml`, `has_keywords`, ...).
- `has_complete_institution_info` (derived, kept for filtering): true if every authorship that has an author ID
  has at least one valid (non-deleted) institution ID; false when there are no authorships.
- `num_authors`: authorships with a non-deleted author ID. `num_locations`, `num_references`: list lengths.
- `has_keywords`, `has_grant_info`: whether the list is non-empty.
- `works_topics.is_primary_topic`: true for the first topic in the list.
- Old `works_authorships`: one row per author, institution, and institution lineage level
  (`institution_lineage_level`, `assigned_institution` true for the institution itself), with
  `raw_affiliation_string` = the raw strings joined by `;`.
- The old flattener skipped `is_xpac` works and rows referencing deleted or merged IDs.
- **Decided 2026-10-29:** keep skipping `is_xpac` works and all their child rows; log the count.
- **Decided 2026-10-29:** `works_authorships` has no lineage expansion: one row per work, author, and named
  institution. `institution_lineage_level` and `assigned_institution` are dropped (derivable via
  `institutions_lineage`; list order was not depth).

## Column names
- Match the old flattener's names as exactly as possible: table names and `columns` lists in `csv_files`
  (`flatten_openalex_files.py`, top of file), e.g. `work_id`, `institution_id`, `institution_name`.
- Fix typos in old names instead of copying them (`display_name_acroynyms` -> `display_name_acronyms`);
  log each fix in `notes.md`.
- Columns in the raw Parquet with no counterpart in the old schema: **list them for the user (name, type,
  example values) and get approval before adding them.** Don't add or rename on your own.
