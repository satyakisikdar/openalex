# Optimized Parquet guide (OpenAlex Parquet snapshot -> optimized tables)

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
- Location: `/data/shared/OpenAlex/processed-snapshots/parquet-files/<mon>-<yyyy>/<table>/`
  (lowercase 3-letter month, like existing `feb-2026`, `mar-2026`; this snapshot is `oct-2026`).
- Must read cleanly in **pandas, pyarrow and DuckDB**. Every new table gets a check that reads it with all
  three and compares schema and row count against the raw table. A table isn't done until that passes.
- Compression: brotli.
- Split each table into multiple files with sensible row groups so readers can parallelize. Starting point,
  to be benchmarked and recorded in `notes.md`: row groups of ~1M rows, files of a few hundred MB.
- Writes follow `data.md`: crash-safe (`src.parquet_io.atomic_write_parquet` or the same partial + replace
  pattern), re-runnable, deterministic (sort by ID before writing).

## Types
- Start from the old flattener's schema: `DTYPES` (`flatten_openalex_files.py:679`) and the per-table
  `pa.schema(...)` blocks in `write_to_csv_and_parquet` (~line 2227 onward).
- Use the smallest type that fits: dates `date32`, timestamps `timestamp('ms')`, counts `uint32`, flags
  `bool`, low-cardinality strings (type, country_code, ...) dictionary-encoded.
- **OpenAlex IDs: keep only the numeric part** (`https://openalex.org/W2741809807` -> `2741809807`), stored
  as `int64` (author IDs such as `5317838346` already exceed `uint32`). Applies to every ID column and to
  ID lists (e.g. `referenced_works` -> `list<int64>`). Reuse the `convert_openalex_id_to_int` /
  `convert_topic_id_to_int` logic from `src/utils.py`, but vectorized (pyarrow/DuckDB string ops), not
  per-row Python.
- External IDs (DOI, ORCID, ROR, ...) are undecided; keep as-is until the user decides.
