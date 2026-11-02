# OpenAlex

Turns OpenAlex snapshots into analysis-ready tables. Two paths:
- **Optimized Parquet** (current): converts the OpenAlex Parquet snapshot into compact per-table Parquet.
- **Flattening** (legacy): parses the JSONL (`.gz`) snapshot into per-table CSV and Parquet.

## Optimized Parquet
`preprocessing/optimize_parquet.py` (helpers in `src/optimize.py`) writes `parquet-files/<month>/`: one folder per
table with brotli `part-NNNNN.parquet` files, `_metadata`, and `_manifest.json`, plus a generated `README.md` that
documents every table and column. OpenAlex IDs become integers, dates are dates, and nulls stay nulls.
- `oct-2026` (snapshot 2026-10-14): 71 tables, 131 GB, from 9 entities (institutions, sources, topics with their
  subfields, fields, and domains, concepts, keywords, funders, authors, awards, works). Works alone is 18 tables
  over 326,989,410 works (xpac works skipped), e.g. 945M authorships and 2.9B references.
- Run: `uv run python -m preprocessing.optimize_parquet --dry-run <scratch>`, then without `--dry-run` for the real
  output. `--entities` picks entities, `--month` the output folder, `--readme-only` regenerates the README.
- Large entities (authors, awards, works) go through memory-capped DuckDB (`DUCKDB_CONFIG`). The full works write
  takes about 2 hours and is resumable: a failed run keeps its helper DuckDB file, and the next run skips the
  tables already written.

## Flattening (legacy JSONL)
0. Install `uv` if needed and create the `openalex` uv environment by running `uv init` inside the `openalex` directory.
1. Download the OpenAlex snapshots from [this](https://docs.openalex.org/download-all-data/download-to-your-machine)
   link to a directory of your choosing (say, `basedir`).
2. To run the flattening script, first activate the uv `openalex` environment (if needed) by running `source .venv/bin/activate` inside the directory, then execute `uv run preprocessing/flatten_openalex_files.py`. 
3. Open `preprocessing/flatten_openalex_files.py` and update the following variables:
   a. `BASEDIR` to the directory in Step 1.
   b. `MONTH` to the month of the snapshot, eg: `may-2025`
4. Scroll down to the `if __name__ == '__main__':` block near the end of the file.
5. Uncomment the lines one at a time and run the script `flatten_<entity>` functions to generate the flattened compressed CSV files.
   a. Start with `flatten_merged_entries`, then
   b. Then `flatten_funders`, `flatten_concepts`, ...., `flatten_topics`. 
6. To flatten `works`, uncomment the block of code for `flatten_works_v3`. You flatten all JSONs at once, or do it `N` files at a time by changing the `files_to_process` variable to either `all` or an integer `N`.

**Warnings**:

* flattening _authors_ and _works_ take anywhere between 15 and 30 hours. The code will cache the files, so you
  should consider running it in batches by setting the `files_to_process` variable.

## Development
- `make setup` once (installs deps, enables the pre-commit hook), `make check` for lint/types/tests.
- Code runs on the processing server, not locally: `make sync` pushes committed `main` to its git checkout. The
  push is refused if the server copy has uncommitted edits; `make yoda-diff` shows them, and they are committed
  there on a branch and merged locally.
