# Data and jobs guide (Yoda)

## Where things live
- Run host: Yoda only (`ssh yoda`). Nothing data-related runs on the Mac.
- Code on Yoda: `/home/ssikdar/PycharmProjects/openalex` (rsync target, no git). Push with `make sync`
  (excludes in `.rsync-exclude`; no `--delete`, so Yoda-only files like `data/`, `test.pq` survive).
  Sync before every run.
- `BASEDIR` is picked by hostname at the top of `preprocessing/flatten_openalex_files.py`
  (`yoda` -> `/data/shared/OpenAlex`, `quartz*` -> IU HPC path). `MONTH` (e.g. `mar-2026`) is hard-coded there.
  - Snapshot: `$BASEDIR/openalex-snapshot/data/<entity>/updated_date=*/part_*.gz` + `manifest`.
  - Parquet snapshot (raw, from OpenAlex): `$BASEDIR/openalex-snapshot/data/parquet/<table>/`; see `parquet.md`.
  - Outputs: `$BASEDIR/processed-snapshots/{csv-files,parquet-files}/<MONTH>/`.
  - Works Parquet: one file per input part per table, `works_<table>/<date>_part_NNN.parquet`.
- Long jobs: run under `tmux`/`nohup` on Yoda with a log file, never in a foreground ssh session.

## Pipeline order (`__main__` in `flatten_openalex_files.py`)
1. `flatten_merged_entries()` -> `<kind>_combined_merged_ids.parquet` (skip-ids). Must run before works.
2. Small entities: funders, concepts, institutions, publishers, sources, topics.
3. Authors (6-7 h), then `flatten_works_v3(files_to_process=N|'all')` (~20 h, ~6 min/file).
   Resume state: `PARQ_DIR/temp/finished_works.txt` (a file is done when all 15 tables are recorded).
   `recompute_tables=[...]` forces those tables to be rebuilt.

## Safety and idempotency (new and touched code)
- Every script logs START and END markers (with timestamps), progress per stage/loop, and handles SIGTERM
  (log, remove its own partials, exit non-zero). A log with no END marker = killed run; flag it next run.
- Every operation is safe to re-run, including after a crash mid-way. Write to `<name>.partial-<pid>`, set
  permissions, then `Path.replace()` onto the target. Never delete the target first. Never treat
  "target exists" as "target complete" unless the write was atomic.
- No blind appends to shared outputs; dedupe on key (`drop_duplicates(keep='last')`) when reading logs.
- Deterministic output: tie-broken sorts, seeded RNGs.
- Cap resources on Yoda (`ulimit -v`, or `systemd-run --user -p MemoryMax=...`); it is a shared box.
- No DB today. If one is added (e.g. DuckDB), all access goes through one helper module with memory/thread
  caps and a per-query deadline; no raw connects; `CREATE OR REPLACE` / `IF NOT EXISTS` / upserts only.
  A new DB operation is done only after two consecutive runs give matching results.
