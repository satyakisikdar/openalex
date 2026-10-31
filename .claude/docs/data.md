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
  - `nohup` from a non-interactive ssh shell has a soft open-file limit of 1,024; reading all 1,898 works
    files needs `ulimit -n "$(ulimit -Hn)"` first ("Too many open files" otherwise).
  - Chain follow-up steps in a script on Yoda that waits on a PID (`while kill -0 <pid>`), not on
    `pgrep -f <name>`: the waiter's own command line matches the pattern. Never `pkill -f` over ssh either (it
    matched and killed the ssh session itself); stop processes by PID.
  - SIGTERM does not interrupt a DuckDB query in progress (the Python handler runs only when the query
    returns); use SIGKILL on that PID if it must stop now, then check the spill dir.
  - pyarrow: forking a pool after pyarrow has started its thread pools deadlocks the workers; use a `spawn`
    pool and share big arrays as memory-mapped `.npy`. `pq.read_table` fails on the raw `updated_date=*/`
    dirs (use `pq.ParquetFile(f).read()`); `pc.list_parent_indices` aborts the process on a `MapArray`.
  - 32-bit offsets cap a `string` or list array at 2 GB. `combine_chunks()` and `take()` (which concatenates
    chunks, so `sort_by` too) overflow on bigger columns, and a small slice of a big `large_string` array still
    can't be cast to `string` until copied (`pa.concat_arrays([s])`). `src/optimize.py` handles all three
    (`_array`, `take`, `_sliced`); proved on Yoda with a 2.4 GB column (`~/scratch/big_offsets.py`).
  - DuckDB: `struct = struct` is NULL when a nested field is NULL (and `not (x or NULL)` drops the row); compare
    structs with `is not distinct from`. A first awards count lost 270k investigator rows this way.

  - Memory: `free` "used" includes the ZFS cache (ARC). Uncapped, it grew to 201 GB after scans of the raw works
    and pushed swap to 30 of 31 GB. Since 2026-10-30 it is capped at 64 GiB (min 16 GiB; `/etc/modprobe.d/zfs.conf`,
    persists across reboots), which left 159 GB available. Check `MemAvailable`, not "used"; run DuckDB at a lower
    cap when it is under cap + 20 GB.

## Pipeline order (`__main__` in `flatten_openalex_files.py`)
1. `flatten_merged_entries()` -> `<kind>_combined_merged_ids.parquet` (skip-ids). Must run before works.
2. Small entities: funders, concepts, institutions, publishers, sources, topics.
3. Authors (6-7 h), then `flatten_works_v3(files_to_process=N|'all')` (~20 h, ~6 min/file).
   Resume state: `PARQ_DIR/temp/finished_works.txt` (a file is done when all 15 tables are recorded).
   `recompute_tables=[...]` forces those tables to be rebuilt.

## Optimized Parquet converter
- `uv run python -m preprocessing.optimize_parquet --dry-run <scratch>` writes under `<scratch>/oct-2026/`;
  without `--dry-run` it writes to `parquet-files/oct-2026/`. `--entities institutions sources topics`.
- Each table is checked with pyarrow, pandas, and DuckDB after writing, from file metadata plus the first file
  (no full load). `--readme-only` regenerates `README.md` from the manifests.
- Authors (and later works) run through DuckDB, a runtime dependency: `DUCKDB_CONFIG` in the converter caps it at
  60 GB and 16 threads and spills to `/home/ssikdar/scratch/duckdb-tmp`. Check `free -g` first; Yoda is shared.

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
