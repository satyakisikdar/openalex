---
name: yoda-run
description: Run a converter or flattener job on Yoda safely - sync, resource checks, dry run, log with START/END, run-twice comparison, README regen. Usage /yoda-run <module> [args], e.g. /yoda-run preprocessing.optimize_parquet --entities authors
disable-model-invocation: true
---

# yoda-run

Runs one data job on Yoda following `.claude/docs/data.md`. Never run data jobs on the Mac.
Arguments: the Python module and its arguments (`$ARGUMENTS`).

## Steps

1. **Code is committed and synced.** `git status --short` must show no changes to the code the job runs. Then
   `set -o pipefail; make check 2>&1 | tail -3` and `make sync`.
2. **Resources.** `ssh yoda 'free -g | sed -n 2p; df -h /data / | tail -2'`. Stop and ask the user if the job's
   output would leave under ~200 GB free on `/data`, or if available memory is below the job's cap plus ~20 GB
   (DuckDB jobs cap at 60 GB; see `DUCKDB_CONFIG`).
3. **Dry run first** (writes only to scratch):
   ```
   ssh yoda 'cd /home/ssikdar/PycharmProjects/openalex && nohup uv run --no-sync python -m $ARGUMENTS \
     --dry-run /home/ssikdar/scratch/optimize-dryrun > /home/ssikdar/scratch/<job>-dry.log 2>&1 & echo $!'
   ```
   Watch it with the Monitor tool, filtering for `wrote|removed|deleted|Error|Traceback|SystemExit|SIGTERM|END`.
   Done means the process exited AND the log has an `END ... ok` line.
4. **Review the dry run.** Every logged drop or normalization count must match the audit/spec numbers; spot-check a
   known row against the raw data and that join keys resolve. Anything unexpected: stop, report, fix, rerun.
5. **Ask the advisor** with the dry-run output before the real write (CLAUDE.md working style).
6. **Real write:** the same command without `--dry-run`, log to `/home/ssikdar/scratch/<job>.log`, same monitoring.
7. **Run-twice proof.** Same writer code: file checksums excluding `_manifest.json` and `README.md`:
   `cd <root>/oct-2026 && find . -type f ! -name _manifest.json ! -name README.md -exec md5sum {} + | sort -k2`
   on both trees, then `diff`. Different writer or layout: per table `count(*)` plus
   `sum(hash(<every column>)::hugeint)` in DuckDB (list columns explicitly; `hash(*)` is invalid).
8. **README:** the real run regenerates `oct-2026/README.md`; otherwise `--readme-only`.
9. **Record:** `progress.md` (today's heading), sizes and timings in `notes.md`, commit, `make sync`.

## Pitfalls (all hit before)
- A waiter loop on the Mac dies when the laptop closes: put "run B after A" waiters on Yoda with `nohup sh -c`.
- `pgrep -f` matches its own command line; use `ps -eo pid,cmd | grep '[p]ython...'`.
- Remote commands run in zsh: unquoted globs abort, so use `find`; pass Python via `python - < script.py`.
- Never pipe a check without `set -o pipefail`.
