# OpenAlex flattening

Parses OpenAlex snapshot JSONL (`.gz`) files into per-table Parquet (and CSV) files. The workhorse is
`preprocessing/flatten_openalex_files.py`: one `flatten_<entity>()` per entity, `flatten_works_v3()` for works
(~20 h, resumable via `temp/finished_works.txt`). Helpers live in `src/`. Ignore `_old/`.

## Non-negotiables
- **Nothing data-related runs on this Mac.** Snapshots, Parquet outputs and all databases live on Yoda
  (`ssh yoda`, repo at `yoda:/home/ssikdar/PycharmProjects/openalex`, data at `/data/shared/OpenAlex`).
  Edit locally, `make sync` (rsync), run there. Only `make check` (lint/types/tests, no data) runs locally.
- The Mac git checkout is the source of truth; Yoda's copy is an rsync target with no git history.
- `make check` (ruff, ty, pytest) must pass before every commit via the pre-commit hook. Never `--no-verify`.
- Every output write is crash-safe and re-runnable: build `<name>.partial-<pid>`, then atomic replace.
- Never commit data, logs or secrets. Never delete outputs on Yoda without asking.

## Before a task, read every guide whose trigger matches

| Trigger | Guide |
|---|---|
| Writing or changing code, tests, deps, tooling | `.claude/docs/dev.md` |
| Running anything on Yoda, touching outputs, long jobs, `flatten_openalex_files.py` | `.claude/docs/data.md` |
| Committing, branching, merging, pushing | `.claude/docs/git.md` |
| Known bugs, debt baseline, dropped approaches, benchmarks | `.claude/docs/notes.md` |
| Finishing a major task | add to `progress.md` (`## YYYY-MM-DD`, newest first, reuse today's heading) |

## Working style
- Requests are a TODO queue: acknowledge mid-task requests as "queued", do them in turn (interrupt only if
  told it's urgent). Don't explore queued items early. On finishing, say what's next and what's still open.
- Get an advisor/second opinion before consequential steps (approach, schema/data changes, shared resources,
  bulk or irreversible ops, review findings, declaring done, committing). Write results to disk first. If
  advice conflicts with evidence, say so.
- README changes only when entry points, architecture or key numbers change; no dated sections in it.
