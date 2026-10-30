---
name: entity-audit
description: Audit a raw OpenAlex Parquet entity on Yoda with capped DuckDB and turn the results into a proposed oct-2026 schema section with decisions. Use before building any new optimized entity.
---

# entity-audit

Read-only. Produces the evidence for a schema proposal in `.claude/docs/schema-oct-2026.md`; nothing is written
under `parquet-files/`. Read `.claude/docs/parquet.md` first (types, IDs, list splitting, naming).

## 1. Schema
On Yoda, print the schema of one file and check every file has the identical schema
(`pq.read_schema(f).to_string(show_schema_metadata=False)`). Compare with the old tables (`csv_files` in
`preprocessing/flatten_openalex_files.py`, and any `may-2025/` Parquet) and list renamed, new, and gone columns.

## 2. Capped DuckDB audit
Write the script to the scratchpad, `scp` it to `/home/ssikdar/scratch/`, run with `nohup`, log to a file:
```python
con = duckdb.connect(config={'memory_limit': '60GB', 'threads': 16, 'temp_directory': '/home/ssikdar/scratch/duckdb-tmp'})
con.execute("set TimeZone='UTC'")
con.execute(f"create temp view r as select * from read_parquet('{RAW}/<entity>/*/*.parquet', hive_partitioning=false)")
```
One labeled query per area, each wrapped so a failure prints and the script continues. Cover:
- row count vs distinct IDs; ID pattern; deleted placeholder IDs (`src.optimize.DELETED_IDS`)
- per column: nulls, min/max (types come from observed ranges), distinct counts for low-cardinality strings
- dates: values with a time of day (`epoch_ms(x) % 86400000 <> 0`)
- every list: element count, max length, duplicate elements, years outside 1500..snapshot year + 1
- nested copies of other entities: do they match the built `oct-2026` tables (drop) or not (keep)?
- redundancy: columns equal to other columns, or derivable (siblings, inverse hierarchy, `ids.*`)
- orphan IDs against built tables; overlap with any `deleted_ids` list

DuckDB 2.0 syntax: lambdas are `lambda x: expr` (not `x -> expr`); `year`/`value` aliases need `as "year"`;
`hash(*)` is invalid (list the columns); give subqueries an alias; `error('msg')` fails a query on purpose.

## 3. Proposal
Append a section to `schema-oct-2026.md` with status "proposed", in the same format as the existing ones:
parent table, then child tables with columns, types, and row counts, then a **Dropped, with proof** table and a
numbered **Decisions for the user** list with a recommendation each. Show the full proposal to the user; build
nothing until they approve. Then use the `schema-coverage-verifier` agent to check it against the audit log.
