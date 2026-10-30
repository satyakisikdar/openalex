"""
Helpers for building optimized Parquet tables from the raw OpenAlex Parquet snapshot.

Rules live in `.claude/docs/parquet.md`; the table specs are in `.claude/docs/schema-oct-2026.md`.
Every normalizer validates its whole input and fails loudly (count and examples) on anything unexpected.
"""

import json
import logging
import os
import shutil
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.compute as _pc
import pyarrow.parquet as pq

log = logging.getLogger(__name__)
pc: Any = _pc  # pyarrow.compute builds its functions at runtime, so type checkers can't see them

DICT = pa.dictionary(pa.int32(), pa.string())
DATE = pa.date32()
NULL_LIKE = pa.array(['', 'NaN'])

type Col = pa.Array | pa.ChunkedArray


def _array(arr: Col) -> pa.Array:
    return arr.combine_chunks() if isinstance(arr, pa.ChunkedArray) else arr


def _count(mask: Col) -> int:
    return pc.sum(pc.fill_null(mask, False)).as_py() or 0


def _fail_unmatched(arr: pa.Array, ok: pa.Array, label: str, pattern: str) -> None:
    bad = arr.filter(pc.and_(pc.is_valid(arr), pc.invert(ok)))
    if len(bad):
        raise SystemExit(
            f'{label}: {len(bad):,} values do not match {pattern!r}, e.g. {bad.slice(0, 3).to_pylist()}; '
            'extend the rule in src/optimize.py before converting.'
        )


def null_like_to_null(arr: Col, label: str) -> pa.Array:
    """Turn '' and 'NaN' strings into nulls (logged)."""
    arr = _array(arr)
    mask = pc.fill_null(pc.is_in(arr, value_set=NULL_LIKE), False)
    if n := _count(mask):
        log.info('%s: %s empty/NaN strings stored as null', label, f'{n:,}')
    return pc.if_else(mask, pa.scalar(None, arr.type), arr)


def extract(arr: Col, pattern: str, label: str) -> pa.StructArray:
    """Full-match `pattern` (named groups) on every non-null value; fail loudly on any mismatch."""
    arr = _array(arr)
    ok = pc.match_substring_regex(arr, pattern)
    _fail_unmatched(arr, ok, label, pattern)
    return pc.extract_regex(arr, pattern)


def openalex_id(arr: Col, prefix: str, label: str, type_: pa.DataType = pa.int64()) -> pa.Array:
    """`https://openalex.org/<prefix><digits>` -> number. `prefix` is a regex fragment, e.g. 'I' or 'subfields/'."""
    n = pc.struct_field(extract(arr, rf'^https://openalex\.org/{prefix}(?P<n>\d+)$', label), 'n')
    return cast(n, type_, label)


def keyword_slug(arr: Col, label: str) -> pa.Array:
    return pc.struct_field(extract(arr, r'^https://openalex\.org/keywords/(?P<s>.+)$', label), 's')


def host_org(arr: Col, label: str) -> tuple[pa.Array, pa.Array]:
    """Host organization URL -> (numeric ID, 'publisher' | 'institution')."""
    parts = extract(arr, r'^https://openalex\.org/(?P<t>[PI])(?P<n>\d+)$', label)
    kind = pc.if_else(pc.equal(pc.struct_field(parts, 't'), 'P'), 'publisher', 'institution')
    return cast(pc.struct_field(parts, 'n'), pa.int64(), label), cast(kind, DICT, label)


def ror(arr: Col, label: str) -> pa.Array:
    return pc.struct_field(extract(arr, r'^https://ror\.org/(?P<r>[0-9a-z]{9})$', label), 'r')


def grid(arr: Col, label: str) -> pa.Array:
    arr = null_like_to_null(arr, label)
    # All-digit values are not GRID IDs (raw has one Crossref Funder ID, 100011307, in this field): store null.
    digits = pc.fill_null(pc.match_substring_regex(arr, r'^\d+$'), False)
    if n := _count(digits):
        log.info('%s: %s all-digit values are not GRID IDs, stored as null', label, f'{n:,}')
    arr = pc.if_else(digits, pa.scalar(None, arr.type), arr)
    parts = extract(arr, r'^(?:https://www\.grid\.ac/institutes/)?(?P<g>grid\.\d+\.[0-9a-z]+)$', label)
    _log_normalized(arr, r'^grid\.', label)
    return pc.struct_field(parts, 'g')


def wikidata(arr: Col, label: str) -> pa.Array:
    """Any wikidata form (http/https, /wiki/ or /entity/, bare Q123) -> Q-number as uint32."""
    arr = null_like_to_null(arr, label)
    parts = extract(arr, r'^(?:https?://www\.wikidata\.org/(?:wiki|entity)/)?Q(?P<q>\d+)$', label)
    _log_normalized(arr, r'^https://www\.wikidata\.org/wiki/Q', label)
    return cast(pc.struct_field(parts, 'q'), pa.uint32(), label)


WIKIPEDIA_HOST = r'^https?://[a-z][a-z0-9-]*(?:\.m)?\.wikipedia\.org/'
WIKIPEDIA = (
    r'^https?://(?P<lang>[a-z][a-z0-9-]*)(?:\.m)?\.wikipedia\.org/'
    r'(?:(?:\?title=|w/index\.php\?title=)(?P<q>[^&#]+)(?:&.*)?'  # ?title= forms: drop extra query params
    r'|(?:wiki/|[a-z]{2,3}(?:-[a-z]+)?/)?(?P<t>[^?].*))$'  # /wiki/, language variants (/zh-hans/), or no prefix
)


def wikipedia(arr: Col, label: str) -> pa.Array:
    """
    Any Wikipedia article URL -> `<lang>:<Title>` (rebuild: `https://<lang>.wikipedia.org/wiki/<Title>`).

    Mobile hosts, language-variant paths, and `?title=` forms are normalized. Non-Wikipedia URLs (company
    sites, Commons, Wikivoyage, ...) are stored as null; both counts are logged.
    """
    arr = null_like_to_null(arr, label)
    other = pc.invert(pc.fill_null(pc.match_substring_regex(arr, WIKIPEDIA_HOST), True))
    if n := _count(other):
        log.info('%s: %s non-Wikipedia URLs stored as null', label, f'{n:,}')
    arr = pc.if_else(other, pa.scalar(None, arr.type), arr)
    parts = extract(arr, WIKIPEDIA, label)
    _log_normalized(arr, r'^https://[a-z][a-z0-9-]*\.wikipedia\.org/wiki/', label)
    q, t = pc.struct_field(parts, 'q'), pc.struct_field(parts, 't')
    title = pc.if_else(pc.equal(q, ''), t, q)
    return pc.binary_join_element_wise(pc.struct_field(parts, 'lang'), title, ':')


def _log_normalized(arr: pa.Array, canonical: str, label: str) -> None:
    """Log how many values differed from the canonical form, so rebuilt URLs aren't byte-identical."""
    if n := _count(pc.invert(pc.fill_null(pc.match_substring_regex(arr, canonical), True))):
        log.info('%s: %s values normalized to the canonical form', label, f'{n:,}')


def cast(arr: Col, type_: pa.DataType, label: str) -> pa.Array:
    """Checked cast: overflow, bad parses, and float overflow to inf all fail loudly."""
    arr = _array(arr)
    if pa.types.is_dictionary(type_):
        return pc.dictionary_encode(arr.cast(pa.string()))
    if pa.types.is_date(type_) and pa.types.is_timestamp(arr.type):
        day = pc.floor_temporal(arr, unit='day')  # spec stores dates; the time of day is dropped on purpose
        if n := _count(pc.not_equal(day, arr)):
            log.info('%s: %s values had a time of day, dropped (date only)', label, f'{n:,}')
        arr = day
    try:
        out = arr.cast(type_, safe=True)
    except (pa.ArrowInvalid, pa.ArrowNotImplementedError) as e:
        raise SystemExit(f'{label}: cannot cast {arr.type} to {type_}: {e}') from e
    if pa.types.is_floating(type_) and pa.types.is_floating(arr.type):
        finite_in, finite_out = _count(pc.is_finite(arr)), _count(pc.is_finite(out))
        if finite_in != finite_out:
            raise SystemExit(f'{label}: {finite_in - finite_out:,} values overflow {type_}')
    return out


def explode(table: pa.Table, keys: list[str], list_col: str) -> pa.Table:
    """One row per list element: parent `keys` repeated, struct fields (or the scalar element) as columns."""
    lists = _array(table.column(list_col))
    parents = pc.list_parent_indices(lists)
    flat = pc.list_flatten(lists)
    cols: dict[str, Any] = {k: pc.take(table.column(k), parents) for k in keys}
    if pa.types.is_struct(flat.type):
        cols |= {f.name: pc.struct_field(flat, f.name) for f in flat.type}
    else:
        cols[list_col] = flat
    return pa.table(cols)


def drop_rows(table: pa.Table, drop: Col, label: str, why: str) -> pa.Table:
    drop = pc.fill_null(drop, False)
    if n := _count(drop):
        log.info('%s: dropped %s rows (%s)', label, f'{n:,}', why)
    return table.filter(pc.invert(drop))


def conform(columns: dict[str, Col], schema: pa.Schema, label: str, sort_keys: list[str]) -> pa.Table:
    """Cast `columns` to exactly `schema` (same names, same order), then stable-sort by `sort_keys`."""
    missing, extra = set(schema.names) - set(columns), set(columns) - set(schema.names)
    if missing or extra:
        raise SystemExit(f'{label}: columns differ from schema; missing {sorted(missing)}, extra {sorted(extra)}')
    arrays = [cast(columns[f.name], f.type, f'{label}.{f.name}') for f in schema]
    return pa.Table.from_arrays(arrays, schema=schema).sort_by([(k, 'ascending') for k in sort_keys])


def assert_unique(arr: Col, label: str) -> None:
    n, distinct = len(arr), pc.count_distinct(arr).as_py()
    if n != distinct:
        raise SystemExit(f'{label}: {n - distinct:,} duplicate IDs; dedupe on updated_date before converting.')


def load_raw(root: Path, entity: str) -> pa.Table:
    """Read every raw file of an entity in sorted order (never directory discovery: see parquet.md)."""
    files = sorted((root / entity).rglob('*.parquet'))
    if not files:
        raise SystemExit(f'No raw Parquet files for {entity!r} under {root}; check the download.')
    table = pa.concat_tables(pq.ParquetFile(f).read() for f in files).combine_chunks()
    log.info('read %s: %s rows from %s files', entity, f'{table.num_rows:,}', len(files))
    return table


def write_table_dir(
    table: pa.Table,
    target: Path,
    *,
    source: dict[str, Any],
    rows_per_file: int = 5_000_000,
    row_group_size: int = 1_000_000,
) -> dict[str, Any]:
    """
    Write `table` as `target/part-NNNNN.parquet` + `_metadata`, `_common_metadata`, `_manifest.json`.

    Built in `<target>.partial-<pid>/`, then swapped in. The old target is renamed aside before the swap and
    removed after, so it is never deleted first.
    """
    parent = target.parent
    parent.mkdir(parents=True, exist_ok=True)
    # ponytail: one writer per table; leftovers from killed runs are removed. A kill between the two renames
    # below leaves only `<target>.old-<pid>`; re-running rebuilds the table.
    for stale in [*parent.glob(f'{target.name}.partial-*'), *parent.glob(f'{target.name}.old-*')]:
        shutil.rmtree(stale)

    partial = parent / f'{target.name}.partial-{os.getpid()}'
    try:
        partial.mkdir()
        collector: list[pq.FileMetaData] = []
        files: list[dict[str, Any]] = []
        for i, start in enumerate(range(0, max(table.num_rows, 1), rows_per_file)):
            name = f'part-{i:05d}.parquet'
            part = table.slice(start, rows_per_file)
            pq.write_table(
                part, partial / name, compression='brotli', row_group_size=row_group_size, metadata_collector=collector
            )
            collector[-1].set_file_path(name)
            files.append(
                {
                    'name': name,
                    'rows': part.num_rows,
                    'bytes': (partial / name).stat().st_size,
                    'row_groups': collector[-1].num_row_groups,
                }
            )
        pq.write_metadata(table.schema, partial / '_common_metadata')
        pq.write_metadata(table.schema, partial / '_metadata', metadata_collector=collector)
        manifest = {
            'table': target.name,
            'rows': table.num_rows,
            'bytes': sum(f['bytes'] for f in files),
            'files': files,
            'schema': [{'name': f.name, 'type': str(f.type)} for f in table.schema],
            'source': source,
            'built_at': datetime.now(UTC).isoformat(timespec='seconds'),
        }
        (partial / '_manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')

        if target.exists():
            old = parent / f'{target.name}.old-{os.getpid()}'
            target.rename(old)
            partial.rename(target)
            shutil.rmtree(old)
        else:
            partial.rename(target)
    except BaseException:
        shutil.rmtree(partial, ignore_errors=True)
        raise
    log.info(
        'wrote %s: %s rows, %s files, %.2f MB', target.name, f'{table.num_rows:,}', len(files), manifest['bytes'] / 1e6
    )
    return manifest


def update_top_manifest(root: Path, manifests: dict[str, dict[str, Any]]) -> None:
    """Merge per-table summaries into `<root>/_manifest.json` (atomic replace)."""
    path = root / '_manifest.json'
    top = json.loads(path.read_text()) if path.exists() else {'tables': {}}
    for name, m in manifests.items():
        top['tables'][name] = {k: m[k] for k in ('rows', 'bytes', 'source', 'built_at')} | {'files': len(m['files'])}
    partial = root / f'_manifest.json.partial-{os.getpid()}'
    partial.write_text(json.dumps(top, indent=2, sort_keys=True) + '\n')
    partial.replace(path)


def read_check(table_dir: Path, schema: pa.Schema) -> int:
    """Read the table with pyarrow, pandas, and DuckDB; all must agree on rows and columns. Returns rows."""
    import duckdb  # dev dependency; only needed for the check
    import pandas as pd

    arrow = pq.read_table(table_dir)
    if not arrow.schema.equals(schema, check_metadata=False):
        raise SystemExit(f'{table_dir.name}: pyarrow schema differs from spec:\n{arrow.schema}\nvs\n{schema}')
    frame = pd.read_parquet(table_dir)
    glob = f"read_parquet('{table_dir}/*.parquet')"
    con = duckdb.connect()
    duck_rows = con.execute(f'select count(*) from {glob}').fetchone()
    duck_cols = [row[0] for row in con.execute(f'describe select * from {glob}').fetchall()]
    counts = (arrow.num_rows, len(frame), duck_rows[0] if duck_rows else None)
    if len(set(counts)) != 1:
        raise SystemExit(f'{table_dir.name}: row counts disagree (pyarrow, pandas, duckdb) = {counts}')
    if list(frame.columns) != schema.names or duck_cols != schema.names:
        raise SystemExit(f'{table_dir.name}: columns differ: pandas {list(frame.columns)}, duckdb {duck_cols}')
    return arrow.num_rows
