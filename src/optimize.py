"""
Helpers for building optimized Parquet tables from the raw OpenAlex Parquet snapshot.

Rules live in `.claude/docs/parquet.md`; the table specs are in `.claude/docs/schema-oct-2026.md`.
Every normalizer validates its whole input and fails loudly (count and examples) on anything unexpected.
"""

import json
import logging
import os
import shutil
from collections.abc import Iterable
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.compute as _pc
import pyarrow.parquet as pq

log = logging.getLogger(__name__)
pc: Any = _pc  # pyarrow.compute builds its functions at runtime, so type checkers can't see them

DICT = pa.dictionary(pa.int32(), pa.string())

# OpenAlex placeholder IDs for deleted entities (also in flatten_openalex_files.get_skip_ids). Rows that carry one
# in an ID column of that entity are dropped from every table.
DELETED_IDS = {'work': 4285719527, 'author': 5317838346, 'institution': 4389424196, 'source': 4317411217}
ID_COLUMNS = {
    'work_id': 'work',
    'referenced_work_id': 'work',
    'author_id': 'author',
    'institution_id': 'institution',
    'associated_institution_id': 'institution',
    'ancestor_institution_id': 'institution',
    'source_id': 'source',
}
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


def dedupe(table: pa.Table, label: str) -> pa.Table:
    """Drop rows that repeat another row in every column (raw lists sometimes repeat an element; logged)."""
    unique = table.group_by(table.column_names, use_threads=False).aggregate([])
    if n := table.num_rows - unique.num_rows:
        log.info('%s: dropped %s exact duplicate rows', label, f'{n:,}')
        return unique.select(table.column_names)
    return table


def conform(columns: dict[str, Col], schema: pa.Schema, label: str, sort_keys: list[str]) -> pa.Table:
    """Cast `columns` to exactly `schema` (same names, same order), drop exact duplicates, sort by `sort_keys`."""
    missing, extra = set(schema.names) - set(columns), set(columns) - set(schema.names)
    if missing or extra:
        raise SystemExit(f'{label}: columns differ from schema; missing {sorted(missing)}, extra {sorted(extra)}')
    arrays = [cast(columns[f.name], f.type, f'{label}.{f.name}') for f in schema]
    table = dedupe(pa.Table.from_arrays(arrays, schema=schema), label)
    return table.sort_by([(k, 'ascending') for k in sort_keys])


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


def drop_deleted(table: pa.Table, label: str) -> tuple[pa.Table, int]:
    """Drop rows whose ID columns hold a deleted-entity placeholder (see DELETED_IDS). Returns (table, dropped)."""
    mask = None
    for col in table.column_names:
        if (entity := ID_COLUMNS.get(col)) is not None:
            hit = pc.fill_null(pc.equal(table[col], DELETED_IDS[entity]), False)
            mask = hit if mask is None else pc.or_(mask, hit)
    if mask is None or not (n := _count(mask)):
        return table, 0
    return table.filter(pc.invert(mask)), n


def _log_deleted(label: str, n: int) -> None:
    if n:
        log.info('%s: dropped %s rows referencing deleted-entity placeholder IDs', label, f'{n:,}')


def _start_partial(target: Path) -> Path:
    parent = target.parent
    parent.mkdir(parents=True, exist_ok=True)
    # ponytail: one writer per table; leftovers from killed runs are removed. A kill between the two renames
    # in _finish below leaves only `<target>.old-<pid>`; re-running rebuilds the table.
    for stale in [*parent.glob(f'{target.name}.partial-*'), *parent.glob(f'{target.name}.old-*')]:
        shutil.rmtree(stale)
    partial = parent / f'{target.name}.partial-{os.getpid()}'
    partial.mkdir()
    return partial


def _finish(partial: Path, target: Path, schema: pa.Schema, names: list[str], source: dict[str, Any]) -> dict[str, Any]:
    """Write `_metadata`, `_common_metadata`, `_manifest.json` for the part files, then swap into `target`."""
    collector, files = [], []
    for name in names:
        md = pq.read_metadata(partial / name)
        md.set_file_path(name)
        collector.append(md)
        files.append(
            {
                'name': name,
                'rows': md.num_rows,
                'bytes': (partial / name).stat().st_size,
                'row_groups': md.num_row_groups,
            }
        )
    pq.write_metadata(schema, partial / '_common_metadata')
    pq.write_metadata(schema, partial / '_metadata', metadata_collector=collector)
    manifest = {
        'table': target.name,
        'rows': sum(f['rows'] for f in files),
        'bytes': sum(f['bytes'] for f in files),
        'files': files,
        'schema': [{'name': f.name, 'type': str(f.type)} for f in schema],
        'source': source,
        'built_at': datetime.now(UTC).isoformat(timespec='seconds'),
    }
    (partial / '_manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
    if target.exists():
        old = target.parent / f'{target.name}.old-{os.getpid()}'
        target.rename(old)
        partial.rename(target)
        shutil.rmtree(old)
    else:
        partial.rename(target)
    log.info(
        'wrote %s: %s rows, %s files, %.2f MB',
        target.name,
        f'{manifest["rows"]:,}',
        len(files),
        manifest['bytes'] / 1e6,
    )
    return manifest


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
    table, n_deleted = drop_deleted(table, target.name)
    _log_deleted(target.name, n_deleted)
    partial = _start_partial(target)
    try:
        names = []
        for i, start in enumerate(range(0, max(table.num_rows, 1), rows_per_file)):
            names.append(f'part-{i:05d}.parquet')
            pq.write_table(
                table.slice(start, rows_per_file),
                partial / names[-1],
                compression='brotli',
                row_group_size=row_group_size,
            )
        return _finish(partial, target, table.schema, names, source)
    except BaseException:
        shutil.rmtree(partial, ignore_errors=True)
        raise


def write_batches_dir(
    batches: Iterable[pa.RecordBatch],
    schema: pa.Schema,
    target: Path,
    *,
    source: dict[str, Any],
    rows_per_file: int = 5_000_000,
    row_group_size: int = 1_000_000,
    writers: int = 8,
) -> dict[str, Any]:
    """
    Same layout and atomic swap as `write_table_dir`, for tables too big for memory.

    Record batches are cast to `schema` and re-cut so part file i always holds exactly rows
    [i * rows_per_file, (i + 1) * rows_per_file), whatever batch sizes the source yields. Up to `writers` files
    are compressed at once on threads (pyarrow releases the GIL while writing); at most `writers` finished
    chunks wait in memory, so memory stays bounded.
    """
    partial = _start_partial(target)
    pool = ThreadPoolExecutor(max_workers=writers, thread_name_prefix='parquet-writer')
    pending: set[Future[None]] = set()
    names: list[str] = []

    def submit(chunk: pa.Table) -> None:
        while len(pending) >= writers:  # bound memory: wait for a writer to free up
            done, _ = wait(pending, return_when=FIRST_COMPLETED)
            for f in done:
                pending.discard(f)
                f.result()  # re-raise a writer's error here
        names.append(f'part-{len(names):05d}.parquet')
        path = partial / names[-1]
        pending.add(pool.submit(pq.write_table, chunk, path, compression='brotli', row_group_size=row_group_size))

    try:
        buf: list[pa.RecordBatch] = []
        buf_rows = n_deleted = 0
        for batch in batches:
            arrays = [cast(batch.column(f.name), f.type, f'{target.name}.{f.name}') for f in schema]
            kept, n = drop_deleted(pa.Table.from_arrays(arrays, schema=schema), target.name)
            n_deleted += n
            if not kept.num_rows:
                continue
            buf.extend(kept.to_batches())
            buf_rows += kept.num_rows
            while buf_rows >= rows_per_file:
                table = pa.Table.from_batches(buf, schema=schema)
                submit(table.slice(0, rows_per_file))
                rest = table.slice(rows_per_file)
                buf, buf_rows = rest.to_batches(), rest.num_rows
        if buf_rows or not names:  # the remainder, or one empty file for an empty table
            submit(pa.Table.from_batches(buf, schema=schema))
        for f in pending:
            f.result()
        pool.shutdown()
        _log_deleted(target.name, n_deleted)
        return _finish(partial, target, schema, names, source)
    except BaseException:
        pool.shutdown(wait=True, cancel_futures=True)  # no writer may still be writing into partial
        shutil.rmtree(partial, ignore_errors=True)
        raise


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
    """
    Read the table with pyarrow, pandas, and DuckDB without materializing it: pyarrow schema and row counts
    from every file's metadata, pandas on the first file, DuckDB count and columns over the whole table.
    All must agree with each other and with `schema`. Returns rows.
    """
    import duckdb
    import pandas as pd

    files = sorted(table_dir.glob('part-*.parquet'))
    for f in files:
        if not pq.read_schema(f).equals(schema, check_metadata=False):
            raise SystemExit(f'{table_dir.name}/{f.name}: schema differs from spec:\n{pq.read_schema(f)}\nvs\n{schema}')
    if not pq.ParquetDataset(table_dir).schema.equals(schema, check_metadata=False):
        raise SystemExit(f'{table_dir.name}: dataset schema differs from spec')
    arrow_rows = sum(pq.read_metadata(f).num_rows for f in files)
    first = pd.read_parquet(files[0])
    glob = f"read_parquet('{table_dir}/*.parquet')"
    con = duckdb.connect()
    duck_rows = con.execute(f'select count(*) from {glob}').fetchone()
    duck_cols = [row[0] for row in con.execute(f'describe select * from {glob}').fetchall()]
    if duck_rows is None or duck_rows[0] != arrow_rows or len(first) != pq.read_metadata(files[0]).num_rows:
        raise SystemExit(
            f'{table_dir.name}: row counts disagree: pyarrow {arrow_rows}, duckdb {duck_rows}, pandas {len(first)}'
        )
    if list(first.columns) != schema.names or duck_cols != schema.names:
        raise SystemExit(f'{table_dir.name}: columns differ: pandas {list(first.columns)}, duckdb {duck_cols}')
    return arrow_rows
