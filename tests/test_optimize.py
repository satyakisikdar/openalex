"""Unit tests for src/optimize.py: normalizers fail loudly, casts are checked, writes are atomic."""

import json
from datetime import UTC, date, datetime
from pathlib import Path

import pyarrow as pa
import pytest

from src import optimize as o


def test_openalex_id_strips_prefix_and_rejects_other_entities() -> None:
    arr = pa.array(['https://openalex.org/I4210086374', None])
    assert o.openalex_id(arr, 'I', 'x').to_pylist() == [4210086374, None]
    with pytest.raises(SystemExit, match='1 values do not match'):
        o.openalex_id(pa.array(['https://openalex.org/S1']), 'I', 'x')


def test_topic_hierarchy_ids_use_small_types() -> None:
    sub = o.openalex_id(pa.array(['https://openalex.org/subfields/3312']), 'subfields/', 'x', pa.uint16())
    assert sub.type == pa.uint16()
    with pytest.raises(SystemExit, match='cannot cast'):
        o.openalex_id(pa.array(['https://openalex.org/fields/300']), 'fields/', 'x', pa.uint8())


def test_host_org_keeps_entity_type() -> None:
    ids, kinds = o.host_org(pa.array(['https://openalex.org/P4310320505', 'https://openalex.org/I1', None]), 'x')
    assert ids.to_pylist() == [4310320505, 1, None]
    assert kinds.to_pylist() == ['publisher', 'institution', None]


def test_wikidata_normalizes_every_form() -> None:
    arr = pa.array(['https://www.wikidata.org/wiki/Q5', 'http://www.wikidata.org/entity/Q6', 'Q7', '', None])
    assert o.wikidata(arr, 'x').to_pylist() == [5, 6, 7, None, None]
    with pytest.raises(SystemExit):
        o.wikidata(pa.array(['https://example.org/Q5']), 'x')


def test_wikipedia_to_lang_title_and_nan_to_null() -> None:
    arr = pa.array(['https://en.wikipedia.org/wiki/HIV/AIDS', 'http://pt.wikipedia.org/wiki/Funda%C3%A7%C3%A3o', 'NaN'])
    assert o.wikipedia(arr, 'x').to_pylist() == ['en:HIV/AIDS', 'pt:Funda%C3%A7%C3%A3o', None]


def test_grid_and_ror() -> None:
    grids = o.grid(pa.array(['grid.1.a', 'https://www.grid.ac/institutes/grid.445690.a']), 'x')
    assert grids.to_pylist() == ['grid.1.a', 'grid.445690.a']
    assert o.grid(pa.array(['100011307']), 'x').to_pylist() == [None]  # funder ID in the grid field
    with pytest.raises(SystemExit):
        o.grid(pa.array(['grid-ish']), 'x')
    assert o.ror(pa.array(['https://ror.org/0013ka569']), 'x').to_pylist() == ['0013ka569']


def test_cast_checks_overflow_float_inf_and_drops_time_of_day() -> None:
    with pytest.raises(SystemExit):
        o.cast(pa.array([-1]), pa.uint32(), 'x')
    with pytest.raises(SystemExit, match='overflow float'):
        o.cast(pa.array([1e300]), pa.float32(), 'x')
    ts = pa.array([datetime(2026, 2, 6, 19, 53, 56, tzinfo=UTC)], pa.timestamp('us', tz='UTC'))
    assert o.cast(ts, o.DATE, 'x').to_pylist() == [date(2026, 2, 6)]
    assert o.cast(pa.array(['a', 'b', 'a']), o.DICT, 'x').type == o.DICT


def test_explode_structs_and_scalars() -> None:
    t = pa.table({'id': [1, 2, 3], 'l': [[{'a': 1}, {'a': 2}], [], None], 's': [['x'], ['y', 'z'], []]})
    assert o.explode(t, ['id'], 'l').to_pylist() == [{'id': 1, 'a': 1}, {'id': 1, 'a': 2}]
    assert o.explode(t, ['id'], 's').to_pylist() == [{'id': 1, 's': 'x'}, {'id': 2, 's': 'y'}, {'id': 2, 's': 'z'}]


def test_conform_requires_exact_columns_and_sorts() -> None:
    schema = pa.schema([('id', pa.int64()), ('n', pa.uint16())])
    out = o.conform({'id': pa.array([2, 1]), 'n': pa.array([5, 6])}, schema, 't', ['id'])
    assert out.schema == schema and out.column('id').to_pylist() == [1, 2]
    with pytest.raises(SystemExit, match='missing'):
        o.conform({'id': pa.array([1])}, schema, 't', ['id'])


def test_write_table_dir_swaps_and_passes_three_reader_check(tmp_path: Path) -> None:
    schema = pa.schema([('id', pa.int64()), ('kind', o.DICT), ('day', o.DATE)])
    cols = {'id': pa.array([3, 1, 2]), 'kind': pa.array(['a', 'b', 'a']), 'day': pa.array([date(2026, 1, 1)] * 3)}
    first = o.conform(cols, schema, 't', ['id'])
    target = tmp_path / 'oct-2026' / 'things'
    (target.parent / 'things.partial-999').mkdir(parents=True)  # leftover from a killed run
    m1 = o.write_table_dir(first, target, source={'snapshot': '2026-10-14'}, rows_per_file=2, row_group_size=1)
    assert [f['name'] for f in m1['files']] == ['part-00000.parquet', 'part-00001.parquet']
    expected = {'part-00000.parquet', 'part-00001.parquet', '_metadata', '_common_metadata', '_manifest.json'}
    assert {p.name for p in target.iterdir()} == expected
    assert o.read_check(target, schema) == 3

    o.write_table_dir(first.slice(0, 1), target, source={'snapshot': '2026-10-14'})  # rewrite over existing
    assert o.read_check(target, schema) == 1
    assert sorted(p.name for p in target.parent.iterdir()) == ['things']  # no partial or old dirs left

    o.update_top_manifest(target.parent, {'things': json.loads((target / '_manifest.json').read_text())})
    assert json.loads((target.parent / '_manifest.json').read_text())['tables']['things']['rows'] == 1
