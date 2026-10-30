"""Builders on one synthetic raw row per entity: outputs match the declared schemas and drop rules hold."""

from datetime import UTC, datetime

import pyarrow as pa

from preprocessing import optimize_parquet as op

OA = 'https://openalex.org/'
TS = datetime(2026, 2, 6, 19, 53, 56, tzinfo=UTC)
STATS = {'2yr_mean_citedness': 1.5, 'h_index': 3, 'i10_index': 2}
TOPIC = {
    'id': f'{OA}T10055',
    'display_name': 't',
    'subfield': {'id': f'{OA}subfields/3312', 'display_name': 's'},
    'field': {'id': f'{OA}fields/33', 'display_name': 'f'},
    'domain': {'id': f'{OA}domains/2', 'display_name': 'd'},
}
CBY = [
    {'year': 2024, 'works_count': 5, 'oa_works_count': 1, 'cited_by_count': 9},
    {'year': 2025002, 'works_count': 1, 'oa_works_count': 0, 'cited_by_count': 0},  # impossible year: dropped
]


def _check(out: dict[str, pa.Table]) -> None:
    for name, table in out.items():
        assert table.schema == op.SCHEMAS[name], name


def test_build_institutions() -> None:
    row = {
        'id': f'{OA}I1',
        'ror': 'https://ror.org/0013ka569',
        'display_name': 'Inst',
        'country_code': 'NO',
        'type': 'company',
        'lineage': [f'{OA}I1', f'{OA}I1', f'{OA}I2'],  # real data: one institution lists itself twice
        'is_super_system': False,
        'homepage_url': 'http://x',
        'image_url': None,
        'display_name_acronyms': ['IN', 'IN'],
        'display_name_alternatives': [],
        'works_count': 10,
        'cited_by_count': 20,
        'ids': {'grid': 'grid.1.a', 'wikipedia': '', 'wikidata': 'Q5'},
        'roles': [
            {'role': 'institution', 'id': f'{OA}I1', 'works_count': 10},
            {'role': 'funder', 'id': f'{OA}F7', 'works_count': 3},
        ],
        'repositories': [{'id': f'{OA}S9', 'host_organization': f'{OA}I1'}],
        'geo': {
            'city': 'Oslo',
            'geonames_city_id': '3143244',
            'region': None,
            'country': 'Norway',
            'latitude': 59.9,
            'longitude': 10.7,
        },
        'topics': [TOPIC | {'count': 4, 'score': 0.9}],
        'topic_share': [TOPIC | {'value': 0.1}],
        'associated_institutions': [{'id': f'{OA}I2', 'relationship': 'parent'}],
        'counts_by_year': CBY,
        'summary_stats': STATS,
        'status': 'active',
        'updated_date': TS,
        'created_date': TS,
    }
    out = op.build_institutions(pa.Table.from_pylist([row]), max_year=2027)
    _check(out)
    assert out['institutions_lineage'].to_pylist() == [{'institution_id': 1, 'ancestor_institution_id': 2}]
    assert out['institutions_roles'].column('role').to_pylist() == ['funder']
    assert out['institutions_display_name_acronyms'].num_rows == 1
    assert out['institutions_counts_by_year'].column('year').to_pylist() == [2024]
    assert out['institutions_ids'].to_pylist() == [
        {'institution_id': 1, 'grid': 'grid.1.a', 'wikipedia': None, 'wikidata': 5}
    ]


def test_build_sources() -> None:
    row = {
        'id': f'{OA}S13343251',
        'issn_l': '1610-093X',
        'issn': ['1610-093X'],
        'display_name': 'J',
        'host_organization': f'{OA}P4310320505',
        'host_organization_name': 'Pub',
        'host_organization_lineage': [f'{OA}P4310320505', f'{OA}P1'],
        'works_count': 3,
        'oa_works_count': 1,
        'cited_by_count': 4,
        'summary_stats': STATS,
        'is_oa': True,
        'is_in_doaj': False,
        'is_in_doaj_since_year': None,
        'is_high_oa_rate': False,
        'is_high_oa_rate_since_year': 2013,
        'is_in_scielo': False,
        'is_ojs': False,
        'is_core': True,
        'listed_in': ['cwts-core'],
        'is_preprint_repository': False,
        'oa_flip_year': None,
        'first_publication_year': 1999,
        'last_publication_year': 2026,
        'ids': {'wikidata': 'http://www.wikidata.org/entity/Q1678597'},
        'homepage_url': None,
        'apc_prices': [{'price': 300000, 'currency': 'IDR'}],
        'apc_usd': 19,
        'apc_usd_by_year': [{'year': 2019, 'price': 1848}],
        'country_code': 'DE',
        'societies': [{'url': None, 'organization': 'Soc'}],
        'alternate_titles': ['JBzG'],
        'type': 'journal',
        'topics': [TOPIC | {'count': 4}],
        'topic_share': [TOPIC | {'value': 0.1}],
        'counts_by_year': CBY,
        'updated_date': TS,
        'created_date': None,
    }
    out = op.build_sources(pa.Table.from_pylist([row]), max_year=2027)
    _check(out)
    assert out['sources'].column('host_organization_type').to_pylist() == ['publisher']
    assert out['sources'].column('wikidata').to_pylist() == [1678597]
    assert out['sources_host_organization_lineage'].to_pylist() == [
        {'source_id': 13343251, 'lineage_id': 1, 'lineage_type': 'publisher'}
    ]


def test_build_topics() -> None:
    ids = {'wikidata': 'https://www.wikidata.org/wiki/Q9418', 'wikipedia': 'https://en.wikipedia.org/wiki/P'}
    common = {
        'description': 'd',
        'works_count': 1,
        'cited_by_count': 2,
        'display_name_alternatives': ['alt'],
        'updated_date': TS,
        'created_date': TS,
        'ids': ids,
    }
    keyword = {'id': f'{OA}keywords/low-cost-smart-devices', 'display_name': 'low-cost smart devices', 'score': 0.5}
    raws = {
        'topics': pa.Table.from_pylist(
            [TOPIC | common | {'keywords': [keyword], 'ids': {'wikidata': None, 'wikipedia': 'NaN'}}]
        ),
        'subfields': pa.Table.from_pylist(
            [
                {'id': f'{OA}subfields/3312', 'display_name': 's', 'field': TOPIC['field'], 'domain': TOPIC['domain']}
                | common
            ]
        ),
        'fields': pa.Table.from_pylist(
            [{'id': f'{OA}fields/33', 'display_name': 'f', 'domain': TOPIC['domain']} | common]
        ),
        'domains': pa.Table.from_pylist([{'id': f'{OA}domains/2', 'display_name': 'd'} | common]),
    }
    out = op.build_topics(raws)
    _check(out)
    assert out['topics'].to_pylist()[0]['wikipedia'] is None  # 'NaN' -> null
    assert out['topics_keywords'].column('keyword_id').to_pylist() == ['low-cost-smart-devices']
    assert out['subfields'].column('field_id').to_pylist() == [33]
    assert out['domains'].column('wikidata').to_pylist() == [9418]


def test_every_table_has_a_description_and_readme_renders(tmp_path) -> None:
    assert set(op.DESCRIPTIONS) == set(op.SCHEMAS)
    import json

    for name in ('topics', 'domains'):
        (tmp_path / name).mkdir()
        manifest = {
            'table': name,
            'rows': 4,
            'bytes': 1000,
            'files': [{}],
            'source': {'snapshot': '2026-10-14'},
            'schema': [
                {'name': 'domain_id', 'type': 'uint8'},
                {'name': 'v', 'type': 'float'},
                {'name': 'k', 'type': 'dictionary<values=string, indices=int32, ordered=0>'},
            ],
        }
        (tmp_path / name / '_manifest.json').write_text(json.dumps(manifest))
    op.write_readme(tmp_path)
    text = (tmp_path / 'README.md').read_text()
    assert '## topics' in text and '| `domain_id` | uint8 |' in text and 'string (dictionary-encoded)' in text
    assert '| `v` | float32 |' in text
    assert '2026-10-14' in text and not list(tmp_path.glob('README.md.partial-*'))


def _author(n: int, **over: object) -> dict:
    inst = {
        'id': f'{OA}I{n}',
        'ror': 'https://ror.org/0013ka569',
        'display_name': 'i',
        'country_code': 'NO',
        'type': 'company',
        'lineage': [f'{OA}I{n}'],
    }
    src = {
        'id': f'{OA}S{n}',
        'display_name': 's',
        'issn_l': None,
        'issn': ['1'],
        'is_oa': True,
        'is_in_doaj': False,
        'is_core': True,
        'listed_in': ['x'],
        'host_organization': None,
        'host_organization_name': None,
        'host_organization_lineage': ['y'],
        'host_organization_lineage_names': ['z'],
        'type': 'journal',
    }
    row = {
        'id': f'{OA}A{n}',
        'display_name': f'Author {n}',
        'display_name_alternatives': [f'Author {n}', f'A. {n}'],
        'raw_author_names': [f'Author {n}', f'A. {n}'],
        'full_name': f'Author {n}',
        'orcid': 'https://orcid.org/0000-0002-1825-009x',
        'observed_orcids': ['https://orcid.org/0000-0002-1825-009x', 'https://orcid.org/0000-0002-1825-009X'],
        'works_count': 3,
        'cited_by_count': 4,
        'summary_stats': STATS,
        'ids': {'openalex': f'{OA}A{n}', 'orcid': None, 'observed_orcids': ['x']},
        'affiliations': [{'institution': inst, 'years': [2020, 2025002]}],
        'last_known_institutions': [inst],
        'topics': [TOPIC | {'count': 4}],
        'topic_share': [TOPIC | {'value': 0.1}],
        'x_concepts': [
            {'id': f'{OA}C41008148', 'wikidata': 'w', 'display_name': 'cs', 'level': 0, 'score': 0.5, 'count': 2}
        ],
        'sources': [src, src],  # real data repeats a source per author
        'counts_by_year': CBY,
        'works_api_url': 'x',
        'updated_date': datetime(2026, 4, 9, 3, 0, tzinfo=UTC),  # still 2026-04-08 in US time zones
        'created_date': None,
    }
    return row | over


def test_write_authors_duckdb_path(tmp_path, monkeypatch) -> None:
    import pyarrow.parquet as pq

    part = tmp_path / 'raw' / 'authors' / 'updated_date=2026-04-09'
    part.mkdir(parents=True)
    rows = [
        _author(2),
        _author(
            1, orcid='https://orcid.org/0000-0001-7221-230', observed_orcids=['https://orcid.org/0000-0001-7221-230']
        ),
    ]
    pq.write_table(pa.Table.from_pylist(rows), part / 'part_0000.parquet')
    monkeypatch.setitem(op.DUCKDB_CONFIG, 'temp_directory', str(tmp_path / 'tmp'))
    monkeypatch.setitem(op.DUCKDB_CONFIG, 'memory_limit', '1GB')
    out = tmp_path / 'oct-2026'
    manifests = op.write_authors(out, 2027, '2026-10-14', raw_root=tmp_path / 'raw')  # runs read_check on each table
    assert set(manifests) == {n for n in op.SCHEMAS if n.startswith('authors')}
    authors = pq.read_table(out / 'authors').to_pylist()
    assert [a['author_id'] for a in authors] == [1, 2]
    assert [a['orcid'] for a in authors] == [None, '0000-0002-1825-009X']  # truncated -> null, x -> X
    assert authors[0]['updated_date'].isoformat() == '2026-04-09'  # UTC day, whatever the local time zone
    assert pq.read_table(out / 'authors_observed_orcids').to_pylist() == [
        {'author_id': 2, 'orcid': '0000-0002-1825-009X'}
    ]
    assert pq.read_table(out / 'authors_affiliations').column('year').to_pylist() == [2020, 2020]
    assert pq.read_table(out / 'authors_sources').to_pylist() == [
        {'author_id': 1, 'source_id': 1},
        {'author_id': 2, 'source_id': 2},
    ]
    assert pq.read_table(out / 'authors_counts_by_year').column('year').to_pylist() == [2024, 2024]


def test_build_concepts() -> None:
    row = {
        'id': f'{OA}C41008148',
        'display_name': 'Computer science',
        'level': 0,
        'description': 'd',
        'wikidata': 'https://www.wikidata.org/wiki/Q21198',
        'image_url': None,
        'image_thumbnail_url': None,
        'works_count': 1,
        'cited_by_count': 2,
        'ids': {'wikipedia': 'https://en.wikipedia.org/wiki/Computer_science'},
        'updated_date': TS,
        'created_date': TS,
    }
    out = op.build_concepts(pa.Table.from_pylist([row]))
    _check(out)
    assert out['concepts'].to_pylist()[0]['wikidata'] == 21198


def _funder(n: int, **over: object) -> dict:
    row = {
        'id': f'{OA}F{n}',
        'display_name': f'Funder {n}',
        'alternate_titles': ['FN', 'FN', 'Fund'],  # real data repeats a title (22 times)
        'country_code': 'US',
        'description': None,
        'homepage_url': None,
        'image_url': 'https://img/x?a=1',
        'image_thumbnail_url': 'https://img/x?a=1&width=300',
        'ids': {
            'openalex': f'{OA}F{n}',
            'ror': 'https://ror.org/048rgv164',
            'wikidata': 'wikidata.org/wiki/Q2217705',  # 1 raw value has no scheme or www
            'crossref': 501100002846,  # exceeds uint32
            'doi': '10.13039/501100002846',
        },
        'works_count': 10,
        'cited_by_count': 20,
        'awards_count': 3,
        'roles': [
            {'role': 'funder', 'id': f'{OA}F{n}', 'works_count': 10},
            {'role': 'institution', 'id': f'{OA}I5', 'works_count': 999},
            {'role': 'publisher', 'id': f'{OA}P6', 'works_count': 7},
        ],
        'counts_by_year': [*CBY, {'year': 2030, 'works_count': 1, 'oa_works_count': 0, 'cited_by_count': 0}],
        'summary_stats': STATS | {'h_index': None},
        'updated_date': TS,
        'created_date': TS,
    }
    return row | over


def test_build_funders() -> None:
    out = op.build_funders(pa.Table.from_pylist([_funder(2), _funder(1)]), max_year=2027)
    _check(out)
    f = out['funders'].to_pylist()
    assert [r['funder_id'] for r in f] == [1, 2] and f[0]['num_awards'] == 3 and f[0]['h_index'] is None
    assert out['funders_ids'].to_pylist()[0] == {
        'funder_id': 1,
        'ror': '048rgv164',
        'wikidata': 2217705,
        'crossref': 501100002846,
    }
    titles = out['funders_alternate_titles'].to_pylist()
    assert [t['alternate_title'] for t in titles if t['funder_id'] == 1] == ['FN', 'Fund']
    assert out['funders_roles'].to_pylist()[:2] == [
        {'funder_id': 1, 'role': 'institution', 'role_id': 5},
        {'funder_id': 1, 'role': 'publisher', 'role_id': 6},
    ]
    assert out['funders_counts_by_year'].column('year').to_pylist() == [2024, 2030, 2024, 2030]  # user: keep to 2030


def test_build_funders_fails_when_doi_is_not_derivable() -> None:
    import pytest

    bad = _funder(1)
    bad['ids'] = bad['ids'] | {'doi': '10.13039/1'}
    with pytest.raises(SystemExit, match='doi'):
        op.build_funders(pa.Table.from_pylist([bad]), max_year=2027)
