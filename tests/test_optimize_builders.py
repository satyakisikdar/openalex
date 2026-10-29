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
        'lineage': [f'{OA}I1', f'{OA}I2'],
        'is_super_system': False,
        'homepage_url': 'http://x',
        'image_url': None,
        'display_name_acronyms': ['IN'],
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
