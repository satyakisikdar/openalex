"""
Build optimized Parquet tables from the raw OpenAlex Parquet snapshot.

Spec: .claude/docs/schema-oct-2026.md. Rules: .claude/docs/parquet.md. Run on Yoda from the repo root:

    uv run python -m preprocessing.optimize_parquet --dry-run /path/to/scratch   # writes to scratch only
    uv run python -m preprocessing.optimize_parquet                               # writes to oct-2026/
"""

import argparse
import json
import logging
import shutil
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pyarrow as pa

from src import optimize as o
from src.optimize import DATE, DICT
from src.runtime import exit_on_sigterm

pc = o.pc
log = logging.getLogger('optimize_parquet')

RAW_ROOT = Path('/data/shared/OpenAlex/openalex-snapshot/data/parquet')
OUT_ROOT = Path('/data/shared/OpenAlex/processed-snapshots/parquet-files')
MIN_FREE_BYTES = 200e9  # CLAUDE.md: stop if a write would leave less than ~200 GB free
YEAR_MIN = 1500

I64, U8, U16, U32, F32, STR, BOOL = (
    pa.int64(),
    pa.uint8(),
    pa.uint16(),
    pa.uint32(),
    pa.float32(),
    pa.string(),
    pa.bool_(),
)
DATES = [('updated_date', DATE), ('created_date', DATE)]
COUNTS = [('num_works', U32), ('num_citations', U32)]


def _schema(*fields: tuple[str, pa.DataType] | list[tuple[str, pa.DataType]]) -> pa.Schema:
    flat: list[tuple[str, pa.DataType]] = []
    for f in fields:
        flat.extend(f if isinstance(f, list) else [f])
    return pa.schema(flat)


INST, SRC = ('institution_id', I64), ('source_id', I64)
STATS = [('2yr_mean_citedness', F32), ('h_index', U16), ('i10_index', U32)]
WIKI = [('wikidata', U32), ('wikipedia', STR)]

SCHEMAS: dict[str, pa.Schema] = {
    'institutions': _schema(
        INST,
        ('institution_name', STR),
        ('ror', STR),
        ('country_code', DICT),
        ('type', DICT),
        ('is_super_system', BOOL),
        ('status', DICT),
        ('homepage_url', STR),
        ('image_url', STR),
        COUNTS,
        STATS,
        DATES,
    ),
    'institutions_ids': _schema(INST, ('grid', STR), ('wikipedia', STR), ('wikidata', U32)),
    'institutions_geo': _schema(
        INST,
        ('city', STR),
        ('geonames_city_id', U32),
        ('region', STR),
        ('country', DICT),
        ('latitude', F32),
        ('longitude', F32),
    ),
    'institutions_associated_institutions': _schema(INST, ('associated_institution_id', I64), ('relationship', DICT)),
    'institutions_counts_by_year': _schema(
        INST, ('year', U16), ('num_works', U32), ('num_oa_works', U32), ('num_citations', U32)
    ),
    'institutions_display_name_acronyms': _schema(INST, ('display_name_acronym', STR)),
    'institutions_display_name_alternatives': _schema(INST, ('display_name_alternative', STR)),
    'institutions_lineage': _schema(INST, ('ancestor_institution_id', I64)),
    'institutions_roles': _schema(INST, ('role', DICT), ('role_id', I64), ('num_works', U32)),
    'institutions_repositories': _schema(INST, ('source_id', I64)),
    'institutions_topics': _schema(INST, ('topic_id', U16), ('num_works', U32), ('score', F32)),
    'institutions_topic_share': _schema(INST, ('topic_id', U16), ('value', F32)),
    'sources': _schema(
        SRC,
        ('source_name', STR),
        ('type', DICT),
        ('issn_l', STR),
        ('host_organization', I64),
        ('host_organization_type', DICT),
        ('host_organization_name', STR),
        ('num_works', U32),
        ('num_oa_works', U32),
        ('num_citations', U32),
        STATS,
        ('country_code', DICT),
        ('is_oa', BOOL),
        ('is_in_doaj', BOOL),
        ('is_in_doaj_since_year', U16),
        ('is_high_oa_rate', BOOL),
        ('is_high_oa_rate_since_year', pa.int32()),
        ('is_in_scielo', BOOL),
        ('is_ojs', BOOL),
        ('is_core', BOOL),
        ('is_preprint_repository', BOOL),
        ('oa_flip_year', U16),
        ('first_publication_year', U16),
        ('last_publication_year', U16),
        ('apc_usd', U16),
        ('homepage_url', STR),
        ('wikidata', U32),
        DATES,
    ),
    'sources_counts_by_year': _schema(
        SRC, ('year', U16), ('num_works', U32), ('num_citations', U32), ('num_oa_works', U32)
    ),
    'sources_issn': _schema(SRC, ('issn', STR)),
    'sources_host_organization_lineage': _schema(SRC, ('lineage_id', I64), ('lineage_type', DICT)),
    'sources_listed_in': _schema(SRC, ('listed_in', DICT)),
    'sources_apc_prices': _schema(SRC, ('price', U32), ('currency', DICT)),
    'sources_apc_usd_by_year': _schema(SRC, ('year', U16), ('price', U16)),
    'sources_societies': _schema(SRC, ('url', STR), ('organization', STR)),
    'sources_alternate_titles': _schema(SRC, ('alternate_title', STR)),
    'sources_topics': _schema(SRC, ('topic_id', U16), ('num_works', U32)),
    'sources_topic_share': _schema(SRC, ('topic_id', U16), ('value', F32)),
    'topics': _schema(
        ('topic_id', U16),
        ('topic_name', STR),
        ('description', STR),
        COUNTS,
        ('subfield_id', U16),
        ('field_id', U8),
        ('domain_id', U8),
        ('wikipedia', STR),
        DATES,
    ),
    'topics_keywords': _schema(('topic_id', U16), ('keyword_id', STR), ('keyword', STR), ('score', F32)),
    'subfields': _schema(
        ('subfield_id', U16),
        ('subfield_name', STR),
        ('description', STR),
        ('field_id', U8),
        ('domain_id', U8),
        COUNTS,
        WIKI,
        DATES,
    ),
    'subfields_display_name_alternatives': _schema(('subfield_id', U16), ('display_name_alternative', STR)),
    'fields': _schema(
        ('field_id', U8), ('field_name', STR), ('description', STR), ('domain_id', U8), COUNTS, WIKI, DATES
    ),
    'fields_display_name_alternatives': _schema(('field_id', U8), ('display_name_alternative', STR)),
    'domains': _schema(('domain_id', U8), ('domain_name', STR), ('description', STR), COUNTS, WIKI, DATES),
    'domains_display_name_alternatives': _schema(('domain_id', U8), ('display_name_alternative', STR)),
}


def _conform(name: str, columns: dict[str, Any], sort_keys: list[str]) -> pa.Table:
    return o.conform(columns, SCHEMAS[name], name, sort_keys)


def _field(arr: Any, name: str) -> Any:
    return pc.struct_field(arr, name)


def _counts_by_year(base: pa.Table, key: str, name: str, max_year: int, order: list[str]) -> pa.Table:
    """Explode counts_by_year, dropping impossible years (spec Decision 1)."""
    c = o.explode(base, [key], 'counts_by_year')
    ok = pc.and_(pc.greater_equal(c['year'], YEAR_MIN), pc.less_equal(c['year'], max_year))
    c = o.drop_rows(c, pc.invert(ok), name, f'year outside {YEAR_MIN}..{max_year}')
    raw = {'num_works': 'works_count', 'num_oa_works': 'oa_works_count', 'num_citations': 'cited_by_count'}
    return _conform(name, {key: c[key], 'year': c['year'], **{n: c[raw[n]] for n in order}}, [key, 'year'])


def _drop_self(table: pa.Table, is_self: Any, expected: int, name: str, why: str) -> pa.Table:
    """Drop rows that only restate the parent; the count must match what the data audit found."""
    n = o._count(is_self)
    if n != expected:
        raise SystemExit(f'{name}: expected {expected:,} {why} rows, found {n:,}; re-check the spec.')
    return o.drop_rows(table, is_self, name, why)


def _topic_lists(base: pa.Table, key: str, prefix: str, with_score: bool) -> dict[str, pa.Table]:
    t = o.explode(base, [key], 'topics')
    cols = {key: t[key], 'topic_id': o.openalex_id(t['id'], 'T', f'{prefix}_topics.id', U16), 'num_works': t['count']}
    if with_score:
        cols['score'] = t['score']
    s = o.explode(base, [key], 'topic_share')
    share = {key: s[key], 'topic_id': o.openalex_id(s['id'], 'T', f'{prefix}_topic_share.id', U16), 'value': s['value']}
    return {
        f'{prefix}_topics': _conform(f'{prefix}_topics', cols, [key]),
        f'{prefix}_topic_share': _conform(f'{prefix}_topic_share', share, [key]),
    }


def build_institutions(raw: pa.Table, max_year: int) -> dict[str, pa.Table]:
    o.assert_unique(raw['id'], 'institutions.id')
    iid = o.openalex_id(raw['id'], 'I', 'institutions.id')
    stats, ids, geo = raw['summary_stats'], raw['ids'], raw['geo']
    out = {
        'institutions': _conform(
            'institutions',
            {
                'institution_id': iid,
                'institution_name': raw['display_name'],
                'ror': o.ror(raw['ror'], 'institutions.ror'),
                'country_code': raw['country_code'],
                'type': raw['type'],
                'is_super_system': raw['is_super_system'],
                'status': raw['status'],
                'homepage_url': raw['homepage_url'],
                'image_url': raw['image_url'],
                'num_works': raw['works_count'],
                'num_citations': raw['cited_by_count'],
                **{k: _field(stats, k) for k in ('2yr_mean_citedness', 'h_index', 'i10_index')},
                'updated_date': raw['updated_date'],
                'created_date': raw['created_date'],
            },
            ['institution_id'],
        ),
        'institutions_ids': _conform(
            'institutions_ids',
            {
                'institution_id': iid,
                'grid': o.grid(_field(ids, 'grid'), 'institutions.ids.grid'),
                'wikipedia': o.wikipedia(_field(ids, 'wikipedia'), 'institutions.ids.wikipedia'),
                'wikidata': o.wikidata(_field(ids, 'wikidata'), 'institutions.ids.wikidata'),
            },
            ['institution_id'],
        ),
        'institutions_geo': _conform(
            'institutions_geo',
            {'institution_id': iid}
            | {k: _field(geo, k) for k in ('city', 'geonames_city_id', 'region', 'country', 'latitude', 'longitude')},
            ['institution_id'],
        ),
    }
    lists = ['associated_institutions', 'counts_by_year', 'display_name_acronyms', 'display_name_alternatives']
    lists += ['lineage', 'roles', 'repositories', 'topics', 'topic_share']
    base = pa.table({'institution_id': iid, 'parent_works': raw['works_count'], **{c: raw[c] for c in lists}})
    key = ['institution_id']

    a = o.explode(base, key, 'associated_institutions')
    out['institutions_associated_institutions'] = _conform(
        'institutions_associated_institutions',
        {
            'institution_id': a['institution_id'],
            'associated_institution_id': o.openalex_id(a['id'], 'I', 'associated_institutions.id'),
            'relationship': a['relationship'],
        },
        key,
    )
    out['institutions_counts_by_year'] = _counts_by_year(
        base, 'institution_id', 'institutions_counts_by_year', max_year, ['num_works', 'num_oa_works', 'num_citations']
    )
    for col, name, out_col in (
        ('display_name_acronyms', 'institutions_display_name_acronyms', 'display_name_acronym'),
        ('display_name_alternatives', 'institutions_display_name_alternatives', 'display_name_alternative'),
    ):
        e = o.explode(base, key, col)
        out[name] = _conform(name, {'institution_id': e['institution_id'], out_col: e[col]}, key)

    lin = o.explode(base, key, 'lineage')
    lin = pa.table(
        {
            'institution_id': lin['institution_id'],
            'ancestor_institution_id': o.openalex_id(lin['lineage'], 'I', 'lineage'),
        }
    )
    lin = _drop_self(
        lin,
        pc.equal(lin['institution_id'], lin['ancestor_institution_id']),
        raw.num_rows,
        'institutions_lineage',
        'self entry',
    )
    out['institutions_lineage'] = _conform('institutions_lineage', {c: lin[c] for c in lin.column_names}, key)

    r = o.explode(base, ['institution_id', 'parent_works'], 'roles')
    r = r.append_column('role_id', o.openalex_id(r['id'], '[IFP]', 'roles.id'))
    is_self = pc.equal(r['role'], 'institution')
    restates = pc.and_(pc.equal(r['role_id'], r['institution_id']), pc.equal(r['works_count'], r['parent_works']))
    if o._count(is_self) != o._count(pc.and_(is_self, restates)):
        raise SystemExit('institutions_roles: an institution role row differs from its parent; re-check the spec.')
    r = _drop_self(r, is_self, raw.num_rows, 'institutions_roles', 'self institution role')
    out['institutions_roles'] = _conform(
        'institutions_roles',
        {
            'institution_id': r['institution_id'],
            'role': r['role'],
            'role_id': r['role_id'],
            'num_works': r['works_count'],
        },
        key,
    )

    rep = o.explode(base, key, 'repositories')
    out['institutions_repositories'] = _conform(
        'institutions_repositories',
        {'institution_id': rep['institution_id'], 'source_id': o.openalex_id(rep['id'], 'S', 'repositories.id')},
        key,
    )
    out |= _topic_lists(base, 'institution_id', 'institutions', with_score=True)
    return out


def build_sources(raw: pa.Table, max_year: int) -> dict[str, pa.Table]:
    o.assert_unique(raw['id'], 'sources.id')
    sid = o.openalex_id(raw['id'], 'S', 'sources.id')
    host_id, host_type = o.host_org(raw['host_organization'], 'sources.host_organization')
    stats = raw['summary_stats']
    same = [
        'issn_l',
        'host_organization_name',
        'country_code',
        'type',
        'is_oa',
        'is_in_doaj',
        'is_in_doaj_since_year',
        'is_high_oa_rate',
        'is_high_oa_rate_since_year',
        'is_in_scielo',
        'is_ojs',
        'is_core',
        'is_preprint_repository',
        'oa_flip_year',
        'first_publication_year',
        'last_publication_year',
        'apc_usd',
        'homepage_url',
        'updated_date',
        'created_date',
    ]
    out = {
        'sources': _conform(
            'sources',
            {
                'source_id': sid,
                'source_name': raw['display_name'],
                'host_organization': host_id,
                'host_organization_type': host_type,
                'num_works': raw['works_count'],
                'num_oa_works': raw['oa_works_count'],
                'num_citations': raw['cited_by_count'],
                **{k: _field(stats, k) for k in ('2yr_mean_citedness', 'h_index', 'i10_index')},
                'wikidata': o.wikidata(_field(raw['ids'], 'wikidata'), 'sources.ids.wikidata'),
                **{c: raw[c] for c in same},
            },
            ['source_id'],
        )
    }
    lists = ['counts_by_year', 'issn', 'host_organization_lineage', 'listed_in', 'apc_prices', 'apc_usd_by_year']
    lists += ['societies', 'alternate_titles', 'topics', 'topic_share']
    base = pa.table({'source_id': sid, 'host_url': raw['host_organization'], **{c: raw[c] for c in lists}})
    key = ['source_id']

    out['sources_counts_by_year'] = _counts_by_year(
        base, 'source_id', 'sources_counts_by_year', max_year, ['num_works', 'num_citations', 'num_oa_works']
    )
    h = o.explode(base, ['source_id', 'host_url'], 'host_organization_lineage')
    n_hosts = o._count(pc.is_valid(raw['host_organization']))
    h = _drop_self(
        h,
        pc.equal(h['host_organization_lineage'], h['host_url']),
        n_hosts,
        'sources_host_organization_lineage',
        'host organization itself',
    )
    lineage_id, lineage_type = o.host_org(h['host_organization_lineage'], 'host_organization_lineage')
    out['sources_host_organization_lineage'] = _conform(
        'sources_host_organization_lineage',
        {'source_id': h['source_id'], 'lineage_id': lineage_id, 'lineage_type': lineage_type},
        key,
    )
    for col, name, fields in (
        ('issn', 'sources_issn', {'issn': 'issn'}),
        ('listed_in', 'sources_listed_in', {'listed_in': 'listed_in'}),
        ('alternate_titles', 'sources_alternate_titles', {'alternate_title': 'alternate_titles'}),
        ('apc_prices', 'sources_apc_prices', {'price': 'price', 'currency': 'currency'}),
        ('apc_usd_by_year', 'sources_apc_usd_by_year', {'year': 'year', 'price': 'price'}),
        ('societies', 'sources_societies', {'url': 'url', 'organization': 'organization'}),
    ):
        e = o.explode(base, key, col)
        out[name] = _conform(name, {'source_id': e['source_id'], **{k: e[v] for k, v in fields.items()}}, key)
    out |= _topic_lists(base, 'source_id', 'sources', with_score=False)
    return out


def build_topics(raws: dict[str, pa.Table]) -> dict[str, pa.Table]:
    """topics, subfields, fields, and domains, plus their child tables."""
    out: dict[str, pa.Table] = {}
    parents = {'subfield': ('subfields/', U16), 'field': ('fields/', U8), 'domain': ('domains/', U8)}
    for entity, (prefix, id_type) in [('topics', ('T', U16)), *[(f'{k}s', v) for k, v in parents.items()]]:
        raw = raws[entity]
        singular = entity.removesuffix('s')
        key = f'{singular}_id'
        o.assert_unique(raw['id'], f'{entity}.id')
        ids = o.openalex_id(raw['id'], prefix, f'{entity}.id', id_type)
        cols: dict[str, Any] = {
            key: ids,
            f'{singular}_name': raw['display_name'],
            'description': raw['description'],
            'num_works': raw['works_count'],
            'num_citations': raw['cited_by_count'],
            'wikipedia': o.wikipedia(_field(raw['ids'], 'wikipedia'), f'{entity}.ids.wikipedia'),
            'updated_date': raw['updated_date'],
            'created_date': raw['created_date'],
        }
        if entity != 'topics':
            cols['wikidata'] = o.wikidata(_field(raw['ids'], 'wikidata'), f'{entity}.ids.wikidata')
        for parent, (p_prefix, p_type) in parents.items():
            if parent in raw.column_names:
                cols[f'{parent}_id'] = o.openalex_id(_field(raw[parent], 'id'), p_prefix, f'{entity}.{parent}', p_type)
        out[entity] = _conform(entity, cols, [key])

        lists = [c for c in ('keywords', 'display_name_alternatives') if c in raw.column_names]
        base = pa.table({key: ids, **{c: raw[c] for c in lists}})
        if entity == 'topics':
            k = o.explode(base, [key], 'keywords')
            out['topics_keywords'] = _conform(
                'topics_keywords',
                {
                    key: k[key],
                    'keyword_id': o.keyword_slug(k['id'], 'topics.keywords.id'),
                    'keyword': k['display_name'],
                    'score': k['score'],
                },
                [key],
            )
        else:
            name = f'{entity}_display_name_alternatives'
            e = o.explode(base, [key], 'display_name_alternatives')
            out[name] = _conform(name, {key: e[key], 'display_name_alternative': e['display_name_alternatives']}, [key])
    return out


ENTITIES: dict[str, tuple[list[str], Callable[[dict[str, pa.Table], int], dict[str, pa.Table]]]] = {
    'institutions': (['institutions'], lambda r, y: build_institutions(r['institutions'], y)),
    'sources': (['sources'], lambda r, y: build_sources(r['sources'], y)),
    'topics': (['topics', 'subfields', 'fields', 'domains'], lambda r, y: build_topics(r)),
}


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--month', default='oct-2026', help='output dir name, e.g. oct-2026')
    parser.add_argument('--entities', nargs='+', choices=list(ENTITIES), default=list(ENTITIES))
    parser.add_argument('--dry-run', metavar='SCRATCH', type=Path, help='write under SCRATCH instead of the real root')
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s', stream=sys.stdout)
    exit_on_sigterm()
    log.info('START optimize_parquet %s', vars(args))

    snapshot = json.loads((RAW_ROOT / 'manifest.json').read_text())['date']
    max_year = int(snapshot[:4]) + 1
    out_root = (args.dry_run if args.dry_run else OUT_ROOT) / args.month
    probe = next(p for p in [out_root, *out_root.parents] if p.exists())
    free = shutil.disk_usage(probe).free
    log.info('snapshot %s, max year %s, output %s, %.0f GB free', snapshot, max_year, out_root, free / 1e9)
    if free < MIN_FREE_BYTES:
        raise SystemExit(f'Only {free / 1e9:.0f} GB free at {probe}; free space or ask the user before writing.')

    manifests = {}
    for entity in args.entities:
        raw_names, build = ENTITIES[entity]
        raws = {n: o.load_raw(RAW_ROOT, n) for n in raw_names}
        for name, table in build(raws, max_year).items():
            target = out_root / name
            manifests[name] = o.write_table_dir(table, target, source={'snapshot': snapshot, 'raw': raw_names})
            o.read_check(target, SCHEMAS[name])
            log.info('checked %s with pyarrow, pandas, and duckdb', name)
    o.update_top_manifest(out_root, manifests)
    total = sum(m['rows'] for m in manifests.values())
    log.info('END optimize_parquet ok: %s tables, %s rows', len(manifests), f'{total:,}')


if __name__ == '__main__':
    main()
