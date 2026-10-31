"""
Build optimized Parquet tables from the raw OpenAlex Parquet snapshot.

Spec: .claude/docs/schema-oct-2026.md. Rules: .claude/docs/parquet.md. Run on Yoda from the repo root:

    uv run python -m preprocessing.optimize_parquet --dry-run /path/to/scratch   # writes to scratch only
    uv run python -m preprocessing.optimize_parquet                               # writes to oct-2026/
"""

import argparse
import json
import logging
import os
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

I64, U8, U16, U32, F32, F64, STR, BOOL = (
    pa.int64(),
    pa.uint8(),
    pa.uint16(),
    pa.uint32(),
    pa.float32(),
    pa.float64(),
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


INST, SRC, AUTH, FUND = ('institution_id', I64), ('source_id', I64), ('author_id', I64), ('funder_id', I64)
AWARD = ('award_id', I64)
WORK, YEAR = ('work_id', I64), ('publication_year', U16)
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
        ('in_doaj_since_year', U16),
        ('is_high_oa_rate', BOOL),
        ('high_oa_rate_since_year', pa.int32()),
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
    'concepts': _schema(
        ('concept_id', I64),
        ('concept_name', STR),
        ('level', U8),
        ('description', STR),
        ('wikidata', U32),
        ('wikipedia', STR),
        ('image_url', STR),
        COUNTS,
        DATES,
    ),
    'funders': _schema(
        FUND,
        ('funder_name', STR),
        ('country_code', DICT),
        ('description', STR),
        ('homepage_url', STR),
        ('image_url', STR),
        COUNTS,
        ('num_awards', U32),
        STATS,
        DATES,
    ),
    'funders_ids': _schema(FUND, ('ror', STR), ('wikidata', U32), ('crossref', I64)),
    'funders_alternate_titles': _schema(FUND, ('alternate_title', STR)),
    'funders_roles': _schema(FUND, ('role', DICT), ('role_id', I64)),
    'funders_counts_by_year': _schema(
        FUND, ('year', U16), ('num_works', U32), ('num_oa_works', U32), ('num_citations', U32)
    ),
    'awards': _schema(
        AWARD,
        ('award_name', STR),
        ('description', STR),
        ('funder_award_id', STR),
        FUND,
        ('unlinked_funder_name', STR),
        ('amount', F64),
        ('currency', DICT),
        ('funding_type', DICT),
        ('funder_scheme', DICT),
        ('provenance', DICT),
        ('start_date', DATE),
        ('end_date', DATE),
        ('start_year', U16),
        ('end_year', U16),
        ('primary_topic_id', U16),
        ('primary_topic_score', F32),
        ('parent_award_id', I64),
        ('parent_relationship', DICT),
        ('parent_asserted_by', DICT),
        ('landing_page_url', STR),
        ('doi', STR),
        ('num_funded_outputs', U32),
        DATES,
    ),
    'awards_investigators': _schema(
        AWARD,
        ('role', DICT),
        ('given_name', STR),
        ('family_name', STR),
        ('orcid', STR),
        ('role_start', DATE),
        ('affiliation_name', STR),
        ('affiliation_country', STR),
        ('affiliation_id_type', DICT),
        ('affiliation_id', STR),
        ('affiliation_id_asserted_by', DICT),
    ),
    'awards_institutions': _schema(AWARD, INST),
    'authors': _schema(
        AUTH,
        ('author_name', STR),
        ('full_name', STR),
        ('orcid', STR),
        COUNTS,
        ('2yr_mean_citedness', F32),
        ('h_index', U16),
        ('i10_index', U16),
        DATES,
    ),
    'authors_display_name_alternatives': _schema(AUTH, ('display_name_alternative', STR)),
    'authors_observed_orcids': _schema(AUTH, ('orcid', STR)),
    'authors_affiliations': _schema(AUTH, ('institution_id', I64), ('year', U16)),
    'authors_last_known_institutions': _schema(AUTH, ('institution_id', I64)),
    'authors_topics': _schema(AUTH, ('topic_id', U16), ('num_works', U32)),
    'authors_topic_share': _schema(AUTH, ('topic_id', U16), ('value', F32)),
    'authors_concepts': _schema(AUTH, ('concept_id', I64), ('score', F32), ('num_works', U32)),
    'authors_sources': _schema(AUTH, ('source_id', I64)),
    'authors_counts_by_year': _schema(
        AUTH, ('year', U16), ('num_works', U32), ('num_oa_works', U32), ('num_citations', U32)
    ),
    'works': _schema(
        WORK,
        ('doi', STR),
        ('title', STR),
        YEAR,
        ('publication_date', DATE),
        ('type', DICT),
        ('language', DICT),
        ('num_authors', U16),
        ('num_locations', U16),
        ('num_references', U16),
        ('num_citations', U32),
        ('num_countries', U8),
        ('num_institutions', U16),
        ('fwci', F32),
        ('citation_percentile', F32),
        ('is_in_top_1_percent', BOOL),
        ('is_in_top_10_percent', BOOL),
        ('cited_by_percentile_year_min', U8),
        ('cited_by_percentile_year_max', U8),
        ('apc_list_value', U32),
        ('apc_list_currency', DICT),
        ('apc_list_usd', U16),
        ('apc_paid_value', U32),
        ('apc_paid_currency', DICT),
        ('apc_paid_usd', U32),
        ('volume_work_id', I64),
        ('volume_name', STR),
        ('has_complete_institution_info', BOOL),
        ('has_grant_info', BOOL),
        ('has_keywords', BOOL),
        ('has_fulltext', BOOL),
        ('has_pdf', BOOL),
        ('has_grobid_xml', BOOL),
        ('primary_source_is_oa', BOOL),
        ('is_retracted', BOOL),
        ('is_paratext', BOOL),
        DATES,
    ),
    'works_ids': _schema(WORK, YEAR, ('mag', U32), ('pmid', U32), ('openalex_curation', STR)),
    'works_open_access': _schema(
        WORK, YEAR, ('is_oa', BOOL), ('oa_status', DICT), ('oa_url', STR), ('any_repository_has_fulltext', BOOL)
    ),
    'works_biblio': _schema(WORK, YEAR, ('volume', DICT), ('issue', DICT), ('first_page', STR), ('last_page', STR)),
    'works_authorships': _schema(
        WORK,
        YEAR,
        ('author_order', U16),
        ('author_position', DICT),
        AUTH,
        ('raw_author_name', STR),
        ('raw_orcid', STR),
        ('is_corresponding', BOOL),
        INST,
        ('country_code', DICT),
        ('raw_affiliation_string', STR),
    ),
    'works_corresponding_institutions': _schema(WORK, YEAR, INST),
    'works_locations': _schema(
        WORK,
        YEAR,
        ('location_order', U16),
        ('is_primary', BOOL),
        ('is_best_oa', BOOL),
        ('native_id', STR),
        SRC,
        ('source_is_oa', BOOL),
        ('doi', STR),
        ('landing_page_url', STR),
        ('pdf_url', STR),
        ('version', DICT),
        ('license', DICT),
        ('is_oa', BOOL),
        ('is_accepted', BOOL),
        ('is_published', BOOL),
        ('raw_source_name', STR),
        ('raw_type', DICT),
        ('provenance', DICT),
    ),
    'works_topics': _schema(
        WORK,
        YEAR,
        ('is_primary_topic', BOOL),
        ('score', F32),
        ('topic_id', U16),
        ('topic_name', DICT),
        ('subfield_id', U16),
        ('subfield_name', DICT),
        ('field_id', U8),
        ('field_name', DICT),
        ('domain_id', U8),
        ('domain_name', DICT),
    ),
    'works_keywords': _schema(WORK, YEAR, ('keyword_id', STR), ('score', F32)),
    'works_concepts': _schema(WORK, YEAR, ('concept_id', I64), ('score', F32)),
    'works_mesh': _schema(
        WORK,
        YEAR,
        ('descriptor_ui', DICT),
        ('descriptor_name', DICT),
        ('qualifier_ui', DICT),
        ('qualifier_name', DICT),
        ('is_major_topic', BOOL),
    ),
    'works_referenced_works': _schema(
        WORK, ('work_publication_year', U16), ('referenced_work_id', I64), ('referenced_work_publication_year', U16)
    ),
    'works_indexed_in': _schema(WORK, YEAR, ('indexed_source', DICT)),
    'works_counts_by_year': _schema(WORK, YEAR, ('year', U16), ('num_citations', U32)),
    'works_awards': _schema(WORK, YEAR, AWARD),
    'works_funders': _schema(WORK, YEAR, FUND),
    'works_sustainable_development_goals': _schema(WORK, YEAR, ('sdg_id', U8), ('score', F32)),
    'works_study_designs': _schema(WORK, YEAR, ('study_design', DICT)),
}


DESCRIPTIONS: dict[str, str] = {
    'institutions': 'One row per institution: name, ROR, country, type, status, and work and citation counts.',
    'institutions_ids': 'External identifiers per institution: GRID, Wikipedia (`lang:Title`), and Wikidata Q-number.',
    'institutions_geo': 'Location per institution: city, GeoNames city ID, region, country, latitude, and longitude.',
    'institutions_associated_institutions': 'Related institutions, one row per pair, with the relationship type.',
    'institutions_counts_by_year': 'Works, open-access works, and citations per institution per year.',
    'institutions_display_name_acronyms': 'Acronyms of each institution, one row per acronym.',
    'institutions_display_name_alternatives': 'Alternative names of each institution, one row per name.',
    'institutions_lineage': 'Ancestor institutions of each institution (the institution itself is excluded).',
    'institutions_roles': 'Other OpenAlex entities (funder or publisher) that represent the same organization.',
    'institutions_repositories': 'Repository sources hosted by each institution.',
    'institutions_topics': 'Topics of each institution with work counts and topic scores.',
    'institutions_topic_share': 'Share of each topic in the work of each institution.',
    'sources': 'One row per source (journal, repository, conference, ...): host, open-access flags, APC, and counts.',
    'sources_counts_by_year': 'Works, open-access works, and citations per source per year.',
    'sources_issn': 'ISSNs of each source, one row per ISSN.',
    'sources_host_organization_lineage': 'Parent organizations of each source host (the host itself is excluded).',
    'sources_listed_in': 'Lists and indexes each source appears in (for example `cwts-core`).',
    'sources_alternate_titles': 'Alternative titles of each source.',
    'sources_apc_prices': 'Article processing charges per source in their original currencies.',
    'sources_apc_usd_by_year': 'Article processing charge per source per year, in US dollars.',
    'sources_societies': 'Societies associated with each source, with URL and organization name.',
    'sources_topics': 'Topics of each source with work counts.',
    'sources_topic_share': 'Share of each topic in the work of each source.',
    'topics': 'One row per topic with description, counts, and its subfield, field, and domain.',
    'topics_keywords': 'Keywords of each topic with keyword slug and score.',
    'subfields': 'One row per subfield with its field and domain.',
    'subfields_display_name_alternatives': 'Alternative names of each subfield.',
    'fields': 'One row per field with its domain.',
    'fields_display_name_alternatives': 'Alternative names of each field.',
    'domains': 'One row per domain (the top level of the topic hierarchy).',
    'domains_display_name_alternatives': 'Alternative names of each domain.',
    'concepts': 'One row per concept (the legacy OpenAlex concept tagging, levels 0 to 5) with counts.',
    'funders': 'One row per funder: name, country, and work, citation, and award counts (counts include xpac works).',
    'funders_ids': 'External identifiers per funder: ROR, Wikidata Q-number, and Crossref Funder ID '
    '(the funder DOI is `10.13039/` + `crossref`).',
    'funders_alternate_titles': 'Alternative names of each funder, one row per name.',
    'funders_roles': 'Other OpenAlex entities (institution or publisher) that represent the same organization.',
    'funders_counts_by_year': 'Works, open-access works, and citations per funder per year (years up to 2030 kept).',
    'awards': 'One row per award (grant): funder and its award number, amount, dates, type, topic, and parent award. '
    '`unlinked_funder_name` names the funder only when it has no OpenAlex ID.',
    'awards_investigators': 'Investigators of each award: role (`lead`, `co_lead`, `investigator`), name, ORCID, '
    'and affiliation as reported by the funder.',
    'awards_institutions': 'Institutions each award was made to.',
    'authors': 'One row per author: name, ORCID, and work and citation counts.',
    'authors_display_name_alternatives': 'Alternative names of each author, one row per name.',
    'authors_observed_orcids': 'Every ORCID observed for each author (includes the main `orcid`).',
    'authors_affiliations': 'Institutions each author was affiliated with, one row per author, institution, and year.',
    'authors_last_known_institutions': 'Institutions each author was last affiliated with.',
    'authors_topics': 'Top topics of each author (at most 5) with work counts.',
    'authors_topic_share': 'Share of each topic in the work of each author (at most 5).',
    'authors_concepts': 'Top concepts of each author (at most 5) with score and work counts.',
    'authors_sources': 'Sources each author has published in.',
    'authors_counts_by_year': 'Works, open-access works, and citations per author per year.',
    'works': 'One row per work (xpac works excluded): DOI, title, year, type, language, counts, citation metrics, '
    'APCs, and flags. `num_authors` counts distinct authors in `works_authorships`.',
    'works_ids': 'External identifiers per work: MAG ID, PubMed ID, and OpenAlex curation ID.',
    'works_open_access': 'Open-access status per work, with the best open-access URL.',
    'works_biblio': 'Volume, issue, and pages per work.',
    'works_authorships': 'One row per work, author, and institution (null when the author has none). Repeated '
    'copies of an author with the same institutions are dropped; `author_order` is the position in the byline.',
    'works_corresponding_institutions': 'Institutions OpenAlex lists as corresponding for each work.',
    'works_locations': 'Every location (version) of each work, with its source, URLs, license, and version; '
    '`is_primary` and `is_best_oa` mark the primary and best open-access location.',
    'works_topics': 'Topics of each work (at most 3) with score and the topic hierarchy names.',
    'works_keywords': 'Keywords of each work: slug and score.',
    'works_concepts': 'Legacy concepts of each work with score.',
    'works_mesh': 'MeSH descriptors and qualifiers of each work (one row per descriptor and qualifier).',
    'works_referenced_works': 'References between works in this build, with the publication year of both works.',
    'works_indexed_in': 'Indexes each work appears in (crossref, datacite, pubmed, doaj, arxiv).',
    'works_counts_by_year': 'Citations received per work per year (2012 onward).',
    'works_awards': 'Awards (grants) that funded each work; details in `awards`.',
    'works_funders': 'Funders of each work; details in `funders`.',
    'works_sustainable_development_goals': 'UN Sustainable Development Goals (1 to 17) of each work with score.',
    'works_study_designs': 'Study designs of each work (for example `case-report`).',
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
    lin = o.dedupe(lin, 'institutions_lineage')  # one institution lists itself twice
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
        'is_high_oa_rate',
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
                # renamed (user, 2026-10-30): an `is_` prefix always means a bool; these are years
                'in_doaj_since_year': raw['is_in_doaj_since_year'],
                'high_oa_rate_since_year': raw['is_high_oa_rate_since_year'],
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


def build_concepts(raw: pa.Table) -> dict[str, pa.Table]:
    """The concepts table only: every list and nested field is null in this snapshot."""
    o.assert_unique(raw['id'], 'concepts.id')
    cols = {
        'concept_id': o.openalex_id(raw['id'], 'C', 'concepts.id'),
        'concept_name': raw['display_name'],
        'level': raw['level'],
        'description': raw['description'],
        'wikidata': o.wikidata(raw['wikidata'], 'concepts.wikidata'),
        'wikipedia': o.wikipedia(_field(raw['ids'], 'wikipedia'), 'concepts.ids.wikipedia'),
        'image_url': raw['image_url'],
        'num_works': raw['works_count'],
        'num_citations': raw['cited_by_count'],
        'updated_date': raw['updated_date'],
        'created_date': raw['created_date'],
    }
    return {'concepts': _conform('concepts', cols, ['concept_id'])}


FUNDER_YEARS_AHEAD = 3  # user: keep funder years to 2030 (snapshot year + 4) to cover long-term grants


def _require_equal(a: Any, b: Any, label: str) -> None:
    """A dropped column must be derivable from a kept one in every row (nulls must match too)."""
    if n := o._count(pc.invert(pc.fill_null(pc.equal(a, b), pc.and_(pc.is_null(a), pc.is_null(b))))):
        raise SystemExit(f'{label}: {n:,} rows are not derivable; re-check the spec before dropping it.')


def build_funders(raw: pa.Table, max_year: int) -> dict[str, pa.Table]:
    o.assert_unique(raw['id'], 'funders.id')
    fid = o.openalex_id(raw['id'], 'F', 'funders.id')
    stats, ids = raw['summary_stats'], raw['ids']
    crossref = _field(ids, 'crossref')
    _require_equal(_field(ids, 'openalex'), raw['id'], 'funders.ids.openalex')
    doi = pc.binary_join_element_wise('10.13039/', pc.cast(crossref, STR), '')
    _require_equal(_field(ids, 'doi'), doi, 'funders.ids.doi')
    thumb = pc.binary_join_element_wise(raw['image_url'], '&width=300', '')
    _require_equal(raw['image_thumbnail_url'], thumb, 'funders.image_thumbnail_url')
    key = ['funder_id']
    out = {
        'funders': _conform(
            'funders',
            {
                'funder_id': fid,
                'funder_name': raw['display_name'],
                'country_code': raw['country_code'],
                'description': raw['description'],
                'homepage_url': raw['homepage_url'],
                'image_url': raw['image_url'],
                'num_works': raw['works_count'],
                'num_citations': raw['cited_by_count'],
                'num_awards': raw['awards_count'],
                **{k: _field(stats, k) for k in ('2yr_mean_citedness', 'h_index', 'i10_index')},
                'updated_date': raw['updated_date'],
                'created_date': raw['created_date'],
            },
            key,
        ),
        'funders_ids': _conform(
            'funders_ids',
            {
                'funder_id': fid,
                'ror': o.ror(_field(ids, 'ror'), 'funders.ids.ror'),
                'wikidata': o.wikidata(_field(ids, 'wikidata'), 'funders.ids.wikidata'),
                'crossref': crossref,
            },
            key,
        ),
    }
    base = pa.table(
        {'funder_id': fid, 'parent_works': raw['works_count']}
        | {c: raw[c] for c in ('alternate_titles', 'roles', 'counts_by_year')}
    )
    t = o.dedupe(o.explode(base, key, 'alternate_titles'), 'funders_alternate_titles')
    out['funders_alternate_titles'] = _conform(
        'funders_alternate_titles', {'funder_id': t['funder_id'], 'alternate_title': t['alternate_titles']}, key
    )
    r = o.explode(base, ['funder_id', 'parent_works'], 'roles')
    r = r.append_column('role_id', o.openalex_id(r['id'], '[IFP]', 'funders.roles.id'))
    is_self = pc.equal(r['role'], 'funder')
    restates = pc.and_(pc.equal(r['role_id'], r['funder_id']), pc.equal(r['works_count'], r['parent_works']))
    if o._count(is_self) != o._count(pc.and_(is_self, restates)):
        raise SystemExit('funders_roles: a funder role row differs from its parent; re-check the spec.')
    r = _drop_self(r, is_self, raw.num_rows, 'funders_roles', 'self funder role')
    out['funders_roles'] = _conform(
        'funders_roles', {'funder_id': r['funder_id'], 'role': r['role'], 'role_id': r['role_id']}, key
    )
    out['funders_counts_by_year'] = _counts_by_year(
        base,
        'funder_id',
        'funders_counts_by_year',
        max_year + FUNDER_YEARS_AHEAD,
        ['num_works', 'num_oa_works', 'num_citations'],
    )
    return out


# Authors are too big for memory: capped DuckDB does the unnest, dedupe, and sort (spilling to disk), and pyarrow
# writes the batches in the same layout as every other table. ORDER BY ALL gives a full, deterministic order.
DUCKDB_CONFIG: dict[str, Any] = {
    'memory_limit': '60GB',
    'threads': 16,
    'temp_directory': '/home/ssikdar/scratch/duckdb-tmp',
}
ORCID = r'[0-9]{4}-[0-9]{4}-[0-9]{4}-[0-9]{3}[0-9X]'
DUCKDB_MACROS = rf"""
create or replace macro oa_id(s, p, label) as case when s is null then null
  when regexp_full_match(s, 'https://openalex\.org/' || p || '[0-9]+')
    then cast(regexp_extract(s, '([0-9]+)$', 1) as bigint)
  else error('bad ' || label || ': ' || s) end;
create or replace macro orcid_clean(s) as case when s is null then null
  when not starts_with(s, 'https://orcid.org/') then error('bad ORCID prefix: ' || s)
  when regexp_full_match(upper(substr(s, 19)), '{ORCID}') then upper(substr(s, 19)) else null end;
create or replace macro orcid_any(s) as case
  when regexp_full_match(upper(s), '(HTTPS?://ORCID\.ORG/)?({ORCID})') then regexp_extract(upper(s), '({ORCID})$', 1)
  else null end;
create or replace macro doi_clean(s) as case when s is null or s = '' then null
  when regexp_full_match(s, '(https://doi\.org/|http://dx\.doi\.org/)?10\.[0-9]{{4,9}}/\S+')
    then regexp_replace(s, '^(https://doi\.org/|http://dx\.doi\.org/)', '')
  else error('bad DOI: ' || s) end;
create or replace macro at_most_one(l, label) as case when len(l) > 1 then error(label || ' has more than one element')
  else l[1] end;
"""


def authors_sql(max_year: int) -> dict[str, str]:
    """One SELECT per output table over the raw view `a`, columns in spec order."""
    aid = "oa_id(id, 'A', 'author id')"
    years = f'between {YEAR_MIN} and {max_year}'
    return {
        'authors': f"""select {aid} author_id, display_name author_name, full_name, orcid_clean(orcid) orcid,
            works_count::uinteger num_works, cited_by_count::uinteger num_citations,
            summary_stats."2yr_mean_citedness"::float "2yr_mean_citedness", summary_stats.h_index::usmallint h_index,
            summary_stats.i10_index::usmallint i10_index, updated_date::date updated_date,
            created_date::date created_date
            from a order by all""",
        'authors_display_name_alternatives': f"""select distinct {aid} author_id, unnest(display_name_alternatives)
            display_name_alternative from a order by all""",
        'authors_observed_orcids': f"""select distinct author_id, orcid_clean(o) orcid from
            (select {aid} author_id, unnest(observed_orcids) o from a) where orcid_clean(o) is not null order by all""",
        'authors_affiliations': f"""select distinct author_id, institution_id, year::usmallint as "year" from
            (select author_id, oa_id(f.institution.id, 'I', 'affiliation institution id') institution_id,
                    unnest(f.years) as "year" from (select {aid} author_id, unnest(affiliations) f from a))
            where year {years} order by all""",
        'authors_last_known_institutions': f"""select distinct author_id,
            oa_id(k.id, 'I', 'last known institution id') institution_id
            from (select {aid} author_id, unnest(last_known_institutions) k from a) order by all""",
        'authors_topics': f"""select distinct author_id, oa_id(t.id, 'T', 'topic id')::usmallint topic_id,
            t.count::uinteger num_works from (select {aid} author_id, unnest(topics) t from a) order by all""",
        'authors_topic_share': f"""select distinct author_id, oa_id(t.id, 'T', 'topic id')::usmallint topic_id,
            t.value::float as "value" from (select {aid} author_id, unnest(topic_share) t from a) order by all""",
        'authors_concepts': f"""select distinct author_id, oa_id(c.id, 'C', 'concept id') concept_id,
            c.score::float score,
            c.count::uinteger num_works from (select {aid} author_id, unnest(x_concepts) c from a) order by all""",
        'authors_sources': f"""select distinct author_id, oa_id(s.id, 'S', 'source id') source_id
            from (select {aid} author_id, unnest(sources) s from a) order by all""",
        'authors_counts_by_year': f"""select distinct author_id, c.year::usmallint as "year",
            c.works_count::uinteger num_works,
            c.oa_works_count::uinteger num_oa_works, c.cited_by_count::uinteger num_citations
            from (select {aid} author_id, unnest(counts_by_year) c from a) where c.year {years} order by all""",
    }


AUTHOR_LISTS = {  # output table -> raw list column, to log how many elements each table dropped
    'authors_display_name_alternatives': 'display_name_alternatives',
    'authors_observed_orcids': 'observed_orcids',
    'authors_last_known_institutions': 'last_known_institutions',
    'authors_topics': 'topics',
    'authors_topic_share': 'topic_share',
    'authors_concepts': 'x_concepts',
    'authors_sources': 'sources',
    'authors_counts_by_year': 'counts_by_year',
}


def duckdb_connection(raw_glob: str) -> Any:
    """Capped DuckDB with the macros and the raw table as view `a`."""
    import duckdb

    con = duckdb.connect(config=DUCKDB_CONFIG)
    con.execute("set TimeZone='UTC'")  # timestamptz -> date must use UTC days, like the pyarrow tables
    con.execute(DUCKDB_MACROS)
    con.execute(f'create or replace temp view a as select * from read_parquet({raw_glob}, hive_partitioning=false)')
    return con


def log_author_cleanup(con: Any) -> dict[str, int]:
    """Log raw element counts (for comparing with rows written) and every ORCID the cleanup changes."""
    sums = ', '.join(f'sum(len({c})) "{c}"' for c in AUTHOR_LISTS.values())
    row = con.execute(
        f'select count(*) n, {sums}, (select sum(len(f.years)) from '
        f'(select unnest(affiliations) f from a)) affiliation_years from a'
    ).fetchone()
    names = ['authors', *AUTHOR_LISTS.values(), 'affiliation_years']
    raw_counts = dict(zip(names, row, strict=True))
    log.info('authors raw element counts: %s', {k: f'{v:,}' for k, v in raw_counts.items()})
    for col, sql in (
        ('orcid', 'select orcid o from a'),
        ('observed_orcids', 'select unnest(observed_orcids) o from a'),
    ):
        changed = con.execute(
            f'select o, orcid_clean(o) from ({sql}) where o is not null and '
            f'orcid_clean(o) is distinct from substr(o, 19)'
        ).fetchall()
        nulled = sorted(o for o, c in changed if c is None)
        log.info(
            'authors %s: %s values uppercased, %s invalid stored as null: %s',
            col,
            sum(c is not None for _, c in changed),
            len(nulled),
            nulled,
        )
    return raw_counts


def write_authors(out_root: Path, max_year: int, snapshot: str, raw_root: Path = RAW_ROOT) -> dict[str, dict[str, Any]]:
    con = duckdb_connection(f"'{raw_root}/authors/*/*.parquet'")
    raw_counts = log_author_cleanup(con)
    manifests = {}
    for name, sql in authors_sql(max_year).items():
        manifests[name] = _write_sql(con, name, sql, out_root, snapshot, 'authors')
        raw = raw_counts.get(
            AUTHOR_LISTS.get(name, '') or ('affiliation_years' if name == 'authors_affiliations' else 'authors')
        )
        log.info(
            '%s: %s raw elements, %s rows written, %s removed (bad year, duplicate, or invalid ORCID)',
            name,
            f'{raw:,}',
            f'{manifests[name]["rows"]:,}',
            f'{raw - manifests[name]["rows"]:,}',
        )
    return manifests


def _write_sql(con: Any, name: str, sql: str, out_root: Path, snapshot: str, raw_name: str) -> dict[str, Any]:
    """Stream one SELECT into a table dir, then check it with all three readers."""
    target = out_root / name
    reader = con.sql(sql).to_arrow_reader(1_000_000)  # streams; no extra copy
    manifest = o.write_batches_dir(reader, SCHEMAS[name], target, source={'snapshot': snapshot, 'raw': [raw_name]})
    o.read_check(target, SCHEMAS[name])
    log.info('checked %s with pyarrow, pandas, and duckdb', name)
    return manifest


# Investigators: lead, co-lead, then list entries that are not the award's lead or co-lead. Structs are compared with
# `is not distinct from` (a struct `=` is NULL when a nested field is NULL). DISTINCT on the whole struct removes
# exact repeats in the list before any field is cleaned, so cleaning can't merge rows.
AWARD_PEOPLE = """select distinct p.id, p.role, p.i from (
    select id, 'lead' as role, lead_investigator as i from a where lead_investigator is not null
    union all select id, 'co_lead', co_lead_investigator from a where co_lead_investigator is not null
    union all select a.id, 'investigator', x.i from a, unnest(a.investigators) x(i)) p
  join a on a.id = p.id
  where p.role <> 'investigator' or not (p.i is not distinct from a.lead_investigator
    or p.i is not distinct from a.co_lead_investigator)"""


def awards_sql() -> dict[str, str]:
    """One SELECT per output table over the raw view `a`, columns in spec order (spec: schema-oct-2026.md)."""
    gid = "oa_id(id, 'G', 'award id')"
    parent = "at_most_one(parent_awards, 'parent_awards')"
    aff_id = "at_most_one(i.affiliation.ids, 'affiliation.ids')"
    return {
        'awards': f"""select {gid} award_id, nullif(display_name, '') award_name, nullif(description, '') description,
            nullif(funder_award_id, '') funder_award_id, oa_id(funder.id, 'F', 'funder id') funder_id,
            case when funder.id is null then nullif(funder.display_name, '') end unlinked_funder_name,
            amount, nullif(currency, '') currency, nullif(funding_type, '') funding_type,
            nullif(funder_scheme, '') funder_scheme, provenance, start_date, end_date,
            start_year::usmallint start_year, end_year::usmallint end_year,
            oa_id(at_most_one(topics, 'topics').id, 'T', 'topic id')::usmallint primary_topic_id,
            primary_topic.score::float primary_topic_score,
            oa_id({parent}.id, 'G', 'parent award id') parent_award_id, {parent}.relationship parent_relationship,
            {parent}.asserted_by parent_asserted_by, nullif(landing_page_url, '') landing_page_url, doi_clean(doi) doi,
            funded_outputs_count::uinteger num_funded_outputs, updated_date::date updated_date,
            created_date::date created_date
            from a order by all""",
        'awards_investigators': f"""select {gid} award_id, role, nullif(i.given_name, '') given_name,
            nullif(i.family_name, '') family_name, orcid_any(i.orcid) orcid, i.role_start role_start,
            nullif(i.affiliation.name, '') affiliation_name, nullif(i.affiliation.country, '') affiliation_country,
            lower({aff_id}.type) affiliation_id_type, nullif({aff_id}.id, '') affiliation_id,
            {aff_id}.asserted_by affiliation_id_asserted_by
            from ({AWARD_PEOPLE}) order by all""",
        'awards_institutions': f"""select distinct {gid} award_id, oa_id(x.id, 'I', 'institution id') institution_id
            from a, unnest(institution_awarded) t(x) order by all""",
    }


def write_awards(out_root: Path, max_year: int, snapshot: str, raw_root: Path = RAW_ROOT) -> dict[str, dict[str, Any]]:
    del max_year  # award dates and years are kept as the source reported them (spec Decision 4)
    con = duckdb_connection(f"'{raw_root}/awards/*/*.parquet'")
    row = con.execute(
        f"""select (select count(*) from a), (select count(*) from ({AWARD_PEOPLE})),
        (select count(*) from ({AWARD_PEOPLE}) where nullif(i.orcid, '') is not null and orcid_any(i.orcid) is null),
        (select count(*) from a where primary_topic is distinct from topics[1])"""
    ).fetchall()  # fetchall, not fetchone: an unfinished result blocks the connection
    n, people, bad_orcid, topic_ne = row[0]
    log.info('awards: %s raw rows, %s investigator rows after merging lead and co-lead', f'{n:,}', f'{people:,}')
    log.info('awards_investigators.orcid: %s invalid values stored as null', f'{bad_orcid:,}')
    if topic_ne:
        raise SystemExit(f'awards: primary_topic differs from topics[1] in {topic_ne:,} rows; re-check the spec.')
    return {name: _write_sql(con, name, sql, out_root, snapshot, 'awards') for name, sql in awards_sql().items()}


WORKS_MACROS = r"""
create or replace macro blank(s) as case when trim(s) = '' then null else s end;
create or replace macro doi_strip(s) as nullif(regexp_replace(s, '^https?://(dx\.)?doi\.org/', ''), '');
create or replace macro slug(s, p, label) as case when s is null then null
  when starts_with(s, p) and length(s) > length(p) then substr(s, length(p) + 1)
  else error('bad ' || label || ': ' || s) end;
create or replace macro pmid_clean(s) as case when s is null then null
  when regexp_full_match(s, 'https://pubmed\.ncbi\.nlm\.nih\.gov/[0-9]+')
    then cast(regexp_extract(s, '([0-9]+)$', 1) as bigint)
  else error('bad PMID: ' || s) end;
create or replace macro cc_left(e) as list_filter(list_distinct(e.countries), lambda c: not list_contains(
  list_transform(e.institutions, lambda i: i.country_code), c));
create or replace macro cc_fallback(e) as case when len(cc_left(e)) = 1
  and len(list_filter(e.institutions, lambda i: i.country_code is null)) + (len(e.institutions) = 0)::int = 1
  then cc_left(e)[1] end;
create or replace macro sdg_id(s) as case when regexp_full_match(s, 'https://metadata\.un\.org/sdg/[0-9]+')
  then cast(regexp_extract(s, '([0-9]+)$', 1) as utinyint) else error('bad SDG id: ' || s) end;
"""
PLACEHOLDER_WORK = o.DELETED_IDS['work']
# A copy of an author is dropped when an earlier copy in the same work has the same author ID and institution set
# (user, 2026-10-30). Same keys as audit_works7 (`ka`), so the row count can be checked against that log.
AUTHOR_KEYS = """list_transform(authorships, lambda e: {'a': e.author.id,
    'i': list_sort(list_distinct(list_transform(e.institutions, lambda i: i.id)))})"""


def _mesh_key(x: str) -> str:
    return f"{{'d': {x}.descriptor_ui, 'q': coalesce({x}.qualifier_ui, '')}}"


def works_sql() -> dict[str, str]:
    """One SELECT per works table over the non-xpac view `w` (spec: schema-oct-2026.md, Works)."""
    wid = "oa_id(id, 'W', 'work id') work_id, publication_year::usmallint publication_year"

    def child(cols: str, src: str, where: str = '') -> str:
        return f'select {wid}, {cols} from (select id, publication_year, {src} from w) {where} order by all'

    return {
        'works': """select oa_id(w.id, 'W', 'work id') work_id, doi_strip(doi) doi, blank(title) title,
            publication_year::usmallint publication_year, publication_date, type, blank(language) language,
            n_auth::usmallint num_authors,
            locations_count::usmallint num_locations, coalesce(nr.n, 0)::usmallint num_references,
            cited_by_count::uinteger num_citations, n_cc::utinyint num_countries,
            n_inst::usmallint num_institutions, fwci::float fwci,
            citation_normalized_percentile.value::float citation_percentile,
            citation_normalized_percentile.is_in_top_1_percent is_in_top_1_percent,
            citation_normalized_percentile.is_in_top_10_percent is_in_top_10_percent,
            cited_by_percentile_year.min::utinyint cited_by_percentile_year_min,
            cited_by_percentile_year.max::utinyint cited_by_percentile_year_max,
            apc_list.value::uinteger apc_list_value, blank(apc_list.currency) apc_list_currency,
            apc_list.value_usd::usmallint apc_list_usd, apc_paid.value::uinteger apc_paid_value,
            blank(apc_paid.currency) apc_paid_currency, apc_paid.value_usd::uinteger apc_paid_usd,
            vy.vid volume_work_id, blank(volume.display_name) volume_name,
            n_auth > 0 and n_noinst = 0 has_complete_institution_info, len(awards) > 0 has_grant_info,
            len(keywords) > 0 has_keywords, has_fulltext, has_content.pdf has_pdf,
            has_content.grobid_xml has_grobid_xml, ps.is_oa primary_source_is_oa,
            is_retracted, is_paratext, updated_date::date updated_date, created_date::date created_date
            from (select w.*, len(list_distinct(list_transform(ka, lambda s: s.author.id))) n_auth,
                len(list_filter(ka, lambda s: len(s.institutions) = 0)) n_noinst,
                len(list_distinct(flatten(list_transform(ka,
                  lambda s: list_transform(s.institutions, lambda i: i.id))))) n_inst,
                len(list_distinct(list_filter(flatten(list_transform(ka, lambda s: list_append(
                  list_transform(s.institutions, lambda i: i.country_code), cc_fallback(s)))),
                  lambda c: c is not null))) n_cc
              from (select w.*, list_filter(authorships, lambda s: s.author.id is not null
                      and not list_contains(coalesce(o.orph_aids, []), s.author.id)) ka
                    from w left join orph o using (id)) w) w
            left join src_oa ps on ps.source_id = oa_id(w.locations[1].source.id, 'S', 'source id')
            left join (select work_id rid, n from nref) nr on nr.rid = oa_id(w.id, 'W', 'work id')
            left join (select work_id vid from yrs) vy on vy.vid = oa_id(w.volume.id, 'W', 'volume id') order by all""",
        'works_ids': child(
            "cast(ids['mag'] as uinteger) mag, pmid_clean(ids['pmid'])::uinteger pmid, "
            "ids['openalex_curation'] openalex_curation",
            'ids',
        ),
        'works_open_access': child(
            'open_access.is_oa is_oa, open_access.oa_status oa_status, blank(open_access.oa_url) oa_url, '
            'open_access.any_repository_has_fulltext any_repository_has_fulltext',
            'open_access',
        ),
        'works_biblio': child(
            'blank(biblio.volume) volume, blank(biblio.issue) issue, blank(biblio.first_page) first_page, '
            'blank(biblio.last_page) last_page',
            'biblio',
        ),
        'works_authorships': f"""select work_id, publication_year, author_order, author_position, author_id,
            raw_author_name, raw_orcid, is_corresponding, oa_id(inst.id, 'I', 'institution id') institution_id,
            coalesce(inst.country_code, fb) country_code,
            raw_affiliation_string from (
              select {wid}, j::usmallint author_order, e.author_position author_position,
                oa_id(e.author.id, 'A', 'author id') author_id, blank(e.raw_author_name) raw_author_name,
                orcid_any(e.raw_orcid) raw_orcid, e.is_corresponding is_corresponding,
                unnest(case when len(e.institutions) = 0 then [null] else e.institutions end) inst,
                cc_fallback(e) fb,
                nullif(array_to_string(list_filter(e.raw_affiliation_strings, lambda r: trim(r) <> ''), ';'), '')
                  raw_affiliation_string
              from (select id, publication_year, unnest(authorships) e, unnest(generate_series(1, len(authorships))) j,
                      unnest(list_transform(k, lambda kk, i: list_position(k, kk) = i)) keep
                    from (select id, publication_year, authorships, {AUTHOR_KEYS} k from w))
              where keep and e.author.id is not null) x
            semi join known on x.author_id = known.author_id order by all""",
        'works_corresponding_institutions': f"""select * from ({
            child(
                "oa_id(c, 'I', 'corresponding institution id') institution_id",
                'unnest(corresponding_institution_ids) c',
            )
        }) x semi join known_inst using (institution_id) order by all""",
        'works_locations': f"""select work_id, publication_year, location_order, is_primary, is_best_oa, native_id,
            x.source_id, so.is_oa source_is_oa, doi, landing_page_url, pdf_url, version, license, x.is_oa, is_accepted,
            is_published, raw_source_name, raw_type, provenance from ({
            child(
                'j::usmallint location_order, j = 1 is_primary, coalesce(bpos = j, false) is_best_oa, l.id native_id, '
                "oa_id(l.source.id, 'S', 'source id') source_id, doi_strip(l.doi) doi, "
                'blank(l.landing_page_url) landing_page_url, blank(l.pdf_url) pdf_url, l.version version, '
                'l.license license, l.is_oa is_oa, l.is_accepted is_accepted, l.is_published is_published, '
                'blank(l.raw_source_name) raw_source_name, blank(l.raw_type) raw_type, l.provenance provenance',
                'unnest(locations) l, unnest(generate_series(1, len(locations))) j, '
                'list_position(locations, best_oa_location) bpos',
            ).removesuffix(' order by all')
        }) x left join src_oa so using (source_id) order by all""",
        'works_topics': child(
            "j = 1 is_primary_topic, t.score::float score, oa_id(t.id, 'T', 'topic id')::usmallint topic_id, "
            "t.display_name topic_name, oa_id(t.subfield.id, 'subfields/', 'subfield id')::usmallint subfield_id, "
            "t.subfield.display_name subfield_name, oa_id(t.field.id, 'fields/', 'field id')::utinyint field_id, "
            "t.field.display_name field_name, oa_id(t.domain.id, 'domains/', 'domain id')::utinyint domain_id, "
            't.domain.display_name domain_name',
            'unnest(topics) t, unnest(generate_series(1, len(topics))) j',
        ),
        'works_keywords': child(
            "slug(k.id, 'https://openalex.org/keywords/', 'keyword id') keyword_id, k.score::float score",
            'unnest(keywords) k',
        ),
        'works_concepts': child(
            "oa_id(c.id, 'C', 'concept id') concept_id, c.score::float score",
            'unnest(concepts) c, unnest(list_transform(concepts, lambda x, i: '
            'list_position(list_transform(concepts, lambda y: y.id), x.id) = i)) keep',
            'where keep',
        ),
        'works_mesh': child(
            'e.descriptor_ui descriptor_ui, e.descriptor_name descriptor_name, e.qualifier_ui qualifier_ui, '
            'e.qualifier_name qualifier_name, '
            f'list_bool_or(list_transform(list_filter(mesh, lambda f: {_mesh_key("f")} = {_mesh_key("e")}), '
            'lambda f: f.is_major_topic)) is_major_topic',
            f'mesh, unnest(mesh) e, unnest(list_transform(mesh, lambda x, i: list_position('
            f'list_transform(mesh, lambda y: {_mesh_key("y")}), {_mesh_key("x")}) = i)) keep',
            'where keep',
        ),
        'works_referenced_works': 'select * from refs order by all',
        'works_indexed_in': child('x indexed_source', 'unnest(indexed_in) x'),
        'works_counts_by_year': child(
            'c.year::usmallint as "year", c.cited_by_count::uinteger num_citations', 'unnest(counts_by_year) c'
        ),
        'works_awards': child("oa_id(x.id, 'G', 'award id') award_id", 'unnest(awards) x'),
        'works_funders': child("oa_id(x.id, 'F', 'funder id') funder_id", 'unnest(funders) x'),
        'works_sustainable_development_goals': child(
            'sdg_id(x.id) sdg_id, x.score::float score', 'unnest(sustainable_development_goals) x'
        ),
        'works_study_designs': child(
            "slug(x.id, 'https://openalex.org/study-designs/', 'study design id') study_design",
            'unnest(study_designs) x',
        ),
    }


def write_works(
    out_root: Path, max_year: int, snapshot: str, raw_root: Path = RAW_ROOT, ref_root: Path | None = None
) -> dict[str, dict[str, Any]]:
    """Works tables via capped DuckDB. `ref_root` holds the built `authors` table (orphan authors are dropped)."""
    del max_year  # years outside 1500 to max_year are kept as reported (spec Decision 15)
    ref_root = ref_root or out_root
    con = duckdb_connection(f"'{raw_root}/works/*/*.parquet'")
    con.execute(WORKS_MACROS)
    con.execute(
        f"create or replace temp table known as select author_id from read_parquet('{ref_root}/authors/*.parquet')"
    )
    con.execute(
        'create or replace temp table src_oa as select source_id, is_oa '
        f"from read_parquet('{ref_root}/sources/*.parquet')"
    )
    # Institution IDs not in the built institutions table (stale IDs; user: null them) are removed from each
    # authorship before anything else reads it, so dedupe keys and per-work flags see the cleaned lists.
    orphan_insts = con.execute(f"""select coalesce(list(distinct x.i order by x.i), []) from
        (select unnest(s.institutions).id i from (select unnest(authorships) s from a where not is_xpac)) x
        anti join read_parquet('{ref_root}/institutions/*.parquet') b
        on oa_id(x.i, 'I', 'institution id') = b.institution_id""").fetchall()[0][0]
    log.info('works: %s orphan institution IDs removed from authorships: %s', len(orphan_insts), orphan_insts)
    con.execute('set variable orphan_insts = $1', [orphan_insts])
    con.execute("""create or replace temp view w as select * replace (list_transform(authorships, lambda s:
        struct_update(s, institutions := list_filter(s.institutions,
            lambda i: not list_contains(getvariable('orphan_insts'), i.id)))) as authorships)
        from a where not is_xpac""")
    con.execute(
        "create or replace temp table yrs as select oa_id(id, 'W', 'work id') work_id, "
        'publication_year::usmallint publication_year from w'
    )
    con.execute(
        'create or replace temp table known_inst as select institution_id '
        f"from read_parquet('{ref_root}/institutions/*.parquet')"
    )
    # Per work: the orphan author IDs (not in the built authors table). Their rows are dropped from
    # works_authorships, so works' author, institution, and country counts skip them too.
    con.execute("""create or replace temp table orph as
        select id, list(distinct aid order by aid) orph_aids
        from (select id, s.author.id aid from (select id, unnest(authorships) s from w) where s.author.id is not null) x
        anti join known k on oa_id(x.aid, 'A', 'author id') = k.author_id group by id""")
    # References kept: targets in the build, minus self-references and the deleted-work placeholder.
    con.execute(f"""create or replace temp view refs as
        select r.work_id, r.work_publication_year, r.referenced_work_id,
          y.publication_year referenced_work_publication_year
        from (select oa_id(id, 'W', 'work id') work_id, publication_year::usmallint work_publication_year,
                oa_id(rr, 'W', 'referenced work id') referenced_work_id
              from (select id, publication_year, unnest(referenced_works) rr from w)) r
        join yrs y on y.work_id = r.referenced_work_id
        where r.referenced_work_id <> {PLACEHOLDER_WORK} and r.referenced_work_id <> r.work_id""")
    con.execute('create or replace temp table nref as select work_id, count(*) n from refs group by work_id')
    row = con.execute(
        f"""select (select count(*) from a where is_xpac), (select count(*) from w),
        (select count(*) from w where primary_topic is distinct from topics[1]),
        (select count(*) from w where apc_paid.value <> floor(apc_paid.value)
            or apc_paid.value_usd <> floor(apc_paid.value_usd)),
        (select sum(len(orph_aids)) from orph),
        (select count(*) from (select unnest(authorships) s from w) where s.author.id is null),
        (select count(*) from (select id, unnest(referenced_works) r from w) where r = id),
        (select count(*) from (select unnest(referenced_works) r from w) where r = 'https://openalex.org/W{PLACEHOLDER_WORK}')"""
    ).fetchall()  # fetchall, not fetchone: an unfinished result blocks the connection
    xpac, n, topic_ne, apc_frac, orphans, null_authors, self_refs, placeholders = row[0]
    log.info('works: %s non-xpac works, %s xpac works skipped', f'{n:,}', f'{xpac:,}')
    log.info(
        'works dropped: %s authorships with no author ID, %s orphan author IDs (summed per work), '
        '%s self-references, %s placeholder references',
        *(f'{v or 0:,}' for v in (null_authors, orphans, self_refs, placeholders)),
    )
    corr_ne = con.execute("""select id from w where list_sort(list_distinct(corresponding_author_ids)) is distinct from
        list_sort(list_distinct(list_transform(list_filter(authorships, lambda s: s.is_corresponding
          and s.author.id is not null), lambda s: s.author.id)))""").fetchall()
    log.info(
        'works: corresponding_author_ids dropped; differs from is_corresponding in %s works: %s',
        len(corr_ne),
        [r[0] for r in corr_ne[:20]],
    )
    dangling = con.execute("""select (select count(*) from
          (select oa_id(volume.id, 'W', 'volume id') v from w where volume.id is not null) x
          anti join yrs y on x.v = y.work_id),
        (select count(*) from (select unnest(corresponding_institution_ids) c from w) x
          anti join known_inst k on oa_id(x.c, 'I', 'institution id') = k.institution_id)""").fetchall()[0]
    log.info(
        'works: %s volume IDs outside the build nulled, %s dangling corresponding institutions dropped',
        *(f'{v:,}' for v in dangling),
    )
    if topic_ne or apc_frac:
        raise SystemExit(
            f'works: primary_topic differs from topics[1] in {topic_ne:,} rows and {apc_frac:,} apc_paid values are '
            'fractional; re-check the spec.'
        )
    return {name: _write_sql(con, name, sql, out_root, snapshot, 'works') for name, sql in works_sql().items()}


ENTITIES: dict[str, tuple[list[str], Callable[[dict[str, pa.Table], int], dict[str, pa.Table]]]] = {
    'institutions': (['institutions'], lambda r, y: build_institutions(r['institutions'], y)),
    'sources': (['sources'], lambda r, y: build_sources(r['sources'], y)),
    'topics': (['topics', 'subfields', 'fields', 'domains'], lambda r, y: build_topics(r)),
    'concepts': (['concepts'], lambda r, y: build_concepts(r['concepts'])),
    'funders': (['funders'], lambda r, y: build_funders(r['funders'], y)),
}
STREAMED: dict[str, Callable[..., dict[str, dict[str, Any]]]] = {
    'authors': write_authors,
    'awards': write_awards,
    'works': lambda out_root, max_year, snapshot: write_works(
        out_root, max_year, snapshot, ref_root=OUT_ROOT / out_root.name
    ),
}


def _type_name(t: str) -> str:
    if t.startswith('dictionary<values=string'):
        return 'string (dictionary-encoded)'
    return {'float': 'float32', 'double': 'float64', 'date32[day]': 'date32'}.get(t, t)


def write_readme(out_root: Path) -> None:
    """Render `<out_root>/README.md` from every table's `_manifest.json` (atomic replace)."""
    manifests = [json.loads(p.read_text()) for p in sorted(out_root.glob('*/_manifest.json'))]
    if not manifests:
        raise SystemExit(f'No tables under {out_root}; build them before the README.')
    missing = [m['table'] for m in manifests if m['table'] not in DESCRIPTIONS]
    if missing:
        raise SystemExit(f'No description for {missing}; add them to DESCRIPTIONS in {__file__}.')
    snapshot = manifests[0]['source']['snapshot']
    lines = [
        f'# OpenAlex optimized Parquet: {out_root.name}',
        '',
        f'Built from the OpenAlex Parquet snapshot dated {snapshot} by `preprocessing/optimize_parquet.py`. '
        'This file is generated; do not edit it by hand.',
        '',
        '- One directory per table, holding `part-NNNNN.parquet` files (brotli) plus `_metadata`, '
        '`_common_metadata`, and `_manifest.json`.',
        "- Read a table with `pd.read_parquet(dir)`, `pq.read_table(dir)`, or DuckDB `read_parquet('dir/*.parquet')`.",
        '- OpenAlex IDs keep only their number (`https://openalex.org/I123` is `123`). External IDs drop their URL '
        'prefix: ROR, ORCID, and GRID are bare, Wikidata is the Q-number, and Wikipedia is `lang:Title`.',
        '- Dates are dates only. Columns typed `string (dictionary-encoded)` read as `category` in pandas.',
        '- Nulls are real nulls in every type (null means unknown; `false` and `0` are values). Plain pandas widens '
        'integer and bool columns that hold nulls to float and object; read with '
        "`pd.read_parquet(dir, dtype_backend='pyarrow')` to keep exact types and nulls.",
        '',
        '| Table | Rows | Files | Size |',
        '|---|---:|---:|---:|',
    ]
    lines += [
        f'| [`{m["table"]}`](#{m["table"]}) | {m["rows"]:,} | {len(m["files"])} | {m["bytes"] / 1e6:,.1f} MB |'
        for m in manifests
    ]
    for m in manifests:
        lines += [
            '',
            f'## {m["table"]}',
            '',
            DESCRIPTIONS[m['table']],
            '',
            f'{m["rows"]:,} rows.',
            '',
            '| Column | Type |',
            '|---|---|',
        ]
        lines += [f'| `{c["name"]}` | {_type_name(c["type"])} |' for c in m['schema']]
    partial = out_root / f'README.md.partial-{os.getpid()}'
    partial.write_text('\n'.join(lines) + '\n')
    partial.replace(out_root / 'README.md')
    log.info('wrote %s', out_root / 'README.md')


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--month', default='oct-2026', help='output dir name, e.g. oct-2026')
    parser.add_argument('--entities', nargs='+', choices=[*ENTITIES, *STREAMED], default=[*ENTITIES, *STREAMED])
    parser.add_argument('--dry-run', metavar='SCRATCH', type=Path, help='write under SCRATCH instead of the real root')
    parser.add_argument('--readme-only', action='store_true', help='only regenerate README.md from the manifests')
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s', stream=sys.stdout)
    exit_on_sigterm()
    log.info('START optimize_parquet %s', vars(args))

    if args.readme_only:
        write_readme((args.dry_run if args.dry_run else OUT_ROOT) / args.month)
        log.info('END optimize_parquet ok: README only')
        return

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
        if entity in STREAMED:
            manifests |= STREAMED[entity](out_root, max_year, snapshot)
            continue
        raw_names, build = ENTITIES[entity]
        raws = {n: o.load_raw(RAW_ROOT, n) for n in raw_names}
        for name, table in build(raws, max_year).items():
            target = out_root / name
            manifests[name] = o.write_table_dir(table, target, source={'snapshot': snapshot, 'raw': raw_names})
            o.read_check(target, SCHEMAS[name])
            log.info('checked %s with pyarrow, pandas, and duckdb', name)
    o.update_top_manifest(out_root, manifests)
    write_readme(out_root)
    total = sum(m['rows'] for m in manifests.values())
    log.info('END optimize_parquet ok: %s tables, %s rows', len(manifests), f'{total:,}')


if __name__ == '__main__':
    main()
