"""
Flatten Openalex JSON line files into individual CSVs.
Original script at https://github.com/ourresearch/openalex-documentation-scripts/blob/main/flatten-openalex-jsonl.py
"""

import csv
import glob
import gzip
import json
import os
import socket
import sys
import warnings
from datetime import datetime
from pathlib import Path
from time import time
from typing import Union, Optional

warnings.simplefilter(action='ignore', category=FutureWarning)  # suppress pandas future warnings

import orjson  # faster JSON library
import pandas as pd
from tqdm.auto import tqdm
import pyarrow as pa

sys.path.extend(['../', './'])
from src.utils import convert_openalex_id_to_int, load_pickle, dump_pickle, reconstruct_abstract, read_manifest, \
    parallel_async, string_to_bool, parse_authorships, convert_topic_id_to_int

hostname = socket.gethostname()
if 'quartz' in hostname:
    BASEDIR = Path('/N/project/openalex/ssikdar')  # directory where you have downloaded the OpenAlex snapshots
    # BASEDIR = Path('/N/scratch/ssikdar')  # directory where you have downloaded the OpenAlex snapshots
else:
    BASEDIR = Path('/home/ssikdar/data')

SNAPSHOT_DIR = BASEDIR / 'openalex-snapshot'
MONTH = 'dec-2024'

CSV_DIR = BASEDIR / 'processed-snapshots' / 'csv-files' / MONTH
PARQ_DIR = BASEDIR / 'processed-snapshots' / 'parquet-files' / MONTH
CSV_DIR.mkdir(parents=True, exist_ok=True)
PARQ_DIR.mkdir(parents=True, exist_ok=True)

FILES_PER_ENTITY = int(os.environ.get('OPENALEX_DEMO_FILES_PER_ENTITY', '0'))

csv_files = dict(
    institutions={
        'institutions': {
            'name': os.path.join(CSV_DIR, 'institutions.csv.gz'),
            'columns': [
                'institution_id', 'institution_name', 'ror', 'country_code', 'type', 'homepage_url',
                'display_name_acroynyms', 'display_name_alternatives', 'works_count', 'cited_by_count',
                'updated_date'
            ]
        },
        'ids': {
            'name': os.path.join(CSV_DIR, 'institutions_ids.csv.gz'),
            'columns': [
                'institution_id', 'institution_name', 'openalex', 'ror', 'grid', 'wikipedia', 'wikidata', 'mag'
            ]
        },
        'geo': {
            'name': os.path.join(CSV_DIR, 'institutions_geo.csv.gz'),
            'columns': [
                'institution_id', 'institution_name', 'city', 'geonames_city_id', 'region', 'country_code',
                'country',
                'latitude',
                'longitude'
            ]
        },
        'associated_institutions': {
            'name': os.path.join(CSV_DIR, 'institutions_associated_institutions.csv.gz'),
            'columns': [
                'institution_id', 'associated_institution_id', 'relationship'
            ]
        },
        'counts_by_year': {
            'name': os.path.join(CSV_DIR, 'institutions_counts_by_year.csv.gz'),
            'columns': [
                'institution_id', 'institution_name', 'year', 'works_count', 'oa_works_count', 'cited_by_count',
            ]
        }
    },
    authors={
        'authors': {
            'name': os.path.join(CSV_DIR, 'authors.csv.gz'),
            'columns': [
                'author_id', 'orcid', 'author_name', 'display_name_alternatives', 'works_count', 'cited_by_count',
                'last_known_institution', 'updated_date'
            ]
        },
        'ids': {
            'name': os.path.join(CSV_DIR, 'authors_ids.csv.gz'),
            'columns': [
                'author_id', 'author_name', 'openalex', 'orcid', 'scopus', 'twitter', 'wikipedia', 'mag'
            ]
        },
        'counts_by_year': {
            'name': os.path.join(CSV_DIR, 'authors_counts_by_year.csv.gz'),
            'columns': [
                'author_id', 'author_name', 'year', 'works_count', 'cited_by_count'
            ]
        },
        'concepts': {
            'name': os.path.join(CSV_DIR, 'authors_concepts.csv.gz'),
            'columns': [
                'author_id', 'author_name', 'works_count', 'cited_by_count', 'concept_id', 'concept_name', 'level',
                'score'
            ]
        },
        'hints': {
            'name': os.path.join(CSV_DIR, 'authors_hints.csv.gz'),
            'columns': [
                'author_id', 'author_name', 'works_count', 'cited_by_count', 'most_cited_work'
            ]
        }
    },
    concepts={
        'concepts': {
            'name': os.path.join(CSV_DIR, 'concepts.csv.gz'),
            'columns': [
                'concept_id', 'concept_name', 'wikidata', 'level', 'description', 'works_count',
                'cited_by_count', 'updated_date'
            ]
        },
        'ancestors': {
            'name': os.path.join(CSV_DIR, 'concepts_ancestors.csv.gz'),
            'columns': ['concept_id', 'ancestor_id']
        },
        'counts_by_year': {
            'name': os.path.join(CSV_DIR, 'concepts_counts_by_year.csv.gz'),
            'columns': ['concept_id', 'concept_name', 'year', 'works_count', 'cited_by_count', 'oa_works_count', ]
        },
        'ids': {
            'name': os.path.join(CSV_DIR, 'concepts_ids.csv.gz'),
            'columns': ['concept_id', 'concept_name', 'openalex', 'wikidata', 'wikipedia', 'umls_aui', 'umls_cui',
                        'mag']
        },
        'related_concepts': {
            'name': os.path.join(CSV_DIR, 'concepts_related_concepts.csv.gz'),
            'columns': ['concept_id', 'related_concept_id', 'score']
        }
    },
    venues={
        'venues': {
            'name': os.path.join(CSV_DIR, 'venues.csv.gz'),
            'columns': [
                'venue_id', 'issn_l', 'issn', 'venue_name', 'type', 'publisher', 'works_count', 'cited_by_count',
                'is_oa',
                'is_in_doaj', 'homepage_url', 'updated_date'
            ]
        },
        'ids': {
            'name': os.path.join(CSV_DIR, 'venues_ids.csv.gz'),
            'columns': ['venue_id', 'venue_name', 'openalex', 'issn_l', 'issn', 'mag']
        },
        'counts_by_year': {
            'name': os.path.join(CSV_DIR, 'venues_counts_by_year.csv.gz'),
            'columns': ['venue_id', 'venue_name', 'year', 'works_count', 'cited_by_count']
        },
    },
    works={
        'works': {
            'name': os.path.join(CSV_DIR, 'works.csv.gz'),
            'columns': [
                'work_id', 'doi', 'title', 'publication_year', 'publication_date', 'type', 'type_crossref',
                'cited_by_count', 'num_authors', 'num_locations', 'num_references',
                'language', 'has_complete_institution_info', 'has_grant_info', 'has_keywords', 'is_retracted',
                'is_paratext', 'created_date', 'gz_path',
            ]
        },
        'indexed_in': {
            'name': os.path.join(CSV_DIR, 'works_indexed_in.csv.gz'),
            'columns': [
                'work_id', 'publication_year', 'indexed_source',
            ],
        },
        'topics': {
            'name': os.path.join(CSV_DIR, 'works_topics.csv.gz'),
            'columns': [
                'work_id', 'publication_year', 'is_primary_topic', 'score', 'topic_id', 'topic_name',
                'subfield_id', 'subfield_name', 'field_id', 'field_name', 'domain_id', 'domain_name',
            ],
        },
        'keywords': {
            'name': os.path.join(CSV_DIR, 'works_keywords.csv.gz'),
            'columns': [
                'work_id', 'keyword', 'score',
            ],
        },
        'grants': {
            'name': os.path.join(CSV_DIR, 'works_grants.csv.gz'),
            'columns': [
                'work_id', 'funder_id', 'funder_name', 'award_id',
            ]
        },
        # Satyaki addition: put abstracts in a different CSV, save some space
        'abstracts': {
            'name': os.path.join(CSV_DIR, 'works_abstracts.csv.gz'),
            'columns': [
                'work_id', 'title', 'publication_year', 'abstract',
            ]
        },
        'primary_location': {
            'name': os.path.join(CSV_DIR, 'works_primary_location.csv.gz'),
            'columns': [
                'work_id', 'source_id', 'source_name', 'source_type', 'version', 'license', 'landing_page_url',
                'pdf_url',
                'is_oa', 'is_accepted', 'is_published'
            ]
        },
        'locations': {
            'name': os.path.join(CSV_DIR, 'works_locations.csv.gz'),
            'columns': [
                'work_id', 'source_id', 'source_name', 'source_type', 'version', 'license', 'landing_page_url',
                'pdf_url', 'is_oa', 'is_accepted', 'is_published'
            ]
        },
        'authorships': {
            'name': os.path.join(CSV_DIR, 'works_authorships.csv.gz'),
            'columns': [
                'work_id', 'author_position', 'author_id', 'author_name', 'raw_author_name',
                'institution_lineage_level', 'assigned_institution',
                'institution_id', 'institution_name', 'raw_affiliation_string',
                'country_code', 'publication_year', 'is_corresponding',
            ]
        },
        'biblio': {
            'name': os.path.join(CSV_DIR, 'works_biblio.csv.gz'),
            'columns': [
                'work_id', 'volume', 'issue', 'first_page', 'last_page'
            ]
        },
        'concepts': {
            'name': os.path.join(CSV_DIR, 'works_concepts.csv.gz'),
            'columns': [
                'work_id', 'publication_year', 'concept_id', 'concept_name', 'level', 'score'
            ]
        },
        'ids': {
            'name': os.path.join(CSV_DIR, 'works_ids.csv.gz'),
            'columns': [
                'work_id', 'openalex', 'doi', 'mag', 'pmid', 'pmcid'
            ]
        },
        'mesh': {
            'name': os.path.join(CSV_DIR, 'works_mesh.csv.gz'),
            'columns': [
                'work_id', 'descriptor_ui', 'descriptor_name', 'qualifier_ui', 'qualifier_name', 'is_major_topic'
            ]
        },
        'open_access': {
            'name': os.path.join(CSV_DIR, 'works_open_access.csv.gz'),
            'columns': [
                'work_id', 'is_oa', 'oa_status', 'oa_url', 'any_repository_has_fulltext',
            ]
        },
        'best_oa_location': {
            'name': os.path.join(CSV_DIR, 'works_best_oa_location.csv.gz'),
            'columns': [
                'work_id', 'pdf_url', 'is_oa', 'is_accepted', 'is_published', 'source_id', 'source_name',
                'source_type'
            ]
        },
        'referenced_works': {
            'name': os.path.join(CSV_DIR, 'works_referenced_works.csv.gz'),
            'columns': [
                'work_id', 'referenced_work_id'
            ]
        },
        'related_works': {
            'name': os.path.join(CSV_DIR, 'works_related_works.csv.gz'),
            'columns': [
                'work_id', 'related_work_id'
            ]
        },
    },
    publishers={
        'publishers': {
            'name': os.path.join(CSV_DIR, 'publishers.csv.gz'),
            'columns': [
                'publisher_id', 'publisher_name', 'alternate_titles', 'country_codes', 'hierarchy_level',
                'parent_publisher',
                'works_count', 'cited_by_count', 'sources_api_url', 'updated_date'
            ]
        },
        'counts_by_year': {
            'name': os.path.join(CSV_DIR, 'publishers_counts_by_year.csv.gz'),
            'columns': ['publisher_id', 'publisher_name', 'year', 'works_count', 'cited_by_count',
                        'oa_works_count'],
        },
        'ids': {
            'name': os.path.join(CSV_DIR, 'publishers_ids.csv.gz'),
            'columns': ['publisher_id', 'publisher_name', 'openalex', 'ror', 'wikidata']
        },
    },
    sources={
        'sources': {
            'name': os.path.join(CSV_DIR, 'sources.csv.gz'),
            'columns': [
                'source_id', 'source_name', 'issn_l', 'issn', 'publisher', 'works_count', 'cited_by_count', 'is_oa',
                'is_in_doaj', 'homepage_url', 'works_api_url', 'updated_date'
            ]
        },
        'ids': {
            'name': os.path.join(CSV_DIR, 'sources_ids.csv.gz'),
            'columns': ['source_id', 'source_name', 'openalex', 'issn_l', 'issn', 'mag', 'wikidata', 'fatcat']
        },
        'counts_by_year': {
            'name': os.path.join(CSV_DIR, 'sources_counts_by_year.csv.gz'),
            'columns': ['source_id', 'source_name', 'year', 'works_count', 'cited_by_count', 'oa_works_count']
        },
    },
    topics={
        'topics': {
            'name': os.path.join(CSV_DIR, 'topics.csv.gz'),
            'columns': [
                'topic_id', 'topic_name', 'description', 'num_works', 'cited_by_count',
                'subfield_id', 'subfield_name', 'field_id', 'field_name', 'domain_id', 'domain_name',
            ]
        },
        'keywords': {
            'name': os.path.join(CSV_DIR, 'topics_keywords.csv.gz'),
            'columns': [
                'topic_id', 'topic_name', 'keyword',
            ],
        },
        'siblings': {
            'name': os.path.join(CSV_DIR, 'topics_siblings.csv.gz'),
            'columns': [
                'topic_id', 'topic_name', 'sibling_topic_id', 'sibling_topic_name',
            ],
        },
    }
)

STRING_DTYPE = 'string[pyarrow]'  # use the more memory efficient PyArrow string datatype
# STRING_DTYPE = 'string[python]'

if STRING_DTYPE == 'string[pyarrow]':
    assert pd.__version__ >= "1.3.0", f'Pandas version >1.3 needed for String[pyarrow] dtype, have {pd.__version__!r}.'

if pd.__version__ < '2':
    raise NotImplementedError(f'Please use Pandas v2')

if pd.__version__ >= '2.1':
    print('Using PyArrow strings!')
    pd.options.future.infer_string = True

inst_info_d = (  # store the instittutions names and country codes in a dictionary
    pd.read_csv(CSV_DIR / 'institutions.csv.gz')
    .set_index('institution_id')
    [['institution_name', 'country_code']]
    .to_dict()
)

topic_info_d = (  # store the topic info in a dictionary
    pd.read_csv(CSV_DIR / 'topics.csv.gz')
    .set_index('topic_id')
    [['topic_name', 'subfield_id', 'subfield_name', 'field_id', 'field_name', 'domain_id', 'domain_name']]
    .convert_dtypes()
    .to_dict()
)


DTYPES = {
    'works': dict(
        work_id='int64', doi=STRING_DTYPE, title=STRING_DTYPE, publication_year='Int16',
        publication_date=STRING_DTYPE, type='category', type_crossref='category',
        cited_by_count='uint32', num_authors='uint16',
        language='category',
        has_complete_institution_info=bool, has_grant_info=bool, has_keywords=bool,
        num_locations='uint16', num_references='uint16',
        is_retracted=bool, is_paratext=bool,
        created_date=STRING_DTYPE,
        gz_path='category',
        # updated_date=STRING_DTYPE,
    ),
    'indexed_in': dict(
        work_id='int64', publication_year='Int16', indexed_source='category',
    ),
    'topics': dict(
        work_id='int64', publication_year='Int16', is_primary_topic=bool, score=float,
        topic_id='uint16', topic_name='category', subfield_id='uint16', subfield_name='category',
        field_id='uint16', field_name='category', domain_id='uint8', domain_name='category',
    ),
    'keywords': dict(
        work_id='int64', keyword=STRING_DTYPE, score=float,
    ),
    'authorships': dict(
        work_id='int64', author_position='category', author_id='Int64', author_name=STRING_DTYPE,
        raw_author_name=STRING_DTYPE,
        institution_lineage_level='Int8', assigned_institution=bool,
        institution_id='Int64',
        institution_name='category', country_code='category',
        raw_affiliation_string=STRING_DTYPE, publication_year='Int16', is_corresponding=bool,
    ),
    'grants': dict(
        work_id='int64', funder_id='Int64', funder_name=STRING_DTYPE, award_id=STRING_DTYPE,
    ),
    'open_access': dict(
        work_id='int64', is_oa=bool, oa_status='category', oa_url=STRING_DTYPE, any_repository_has_fulltext=bool,
    ),
    'best_oa_location': dict(
        work_id='int64', is_oa=bool, is_accepted=bool, is_published=bool, pdf_url=STRING_DTYPE,
        source_id='Int64', source_name=STRING_DTYPE, source_type='category',
    ),
    'primary_location': dict(
        work_id='int64', source_id='Int64', source_name=STRING_DTYPE, source_type='category', version=STRING_DTYPE,
        license=STRING_DTYPE, landing_page_url=STRING_DTYPE, pdf_url=STRING_DTYPE,
        is_oa=bool, is_accepted=bool, is_published=bool,
    ),
    'locations': dict(
        work_id='int64', source_id='Int64', source_name=STRING_DTYPE, source_type='category', version=STRING_DTYPE,
        license=STRING_DTYPE, landing_page_url=STRING_DTYPE, pdf_url=STRING_DTYPE,
        is_oa=bool, is_accepted=bool, is_published=bool,
    ),
    'referenced_works': dict(
        work_id='int64', referenced_work_id='int64'
    ),
    'related_works': dict(
        work_id='int64', related_work_id='int64'
    ),
    'concepts': dict(
        work_id='int64', publication_year='Int16', concept_id='int64', concept_name='category', level='uint8',
        score=float,
    ),
    'abstracts': dict(
        work_id='int64', publication_year='Int16', title=STRING_DTYPE, abstract=STRING_DTYPE,
    ),
    'ids': dict(
        work_id='int64', openalex=STRING_DTYPE, doi=STRING_DTYPE, mag='Int64', pmid=STRING_DTYPE, pmcid=STRING_DTYPE
    ),
    'biblio': dict(
        work_id='int64', volume=STRING_DTYPE, issue=STRING_DTYPE, first_page=STRING_DTYPE, last_page=STRING_DTYPE,
    ),
    'mesh': dict(
        work_id='int64', descriptor_ui=STRING_DTYPE, descriptor_name=STRING_DTYPE,
        qualifier_ui=STRING_DTYPE, qualifier_name=STRING_DTYPE, is_major_topic=STRING_DTYPE,
    )
}


def read_csvs(paths):
    """
    Return the concatenated df after reaching CSVs from paths
    """
    df = pd.concat([pd.read_csv(path, engine='pyarrow') for path in paths], ignore_index=True)
    return df


def str_to_int(s):
    return int(s[1:])


def merge_all_skip_ids(kind, overwrite):
    merged_entries_path = SNAPSHOT_DIR / 'data' / 'merged_ids' / kind
    merged_parq_path = merged_entries_path / f'{kind}_combined.parquet'

    if not overwrite and merged_parq_path.exists():
        print(f'Merged parquet for {kind} exists! Skipping....')
        return

    csvs = list(merged_entries_path.glob('*.csv.gz'))
    merged_dfs = []

    for csv_file in tqdm(csvs, desc=f'{kind}'):
        merged_df = pd.read_csv(
            csv_file, engine='c', usecols=['id', 'merge_into_id'],
            converters={'id': str_to_int, 'merge_into_id': str_to_int},
            memory_map=True,
        )
        merged_dfs.append(merged_df)

    combined_df = pd.concat(merged_dfs, ignore_index=True).drop_duplicates()
    print(f'Writing {len(combined_df):,} rows of merged skip ids for {kind!r} at {str(merged_entries_path)!r}')
    combined_df.to_parquet(merged_parq_path, engine='pyarrow')
    return


def get_skip_ids(kind):
    """
    Get the set of IDs that have been merged with other IDs to skip over them
    """
    DELETED_WORK_ID, DELETED_AUTHOR_ID, DELETED_INST_ID, DELETED_SOURCE_ID = 4285719527, 5317838346, 4389424196, 4317411217
    deleted_ids = {'works': DELETED_WORK_ID, 'authors': DELETED_AUTHOR_ID, 'institutions': DELETED_INST_ID,
                   'source': DELETED_SOURCE_ID}

    merged_entries_path = SNAPSHOT_DIR / 'data' / 'merged_ids' / kind
    if merged_entries_path.exists():
        # check for merged parquet
        merged_parq_path = merged_entries_path / f'{kind}_combined.parquet'
        if merged_parq_path.exists():
            merged_df = pd.read_parquet(merged_parq_path, columns=['id'])
            skip_ids = set(merged_df['id'])
        else:
            merged_df = read_csvs(merged_entries_path.glob('*.csv.gz'))
            skip_ids = set(
                merged_df
                .id
            )
            skip_ids = {convert_openalex_id_to_int(id_) for id_ in skip_ids}
    else:
        skip_ids = set()

    if kind in deleted_ids:  # add deleted ids for tables
        skip_ids.add(deleted_ids[kind])

    print(f'{kind!r} {len(skip_ids):,} merged {kind} IDs')
    return skip_ids


def flatten_publishers():
    with gzip.open(csv_files['publishers']['publishers']['name'], 'wt', encoding='utf-8') as publishers_csv, \
            gzip.open(csv_files['publishers']['counts_by_year']['name'], 'wt', encoding='utf-8') as counts_by_year_csv, \
            gzip.open(csv_files['publishers']['ids']['name'], 'wt', encoding='utf-8') as ids_csv:

        publishers_writer = csv.DictWriter(
            publishers_csv, fieldnames=csv_files['publishers']['publishers']['columns'], extrasaction='ignore'
        )
        publishers_writer.writeheader()

        counts_by_year_writer = csv.DictWriter(counts_by_year_csv,
                                               fieldnames=csv_files['publishers']['counts_by_year']['columns'])
        counts_by_year_writer.writeheader()

        ids_writer = csv.DictWriter(ids_csv, fieldnames=csv_files['publishers']['ids']['columns'])
        ids_writer.writeheader()

        seen_publisher_ids = set()

        files_done = 0
        for jsonl_file_name in glob.glob(os.path.join(SNAPSHOT_DIR, 'data', 'publishers', '*', '*.gz')):
            # print(jsonl_file_name)
            with gzip.open(jsonl_file_name, 'r') as concepts_jsonl:
                for publisher_json in concepts_jsonl:
                    if not publisher_json.strip():
                        continue

                    publisher = json.loads(publisher_json)

                    if not (publisher_id := publisher.get('id')) or publisher_id in seen_publisher_ids:
                        continue

                    publisher_id = convert_openalex_id_to_int(publisher_id)
                    publisher['publisher_id'] = publisher_id
                    publisher['publisher_name'] = publisher['display_name']
                    seen_publisher_ids.add(publisher_id)

                    # publishers
                    publisher['alternate_titles'] = json.dumps(publisher.get('alternate_titles'), ensure_ascii=False)
                    publisher['country_codes'] = json.dumps(publisher.get('country_codes'), ensure_ascii=False)
                    publishers_writer.writerow(publisher)

                    if publisher_ids := publisher.get('ids'):
                        publisher_ids['publisher_id'] = publisher_id
                        publisher_ids['publisher_name'] = publisher['display_name']
                        ids_writer.writerow(publisher_ids)

                    if counts_by_year := publisher.get('counts_by_year'):
                        for count_by_year in counts_by_year:
                            count_by_year['publisher_id'] = publisher_id
                            count_by_year['publisher_name'] = publisher['display_name']
                            counts_by_year_writer.writerow(count_by_year)

            files_done += 1
            if FILES_PER_ENTITY and files_done >= FILES_PER_ENTITY:
                break


def flatten_sources():
    with gzip.open(csv_files['sources']['sources']['name'], 'wt', encoding='utf-8') as sources_csv, \
            gzip.open(csv_files['sources']['ids']['name'], 'wt', encoding='utf-8') as ids_csv, \
            gzip.open(csv_files['sources']['counts_by_year']['name'], 'wt', encoding='utf-8') as counts_by_year_csv:

        sources_writer = csv.DictWriter(
            sources_csv, fieldnames=csv_files['sources']['sources']['columns'], extrasaction='ignore'
        )
        sources_writer.writeheader()

        ids_writer = csv.DictWriter(ids_csv, fieldnames=csv_files['sources']['ids']['columns'])
        ids_writer.writeheader()

        counts_by_year_writer = csv.DictWriter(counts_by_year_csv,
                                               fieldnames=csv_files['sources']['counts_by_year']['columns'])
        counts_by_year_writer.writeheader()

        seen_source_ids = set()

        files_done = 0
        for jsonl_file_name in glob.glob(os.path.join(SNAPSHOT_DIR, 'data', 'sources', '*', '*.gz')):
            # print(jsonl_file_name)
            with gzip.open(jsonl_file_name, 'r') as sources_jsonl:
                for source_json in sources_jsonl:
                    if not source_json.strip():
                        continue

                    source = json.loads(source_json)

                    if not (source_id := source.get('id')) or source_id in seen_source_ids:
                        continue
                    source_id = convert_openalex_id_to_int(source_id)
                    source['source_id'] = source_id
                    source['source_name'] = source['display_name']

                    seen_source_ids.add(source_id)

                    source['issn'] = json.dumps(source.get('issn'))
                    sources_writer.writerow(source)

                    if source_ids := source.get('ids'):
                        source_ids['source_id'] = source_id
                        source_ids['source_name'] = source['source_name']
                        source_ids['issn'] = json.dumps(source_ids.get('issn'))
                        ids_writer.writerow(source_ids)

                    if counts_by_year := source.get('counts_by_year'):
                        for count_by_year in counts_by_year:
                            count_by_year['source_name'] = source['source_name']
                            count_by_year['source_id'] = source_id
                            counts_by_year_writer.writerow(count_by_year)

            files_done += 1
            if FILES_PER_ENTITY and files_done >= FILES_PER_ENTITY:
                break


def flatten_topics():
    print(f'Flattening topics.....')
    with gzip.open(csv_files['topics']['topics']['name'], 'wt', encoding='utf-8') as topics_csv, \
            gzip.open(csv_files['topics']['keywords']['name'], 'wt', encoding='utf-8') as keywords_csv, \
            gzip.open(csv_files['topics']['siblings']['name'], 'wt', encoding='utf-8') as siblings_csv:

        topics_writer = csv.DictWriter(
            topics_csv, fieldnames=csv_files['topics']['topics']['columns'], extrasaction='ignore'
        )
        topics_writer.writeheader()

        keywords_writer = csv.DictWriter(keywords_csv, fieldnames=csv_files['topics']['keywords']['columns'],
                                         extrasaction='ignore')
        keywords_writer.writeheader()

        siblings_writer = csv.DictWriter(siblings_csv, fieldnames=csv_files['topics']['siblings']['columns'],
                                         extrasaction='ignore')
        siblings_writer.writeheader()

        seen_topic_ids = set()

        files_done = 0
        for jsonl_file_name in glob.glob(os.path.join(SNAPSHOT_DIR, 'data', 'topics', '*', '*.gz')):
            # print(jsonl_file_name)
            with gzip.open(jsonl_file_name, 'r') as topics_jsonl:
                for topic_json in topics_jsonl:
                    if not topic_json.strip():
                        continue

                    topic = json.loads(topic_json)

                    if not (topic_id := topic.get('id')) or topic_id in seen_topic_ids:
                        continue
                    topic_id = convert_topic_id_to_int(topic_id)
                    topic['topic_id'] = topic_id
                    topic['topic_name'] = topic['display_name']
                    topic['num_works'] = topic['works_count']

                    for genre in ['subfield', 'field', 'domain']:
                        # print(f'\n---------\n{topic_id=}\n{=}\n{topic=}\n\n----------')
                        topic[f'{genre}_id'] = convert_topic_id_to_int(topic[genre]['id'])
                        topic[f'{genre}_name'] = topic[genre]['display_name']

                    seen_topic_ids.add(topic_id)
                    topics_writer.writerow(topic)

                    if keywords := topic.get('keywords'):
                        keywords_d = {}
                        for keyword in keywords:
                            keywords_d['topic_id'] = topic['topic_id']
                            keywords_d['topic_name'] = topic['topic_name']
                            keywords_d['keyword'] = keyword
                            keywords_writer.writerow(keywords_d)

                    # print(f'{str(jsonl_file_name)=} {topic_id=} {topic.keys()=}')

                    if siblings := topic.get('siblings'):
                        for sibling in siblings:
                            sibling['topic_id'] = topic_id
                            sibling['topic_name'] = topic['topic_name']
                            sibling['sibling_topic_name'] = sibling['display_name']
                            sibling['sibling_topic_id'] = convert_topic_id_to_int(sibling['id'])
                            siblings_writer.writerow(sibling)

            files_done += 1
            if FILES_PER_ENTITY and files_done >= FILES_PER_ENTITY:
                break


def flatten_concepts():
    # read the merged entries and skip over those entries
    skip_ids = get_skip_ids('concepts')

    with gzip.open(csv_files['concepts']['concepts']['name'], 'wt', encoding='utf-8') as concepts_csv, \
            gzip.open(csv_files['concepts']['ancestors']['name'], 'wt', encoding='utf-8') as ancestors_csv, \
            gzip.open(csv_files['concepts']['counts_by_year']['name'], 'wt', encoding='utf-8') as counts_by_year_csv, \
            gzip.open(csv_files['concepts']['ids']['name'], 'wt', encoding='utf-8') as ids_csv, \
            gzip.open(csv_files['concepts']['related_concepts']['name'], 'wt',
                      encoding='utf-8') as related_concepts_csv:

        concepts_writer = csv.DictWriter(
            concepts_csv, fieldnames=csv_files['concepts']['concepts']['columns'], extrasaction='ignore'
        )
        concepts_writer.writeheader()

        ancestors_writer = csv.DictWriter(ancestors_csv, fieldnames=csv_files['concepts']['ancestors']['columns'])
        ancestors_writer.writeheader()

        counts_by_year_writer = csv.DictWriter(counts_by_year_csv,
                                               fieldnames=csv_files['concepts']['counts_by_year']['columns'])
        counts_by_year_writer.writeheader()

        ids_writer = csv.DictWriter(ids_csv, fieldnames=csv_files['concepts']['ids']['columns'])
        ids_writer.writeheader()

        related_concepts_writer = csv.DictWriter(related_concepts_csv,
                                                 fieldnames=csv_files['concepts']['related_concepts']['columns'])
        related_concepts_writer.writeheader()

        seen_concept_ids = set()

        files = list(glob.glob(os.path.join(SNAPSHOT_DIR, 'data', 'concepts', '*', '*.gz')))
        for jsonl_file_name in tqdm(files, desc='Flattening concepts...', unit=' file'):
            with gzip.open(jsonl_file_name, 'r') as concepts_jsonl:
                for concept_json in concepts_jsonl:
                    if not concept_json.strip():
                        continue

                    concept = json.loads(concept_json)

                    if not (concept_id := concept.get('id')) or concept_id in seen_concept_ids:
                        continue

                    concept_id = convert_openalex_id_to_int(concept_id)  # convert to int
                    if concept_id in skip_ids:  # skip over already merged IDs
                        continue

                    concept_name = concept['display_name']
                    seen_concept_ids.add(concept_id)

                    concept['concept_id'] = concept_id
                    concept['concept_name'] = concept_name
                    concepts_writer.writerow(concept)

                    if concept_ids := concept.get('ids'):
                        concept_ids['concept_id'] = concept_id
                        concept_ids['concept_name'] = concept_name
                        concept_ids['umls_aui'] = json.dumps(concept_ids.get('umls_aui'), ensure_ascii=False)
                        concept_ids['umls_cui'] = json.dumps(concept_ids.get('umls_cui'), ensure_ascii=False)
                        ids_writer.writerow(concept_ids)

                    if ancestors := concept.get('ancestors'):
                        for ancestor in ancestors:
                            if ancestor_id := ancestor.get('id'):
                                ancestors_writer.writerow({
                                    'concept_id': concept_id,
                                    'ancestor_id': ancestor_id
                                })

                    if counts_by_year := concept.get('counts_by_year'):
                        for count_by_year in counts_by_year:
                            count_by_year['concept_id'] = concept_id
                            count_by_year['concept_name'] = concept_name
                            counts_by_year_writer.writerow(count_by_year)

                    if related_concepts := concept.get('related_concepts'):
                        for related_concept in related_concepts:
                            if related_concept_id := related_concept.get('id'):
                                related_concepts_writer.writerow({
                                    'concept_id': concept_id,
                                    'related_concept_id': related_concept_id,
                                    'score': related_concept.get('score')
                                })
    return


def flatten_institutions():
    skip_ids = get_skip_ids('institutions')

    file_spec = csv_files['institutions']

    with gzip.open(file_spec['institutions']['name'], 'wt', encoding='utf-8') as institutions_csv, \
            gzip.open(file_spec['ids']['name'], 'wt', encoding='utf-8') as ids_csv, \
            gzip.open(file_spec['geo']['name'], 'wt', encoding='utf-8') as geo_csv, \
            gzip.open(file_spec['associated_institutions']['name'], 'wt',
                      encoding='utf-8') as associated_institutions_csv, \
            gzip.open(file_spec['counts_by_year']['name'], 'wt', encoding='utf-8') as counts_by_year_csv:

        institutions_writer = csv.DictWriter(
            institutions_csv, fieldnames=file_spec['institutions']['columns'], extrasaction='ignore'
        )
        institutions_writer.writeheader()

        ids_writer = csv.DictWriter(ids_csv, fieldnames=file_spec['ids']['columns'])
        ids_writer.writeheader()

        geo_writer = csv.DictWriter(geo_csv, fieldnames=file_spec['geo']['columns'])
        geo_writer.writeheader()

        associated_institutions_writer = csv.DictWriter(
            associated_institutions_csv, fieldnames=file_spec['associated_institutions']['columns']
        )
        associated_institutions_writer.writeheader()

        counts_by_year_writer = csv.DictWriter(counts_by_year_csv, fieldnames=file_spec['counts_by_year']['columns'])
        counts_by_year_writer.writeheader()

        seen_institution_ids = set()

        files = list(glob.glob(os.path.join(SNAPSHOT_DIR, 'data', 'institutions', '*', '*.gz')))
        for jsonl_file_name in tqdm(files, desc='Flattening Institutions...'):
            with gzip.open(jsonl_file_name, 'r') as institutions_jsonl:
                for institution_json in institutions_jsonl:
                    if not institution_json.strip():
                        continue

                    institution = orjson.loads(institution_json)

                    if not (institution_id := institution.get('id')) or institution_id in seen_institution_ids:
                        continue

                    institution_id = convert_openalex_id_to_int(institution_id)
                    if institution_id in skip_ids:
                        continue

                    institution_name = institution['display_name']
                    seen_institution_ids.add(institution_id)

                    # institutions
                    institution['institution_id'] = institution_id
                    institution['institution_name'] = institution_name
                    institution['display_name_acroynyms'] = json.dumps(institution.get('display_name_acroynyms'),
                                                                       ensure_ascii=False)
                    institution['display_name_alternatives'] = json.dumps(institution.get('display_name_alternatives'),
                                                                          ensure_ascii=False)
                    institutions_writer.writerow(institution)

                    # ids
                    if institution_ids := institution.get('ids'):
                        institution_ids['institution_id'] = institution_id
                        institution_ids['institution_name'] = institution_name
                        ids_writer.writerow(institution_ids)

                    # geo
                    if institution_geo := institution.get('geo'):
                        institution_geo['institution_id'] = institution_id
                        institution_geo['institution_name'] = institution_name
                        geo_writer.writerow(institution_geo)

                    # associated_institutions
                    if associated_institutions := institution.get(
                            'associated_institutions', institution.get('associated_insitutions')  # typo in api
                    ):
                        for associated_institution in associated_institutions:
                            if associated_institution_id := associated_institution.get('id'):
                                associated_institutions_writer.writerow({
                                    'institution_id': institution_id,
                                    'associated_institution_id': convert_openalex_id_to_int(associated_institution_id),
                                    'relationship': associated_institution.get('relationship'),
                                })

                    # counts_by_year
                    if counts_by_year := institution.get('counts_by_year'):
                        for count_by_year in counts_by_year:
                            count_by_year['institution_id'] = institution_id
                            count_by_year['institution_name'] = institution_name
                            counts_by_year_writer.writerow(count_by_year)
    return


def flatten_authors(files_to_process: Union[str, int] = 'all'):
    skip_ids = get_skip_ids('authors')

    file_spec = csv_files['authors']

    authors_csv_exists = Path(file_spec['authors']['name']).exists()
    ids_csv_exists = Path(file_spec['ids']['name']).exists()
    counts_by_year_csv_exists = Path(file_spec['counts_by_year']['name']).exists()
    authors_concepts_csv_exists = Path(file_spec['concepts']['name']).exists()
    authors_hints_csv_exists = Path(file_spec['hints']['name']).exists()

    with gzip.open(file_spec['authors']['name'], 'at', encoding='utf-8') as authors_csv, \
            gzip.open(file_spec['ids']['name'], 'at', encoding='utf-8') as ids_csv, \
            gzip.open(file_spec['counts_by_year']['name'], 'at', encoding='utf-8') as counts_by_year_csv, \
            gzip.open(file_spec['concepts']['name'], 'at', encoding='utf-8') as authors_concepts_csv, \
            gzip.open(file_spec['hints']['name'], 'at', encoding='utf-8') as authors_hints_csv:

        authors_writer = csv.DictWriter(
            authors_csv, fieldnames=file_spec['authors']['columns'], extrasaction='ignore'
        )
        if not authors_csv_exists:
            authors_writer.writeheader()

        ids_writer = csv.DictWriter(ids_csv, fieldnames=file_spec['ids']['columns'])
        if not ids_csv_exists:
            ids_writer.writeheader()

        counts_by_year_writer = csv.DictWriter(counts_by_year_csv, fieldnames=file_spec['counts_by_year']['columns'])
        if not counts_by_year_csv_exists:
            counts_by_year_writer.writeheader()

        authors_concepts_writer = csv.DictWriter(
            authors_concepts_csv, fieldnames=file_spec['concepts']['columns'], extrasaction='ignore'
        )
        if not authors_concepts_csv_exists:
            authors_concepts_writer.writeheader()

        authors_hints_writer = csv.DictWriter(
            authors_hints_csv, fieldnames=file_spec['hints']['columns'], extrasaction='ignore'
        )
        if not authors_hints_csv_exists:
            authors_hints_writer.writeheader()

        print(f'This might take a while, like 6-7 hours..')

        finished_files_pickle_path = CSV_DIR / 'temp' / 'finished_authors.pkl'
        finished_files_pickle_path.parent.mkdir(exist_ok=True)  # make the temp directory if needed

        if finished_files_pickle_path.exists():
            finished_files = load_pickle(finished_files_pickle_path)  # load the pickle
            print(f'{len(finished_files)} existing files found!')
        else:
            finished_files = set()

        authors_manifest = read_manifest(kind='authors', snapshot_dir=SNAPSHOT_DIR / 'data')
        files = [str(entry.filename) for entry in authors_manifest.entries]
        # files = map(str, glob.glob(os.path.join(SNAPSHOT_DIR, 'data', 'authors', '*', '*.gz')))
        files = [f for f in files if f not in finished_files]

        if files_to_process == 'all':
            files_to_process = len(files)

        print(f'{files_to_process=}')

        for i, jsonl_file_name in tqdm(enumerate(files), desc='Flattening authors...', unit=' file',
                                       total=files_to_process):
            if i > files_to_process:
                break

            with gzip.open(jsonl_file_name, 'r') as authors_jsonl:
                authors_jsonls = authors_jsonl.readlines()

            authors_rows, ids_rows, counts_by_year_rows, authors_concepts_rows, author_hints_rows = [], [], [], [], []

            for author_json in tqdm(authors_jsonls, desc='Parsing JSONs', leave=False, unit=' line', unit_scale=True):
                if not author_json.strip():
                    continue

                author = orjson.loads(author_json)

                if not (author_id := author.get('id')):
                    continue
                author_id = convert_openalex_id_to_int(author_id)
                if author_id in skip_ids:
                    continue

                author_name = author['display_name']

                # authors
                author['author_id'] = author_id
                author['author_name'] = author_name
                author['display_name_alternatives'] = json.dumps(author.get('display_name_alternatives'),
                                                                 ensure_ascii=False)
                last_known_institution = (author.get('last_known_institution') or {}).get('id')
                if last_known_institution is not None:
                    last_known_institution = convert_openalex_id_to_int(last_known_institution)
                author['last_known_institution'] = last_known_institution

                orcid = author['orcid']
                orcid = orcid.replace('https://orcid.org/', '') if orcid is not None else None
                author['orcid'] = orcid

                authors_rows.append(author)
                # authors_writer.writerow(author)

                # ids
                if author_ids := author.get('ids'):
                    author_ids['author_id'] = author_id
                    author_ids['author_name'] = author_name
                    # ids_writer.writerow(author_ids)
                    ids_rows.append(author_ids)

                # counts_by_year
                if counts_by_year := author.get('counts_by_year'):
                    for count_by_year in counts_by_year:
                        count_by_year['author_id'] = author_id
                        count_by_year['author_name'] = author_name
                        counts_by_year_rows.append(count_by_year)
                        # counts_by_year_writer.writerow(count_by_year)

                # concepts
                if x_concepts := author.get('x_concepts'):
                    for x_concept in x_concepts:
                        x_concept['author_id'] = author_id
                        x_concept['author_name'] = author_name
                        x_concept['concept_id'] = convert_openalex_id_to_int(x_concept['id'])
                        x_concept['concept_name'] = x_concept['display_name']

                        # authors_concepts_writer.writerow(x_concept)
                        authors_concepts_rows.append(x_concept)

                # hints
                author_hints_row = {}
                author_name = author['display_name']
                author_hints_row['author_id'] = author_id
                author_hints_row['author_name'] = author_name
                author_hints_row['works_count'] = author.get('works_count', 0)
                author_hints_row['cited_by_count'] = author.get('cited_by_count', 0)
                author_hints_row['most_cited_work'] = author.get('most_cited_work', '')

                # authors_hints_writer.writerow(author_hints_row)
                author_hints_rows.append(author_hints_row)

            ## write all the lines to the CSVs
            authors_writer.writerows(authors_rows)
            ids_writer.writerows(ids_rows)
            counts_by_year_writer.writerows(counts_by_year_rows)
            authors_concepts_writer.writerows(authors_concepts_rows)
            authors_hints_writer.writerows(author_hints_rows)

            finished_files.add(str(jsonl_file_name))
            dump_pickle(obj=finished_files, path=finished_files_pickle_path)

    return


def flatten_authors_concepts(files_to_process: Union[str, int] = 'all'):
    skip_ids = get_skip_ids('authors')
    file_spec = csv_files['authors']
    authors_concepts_zero_filename = CSV_DIR / 'authors_concepts_zero.csv.gz'

    authors_concepts_csv_exists = Path(file_spec['concepts']['name']).exists()
    authors_concepts_zero_csv_exists = authors_concepts_zero_filename.exists()

    with gzip.open(file_spec['concepts']['name'], 'at', encoding='utf-8') as authors_concepts_csv, \
            gzip.open(authors_concepts_zero_filename, 'at', encoding='utf-8') as authors_concepts_zero_csv:

        authors_concepts_writer = csv.DictWriter(
            authors_concepts_csv, fieldnames=file_spec['concepts']['columns'], extrasaction='ignore'
        )
        if not authors_concepts_csv_exists:
            authors_concepts_writer.writeheader()

        authors_concepts_zero_writer = csv.DictWriter(
            authors_concepts_zero_csv, fieldnames=file_spec['concepts']['columns'], extrasaction='ignore'
        )
        if not authors_concepts_zero_csv_exists:
            authors_concepts_zero_writer.writeheader()

        print(f'This might take a while, like 6-7 hours..')

        finished_files_pickle_path = CSV_DIR / 'temp' / 'finished_authors_concepts.pkl'
        finished_files_pickle_path.parent.mkdir(exist_ok=True)  # make the temp directory if needed

        if finished_files_pickle_path.exists():
            finished_files = load_pickle(finished_files_pickle_path)  # load the pickle
            print(f'{len(finished_files)} existing files found!')
        else:
            finished_files = set()

        authors_manifest = read_manifest(kind='authors', snapshot_dir=SNAPSHOT_DIR / 'data')

        files = [str(entry.filename) for entry in authors_manifest.entries]
        # files = map(str, glob.glob(os.path.join(SNAPSHOT_DIR, 'data', 'authors', '*', '*.gz')))
        files = [f for f in files if f not in finished_files]

        if files_to_process == 'all':
            files_to_process = len(files)
        print(f'{files_to_process=}')

        for i, jsonl_file_name in tqdm(enumerate(files), desc='Flattening author concepts...', unit=' file',
                                       total=len(files)):
            if i > files_to_process:
                break

            with gzip.open(jsonl_file_name, 'r') as authors_jsonl:
                authors_jsonls = authors_jsonl.readlines()
            author_concept_rows = []
            author_concept_zero_rows = []

            for author_json in tqdm(authors_jsonls, desc='Parsing JSONs', leave=False, unit=' line', unit_scale=True):
                if not author_json.strip():
                    continue

                author = orjson.loads(author_json)

                if not (author_id := author.get('id')):
                    continue
                author_id = convert_openalex_id_to_int(author_id)
                if author_id in skip_ids:
                    continue

                author_name = author['display_name']
                if x_concepts := author.get('x_concepts'):
                    for x_concept in x_concepts:
                        x_concept['author_id'] = author_id
                        x_concept['author_name'] = author_name
                        x_concept['concept_id'] = convert_openalex_id_to_int(x_concept['id'])
                        x_concept['concept_name'] = x_concept['display_name']
                        x_concept['works_count'] = author.get('works_count', 0)
                        x_concept['cited_by_count'] = author.get('cited_by_count', 0)
                        author_concept_rows.append(x_concept)

                        # authors_concepts_writer.writerow(x_concept)

                        if x_concept['level'] == 0:  # store only level 0 concepts here
                            # authors_concepts_zero_writer.writerow(x_concept)
                            author_concept_zero_rows.append(x_concept)

            authors_concepts_writer.writerows(author_concept_rows)
            authors_concepts_zero_writer.writerows(author_concept_zero_rows)

            finished_files.add(str(jsonl_file_name))
            dump_pickle(obj=finished_files, path=finished_files_pickle_path)

    return


def flatten_authors_hints(files_to_process: Union[str, int] = 'all'):
    skip_ids = get_skip_ids('authors')
    file_spec = csv_files['authors']

    authors_hints_csv_exists = Path(file_spec['hints']['name']).exists()

    with gzip.open(file_spec['hints']['name'], 'at', encoding='utf-8') as authors_hints_csv:
        authors_hints_writer = csv.DictWriter(
            authors_hints_csv, fieldnames=file_spec['hints']['columns'], extrasaction='ignore'
        )
        if not authors_hints_csv_exists:
            authors_hints_writer.writeheader()

        print(f'This might take a while, like 6-7 hours..')

        finished_files_pickle_path = CSV_DIR / 'temp' / 'finished_authors_hints.pkl'
        finished_files_pickle_path.parent.mkdir(exist_ok=True)  # make the temp directory if needed

        if finished_files_pickle_path.exists():
            finished_files = load_pickle(finished_files_pickle_path)  # load the pickle
            print(f'{len(finished_files)} existing files found!')
        else:
            finished_files = set()

        authors_manifest = read_manifest(kind='authors', snapshot_dir=SNAPSHOT_DIR / 'data')

        files = [str(entry.filename) for entry in authors_manifest.entries]
        # files = map(str, glob.glob(os.path.join(SNAPSHOT_DIR, 'data', 'authors', '*', '*.gz')))
        files = [f for f in files if f not in finished_files]

        if files_to_process == 'all':
            files_to_process = len(files)
        print(f'{files_to_process=}')

        for i, jsonl_file_name in tqdm(enumerate(files), desc='Flattening author hints...', unit=' file',
                                       total=len(files)):
            if i > files_to_process:
                break

            with gzip.open(jsonl_file_name, 'r') as authors_jsonl:
                authors_jsonls = authors_jsonl.readlines()

            author_hints_rows = []
            for author_json in tqdm(authors_jsonls, desc='Parsing JSONs', leave=False, unit=' line', unit_scale=True):
                if not author_json.strip():
                    continue

                author = orjson.loads(author_json)
                # print(f'{author=}')
                # break

                if not (author_id := author.get('id')):
                    continue
                author_id = convert_openalex_id_to_int(author_id)
                if author_id in skip_ids:
                    continue

                author_hints_row = {}
                author_name = author['display_name']
                author_hints_row['author_id'] = author_id
                author_hints_row['author_name'] = author_name
                author_hints_row['works_count'] = author.get('works_count', 0)
                author_hints_row['cited_by_count'] = author.get('cited_by_count', 0)
                author_hints_row['most_cited_work'] = author.get('most_cited_work', '')

                author_hints_rows.append(author_hints_row)

            authors_hints_writer.writerows(author_hints_rows)
            finished_files.add(str(jsonl_file_name))
            dump_pickle(obj=finished_files, path=finished_files_pickle_path)

    return


def _process_work_json(skip_ids, jsonl_file_name, finished_files, finished_files_txt_path, make_abstracts=True):
    """
    Process each work JSON lines file in parallel
    """
    with gzip.open(jsonl_file_name, 'r') as works_jsonl:
        works_jsonls = works_jsonl.readlines()

    work_rows, id_rows, primary_location_rows, location_rows, authorship_rows, biblio_rows = [], [], [], [], [], []
    concept_rows, mesh_rows, oa_rows, best_oa_loc_rows, refs_rows, rels_rows, abstract_rows, grant_rows = [], [], [], [], [], [], [], []
    desc = 'Parsing JSONs...' + '/'.join(Path(jsonl_file_name).parts[-2:])

    for work_json in tqdm(works_jsonls, desc=desc, unit=' line', unit_scale=True, colour='blue', leave=False):
        if not work_json.strip():
            continue

        work = orjson.loads(work_json)

        if not (work_id := work.get('id')):
            continue

        ## works
        work_id = convert_openalex_id_to_int(work_id)
        if work_id in skip_ids:
            continue

        num_authors, num_references, num_locations = 0, 0, 0
        type_crossref = work.get('type_crossref')
        work['type_crossref'] = type_crossref
        # if type_crossref is not None:
        #     print(f'{work_id=} {jsonl_file_name=} {type_crossref=}')
        work['work_id'] = work_id
        doi = work['doi']
        doi = doi.replace('https://doi.org/', '') if doi is not None else None
        work['doi'] = doi

        if work['title'] is None:
            title = None
        else:
            title = work['title'].replace(r'\n', ' ')  # deleting stray \n's in title
        work['title'] = title

        work['language'] = work.get('language')  # works languages

        ## works grants
        has_grant = False
        if grants := work.get('grants'):
            for grant_d in grants:
                has_grant = True  # set the flag to True
                grant_rows.append({
                    'work_id': work_id,
                    'funder_id': convert_openalex_id_to_int(grant_d.get('funder')),
                    'funder_name': grant_d.get('funder_display_name'),
                    'award_id': grant_d.get('award_id'),
                })
        work['has_grant_info'] = has_grant

        # authorships
        if authorships := work.get('authorships'):
            for authorship in authorships:
                if author_id := authorship.get('author', {}).get('id'):
                    num_authors += 1  # increase the count of authors
                    author_id = convert_openalex_id_to_int(author_id)
                    author_name = authorship.get('author', {}).get('display_name')

                    # join list of country codes with ;
                    countries = ';'.join(authorship.get('countries', []))

                    institutions = authorship.get('institutions')
                    institution_ids = [convert_openalex_id_to_int(i.get('id')) for i in institutions]
                    institution_ids = [i for i in institution_ids if i]
                    institution_ids = institution_ids or [None]

                    institution_names = [i.get('display_name') for i in institutions]
                    institution_names = [i for i in institution_names if i]
                    institution_names = institution_names or [None]

                    for institution_id, institution_name in zip(institution_ids, institution_names):
                        authorship_rows.append({
                            'work_id': work_id,
                            'author_position': authorship.get('author_position'),
                            'author_id': author_id,
                            'author_name': author_name,
                            'institution_id': institution_id,
                            'institution_name': institution_name,
                            'raw_affiliation_string': authorship.get('raw_affiliation_string'),
                            'countries': countries,
                            'publication_year': work.get('publication_year'),
                            'is_corresponding': authorship.get('is_corresponding'),
                        })

        work['num_authors'] = num_authors

        ## primary location
        if primary_location := (work.get('primary_location') or {}):
            if primary_location.get('source') and primary_location.get('source').get('id'):
                primary_location_d = primary_location.get('source', {})

                primary_location_rows.append({
                    'work_id': work_id,
                    'source_id': convert_openalex_id_to_int(primary_location_d.get('id')),
                    'source_name': primary_location_d.get('display_name'),
                    'source_type': primary_location_d.get('type'),
                    'is_oa': primary_location.get('is_oa'),
                    'version': primary_location.get('version'),
                    'license': primary_location.get('license'),
                })

        # locations
        if locations := work.get('locations'):
            for location in locations:
                if location.get('source') and location.get('source').get('id'):
                    location_d = location.get('source', {})
                    num_locations += 1
                    location_rows.append({
                        'work_id': work_id,
                        'source_id': convert_openalex_id_to_int(location_d.get('id')),
                        'source_name': location_d.get('display_name'),
                        'source_type': location_d.get('type'),
                        'is_oa': location.get('is_oa'),
                        'version': location.get('version'),
                        'license': location.get('license'),
                    })

        work['num_locations'] = num_locations

        ## open access
        if oa := work.get('open_access'):
            oa['work_id'] = work_id
            oa['is_oa'] = string_to_bool(oa.get('is_oa'))
            oa['any_repository_has_fulltext'] = string_to_bool(oa.get('any_repository_has_fulltext'))
            oa_rows.append(oa)

        ## best oa location
        if best_oa_location := work.get('best_oa_location'):
            if best_oa_location.get('source') and best_oa_location.get('source').get('id'):
                best_oa_location_d = best_oa_location.get('source', {})
                best_oa_loc_rows.append({
                    'work_id': work_id,
                    'pdf_url': best_oa_location['pdf_url'],
                    'is_oa': string_to_bool(best_oa_location.get('is_oa')),
                    'is_accepted': string_to_bool(best_oa_location.get('is_accepted')),
                    'is_published': string_to_bool(best_oa_location.get('is_published')),
                    'source_id': convert_openalex_id_to_int(best_oa_location_d.get('id')),
                    'source_name': best_oa_location_d.get('display_name'),
                    'source_type': best_oa_location_d.get('type'),
                })

        # biblio
        if biblio := work.get('biblio'):
            biblio['work_id'] = work_id
            biblio_rows.append(biblio)

        # concepts
        for concept in work.get('concepts'):
            if concept_id := concept.get('id'):
                concept_id = convert_openalex_id_to_int(concept_id)
                concept_name = concept.get('display_name')
                level = concept.get('level')

                concept_rows.append({
                    'work_id': work_id,
                    'publication_year': work.get('publication_year'),
                    'concept_id': concept_id,
                    'concept_name': concept_name,
                    'level': level,
                    'score': concept.get('score'),
                })

        # ids
        if ids := work.get('ids'):
            ids['work_id'] = work_id
            ids['doi'] = doi
            id_rows.append(ids)

        # mesh
        for mesh in work.get('mesh'):
            mesh['work_id'] = work_id
            mesh_rows.append(mesh)

        # referenced_works
        for referenced_work in work.get('referenced_works'):
            if referenced_work:
                num_references += 1
                referenced_work = convert_openalex_id_to_int(referenced_work)
                refs_rows.append({
                    'work_id': work_id,
                    'referenced_work_id': referenced_work
                })
        work['num_references'] = num_references
        work_rows.append(work)  # after adding number of references and locations

        # related_works
        for related_work in work.get('related_works'):
            if related_work:
                related_work = convert_openalex_id_to_int(related_work)

                rels_rows.append({
                    'work_id': work_id,
                    'related_work_id': related_work
                })

        # abstracts
        if make_abstracts:
            if (abstract_inv_index := work.get('abstract_inverted_index')) is not None:
                if 'InvertedIndex' in abstract_inv_index:  # new format of nested abstract dictionaries
                    abstract_inv_index = abstract_inv_index['InvertedIndex']
                    # print(f'New format abstract', repr(abstract_inv_index)[: 50])
                try:
                    abstract = reconstruct_abstract(abstract_inv_index)
                except orjson.JSONDecodeError as e:
                    abstract = pd.NA

                abstract_row = {'work_id': work_id, 'title': title, 'abstract': abstract,
                                'publication_year': work.get('publication_year')}
                abstract_rows.append(abstract_row)

    # write the batched parquets here
    kinds = ['works', 'ids', 'primary_location', 'locations', 'authorships', 'biblio', 'concepts', 'mesh',
             'referenced_works', 'related_works', 'abstracts', 'grants', 'open_access', 'best_oa_location']
    row_names = [work_rows, id_rows, primary_location_rows, location_rows, authorship_rows, biblio_rows,
                 concept_rows, mesh_rows, refs_rows, rels_rows, abstract_rows, grant_rows, oa_rows, best_oa_loc_rows]

    lines = []
    with tqdm(total=len(kinds), desc='Writing CSVs and parquets', leave=False, colour='green') as pbar:
        for kind, rows in zip(kinds, row_names):
            pbar.set_postfix_str(kind)
            new_file = write_to_csv_and_parquet(json_filename=jsonl_file_name, kind=kind, rows=rows)
            if new_file:
                lines.append(
                    f'{datetime.now().strftime("%c").strip()},{str(jsonl_file_name)},{kind},{len(work_rows)}\n')
                ## write to csv file
            pbar.update(1)

    with open(finished_files_txt_path, 'a') as fp:
        for line in lines:
            fp.write(line)

    return len(work_rows)


def _flatten_works_v2(files_to_process: Union[str, int] = 'all', threads=1, make_abstracts=True):
    """
    New flattening function that only writes Parquets, uses the Sources
    """
    skip_ids = get_skip_ids('works')

    kinds = ['works', 'ids', 'grants', 'primary_location', 'locations', 'authorships', 'biblio', 'concepts', 'mesh',
             'referenced_works', 'related_works', 'abstracts', 'open_access', 'best_oa_location']
    for kind in kinds:
        # ensure directories exist
        if kind != 'works':
            kind = f'works_{kind}'
        path = (PARQ_DIR / kind)
        if not path.exists():
            print(f'Creating dir at {str(path)}')
            path.mkdir(parents=True)

    finished_files_txt_path = PARQ_DIR / 'temp' / 'finished_works.txt'  # store finished paths in a text file
    finished_files_txt_path.parent.mkdir(exist_ok=True)  # make the temp directory if needed

    if finished_files_txt_path.exists():
        finished_files = set(
            pd.read_csv(finished_files_txt_path)  # load the pickle
            .drop_duplicates(subset=['path', 'table'], keep='last')  # drop duplicates
            .groupby('path', as_index=False)
            .count()
            .query('records==14')  # finished files will have 14 tables
            .path
        )
        print(f'{len(finished_files)} existing files found!')
    else:
        with open(finished_files_txt_path, 'w') as fp:
            fp.write(f'timestamp,path,table,records\n')
        finished_files = set()

    works_manifest = read_manifest(kind='works', snapshot_dir=SNAPSHOT_DIR / 'data')
    files = [str(entry.filename) for entry in works_manifest.entries]
    files = [f for f in files if f not in finished_files]
    # print(f'{files[: 2]}')

    print(f'This might take a while, like 20 hours..')

    if files_to_process == 'all':
        files_to_process = len(files)
    print(f'{files_to_process=}')

    args = []
    total_works_count = 0
    with tqdm(desc='Flattening works...', total=files_to_process, unit=' files') as pbar:
        for i, jsonl_file_name in enumerate(files):
            if i >= files_to_process:
                break
            if threads > 1:
                args.append((skip_ids, jsonl_file_name, finished_files, finished_files_txt_path))
            else:
                records = _process_work_json(skip_ids=skip_ids, jsonl_file_name=jsonl_file_name,
                                             finished_files=finished_files,
                                             finished_files_txt_path=finished_files_txt_path,
                                             make_abstracts=make_abstracts)
                total_works_count += records
                pbar.update(1)
                pbar.set_postfix_str(f'{total_works_count:,} works')

    if threads > 1:
        print(f'Spinning up {threads} parallel threads')
        parallel_async(func=_process_work_json, args=args, num_workers=threads)

    return


def process_work_json_v2(skip_ids, author_skip_ids, inst_skip_ids, jsonl_filename, entry_count,
                         finished_files_txt_path, inst_info_d,
                         publ_skip_ids, source_skip_ids, topic_info_d, overwrite_existing=False, make_abstracts=False):
    """
    Process each work JSON lines file in parallel
    Skip over already processed tables
    """
    jsonl_filename = Path(jsonl_filename)

    work_rows, id_rows, primary_location_rows, location_rows, authorship_rows, biblio_rows = [], [], [], [], [], []
    concept_rows, mesh_rows, oa_rows, best_oa_loc_rows, refs_rows, rels_rows, abstract_rows, grant_rows = [], [], [], [], [], [], [], []
    keywords_rows, topics_rows, indexed_rows = [], [], []

    is_missing_rows = {}  # dictionary where keys are table names and values are True if the table is missing rows

    for kind in DTYPES:
        kind_ = f'works_{kind}' if kind != 'works' else 'works'
        parq_filename = PARQ_DIR / kind_ / (
                '_'.join(jsonl_filename.parts[-2:]).replace('updated_date=', '').replace('.gz', '')
                + '.parquet')

        # missing if parquet filename doesnt exist or overwrite flag is ON
        missing = (not parq_filename.exists()) or overwrite_existing
        if kind == 'abstracts':
            missing = make_abstracts and ((not parq_filename.exists()) or overwrite_existing)

        if kind == 'related_works':
            missing = False

        is_missing_rows[kind] = missing
        # if missing:
        #     print(f'Missing {kind!r} for {"/".join(jsonl_filename.parts[-2:])!r}')

    desc = 'Parsing...' + '/'.join(Path(jsonl_filename).parts[-2:])
    desc = desc.replace('updated_date=', '')
    if not any(is_missing_rows.values()):
        print(f'All tables accounted for in {"/".join(jsonl_filename.parts[-2:])!r}')
        return 0

    work_keep_cols = set(DTYPES['works'].keys())

    ## streaming gzipped json one line at a time
    with gzip.open(jsonl_filename, 'r') as works_jsonl:
        for work_json in tqdm(works_jsonl, total=entry_count, desc=desc, unit=' line', unit_scale=True, colour='blue',
                              leave=False):
            if not work_json.strip():
                continue

            work = orjson.loads(work_json)

            if not (work_id := work.get('id')):
                continue

            # works
            work_id = convert_openalex_id_to_int(work_id)
            if work_id in skip_ids:
                continue

            num_authors, num_references, num_locations = 0, 0, 0
            type_crossref = work.get('type_crossref', pd.NA)
            work['type_crossref'] = type_crossref

            work['is_retracted'] = string_to_bool(work['is_retracted'])
            work['is_paratext'] = string_to_bool(work['is_paratext'])

            # if type_crossref is not None:
            #     print(f'{work_id=} {jsonl_file_name=} {type_crossref=}')
            work['work_id'] = work_id
            doi = work['doi']
            doi = doi.replace('https://doi.org/', '') if doi is not None else None
            work['doi'] = doi

            if work['title'] is None:
                title = None
            else:
                title = work['title'].replace(r'\n', ' ')  # deleting stray \n's in title
            work['title'] = title

            work['language'] = work.get('language', pd.NA)  # works languages

            # works indexed in
            if is_missing_rows['indexed_in']:
                if indexed_in := work.get('indexed_in'):
                    for ix_source in indexed_in:
                        indexed_rows.append(
                            dict(work_id=work_id, publication_year=work.get('publication_year'),
                                 indexed_source=ix_source)
                        )

            # works topics
            if is_missing_rows['topics']:
                if topics := work.get('topics'):
                    for i, topic in enumerate(topics):
                        topic_id = convert_topic_id_to_int(topic['id'])
                        d = dict(
                            work_id=work_id, publication_year=work.get('publication_year'),
                            is_primary_topic=True if i == 0 else False,
                            score=topic['score'],
                            topic_id=topic_id,
                            topic_name=topic_info_d['topic_name'][topic_id],
                        )
                        for genre in ['subfield', 'field', 'domain']:
                            # print(f'\n---------\n{work_id=}\n{topics=}\n{topic=}\n\n----------')
                            d[f'{genre}_id'] = topic_info_d[f'{genre}_id'][topic_id]
                            d[f'{genre}_name'] = topic_info_d[f'{genre}_name'][topic_id]

                        topics_rows.append(d)

            # works keywords
            if is_missing_rows['keywords']:
                has_keywords = False
                if keywords := work.get('keywords'):
                    for keyword_d in keywords:
                        has_keywords = True
                        keywords_rows.append({
                            'work_id': work_id,
                            'keyword': keyword_d.get('keyword', pd.NA),
                            'score': keyword_d.get('score', pd.NA),
                        })
            else:
                has_keywords = pd.NA
            work['has_keywords'] = has_keywords

            # works grants
            if is_missing_rows['grants']:
                has_grant = False
                if grants := work.get('grants'):
                    for grant_d in grants:
                        has_grant = True  # set the flag to True
                        funder_id = convert_openalex_id_to_int(grant_d.get('funder'))
                        funder_id = funder_id if funder_id is not None else pd.NA

                        funder_name = grant_d.get('funder_display_name', pd.NA)
                        funder_name = funder_name if funder_name != '' else pd.NA

                        grant_rows.append({
                            'work_id': work_id,
                            'funder_id': funder_id,
                            'funder_name': funder_name,
                            'award_id': grant_d.get('award_id', pd.NA),
                        })
            else:
                has_grant = pd.NA
            work['has_grant_info'] = has_grant

            # authorships
            has_complete_institution_info = False
            num_authors = 0
            if authorships := work.get('authorships'):
                new_authorship_rows, num_authors, has_complete_institution_info = parse_authorships(
                    authorships=authorships, work_id=work_id, publication_year=work.get('publication_year'),
                    author_skip_ids=author_skip_ids, inst_skip_ids=inst_skip_ids, inst_info_d=inst_info_d,
                )

                if is_missing_rows['authorships']:
                    authorship_rows.extend(new_authorship_rows)

            work['num_authors'] = num_authors
            work['has_complete_institution_info'] = has_complete_institution_info

            # primary location
            if is_missing_rows['primary_location']:
                if primary_location := (work.get('primary_location') or {}):
                    if primary_location.get('source') and primary_location.get('source').get('id'):
                        primary_location_d = primary_location.get('source', {})

                        source_id = convert_openalex_id_to_int(primary_location_d.get('id'))
                        if source_id in source_skip_ids:  # skip if invalid source
                            continue

                        primary_location_rows.append({
                            'work_id': work_id,
                            'source_id': source_id,
                            'source_name': primary_location_d.get('display_name'),
                            'source_type': primary_location_d.get('type'),
                            'version': primary_location.get('version'),
                            'license': primary_location.get('license'),
                            'landing_page_url': primary_location.get('landing_page_url'),
                            'pdf_url': primary_location.get('pdf_url'),
                            'is_oa': string_to_bool(primary_location.get('is_oa')),
                            'is_accepted': string_to_bool(primary_location.get('is_accepted')),
                            'is_published': string_to_bool(primary_location.get('is_published')),
                        })

            # locations
            if is_missing_rows['locations']:
                if locations := work.get('locations'):
                    for location in locations:
                        if location.get('source') and location.get('source').get('id'):
                            location_d = location.get('source', {})

                            source_id = convert_openalex_id_to_int(location_d.get('id'))
                            if source_id in source_skip_ids:  # skip if invalid source
                                continue

                            num_locations += 1
                            location_rows.append({
                                'work_id': work_id,
                                'source_id': source_id,
                                'source_name': location_d.get('display_name'),
                                'source_type': location_d.get('type'),
                                'version': location.get('version'),
                                'license': location.get('license'),
                                'landing_page_url': location.get('landing_page_url'),
                                'pdf_url': location.get('pdf_url'),
                                'is_oa': string_to_bool(location.get('is_oa')),
                                'is_accepted': string_to_bool(location.get('is_accepted')),
                                'is_published': string_to_bool(location.get('is_published')),
                            })
            else:
                num_locations = work.get('locations_count', 0)
            work['num_locations'] = num_locations

            # open access
            if is_missing_rows['open_access']:
                if oa := work.get('open_access'):
                    oa['work_id'] = work_id
                    oa['is_oa'] = string_to_bool(oa.get('is_oa'))
                    oa['any_repository_has_fulltext'] = string_to_bool(oa.get('any_repository_has_fulltext'))
                    oa_rows.append(oa)

            # best oa location
            if is_missing_rows['best_oa_location']:
                if best_oa_location := work.get('best_oa_location'):
                    if best_oa_location.get('source') and best_oa_location.get('source').get('id'):

                        best_oa_location_d = best_oa_location.get('source', {})
                        source_id = convert_openalex_id_to_int(best_oa_location_d.get('id'))
                        if source_id in source_skip_ids:  # skip if invalid source
                            continue

                        best_oa_loc_rows.append({
                            'work_id': work_id,
                            'pdf_url': best_oa_location['pdf_url'],
                            'is_oa': string_to_bool(best_oa_location.get('is_oa')),
                            'is_accepted': string_to_bool(best_oa_location.get('is_accepted')),
                            'is_published': string_to_bool(best_oa_location.get('is_published')),
                            'source_id': source_id,
                            'source_name': best_oa_location_d.get('display_name'),
                            'source_type': best_oa_location_d.get('type'),
                        })

            # biblio
            if is_missing_rows['biblio']:
                if biblio := work.get('biblio'):
                    biblio['work_id'] = work_id
                    biblio_rows.append(biblio)

            # concepts
            if is_missing_rows['concepts']:
                for concept in work.get('concepts'):
                    if concept_id := concept.get('id'):
                        concept_id = convert_openalex_id_to_int(concept_id)
                        concept_name = concept.get('display_name')
                        level = concept.get('level')

                        concept_rows.append({
                            'work_id': work_id,
                            'publication_year': work.get('publication_year'),
                            'concept_id': concept_id,
                            'concept_name': concept_name,
                            'level': level,
                            'score': concept.get('score'),
                        })

            # ids
            if is_missing_rows['ids']:
                if ids := work.get('ids'):
                    ids['work_id'] = work_id
                    ids['doi'] = doi
                    id_rows.append(ids)

            # mesh
            if is_missing_rows['mesh']:
                for mesh in work.get('mesh'):
                    mesh['work_id'] = work_id
                    mesh_rows.append(mesh)

            # referenced_works  -- make sure referenced works are not in skip_ids
            num_references = 0
            for referenced_work in work.get('referenced_works'):
                if referenced_work:
                    referenced_work = convert_openalex_id_to_int(referenced_work)
                    if referenced_work in skip_ids:
                        continue

                    num_references += 1
                    if is_missing_rows['referenced_works']:
                        refs_rows.append({
                            'work_id': work_id,
                            'referenced_work_id': referenced_work
                        })

            work['num_references'] = num_references

            filt_work = {k: v for k, v in work.items() if k in work_keep_cols}
            work_rows.append(filt_work)  # after adding number of references and locations

            # related_works
            if is_missing_rows['related_works']:
                for related_work in work.get('related_works'):
                    if related_work:
                        related_work = convert_openalex_id_to_int(related_work)
                        if related_work in skip_ids:  # skip if related work has been deleted
                            continue
                        rels_rows.append({
                            'work_id': work_id,
                            'related_work_id': related_work
                        })

            # abstracts
            if is_missing_rows['abstracts'] and make_abstracts:
                if (abstract_inv_index := work.get('abstract_inverted_index')) is not None:
                    if 'InvertedIndex' in abstract_inv_index:  # new format of nested abstract dictionaries
                        abstract_inv_index = abstract_inv_index['InvertedIndex']
                        # print(f'New format abstract', repr(abstract_inv_index)[: 50])
                    try:
                        abstract = reconstruct_abstract(abstract_inv_index)
                    except orjson.JSONDecodeError as e:
                        abstract = pd.NA

                    abstract_row = {'work_id': work_id, 'title': title, 'abstract': abstract,
                                    'publication_year': work.get('publication_year')}
                    abstract_rows.append(abstract_row)

    # write the batched parquets here
    kinds = ['works', 'ids', 'primary_location', 'locations', 'authorships', 'biblio', 'concepts', 'mesh',
             'referenced_works', 'related_works', 'abstracts', 'grants', 'open_access', 'best_oa_location', 'keywords',
             'topics', 'indexed_in', ]
    row_names = [work_rows, id_rows, primary_location_rows, location_rows, authorship_rows, biblio_rows,
                 concept_rows, mesh_rows, refs_rows, rels_rows, abstract_rows, grant_rows, oa_rows, best_oa_loc_rows,
                 keywords_rows, topics_rows, indexed_rows]

    lines = []
    with tqdm(total=len(kinds), desc='Writing CSVs and parquets', leave=False, colour='green') as pbar:
        for kind, rows in zip(kinds, row_names):
            if not is_missing_rows[kind]:  # skip over the non missing rows
                continue
            pbar.set_postfix_str(kind)
            new_file = write_to_csv_and_parquet(json_filename=jsonl_filename, kind=kind, rows=rows)
            if new_file:
                lines.append(
                    f'{datetime.now().strftime("%c").strip()},{str(jsonl_filename)},{kind},{len(work_rows)}\n')
                ## write to csv file
            pbar.update(1)

    with open(finished_files_txt_path, 'a') as fp:
        for line in lines:
            fp.write(line)

    return len(work_rows)


def flatten_works_v3(files_to_process: Union[str, int] = 'all', threads=1, make_abstracts=False, overwrite=False,
                     recompute_tables=[]):
    """
    SKIP over creating tables that already exists to save on memory
    """
    skip_ids, author_skip_ids, inst_skip_ids, publ_skip_ids, source_skip_ids = get_skip_ids('works'), \
        get_skip_ids('authors'), get_skip_ids('institutions'), get_skip_ids('publishers'), get_skip_ids('sources')

    kinds = ['works', 'ids', 'grants', 'primary_location', 'locations', 'authorships', 'biblio', 'concepts', 'mesh',
             'referenced_works', 'related_works', 'abstracts', 'open_access', 'best_oa_location']
    for kind in kinds:
        # ensure directories exist
        if kind != 'works':
            kind = f'works_{kind}'
        path = (PARQ_DIR / kind)
        if not path.exists():
            print(f'Creating dir at {str(path)}')
            path.mkdir(parents=True)

    finished_files_txt_path = PARQ_DIR / 'temp' / 'finished_works.txt'  # store finished paths in a text file
    finished_files_txt_path.parent.mkdir(exist_ok=True)  # make the temp directory if needed

    if finished_files_txt_path.exists():
        # final_table_count = len(DTYPES) if make_abstracts else len(DTYPES) - 1
        final_table_count = 15

        print(f'\n------\n{final_table_count=}\n------\n')
        finished_files = set(
            pd.read_csv(finished_files_txt_path, engine='c', parse_dates=['timestamp'])  # load the pickle
            .pipe(lambda df_: df_[~df_.table.isin(recompute_tables)])
            .drop_duplicates(subset=['path', 'table'], keep='last')  # drop duplicates
            .groupby('path', as_index=False)
            .count()
            .query('records==@final_table_count')  # finished files will have 14 tables
            .path
        )
        print(f'{len(finished_files)} existing files found!')
    else:
        with open(finished_files_txt_path, 'w') as fp:
            fp.write(f'timestamp,path,table,records\n')
        finished_files = set()

    works_manifest = read_manifest(kind='works', snapshot_dir=SNAPSHOT_DIR / 'data')
    files = [(str(entry.filename), entry.count) for entry in works_manifest.entries]
    files = [(f, c) for f, c in files if f not in finished_files]  # [:: -1]

    # files = [Path('/home/ssikdar/data/openalex-snapshot/data/works/updated_date=2023-11-26/part_000.gz')]
    # print(f'{files[: 2]}')

    print(f'This might take a while, like 20 hours..')

    if files_to_process == 'all':
        files_to_process = len(files)
    print(f'{files_to_process=}')

    args = []
    total_works_count = 0
    with tqdm(desc='Flattening works...', total=files_to_process, unit='files') as pbar:
        for i, (jsonl_file_name, entry_count) in enumerate(files):
            if i >= files_to_process:
                break
            if threads > 1:
                args.append((skip_ids, author_skip_ids, inst_skip_ids, jsonl_file_name, finished_files_txt_path,
                             inst_info_d, overwrite, make_abstracts))
            else:
                records = process_work_json_v2(
                    skip_ids=skip_ids, author_skip_ids=author_skip_ids, inst_skip_ids=inst_skip_ids,
                    publ_skip_ids=publ_skip_ids, source_skip_ids=source_skip_ids,
                    jsonl_filename=jsonl_file_name, entry_count=entry_count,
                    overwrite_existing=overwrite,
                    finished_files_txt_path=finished_files_txt_path,
                    make_abstracts=make_abstracts, inst_info_d=inst_info_d, topic_info_d=topic_info_d,
                )
                total_works_count += records
                pbar.update(1)
                pbar.set_postfix_str(f'{total_works_count:,} works')

    if threads > 1:
        print(f'Spinning up {threads} parallel threads')
        parallel_async(func=process_work_json_v2, args=args, num_workers=threads)

    return


def write_to_csv_and_parquet(rows: list, kind: str, json_filename: str, debug: bool = False,
                             csv_writer: Optional[csv.DictWriter] = None):
    """
    Write rows to the CSV using the CSV writer
    Also create a new file inside the respective parquet directory
    return True or False based on whether the file is new
    """
    if len(rows) == 0:
        return True

    if csv_writer is not None:
        csv_writer.writerows(rows)

    json_filename = Path(json_filename)

    kind_ = f'works_{kind}' if kind != 'works' else 'works'
    parq_filename = PARQ_DIR / kind_ / (
            '_'.join(json_filename.parts[-2:]).replace('updated_date=', '').replace('.gz', '')
            + '.parquet')

    if parq_filename.exists():
        # print(f'Parquet already exists {str(parq_filename.parts[-2:])}')
        return False

    if not parq_filename.parent.exists():
        parq_filename.parent.mkdir(exist_ok=True, parents=True)

    if debug:
        print(f'{kind=} {parq_filename=} {len(rows)=:,}')

    keep_cols = csv_files['works'][kind]['columns']

    df = (
        pd.DataFrame(rows)
    )
    missing_cols = [col for col in keep_cols if col not in df.columns.tolist()]
    if debug:
        print(f'{missing_cols=}')
    for missing_col in missing_cols:
        df.loc[:, missing_col] = pd.NA

    df = df[keep_cols]

    if kind in DTYPES:
        df = df.astype(dtype=DTYPES[kind], errors='ignore')  # handle pesky dates

    if pd.__version__ < "2":
        args = dict(infer_datetime_format=True)
    else:
        args = dict(format='ISO8601')

    write_args = dict()
    if kind == 'works':
        schema = pa.schema([
            ('work_id', pa.int64()),
            ('doi', pa.utf8()),
            ('title', pa.utf8()),
            ('publication_year', pa.int16()),
            ('publication_date', pa.timestamp('ms')),
            ('type', pa.dictionary(pa.int8(), pa.utf8())),
            ('type_crossref', pa.dictionary(pa.int8(), pa.utf8())),
            ('cited_by_count', pa.uint32()),
            ('num_authors', pa.uint16()),
            ('num_locations', pa.uint16()),
            ('num_references', pa.uint16()),
            ('language', pa.dictionary(pa.uint8(), pa.utf8())),
            ('has_complete_institution_info', pa.bool_()),
            ('has_grant_info', pa.bool_()),
            ('has_keywords', pa.bool_()),
            ('is_retracted', pa.bool_()),
            ('is_paratext', pa.bool_()),
            ('created_date', pa.timestamp('ns')),
            ('gz_path', pa.dictionary(pa.uint16(), pa.utf8())),
            # ('updated_date', pa.timestamp('ns')),
        ])
        write_args = dict(schema=schema)
        df = (
            df
            .assign(
                publication_date=lambda df_: pd.to_datetime(df_.publication_date, errors='coerce', **args),
                created_date=lambda df_: pd.to_datetime(df_.created_date, errors='coerce', **args),
                gz_path=parq_filename.stem,
                # updated_date=lambda df_: pd.to_datetime(df_.updated_date, errors='coerce', **args),
            )
            .astype(
                {'gz_path': 'category'},
            )
        )
        # don't set the index here
        # df.set_index('work_id', inplace=True)
        # df.sort_values(by='work_id', inplace=True)  # helps with setting the index later

    elif kind == 'authorships':
        schema = pa.schema([
            ('work_id', pa.int64()),
            ('author_position', pa.dictionary(pa.int8(), pa.utf8())),
            ('author_id', pa.int64()),
            ('author_name', pa.utf8()),
            ('raw_author_name', pa.utf8()),
            ('institution_lineage_level', pa.uint8()),
            ('assigned_institution', pa.bool_()),
            ('institution_id', pa.int64()),
            ('institution_name', pa.dictionary(pa.int32(), pa.utf8())),
            ('country_code', pa.dictionary(pa.uint8(), pa.utf8())),
            ('raw_affiliation_string', pa.utf8()),
            ('publication_year', pa.int16()),
            ('is_corresponding', pa.bool_()),
        ])
        write_args = dict(schema=schema)
        df.drop_duplicates(inplace=True)  # weird bug causes authorships table to have repeated rows sometimes

        if debug:
            print(f'{kind=} {parq_filename=} {len(rows)=:,}')
            sub_df = df.query('~institution_id.isna()')
            if len(sub_df) > 0:
                inst_names = sub_df.institution_name.unique()
                print(
                    f'{sub_df.shape[0]=:,}\t {sub_df.institution_id.nunique()=:,} {len(inst_names)=:,} {list(inst_names)[: 4]}')
                sub_df.to_csv('../data/sub_df.csv', index=False, mode='a', header=False, quoting=csv.QUOTE_ALL)

    elif kind == 'topics':
        schema = pa.schema([
            ('work_id', pa.int64()),
            ('publication_year', pa.int16()),
            ('is_primary_topic', pa.bool_()),
            ('score', pa.float32()),
            ('topic_id', pa.uint32()),
            ('topic_name', pa.dictionary(pa.uint16(), pa.utf8())),
            ('subfield_id', pa.uint32()),
            ('subfield_name', pa.dictionary(pa.uint8(), pa.utf8())),
            ('field_id', pa.uint32()),
            ('field_name', pa.dictionary(pa.uint16(), pa.utf8())),
            ('domain_id', pa.uint32()),
            ('domain_name', pa.dictionary(pa.int8(), pa.utf8())),
        ])
        write_args = dict(schema=schema)
        df.drop_duplicates(inplace=True)  # weird bug causes authorships table to have repeated rows sometimes

    # if kind == 'topics':
    #     print(f'Writing {kind=} {parq_filename.stem}\n{df.info()}\n'); df.to_csv(PARQ_DIR / 'temp' / f'{parq_filename.stem}.csv')
    df.to_parquet(parq_filename, engine='pyarrow', coerce_timestamps='ms', allow_truncated_timestamps=True,
                  **write_args)
    return True


def init_dict_writer(csv_file, file_spec, **kwargs):
    writer = csv.DictWriter(
        csv_file, fieldnames=file_spec['columns'], **kwargs
    )

    if os.stat(file_spec['name']).st_size == 0:  # write header only if file is empty
        writer.writeheader()
    return writer


def flatten_merged_entries(overwrite=False):
    """
    Flatten all merged entries into a single parquet file
    """
    merged_entries_path = SNAPSHOT_DIR / 'data' / 'merged_ids'
    kinds = list(map(lambda s: s.stem, merged_entries_path.glob('*')))
    for kind in tqdm(kinds, desc=f'Flattening merged entries'):
        merge_all_skip_ids(kind=kind, overwrite=False)


if __name__ == '__main__':
    start_time = time()
    print(f'Starting at {datetime.now().strftime("%c").strip()}')

    # # flatten_merged_entries()  # merges all skip_ids into a single parquet - RUN FIRST
    # # flatten_concepts()  # takes about 30s
    # # flatten_institutions()  # takes about 20s
    # # flatten_publishers()
    # # flatten_sources()
    #
    # # w/ abstracts => 200 lines/s
    #
    files_to_process = 'all'  # to do everything
    # files_to_process = 100 # or any other number
    #
    threads = 1
    # # recompute_tables = []
    # # recompute_tables = ['abstracts']
    #
    abstracts, overwrite = False, False
    #
    # # flatten_topics()
    # # flatten_authors(files_to_process=files_to_process)  # takes 6-7 hours for the whole thing! ~3 mins per file
    # # flatten_authors_concepts(files_to_process=files_to_process)
    # # flatten_authors_hints(files_to_process=files_to_process)
    #
    flatten_works_v3(
        files_to_process=files_to_process, threads=threads, make_abstracts=abstracts,
        overwrite=overwrite,
    )  # takes about 20 hours  ~6 mins per file

    print(f'End time: {datetime.now().strftime("%c").strip()}...', f'Time taken: {time() - start_time:.2f} seconds')
