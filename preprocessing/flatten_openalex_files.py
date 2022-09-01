"""
Flatten Openalex JSON line files into individual CSVs.
Original script at https://gist.github.com/richard-orr/152d828356a7c47ed7e3e22d2253708d
"""
import csv
import glob
import gzip
import os
import sys
from pathlib import Path

import orjson as json  # faster JSON library
from tqdm.auto import tqdm

sys.path.extend(['../', './'])
from src.utils import convert_openalex_id_to_int

basedir = Path('/N/project/openalex/ssikdar')
SNAPSHOT_DIR = basedir / 'openalex-snapshot'
CSV_DIR = basedir / 'csv-files-new'

# remove references to URLs and rename display_name to <entity>_name

csv_files = {
    'institutions': {
        'institutions': {
            'name': os.path.join(CSV_DIR, 'institutions.csv.gz'),
            'columns': [
                'institution_id', 'institution_name', 'ror', 'country_code', 'type', 'homepage_url',
                'display_name_acroynyms', 'display_name_alternatives', 'works_count', 'cited_by_count', 'updated_date'
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
                'institution_id', 'institution_name', 'city', 'geonames_city_id', 'region', 'country_code', 'country',
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
                'institution_id', 'institution_name', 'year', 'works_count', 'cited_by_count'
            ]
        }
    },

    'authors': {
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
        }
    },

    'concepts': {
        'concepts': {
            'name': os.path.join(CSV_DIR, 'concepts.csv.gz'),
            'columns': [
                'concept_id', 'concept_name', 'wikidata', 'level', 'description', 'works_count', 'cited_by_count',
                'updated_date'
            ]
        },
        'ancestors': {
            'name': os.path.join(CSV_DIR, 'concepts_ancestors.csv.gz'),
            'columns': ['concept_id', 'ancestor_id']
        },
        'counts_by_year': {
            'name': os.path.join(CSV_DIR, 'concepts_counts_by_year.csv.gz'),
            'columns': ['concept_id', 'concept_name', 'year', 'works_count', 'cited_by_count']
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

    'venues': {
        'venues': {
            'name': os.path.join(CSV_DIR, 'venues.csv.gz'),
            'columns': [
                'venue_id', 'issn_l', 'issn', 'venue_name', 'publisher', 'works_count', 'cited_by_count', 'is_oa',
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

    'works': {
        'works': {
            'name': os.path.join(CSV_DIR, 'works.csv.gz'),
            'columns': [
                'work_id', 'doi', 'title', 'publication_year', 'publication_date', 'type', 'cited_by_count',
                'is_retracted', 'is_paratext'
            ]
        },
        # Satyaki addition: put abstracts in a different CSV, save some space
        'abstracts': {
            'name': os.path.join(CSV_DIR, 'works_abstracts.csv.gz'),
            'columns': [
                'work_id', 'title', 'abstract'
            ]
        },
        'host_venues': {
            'name': os.path.join(CSV_DIR, 'works_host_venues.csv.gz'),
            'columns': [
                'work_id', 'venue_id', 'venue_name', 'url', 'is_oa', 'version', 'license'
            ]
        },
        'alternate_host_venues': {
            'name': os.path.join(CSV_DIR, 'works_alternate_host_venues.csv.gz'),
            'columns': [
                'work_id', 'venue_id', 'venue_name', 'url', 'is_oa', 'version', 'license'
            ]
        },
        'authorships': {
            'name': os.path.join(CSV_DIR, 'works_authorships.csv.gz'),
            'columns': [
                'publication_year', 'work_id', 'author_position', 'author_id', 'author_name', 'institution_id',
                'institution_name', 'raw_affiliation_string'
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
                'work_id', 'is_oa', 'oa_status', 'oa_url'
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
}


def flatten_concepts():
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
        for jsonl_file_name in tqdm(files, desc='Flatting concepts...', unit=' file'):
            with gzip.open(jsonl_file_name, 'r') as concepts_jsonl:
                for concept_json in concepts_jsonl:
                    if not concept_json.strip():
                        continue

                    concept = json.loads(concept_json)

                    if not (concept_id := concept.get('id')) or concept_id in seen_concept_ids:
                        continue

                    concept_id = convert_openalex_id_to_int(concept_id)  # convert to int
                    concept_name = concept['display_name']
                    seen_concept_ids.add(concept_id)

                    concept['concept_id'] = concept_id
                    concept['concept_name'] = concept_name
                    concepts_writer.writerow(concept)

                    if concept_ids := concept.get('ids'):
                        concept_ids['concept_id'] = concept_id
                        concept_ids['concept_name'] = concept_name
                        concept_ids['umls_aui'] = concept_ids.get('umls_aui')
                        concept_ids['umls_cui'] = concept_ids.get('umls_cui')
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


def flatten_venues():
    with gzip.open(csv_files['venues']['venues']['name'], 'wt', encoding='utf-8') as venues_csv, \
            gzip.open(csv_files['venues']['ids']['name'], 'wt', encoding='utf-8') as ids_csv, \
            gzip.open(csv_files['venues']['counts_by_year']['name'], 'wt', encoding='utf-8') as counts_by_year_csv:

        venues_writer = csv.DictWriter(
            venues_csv, fieldnames=csv_files['venues']['venues']['columns'], extrasaction='ignore'
        )
        venues_writer.writeheader()

        ids_writer = csv.DictWriter(ids_csv, fieldnames=csv_files['venues']['ids']['columns'])
        ids_writer.writeheader()

        counts_by_year_writer = csv.DictWriter(counts_by_year_csv,
                                               fieldnames=csv_files['venues']['counts_by_year']['columns'])
        counts_by_year_writer.writeheader()

        seen_venue_ids = set()

        files_done = 0
        for jsonl_file_name in glob.glob(os.path.join(SNAPSHOT_DIR, 'data', 'venues', '*', '*.gz')):
            print(jsonl_file_name)
            with gzip.open(jsonl_file_name, 'r') as venues_jsonl:
                for venue_json in venues_jsonl:
                    if not venue_json.strip():
                        continue

                    venue = json.loads(venue_json)

                    if not (venue_id := venue.get('id')) or venue_id in seen_venue_ids:
                        continue

                    seen_venue_ids.add(venue_id)

                    venue['issn'] = json.dumps(venue.get('issn'))
                    venues_writer.writerow(venue)

                    if venue_ids := venue.get('ids'):
                        venue_ids['venue_id'] = venue_id
                        venue_ids['issn'] = json.dumps(venue_ids.get('issn'))
                        ids_writer.writerow(venue_ids)

                    if counts_by_year := venue.get('counts_by_year'):
                        for count_by_year in counts_by_year:
                            count_by_year['venue_id'] = venue_id
                            counts_by_year_writer.writerow(count_by_year)

    return


def flatten_institutions():
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

        files_done = 0
        for jsonl_file_name in glob.glob(os.path.join(SNAPSHOT_DIR, 'data', 'institutions', '*', '*.gz')):
            print(jsonl_file_name)
            with gzip.open(jsonl_file_name, 'r') as institutions_jsonl:
                for institution_json in institutions_jsonl:
                    if not institution_json.strip():
                        continue

                    institution = json.loads(institution_json)

                    if not (institution_id := institution.get('id')) or institution_id in seen_institution_ids:
                        continue

                    seen_institution_ids.add(institution_id)

                    # institutions
                    institution['display_name_acroynyms'] = json.dumps(institution.get('display_name_acroynyms'))
                    institution['display_name_alternatives'] = json.dumps(institution.get('display_name_alternatives'))
                    institutions_writer.writerow(institution)

                    # ids
                    if institution_ids := institution.get('ids'):
                        institution_ids['institution_id'] = institution_id
                        ids_writer.writerow(institution_ids)

                    # geo
                    if institution_geo := institution.get('geo'):
                        institution_geo['institution_id'] = institution_id
                        geo_writer.writerow(institution_geo)

                    # associated_institutions
                    if associated_institutions := institution.get(
                            'associated_institutions', institution.get('associated_insitutions')  # typo in api
                    ):
                        for associated_institution in associated_institutions:
                            if associated_institution_id := associated_institution.get('id'):
                                associated_institutions_writer.writerow({
                                    'institution_id': institution_id,
                                    'associated_institution_id': associated_institution_id,
                                    'relationship': associated_institution.get('relationship')
                                })

                    # counts_by_year
                    if counts_by_year := institution.get('counts_by_year'):
                        for count_by_year in counts_by_year:
                            count_by_year['institution_id'] = institution_id
                            counts_by_year_writer.writerow(count_by_year)

            files_done += 1
    return


def flatten_authors():
    file_spec = csv_files['authors']

    with gzip.open(file_spec['authors']['name'], 'wt', encoding='utf-8') as authors_csv, \
            gzip.open(file_spec['ids']['name'], 'wt', encoding='utf-8') as ids_csv, \
            gzip.open(file_spec['counts_by_year']['name'], 'wt', encoding='utf-8') as counts_by_year_csv:

        authors_writer = csv.DictWriter(
            authors_csv, fieldnames=file_spec['authors']['columns'], extrasaction='ignore'
        )
        authors_writer.writeheader()

        ids_writer = csv.DictWriter(ids_csv, fieldnames=file_spec['ids']['columns'])
        ids_writer.writeheader()

        counts_by_year_writer = csv.DictWriter(counts_by_year_csv, fieldnames=file_spec['counts_by_year']['columns'])
        counts_by_year_writer.writeheader()

        files_done = 0
        for jsonl_file_name in glob.glob(os.path.join(SNAPSHOT_DIR, 'data', 'authors', '*', '*.gz')):
            print(jsonl_file_name)
            with gzip.open(jsonl_file_name, 'r') as authors_jsonl:
                for author_json in authors_jsonl:
                    if not author_json.strip():
                        continue

                    author = json.loads(author_json)

                    if not (author_id := author.get('id')):
                        continue

                    # authors
                    author['display_name_alternatives'] = json.dumps(author.get('display_name_alternatives'))
                    author['last_known_institution'] = (author.get('last_known_institution') or {}).get('id')
                    authors_writer.writerow(author)

                    # ids
                    if author_ids := author.get('ids'):
                        author_ids['author_id'] = author_id
                        ids_writer.writerow(author_ids)

                    # counts_by_year
                    if counts_by_year := author.get('counts_by_year'):
                        for count_by_year in counts_by_year:
                            count_by_year['author_id'] = author_id
                            counts_by_year_writer.writerow(count_by_year)
            files_done += 1
    return


def flatten_works():
    file_spec = csv_files['works']

    with gzip.open(file_spec['works']['name'], 'wt', encoding='utf-8') as works_csv, \
            gzip.open(file_spec['host_venues']['name'], 'wt', encoding='utf-8') as host_venues_csv, \
            gzip.open(file_spec['alternate_host_venues']['name'], 'wt', encoding='utf-8') as alternate_host_venues_csv, \
            gzip.open(file_spec['authorships']['name'], 'wt', encoding='utf-8') as authorships_csv, \
            gzip.open(file_spec['biblio']['name'], 'wt', encoding='utf-8') as biblio_csv, \
            gzip.open(file_spec['concepts']['name'], 'wt', encoding='utf-8') as concepts_csv, \
            gzip.open(file_spec['ids']['name'], 'wt', encoding='utf-8') as ids_csv, \
            gzip.open(file_spec['mesh']['name'], 'wt', encoding='utf-8') as mesh_csv, \
            gzip.open(file_spec['open_access']['name'], 'wt', encoding='utf-8') as open_access_csv, \
            gzip.open(file_spec['referenced_works']['name'], 'wt', encoding='utf-8') as referenced_works_csv, \
            gzip.open(file_spec['related_works']['name'], 'wt', encoding='utf-8') as related_works_csv:

        works_writer = init_dict_writer(works_csv, file_spec['works'], extrasaction='ignore')
        host_venues_writer = init_dict_writer(host_venues_csv, file_spec['host_venues'])
        alternate_host_venues_writer = init_dict_writer(alternate_host_venues_csv, file_spec['alternate_host_venues'])
        authorships_writer = init_dict_writer(authorships_csv, file_spec['authorships'])
        biblio_writer = init_dict_writer(biblio_csv, file_spec['biblio'])
        concepts_writer = init_dict_writer(concepts_csv, file_spec['concepts'])
        ids_writer = init_dict_writer(ids_csv, file_spec['ids'], extrasaction='ignore')
        mesh_writer = init_dict_writer(mesh_csv, file_spec['mesh'])
        open_access_writer = init_dict_writer(open_access_csv, file_spec['open_access'])
        referenced_works_writer = init_dict_writer(referenced_works_csv, file_spec['referenced_works'])
        related_works_writer = init_dict_writer(related_works_csv, file_spec['related_works'])

        files_done = 0
        for jsonl_file_name in glob.glob(os.path.join(SNAPSHOT_DIR, 'data', 'works', '*', '*.gz')):
            print(jsonl_file_name)
            with gzip.open(jsonl_file_name, 'r') as works_jsonl:
                for work_json in works_jsonl:
                    if not work_json.strip():
                        continue

                    work = json.loads(work_json)

                    if not (work_id := work.get('id')):
                        continue

                    # works
                    if (abstract := work.get('abstract_inverted_index')) is not None:
                        work['abstract_inverted_index'] = json.dumps(abstract)

                    works_writer.writerow(work)

                    # host_venues
                    if host_venue := (work.get('host_venue') or {}):
                        if host_venue_id := host_venue.get('id'):
                            host_venues_writer.writerow({
                                'work_id': work_id,
                                'venue_id': host_venue_id,
                                'url': host_venue.get('url'),
                                'is_oa': host_venue.get('is_oa'),
                                'version': host_venue.get('version'),
                                'license': host_venue.get('license'),
                            })

                    # alternate_host_venues
                    if alternate_host_venues := work.get('alternate_host_venues'):
                        for alternate_host_venue in alternate_host_venues:
                            if venue_id := alternate_host_venue.get('id'):
                                alternate_host_venues_writer.writerow({
                                    'work_id': work_id,
                                    'venue_id': venue_id,
                                    'url': alternate_host_venue.get('url'),
                                    'is_oa': alternate_host_venue.get('is_oa'),
                                    'version': alternate_host_venue.get('version'),
                                    'license': alternate_host_venue.get('license'),
                                })

                    # authorships
                    if authorships := work.get('authorships'):
                        for authorship in authorships:
                            if author_id := authorship.get('author', {}).get('id'):
                                institutions = authorship.get('institutions')
                                institution_ids = [i.get('id') for i in institutions]
                                institution_ids = [i for i in institution_ids if i]
                                institution_ids = institution_ids or [None]

                                for institution_id in institution_ids:
                                    authorships_writer.writerow({
                                        'work_id': work_id,
                                        'author_position': authorship.get('author_position'),
                                        'author_id': author_id,
                                        'institution_id': institution_id,
                                        'raw_affiliation_string': authorship.get('raw_affiliation_string'),
                                    })

                    # biblio
                    if biblio := work.get('biblio'):
                        biblio['work_id'] = work_id
                        biblio_writer.writerow(biblio)

                    # concepts
                    for concept in work.get('concepts'):
                        if concept_id := concept.get('id'):
                            concepts_writer.writerow({
                                'work_id': work_id,
                                'concept_id': concept_id,
                                'score': concept.get('score'),
                            })

                    # ids
                    if ids := work.get('ids'):
                        ids['work_id'] = work_id
                        ids_writer.writerow(ids)

                    # mesh
                    for mesh in work.get('mesh'):
                        mesh['work_id'] = work_id
                        mesh_writer.writerow(mesh)

                    # open_access
                    if open_access := work.get('open_access'):
                        open_access['work_id'] = work_id
                        open_access_writer.writerow(open_access)

                    # referenced_works
                    for referenced_work in work.get('referenced_works'):
                        if referenced_work:
                            referenced_works_writer.writerow({
                                'work_id': work_id,
                                'referenced_work_id': referenced_work
                            })

                    # related_works
                    for related_work in work.get('related_works'):
                        if related_work:
                            related_works_writer.writerow({
                                'work_id': work_id,
                                'related_work_id': related_work
                            })

            files_done += 1
    return


def init_dict_writer(csv_file, file_spec, **kwargs):
    writer = csv.DictWriter(
        csv_file, fieldnames=file_spec['columns'], **kwargs
    )
    writer.writeheader()
    return writer


if __name__ == '__main__':
    flatten_concepts()  # takes about 30s
    # flatten_venues()
    # flatten_institutions()
    # flatten_authors()
    # flatten_works()
