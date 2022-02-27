"""
Make individual classes
"""
import abc
import gzip
from pathlib import Path
from typing import List, Dict

import orjson as json
import pandas as pd
from box import Box
from joblib import Parallel, delayed
from tqdm.auto import tqdm

from src.globals import path_type
from src.utils import Paths, read_manifest, ensure_dir


class Entities:
    """
    Base class for all entities
    """

    def __init__(self, kind: str, paths: Paths):
        self.kind: str = kind
        self.schema: Box = self.get_schema()
        self.paths = paths
        self.parquet_path: Path = self.paths.processed_dir / f'{self.kind}.parquet'
        self.manifest = read_manifest(kind=self.kind, paths=self.paths)
        self.finished_files_path = self.paths.temp_dir / f'{self.kind}.txt'
        self.finished_files: List[path_type] = self.get_finished_files()  # stores the list of finished entities
        self.dtypes = {}  # dtype dictionary
        self.init_dtype_dicts()
        return

    @abc.abstractmethod
    def init_dtype_dicts(self):
        pass

    def process(self, num_workers: int, max_len=None):
        """
        Process entries from the manifest
        :return:
        """
        if max_len is not None:
            entries = self.manifest.entries[: max_len]
        else:
            entries = self.manifest.entries

        Parallel(backend='multiprocessing', n_jobs=num_workers)(
            delayed(self.process_entry)(entry) for entry in entries
        )

        return

    def process_entry(self, entry: Box) -> None:
        """
        Process individual entries in the manifest
        :return:
        """

        if str(entry.updated_date) in self.get_finished_files():
            # tqdm.write(f'Skipping {self.kind!r} {entry.updated_date!r}!')
            return

        tqdm.write(f'\nProcessing {self.kind!r} {entry.updated_date!r}')
        rows_dict = {col: [] for col in self.dtypes}

        with tqdm(total=entry.count, ncols=100, colour='blue', position=2, unit='lines') as pbar:
            with gzip.open(entry.filename) as fp:
                for line in fp:
                    json_line = json.loads(line)
                    json_rows_dict = self.process_json(json_line)  # keys are table names, vals are list of dicts
                    for name, rows in json_rows_dict.items():
                        rows_dict[name].extend(rows)
                    pbar.update(1)

        for table_name, rows in rows_dict.items():
            self.write_to_disk(table_name=table_name, updated_date=entry.updated_date, rows=rows, verbose=False)
        return rows

    @abc.abstractmethod
    def process_json(self, jsons: Dict):
        """
        Process JSONs and write to a parquet file
        :return:
        """
        pass

    def __len__(self) -> int:
        return self.manifest.len

    def __str__(self) -> str:
        st = f'<{self.kind!r} entries: {len(self.manifest.entries):,} total records: {len(self):,}>'
        return st

    def get_schema(self) -> Box:
        schema = Box({
            'institutions': {
                'institutions': {
                    'columns': [
                        'id', 'ror', 'display_name', 'country_code', 'type', 'homepage_url', 'image_url',
                        'image_thumbnail_url',
                        'display_name_acroynyms', 'display_name_alternatives', 'works_count', 'cited_by_count',
                        'works_api_url',
                        'updated_date'
                    ]
                },
                'ids': {
                    'columns': [
                        'institution_id', 'openalex', 'ror', 'grid', 'wikipedia', 'wikidata', 'mag'
                    ]
                },
                'geo': {
                    'columns': [
                        'institution_id', 'city', 'geonames_city_id', 'region', 'country_code', 'country', 'latitude',
                        'longitude'
                    ]
                },
                'associated_institutions': {
                    'columns': [
                        'institution_id', 'associated_institution_id', 'relationship'
                    ]
                },
                'counts_by_year': {
                    'columns': [
                        'institution_id', 'year', 'works_count', 'cited_by_count'
                    ]
                }
            },
            'authors': {
                'authors': {
                    'columns': [
                        'id', 'orcid', 'display_name', 'display_name_alternatives', 'works_count', 'cited_by_count',
                        'last_known_institution', 'works_api_url', 'updated_date'
                    ]
                },
                'ids': {
                    'columns': [
                        'author_id', 'openalex', 'orcid', 'scopus', 'twitter', 'wikipedia', 'mag'
                    ]
                },
                'counts_by_year': {
                    'columns': [
                        'author_id', 'year', 'works_count', 'cited_by_count'
                    ]
                }
            },
            'concepts': {
                'concepts': {
                    'columns': [
                        'id', 'wikidata', 'display_name', 'level', 'description', 'works_count', 'cited_by_count',
                        'image_url',
                        'image_thumbnail_url', 'works_api_url', 'updated_date'
                    ]
                },
                'ancestors': {
                    'columns': ['concept_id', 'ancestor_id']
                },
                'counts_by_year': {
                    'columns': ['concept_id', 'year', 'works_count', 'cited_by_count']
                },
                'ids': {
                    'columns': ['concept_id', 'openalex', 'wikidata', 'wikipedia', 'umls_aui', 'umls_cui', 'mag']
                },
                'related_concepts': {
                    'columns': ['concept_id', 'related_concept_id', 'score']
                }
            },
            'venues': {
                'venues': {
                    'columns': [
                        'id', 'issn_l', 'issn', 'display_name', 'publisher', 'works_count', 'cited_by_count', 'is_oa',
                        'is_in_doaj', 'homepage_url', 'works_api_url', 'updated_date'
                    ]
                },
                'ids': {
                    'columns': ['venue_id', 'openalex', 'issn_l', 'issn', 'mag']
                },
                'counts_by_year': {
                    'columns': ['venue_id', 'year', 'works_count', 'cited_by_count']
                },
            },
            'works': {
                'works': {
                    'columns': [
                        'id', 'doi', 'title', 'display_name', 'publication_year', 'publication_date', 'type',
                        'cited_by_count',
                        'is_retracted', 'is_paratext', 'cited_by_api_url', 'abstract_inverted_index'
                    ]
                },
                'host_venues': {
                    'columns': [
                        'work_id', 'venue_id', 'url', 'is_oa', 'version', 'license'
                    ]
                },
                'alternate_host_venues': {
                    'columns': [
                        'work_id', 'venue_id', 'url', 'is_oa', 'version', 'license'
                    ]
                },
                'authorships': {
                    'columns': [
                        'work_id', 'author_position', 'author_id', 'institution_id', 'raw_affiliation_string'
                    ]
                },
                'biblio': {
                    'columns': [
                        'work_id', 'volume', 'issue', 'first_page', 'last_page'
                    ]
                },
                'concepts': {
                    'columns': [
                        'work_id', 'concept_id', 'score'
                    ]
                },
                'ids': {
                    'columns': [
                        'work_id', 'openalex', 'doi', 'mag', 'pmid', 'pmcid'
                    ]
                },
                'mesh': {
                    'columns': [
                        'work_id', 'descriptor_ui', 'descriptor_name', 'qualifier_ui', 'qualifier_name',
                        'is_major_topic'
                    ]
                },
                'open_access': {
                    'columns': [
                        'work_id', 'is_oa', 'oa_status', 'oa_url'
                    ]
                },
                'referenced_works': {
                    'columns': [
                        'work_id', 'referenced_work_id'
                    ]
                },
                'related_works': {
                    'columns': [
                        'work_id', 'related_work_id'
                    ]
                },
            },
        })
        return schema[self.kind]

    def get_finished_files(self) -> List[path_type]:
        finished_files = [file.stem for file in (self.paths.processed_dir / self.kind).glob('*.pq')]
        return finished_files

    def write_to_disk(self, table_name: str, updated_date: str, rows: List, fmt: str = 'parquet', verbose: bool = False,
                      **args):
        """
        Write list of rows to path. Formats could be parquet or hdf5.
        Path is obtained from Paths object processed directory
        """

        df = pd.DataFrame(rows, columns=self.dtypes[table_name])

        df = df.astype(dtype=self.dtypes[table_name])

        if fmt == 'hdf' or fmt == 'hdf5':
            ext = 'h5'
            min_itemsize = self.min_sizes[table_name]
            writer_fn = pd.DataFrame.to_hdf
            fun_args = dict()
        else:  # parquet
            ext = '.pq'
            writer_fn = pd.DataFrame.to_parquet
            fun_args = dict(engine='pyarrow')

        path = self.paths.processed_dir / table_name / f'{updated_date}{ext}'
        ensure_dir(path.parents[0], recursive=True)  # makes sure the parent dirs exist
        if verbose:
            tqdm.write(f'Writing {table_name!r} {len(df):,} rows to {path}')
        writer_fn(df, path=path, **fun_args, **args)
        return

    def find_missing_tables(self):
        """
        Validate the entries from the manifest and generate a small report
        :return:
        """
        table_names = self.dtypes.keys()
        missing_tables = {}
        for fname in (self.paths.processed_dir / self.kind).glob('*.pq'):
            updated_date = fname.stem
            for table_name in table_names:
                parq_filename = self.paths.processed_dir / table_name / f'{updated_date}.pq'
                if not parq_filename.exists():
                    if updated_date not in missing_tables:
                        missing_tables[updated_date] = []
                    missing_tables[updated_date].append(table_name)

        return missing_tables

    def compute_missing_tables(self):
        """
        Compute missing entries
        """
        missing_tables = self.find_missing_tables()
        print(f'\n\nMissing tables for {len(missing_tables)} entries')

        for updated_dated_info, missing_table_names in missing_tables.items():
            parts = updated_dated_info.split('_')
            updated_date = '_'.join(parts[: 2])
            gz_filename = self.paths.snapshot_dir / self.kind / updated_date / ('_'.join(parts[2:]) + '.gz')

            rows_dict = {col: [] for col in missing_table_names}
            tqdm.write(f'\nProcessing {missing_table_names} {gz_filename}')
            with gzip.open(gz_filename) as fp:
                for line in fp:
                    json_line = json.loads(line)
                    json_rows_dict = self.process_json(json_line)
                    for name, rows in json_rows_dict.items():
                        if name not in missing_table_names:
                            continue
                        rows_dict[name].extend(rows)

            for table_name, rows in rows_dict.items():
                self.write_to_disk(table_name=table_name, updated_date=updated_dated_info, rows=rows, verbose=True)
            # break
        return

    def validate_tables(self):
        """
        Validate individual parquet files with the manifests -- record counts should match up for the main table
        """
        for entry in tqdm(self.manifest.entries, unit='entries', ncols=100, colour='cyan'):
            parq_df = pd.read_parquet(self.paths.processed_dir / self.kind / f'{entry.updated_date}.pq')
            assert len(parq_df) == entry.count, f'Incorrect count: {len(parq_df)=:,} != {entry.count=:,}'
        print(f'Individual parquets check out for {self.kind!r}!')
        return


class Authors(Entities):
    def __init__(self, paths: Paths):
        super().__init__(kind='authors', paths=paths)
        self.min_sizes = {}  # dictionary of min item sizes for string columns for HDF5
        self.init_dtype_dicts()
        return

    def init_dtype_dicts(self):
        """
        Initialize data type dictionaries
        :return:
        """
        self.dtypes['authors'] = {'id': 'string', 'orcid': 'string', 'display_name': 'string',
                                  'display_name_alternatives': 'string', 'works_count': 'Int64',
                                  'cited_by_count': 'Int64', 'last_known_institution': 'string',
                                  'works_api_url': 'string', 'updated_date': 'string'}

        # self.min_sizes['authors'] = {'id': ID_LEN, 'orcid': ID_LEN, 'display_name': NAME_LEN,
        #                              'display_name_alternatives': LIST_LEN * 2,
        #                              'last_known_institution': ID_LEN, 'works_api_url': URL_LEN, 'updated_date': ID_LEN}

        self.dtypes['authors_ids'] = {'author_id': 'string', 'openalex': 'string', 'orcid': 'string',
                                      'scopus': 'string', 'twitter': 'string', 'wikipedia': 'string', 'mag': 'string'}
        # self.min_sizes['authors_ids'] = {'author_id': ID_LEN, 'openalex': ID_LEN, 'orcid': ID_LEN, 'scopus': ID_LEN,
        #                                  'twitter': ID_LEN, 'wikipedia': ID_LEN, 'mag': ID_LEN}

        self.dtypes['authors_counts_by_year'] = {'author_id': 'string', 'year': 'Int64', 'works_count': 'Int64',
                                                 'cited_by_count': 'Int64'}
        # self.min_sizes['authors_counts_by_year'] = {'author_id': ID_LEN}

        return

    def process_json(self, author_json: Dict):
        author_cols, id_cols, yearly_counts_cols = self.schema.authors.columns, self.schema.ids.columns, self.schema.counts_by_year.columns
        author_rows, ids_rows, yearly_counts_rows = [], [], []

        if not (author_id := author_json.get('id')):
            return author_rows, ids_rows, yearly_counts_rows

        author_json['display_name_alternatives'] = json.dumps(author_json.get('display_name_alternatives'))
        author_json['last_known_institution'] = (author_json.get('last_known_institution') or {}).get('id', '')
        author_json['orcid'] = str(author_json.get('orcid', ''))
        author_json['display_name'] = str(author_json.get('display_name', ''))
        author_row = {col: author_json.get(col, '') for col in author_cols}
        author_rows.append(author_row)

        if author_ids := author_json.get('ids'):
            author_ids['author_id'] = author_id
            author_ids_row = {col: str(author_ids.get(col, '')) for col in id_cols}  # force ids to be strings
            ids_rows.append(author_ids_row)

        # counts_by_year
        if counts_by_year := author_json.get('counts_by_year'):
            for count_by_year in counts_by_year:
                count_by_year['author_id'] = author_id
                count_row = {col: count_by_year.get(col, '') for col in yearly_counts_cols}
                yearly_counts_rows.append(count_row)

        return {'authors': author_rows, 'authors_ids': ids_rows, 'authors_counts_by_year': yearly_counts_rows}


class Works(Entities):
    def __init__(self, paths: Paths):
        super().__init__(kind='works', paths=paths)
        self.init_dtype_dicts()
        return

    def process_json(self, work_json):
        work_cols = self.schema.works.columns

        work_rows, host_venue_rows, alt_host_venue_rows, auths_rows, concepts_rows, ids_rows, mesh_rows, ref_rows, \
        rel_rows = [], [], [], [], [], [], [], [], []

        if not (work_id := work_json.get('id')):
            return {'works': work_rows, 'works_host_venues': host_venue_rows,
                    'works_alternate_host_venues': alt_host_venue_rows, 'works_authorships': auths_rows,
                    'works_concepts': concepts_rows, 'works_ids': ids_rows, 'works_mesh': mesh_rows,
                    'works_referenced_works': ref_rows, 'works_related_works': rel_rows}

        if (abstract := work_json.get('abstract_inverted_index')) is not None:
            work_json['abstract_inverted_index'] = json.dumps(abstract)
        work_row = {col: work_json.get(col, '') for col in work_cols}
        work_rows.append(work_row)

        # host venues
        if host_venue := (work_json.get('host_venue') or {}):
            if host_venue_id := host_venue.get('id'):
                host_venue_rows.append({
                    'work_id': work_id,
                    'venue_id': host_venue_id,
                    'url': host_venue.get('url', ''),
                    'is_oa': host_venue.get('is_oa', ''),
                    'version': host_venue.get('version', ''),
                    'license': host_venue.get('license', ''),
                })

        # alt host venues
        if alternate_host_venues := work_json.get('alternate_host_venues'):
            for alternate_host_venue in alternate_host_venues:
                if venue_id := alternate_host_venue.get('id'):
                    alt_host_venue_rows.append({
                        'work_id': work_id,
                        'venue_id': venue_id,
                        'url': alternate_host_venue.get('url', ''),
                        'is_oa': alternate_host_venue.get('is_oa', ''),
                        'version': alternate_host_venue.get('version', ''),
                        'license': alternate_host_venue.get('license', ''),
                    })

        # authorships
        if authorships := work_json.get('authorships'):
            for authorship in authorships:
                if author_id := authorship.get('author', {}).get('id'):
                    institutions = authorship.get('institutions')
                    institution_ids = [i.get('id') for i in institutions]
                    institution_ids = [i for i in institution_ids if i]
                    institution_ids = institution_ids or [None]

                    for institution_id in institution_ids:
                        auths_rows.append({
                            'work_id': work_id,
                            'author_position': authorship.get('author_position', ''),
                            'author_id': author_id,
                            'institution_id': institution_id,
                            'raw_affiliation_string': authorship.get('raw_affiliation_string', ''),
                        })

        # concepts
        for concept in work_json.get('concepts'):
            if concept_id := concept.get('id'):
                concepts_rows.append({
                    'work_id': work_id,
                    'concept_id': concept_id,
                    'score': concept.get('score'),
                })

        # ids
        if ids := work_json.get('ids'):
            ids['work_id'] = work_id
            ids_rows.append(ids)

        # mesh
        for mesh in work_json.get('mesh'):
            mesh['work_id'] = work_id
            mesh_rows.append(mesh)

        # referenced_works
        for referenced_work in work_json.get('referenced_works'):
            if referenced_work:
                ref_rows.append({
                    'work_id': work_id,
                    'referenced_work_id': referenced_work
                })

        # related_works
        for related_work in work_json.get('related_works'):
            if related_work:
                rel_rows.append({
                    'work_id': work_id,
                    'related_work_id': related_work
                })

        return {'works': work_rows, 'works_host_venues': host_venue_rows,
                'works_alternate_host_venues': alt_host_venue_rows, 'works_authorships': auths_rows,
                'works_concepts': concepts_rows, 'works_ids': ids_rows, 'works_mesh': mesh_rows,
                'works_referenced_works': ref_rows, 'works_related_works': rel_rows}

    def init_dtype_dicts(self):
        self.dtypes['works'] = {
            'id': 'string', 'doi': 'string', 'title': 'string', 'display_name': 'string', 'publication_year': 'Int64',
            'publication_date': 'string', 'type': 'string', 'cited_by_count': 'Int64',
            'is_retracted': 'bool', 'is_paratext': 'bool', 'cited_by_api_url': 'string',
            'abstract_inverted_index': 'string'}

        self.dtypes['works_host_venues'] = {
            'work_id': 'string', 'venue_id': 'string', 'url': 'string', 'is_oa': 'bool', 'version': 'string',
            'license': 'string'}

        self.dtypes['works_ids'] = {
            'work_id': 'string', 'openalex': 'string', 'doi': 'string', 'mag': 'string', 'pmid': 'string',
            'pmcid': 'string'
        }

        self.dtypes['works_alternate_host_venues'] = {
            'work_id': 'string', 'venue_id': 'string', 'url': 'string', 'is_oa': 'bool', 'version': 'string',
            'license': 'string'}

        self.dtypes['works_authorships'] = {
            'work_id': 'string', 'author_position': 'string', 'author_id': 'string', 'institution_id': 'string',
            'raw_affiliation_string': 'string'
        }

        self.dtypes['works_concepts'] = {
            'work_id': 'string', 'concept_id': 'string', 'score': 'float'
        }

        self.dtypes['works_mesh'] = {
            'work_id': 'string', 'descriptor_ui': 'string', 'descriptor_name': 'string', 'qualifier_ui': 'string',
            'qualifier_name': 'string', 'is_major_topic': 'bool'
        }

        self.dtypes['works_referenced_works'] = {
            'work_id': 'string', 'referenced_work_id': 'string'
        }

        self.dtypes['works_related_works'] = {
            'work_id': 'string', 'related_work_id': 'string'
        }
        return

    def flatten(self):
        """
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
if FILES_PER_ENTITY and files_done >= FILES_PER_ENTITY:
break
        """


class Institutions(Entities):
    def __init__(self, paths: Paths):
        super().__init__(kind='institutions', paths=paths)
        self.init_dtype_dicts()
        return

    def flatten(self):
        """
        for institution_json in institutions_jsonl:
            if not institution_json.strip():
                continue

            institution = json.loads(institution_json)

            if not (institution_id := institution.get('id')) or institution_id in seen_institution_ids:
                continue

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

        """

    def process_json(self, inst_json: Dict):
        inst_rows, ids_rows, geo_rows, assoc_rows, counts_rows = [], [], [], [], []
        inst_cols = self.schema.institutions.columns
        if not (institution_id := inst_json.get('id')):
            return {'institutions': inst_rows, 'institutions_ids': ids_rows, 'institutions_geo': geo_rows,
                    'institutions_associated_institutions': geo_rows, 'institutions_counts_by_year': counts_rows}

        # institutions
        inst_json['display_name_acroynyms'] = json.dumps(inst_json.get('display_name_acroynyms'))
        inst_json['display_name_alternatives'] = json.dumps(inst_json.get('display_name_alternatives'))
        inst_rows.append({col: inst_json.get(col) for col in inst_cols})

        # ids
        if institution_ids := inst_json.get('ids'):
            institution_ids['institution_id'] = institution_id
            ids_rows.append(institution_ids)

        # geo
        if institution_geo := inst_json.get('geo'):
            institution_geo['institution_id'] = institution_id
            geo_rows.append(institution_geo)

            # associated_institutions
            if associated_institutions := inst_json.get(
                    'associated_institutions', inst_json.get('associated_insitutions')  # typo in api
            ):
                for associated_institution in associated_institutions:
                    if associated_institution_id := associated_institution.get('id'):
                        assoc_rows.append({
                            'institution_id': institution_id,
                            'associated_institution_id': associated_institution_id,
                            'relationship': associated_institution.get('relationship')
                        })

            # counts_by_year
            if counts_by_year := inst_json.get('counts_by_year'):
                for count_by_year in counts_by_year:
                    count_by_year['institution_id'] = institution_id
                    counts_rows.append(count_by_year)

        return {'institutions': inst_rows, 'institutions_ids': ids_rows, 'institutions_geo': geo_rows,
                    'institutions_associated_institutions': geo_rows, 'institutions_counts_by_year': counts_rows}

    def init_dtype_dicts(self):
        self.dtypes['institutions'] = {
            'id': 'string', 'ror': 'string', 'display_name': 'string', 'country_code': 'string', 'type': 'string',
            'homepage_url': 'string', 'image_url': 'string', 'image_thumbnail_url': 'string',
            'display_name_acroynyms': 'string', 'display_name_alternatives': 'string', 'works_count': 'Int64',
            'cited_by_count': 'Int64', 'works_api_url': 'string', 'updated_date': 'string'
        }

        self.dtypes['institutions_ids'] = {
            'institution_id': 'string', 'openalex': 'string', 'ror': 'string', 'grid': 'string',
            'wikipedia': 'string', 'wikidata': 'string', 'mag': 'string'
        }

        self.dtypes['institutions_geo'] = {
            'institution_id': 'string', 'city': 'string', 'geonames_city_id': 'string', 'region': 'string',
            'country_code': 'string', 'country': 'string', 'latitude': 'float', 'longitude': 'float'
        }

        self.dtypes['institutions_associated_institutions'] = {
            'institution_id': 'string', 'associated_institution_id': 'string', 'relationship': 'string'
        }

        self.dtypes['institutions_counts_by_year'] = {
            'institution_id': 'string', 'year': 'Int64', 'works_count': 'Int64', 'cited_by_count': 'Int64'
        }
        return


class Concepts(Entities):
    def __init__(self, paths: Paths):
        super().__init__(kind='concepts', paths=paths)
        return

    def process_json(self, concept_json: Dict):
        concepts_rows, ids_rows, ancestor_rows, counts_rows, related_rows = [], [], [], [], []
        concepts_cols = self.schema.concepts.columns
        if not (concept_id := concept_json.get('id')):
            return {'concepts': concepts_rows, 'concepts_ids': ids_rows, 'concepts_ancestors': ancestor_rows,
                    'concepts_counts_by_year': counts_rows, 'concepts_related_concepts': related_rows}

        # concepts
        concepts_rows.append({col: concept_json.get(col) for col in concepts_cols})

        # ids
        if concept_ids := concept_json.get('ids'):
            concept_ids['concept_id'] = concept_id
            concept_ids['umls_aui'] = json.dumps(concept_ids.get('umls_aui'))
            concept_ids['umls_cui'] = json.dumps(concept_ids.get('umls_cui'))
            ids_rows.append(concept_ids)

        # ancestors
        if ancestors := concept_json.get('ancestors'):
            for ancestor in ancestors:
                if ancestor_id := ancestor.get('id'):
                    ancestor_rows.append({
                        'concept_id': concept_id,
                        'ancestor_id': ancestor_id
                    })

        # counts by year
        if counts_by_year := concept_json.get('counts_by_year'):
            for count_by_year in counts_by_year:
                count_by_year['concept_id'] = concept_id
                counts_rows.append(count_by_year)

        # related concepts
        if related_concepts := concept_json.get('related_concepts'):
            for related_concept in related_concepts:
                if related_concept_id := related_concept.get('id'):
                    related_rows.append({
                        'concept_id': concept_id,
                        'related_concept_id': related_concept_id,
                        'score': related_concept.get('score')
                    })

        return {'concepts': concepts_rows, 'concepts_ids': ids_rows, 'concepts_ancestors': ancestor_rows,
                'concepts_counts_by_year': counts_rows, 'concepts_related_concepts': related_rows}

    def flatten(self):
        """
        for concept_json in concepts_jsonl:
            if not concept_json.strip():
                continue

            concept = json.loads(concept_json)

            if not (concept_id := concept.get('id')) or concept_id in seen_concept_ids:
                continue

            seen_concept_ids.add(concept_id)

            concepts_writer.writerow(concept)

            if concept_ids := concept.get('ids'):
                concept_ids['concept_id'] = concept_id
                concept_ids['umls_aui'] = json.dumps(concept_ids.get('umls_aui'))
                concept_ids['umls_cui'] = json.dumps(concept_ids.get('umls_cui'))
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
                    counts_by_year_writer.writerow(count_by_year)

            if related_concepts := concept.get('related_concepts'):
                for related_concept in related_concepts:
                    if related_concept_id := related_concept.get('id'):
                        related_concepts_writer.writerow({
                            'concept_id': concept_id,
                            'related_concept_id': related_concept_id,
                            'score': related_concept.get('score')
                        })
        """

    def init_dtype_dicts(self):
        self.dtypes['concepts'] = {
            'id': 'string', 'wikidata': 'string', 'display_name': 'string', 'level': 'Int64', 'description': 'string',
            'works_count': 'Int64', 'cited_by_count': 'Int64', 'image_url': 'string',
            'image_thumbnail_url': 'string', 'works_api_url': 'string', 'updated_date': 'string'
        }

        self.dtypes['concepts_ancestors'] = {
            'concept_id': 'string', 'ancestor_id': 'string'
        }

        self.dtypes['concepts_counts_by_year'] = {
            'concept_id': 'string', 'year': 'Int64', 'works_count': 'Int64', 'cited_by_count': 'Int64'
        }

        self.dtypes['concepts_ids'] = {
            'concept_id': 'string', 'openalex': 'string', 'wikidata': 'string', 'wikipedia': 'string',
            'umls_aui': 'string', 'umls_cui': 'string', 'mag': 'string'
        }

        self.dtypes['concepts_related_concepts'] = {
            'concept_id': 'string', 'related_concept_id': 'string', 'score': 'float'
        }
        return


class Venues(Entities):
    def process_json(self, venue_json: Dict):
        venue_rows, ids_rows, counts_rows = [], [], []
        venue_cols = self.schema.venues.columns

        if not (venue_id := venue_json.get('id')):
            return {'venues': venue_rows, 'venues_ids': ids_rows, 'venues_counts_by_year': counts_rows}

        venue_json['issn'] = json.dumps(venue_json.get('issn'))
        venue_rows.append({col: venue_json.get(col) for col in venue_cols})

        # ids
        if venue_ids := venue_json.get('ids'):
            venue_ids['venue_id'] = venue_id
            venue_ids['issn'] = json.dumps(venue_ids.get('issn'))
            ids_rows.append(venue_ids)

        # counts by year
        if counts_by_year := venue_json.get('counts_by_year'):
            for count_by_year in counts_by_year:
                count_by_year['venue_id'] = venue_id
                counts_rows.append(count_by_year)

        return {'venues': venue_rows, 'venues_ids': ids_rows, 'venues_counts_by_year': counts_rows}

    def __init__(self, paths: Paths):
        super().__init__(kind='venues', paths=paths)
        return

    def init_dtype_dicts(self):
        self.dtypes['venues'] = {
            'id': 'string', 'issn_l': 'string', 'issn': 'string', 'display_name': 'string', 'publisher': 'string',
            'works_count': 'Int64', 'cited_by_count': 'Int64', 'is_oa': 'bool', 'is_in_doaj': 'bool',
            'homepage_url': 'string', 'works_api_url': 'string', 'updated_date': 'string'
        }

        self.dtypes['venues_ids'] = {
            'venue_id': 'string', 'openalex': 'string', 'issn_l': 'string', 'issn': 'string', 'mag': 'string'
        }

        self.dtypes['venues_counts_by_year'] = {
            'venue_id': 'string', 'year': 'Int64', 'works_count': 'Int64', 'cited_by_count': 'Int64'
        }
        return

    def flatten(self):
        """
        if not (venue_id := venue.get('id')) or venue_id in seen_venue_ids:
                        continue

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
        """
        return
