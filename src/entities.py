"""
Make individual classes
"""
import abc
import gzip
from pathlib import Path
from typing import List, Dict

import pandas as pd
import ujson as json
from box import Box
from joblib import Parallel, delayed
from tqdm.auto import tqdm

from src.globals import path_type, NAME_LEN, ID_LEN, LIST_LEN, URL_LEN
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
        return

    def process(self, num_workers: int):
        """
        Process entries from the manifest
        :return:
        """
        entries = self.manifest.entries[: 50]

        Parallel(backend='multiprocessing', n_jobs=num_workers)(
            delayed(self.process_entry)(entry) for entry in entries
        )

        # with tqdm(total=len(self.manifest.entries), unit='entries', colour='green', ncols=100, miniters=0) as pbar:
        #     for i, entry in enumerate(self.manifest.entries):
        #         pbar.set_description(f'{self.kind!r} {entry.filename.stem!r}')
        #         self.process_entry(entry=entry)
        #         pbar.update(1)
        #         if i == 1:
        #             break
        return

    @abc.abstractmethod
    def process_entry(self, entry: Box) -> None:
        """
        Process individual entries in the manifest
        :return:
        """

        if str(entry.updated_date) in self.get_finished_files():
            tqdm.write(f'Skipping {self.kind!r} {entry.updated_date!r}!')
            return

        tqdm.write(f'Processing {self.kind!r} {entry.updated_date!r}')
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
            self.write_to_disk(table_name=table_name, updated_date=entry.updated_date, rows=rows, verbose=True)
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
        st = f'<{self.kind} total entities: {len(self):,}>'
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

        df = pd.DataFrame(rows)  # [self.dtypes[table_name].keys()]  # problem!

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
        print(path)
        ensure_dir(path.parents[0], recursive=True)  # makes sure the parent dirs exist
        if verbose:
            tqdm.write(f'Writing {table_name!r} {len(df):,} rows to {path}')
        writer_fn(df, path=path, **fun_args, **args)
        return


class Authors(Entities):
    def __init__(self, paths: Paths):
        super().__init__(kind='authors', paths=paths)
        self.dtypes = {}  # dictionary of data types for pandas dataframes
        self.min_sizes = {}  # dictionary of min item sizes for string columns for HDF5
        self.init_dtype_dicts()
        return

    def init_dtype_dicts(self):
        """
        Initialize data type dictionaries
        :return:
        """
        self.dtypes['authors'] = {'id': 'string', 'orcid': 'string', 'display_name': 'string',
                                  'display_name_alternatives': 'string', 'works_count': 'int',
                                  'cited_by_count': 'int', 'last_known_institution': 'string',
                                  'works_api_url': 'string', 'updated_date': 'string'}

        self.min_sizes['authors'] = {'id': ID_LEN, 'orcid': ID_LEN, 'display_name': NAME_LEN,
                                     'display_name_alternatives': LIST_LEN * 2,
                                     'last_known_institution': ID_LEN, 'works_api_url': URL_LEN, 'updated_date': ID_LEN}

        self.dtypes['authors_ids'] = {'author_id': 'string', 'openalex': 'string', 'orcid': 'string',
                                      'scopus': 'string', 'twitter': 'string', 'wikipedia': 'string', 'mag': 'string'}
        self.min_sizes['authors_ids'] = {'author_id': ID_LEN, 'openalex': ID_LEN, 'orcid': ID_LEN, 'scopus': ID_LEN,
                                         'twitter': ID_LEN, 'wikipedia': ID_LEN, 'mag': ID_LEN}

        self.dtypes['authors_counts_by_year'] = {'author_id': 'string', 'year': 'int', 'works_count': 'int',
                                                 'cited_by_count': 'int'}
        self.min_sizes['authors_counts_by_year'] = {'author_id': ID_LEN}

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
        return

    def process_json(self, jsons):
        pass


class Institutions(Entities):
    def __init__(self, paths: Paths):
        super().__init__(kind='institutions', paths=paths)
        return

    def process_json(self, jsons):
        pass


class Concepts(Entities):
    def __init__(self, paths: Paths):
        super().__init__(kind='concepts', paths=paths)
        return

    def process_json(self, jsons):
        pass
