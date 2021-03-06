import ast
import gzip
import multiprocessing
import pickle
import re
import time
import unicodedata
from datetime import datetime
from multiprocessing import Pool
from pathlib import Path
from typing import List, Dict, Optional, Union

import pandas as pd
import requests
import ujson as json
from box import Box
from fastparquet import ParquetFile
from tqdm import tqdm

from src.globals import path_type


class Paths:
    def __init__(self, basepath: path_type = '/N/project/openalex'):
        self.basepath: Path = Path(basepath)
        self.snapshot_dir = self.basepath / 'OpenAlex' / 'openalex-snapshot' / 'data'
        self.processed_dir = self.basepath / 'ssikdar' / 'processed'
        self.temp_dir = self.basepath / 'ssikdar' / 'temp'
        self.aps_parq_dir = self.basepath / 'APS' / 'new' / 'parquets'
        self.aps_csv_dir = self.basepath / 'APS' / 'new' / 'csvs'

        self.scratch_dir = Path('/N/scratch/ssikdar/openalex-scratch')
        self.compressed_path = self.scratch_dir / 'compressed'
        self.ix_path = self.scratch_dir / 'indices'  # compressed index stored here
        return


class IDMap:
    """
    Stores the id to name mappings for concepts and venues
    store concept levels as well
    """

    def __init__(self, paths: Paths):
        self.venue_id2name = (
            pd.read_parquet(paths.processed_dir / 'venues', columns=['id', 'display_name'], engine='fastparquet')
                .assign(idx=lambda df_: pd.to_numeric(
                df_.id.str.replace('https://openalex.org/V', '', regex=False)
            ))
                .set_index('idx')
                .display_name
                .to_dict()
        )

        concepts_df = (
            pd.read_parquet(paths.processed_dir / 'concepts', columns=['id', 'display_name', 'level'],
                            engine='fastparquet')
                .assign(idx=lambda df_: pd.to_numeric(
                df_.id.str.replace('https://openalex.org/C', '', regex=False)))
                .set_index('idx')
        )

        insts_df = (
            pd.read_parquet(paths.processed_dir / 'institutions')
                .assign(idx=lambda df_: pd.to_numeric(
                df_.id.str.replace('https://openalex.org/I', '', regex=False)))
                .set_index('idx')
        )

        self.inst_id2name = (
            insts_df
                .display_name
                .to_dict()
        )

        self.concept_id2name = (
            concepts_df
                .display_name
                .to_dict()
        )

        self.concept_id2level = (
            concepts_df
                .level
                .to_dict()
        )

        return


def convert_openalex_id_to_int(openalex_id):
    if not openalex_id:
        return None
    openalex_id = openalex_id.strip().replace('https://openalex.org/', '')
    return int(openalex_id[1:])


def convert_openalex_id_to_int_old(openalex_id):
    if not openalex_id:
        return None
    openalex_id = openalex_id.strip().upper().replace('https://openalex.org/', '')
    p = re.compile("([WAICV]\d{2,})")
    matches = re.findall(p, openalex_id)
    if len(matches) == 0:
        return None
    clean_openalex_id = matches[0]
    clean_openalex_id = clean_openalex_id.replace('\0', '')[1:]  # delete the first letter
    return int(clean_openalex_id)


def combined_dump_work_refs(work_id, work_indexer, ref_indexer):
    """
    Dump bytes of work and its references to the respective dump files
    """
    process_and_dump_work(work_id=work_id, work_indexer=work_indexer)
    process_and_dump_references(work_id=work_id, ref_indexer=ref_indexer)
    return


def process_and_dump_work(work_id, work_indexer):
    """
    Process a new work id and dump the bytes
    """
    if work_id in work_indexer:
        return
    try:
        work = work_indexer[work_id]
        bites = work_indexer.convert_to_bytes(work)
        work_indexer.dump_bytes(work_id=work_id, bites=bites)
    except Exception as e:
        print(f'Exception {e=} for {work_id=}')
    return


def process_and_dump_references(work_id, ref_indexer):
    """
    Process a new work id  and dump the bytes
    """
    if work_id in ref_indexer:
        return
    try:
        work = ref_indexer[work_id]
        bites = ref_indexer.convert_to_bytes(work)
        # print(work_id, len(work.references), work.cited_by_count)
        ref_indexer.dump_bytes(work_id=work_id, bites=bites)
    except Exception as e:
        print(f'Exception {e=} for {work_id=}')
    return


def get_concept_id(name) -> int:
    cached = {'Complex network': 'C34947359', 'Computer science': 'C41008148', 'Physics': 'C121332964',
              'Network science': 'C137753397', 'Graphene': 'C30080830', 'Feshbach resonance': 'C39190425',
              'Soliton': 'C87651913', 'Dark matter': 'C159249277', 'Mathematics': 'C33923547', 'Biology': 'C86803240',
              'Math': 'C33923547', 'Neutrino oscillation': 'C107966497', 'Photoionization': 'C158749347',
              'String theory': 'C49987212', 'General relativity': 'C147452769', 'Percolation theory': 'C11557063',
              'Magnetoresistance': 'C117958382', 'Quantum gravity': 'C108568745', 'Josephson effect': 'C12038964',
              'Quantum Hall effect': 'C200369452', 'Inflation (cosmology)': 'C200941418',
              'Photoemission spectroscopy': 'C51286037', 'Supersymmetry': 'C116674579',
              'Artificial intelligence': 'C154945302', 'Machine learning': 'C119857082', 'Data mining': 'C124101348',
              'Chemistry': 'C185592680', 'Medicine': 'C71924100', 'Quantum electrodynamics': 'C3079626',
              'Quantum mechanics': 'C62520636'}
    if name in cached:
        id_ = cached[name]
    else:
        url = f'https://api.openalex.org/concepts?filter=display_name.search:{name}'
        json = requests.get(url, params={'mailto': 'ssikdar@iu.edu'}).json()
        # pick the top result
        id_ = json['results'][0]['id'].replace('https://openalex.org/', '')
        cached[name] = id_
    return int(id_.replace('C', ''))


def get_author_id(name) -> int:
    cached = {'Santo': 'A2122189410', 'Santo Fortunato': 'A2122189410', 'Barabasi': 'A2195478976',
              'Mark Newman': 'A2394749673', 'Parisi': 'A2163147449', 'Cirac': 'A2103728845',
              'Vespignani': 'A2707826896', 'Satyaki': 'A2297758725', 'Fortunato': 'A2122189410',
              'Filippo': 'A842233868', 'Tim': 'A2037649753'}
    if name in cached:
        id_ = cached[name]
    else:
        url = f'https://api.openalex.org/authors?filter=display_name.search:{name}'
        json = requests.get(url, params={'mailto': 'ssikdar@iu.edu'}).json()
        # pick the top result
        id_ = json['results'][0]['id'].replace('https://openalex.org/', '')
    return int(id_.replace('A', ''))


class ParquetIndices:
    """
    container for indices, lazily load stuff
    implement __get__ method
    """

    def __init__(self, paths: Paths):
        self.paths = paths
        self.works = None
        self.works_authorships = None
        self.works_concepts = None  # stores the mapping works -> concepts
        self.concepts_works = None  # stores the inverse mapping: concepts -> work_ids
        self.works_citing_works = None
        self.works_referenced_works = None
        self.authors = None
        self.works_host_venues = None
        return

    def initialize(self):
        """
        Initialize the indices
        :return:
        """
        self['works']
        self['works_authorships']
        self['works_concepts']
        self['authors']
        self['works_host_venues']
        self['works_referenced_works']
        self['works_citing_works']
        return

    def load_index(self, kind: str):
        """
        Populate the corresponding index
        """
        ix_path = self.paths.ix_path / kind
        if not ix_path.exists():
            # print(f'Index not found for {kind!r}')
            self.write_index(kind=kind)

        ix = pd.read_parquet(self.paths.ix_path / kind, engine='fastparquet', columns=[f'{kind}_part'])
        setattr(self, kind, ix)
        return

    def write_index(self, kind: str):
        pf = ParquetFile(self.paths.parq_dir / f'{kind}')
        print(f'{kind}, {pf.info["rows"]=:,} ')

        files = list((self.paths.parq_dir / kind).glob('*.parquet'))
        dfs = []
        for file in tqdm(files):
            part = int(file.stem.split('.')[-1])

            if kind == 'authors':
                cols = ['author_id']
            elif kind == 'works_citing_works':
                cols = ['referenced_work_id']
            elif kind == 'concepts_works':
                cols = ['concept_id']
            else:
                cols = ['work_id']

            df = pd.read_parquet(file, engine='fastparquet', columns=cols)
            df = df[~df.index.duplicated(keep='first')]
            df[f'{kind}_part'] = part
            df.astype({f'{kind}_part': 'uint16'}, copy=False)
            dfs.append(df)

        DF = pd.concat(dfs)
        print(f'{len(DF)=:,} rows')
        DF = DF.sort_index()
        print(f'{len(DF)=:,} rows')
        DF.to_parquet(self.paths.ix_path / kind, engine='pyarrow', row_group_size=500_000)
        return

    def __getitem__(self, item: str):

        index = getattr(self, item)
        if index is None:
            tic = time.time()
            # print(f'Loading index for {item!r} from {str(self.paths.ix_path/item)}')
            self.load_index(kind=item)
            toc = time.time()
            print(f'{item!r} index loaded in {toc - tic:.2g} seconds')
        index = getattr(self, item)  # try again
        assert index is not None, 'Setting index did not work'
        return index


def get_partition_no(id_: int, kind: Optional[str] = None, ix_df: Optional[pd.DataFrame] = None) -> Union[int, List]:
    """
     Return the partition number for the entity in the table
    """
    try:
        stuff = ix_df.at[id_, ix_df.columns[0]]
        if isinstance(stuff, pd.Series):
            part_no = stuff.unique().tolist()
        else:
            part_no = int(stuff)
    except KeyError:
        # print(f'{id_} not found in {kind!r}')
        return None

    return part_no


def get_rows(id_: int, kind: str, paths: Paths, part_no: int, id_col: str = 'work_id'):
    if isinstance(part_no, list):  # multiple partitions
        part_df = None
        for n in part_no:
            # df = pd.read_parquet(paths.parq_dir / kind / f'part.{n}.parquet', filters=[(id_col, '=', id_)])
            df = (
                ParquetFile(str(paths.parq_dir / kind / f'part.{n}.parquet'))
                    .to_pandas(filters=[(id_col, '=', id_)])
                    .loc[[id_]]
            )
            if part_df is None:
                part_df = df
            else:
                part_df = pd.concat([part_df, df])

    else:  # one partition
        if pd.isna(part_no):
            return
        # part_df = pd.read_parquet(paths.parq_dir / kind / f'part.{part_no}.parquet', filters=[(id_col, '=', id_)])
        part_df = (
            ParquetFile(paths.parq_dir / kind / f'part.{part_no}.parquet')
                .to_pandas(filters=[(id_col, '=', id_)])
                .loc[[id_]]
        )
    return part_df


def strip_accents(s):
    return ''.join(c for c in unicodedata.normalize('NFD', s)
                   if unicodedata.category(c) != 'Mn')


def clean_string(s: str) -> str:
    if s is None:
        return ''
    s_ = strip_accents(s)
    s_ = ''.join([i if ord(i) < 128 else ' ' for i in s_])
    if s_.isascii():
        return s_
    else:
        return ''


# Read file list from MANIFEST
def read_manifest(kind: str, paths: Paths) -> Box:
    manifest_path = paths.snapshot_dir / kind / 'manifest'
    create_date = datetime.fromtimestamp(manifest_path.stat().st_ctime).strftime("%a, %b %d %Y")

    raw_data = Box(json.load(open(manifest_path)))
    print(f'Reading {kind!r} manifest created on {create_date}. {len(raw_data.entries):,} files, '
          f'{raw_data.meta.record_count:,} records.')
    data = Box({'len': raw_data.meta.record_count})

    entries = []
    for raw_entry in raw_data.entries:
        filename = paths.snapshot_dir / raw_entry.url.replace('s3://openalex/data/', '')
        entry = Box({'filename': filename, 'kind': kind,
                     'count': raw_entry.meta.record_count,
                     'updated_date': '_'.join(filename.parts[-2:]).replace('.gz', '')})
        entries.append(entry)
    data['entries'] = entries
    return data


def read_gz_in_chunks(fname: path_type, jsons_per_chunk: int, num_lines: int) -> List[Dict]:
    """
    Read line by line, make individual chunks with num_lines, and yield a list of num_chunks many chunks
    Ideally, each worker will handle one chunk, and num_chunks would be the same as number of workers
    num_entries from the manifest should give the number of records to parse
    process each chunk and write to disk using parquet
    """
    with gzip.open(fname) as fp:
        chunk = []
        for i, line in enumerate(fp):
            content = json.loads(line)
            chunk.append(content)
            if (i > 0 and i % jsons_per_chunk == 0) or (i == num_lines - 1):
                yield chunk
                chunk = []


def write_parquet(rows: List, col_names: List, path: path_type, dtypes: Dict, min_size=None, **args):
    """
    Convert rows to pandas dataframe and write a parquet file
    :param rows:
    :param path:
    :param args:
    """
    df = pd.DataFrame(rows)[col_names]
    df = df.astype(dtype=dtypes)
    # tqdm.write(f'Writing HDF file at {path.stem!r}, entries: {len(df):,}')
    path = Path(path)
    if min_size is not None:
        min_itemsize = min_size
    else:
        min_itemsize = {'values': 1500}
    write_lock = multiprocessing.Lock()
    with write_lock:
        with pd.HDFStore(str(path)) as store:
            store.append('df', df, min_itemsize=min_itemsize, append=True)
            store.flush(fsync=True)
            time.sleep(1)  # sleep for a second to prevent race?

        # try:
        #     df.to_hdf(path, key='df', append=True, min_itemsize=min_itemsize)
        # except Exception as e:
        #     tqdm.write(f'\nWrite Error! {e}\n')
    return


def parallel_async(func, args, num_workers: int):
    def update_result(result):
        return result

    results = []
    async_promises = []
    with Pool(num_workers) as pool:
        for arg in args:
            r = pool.apply_async(func, arg, callback=update_result)
            async_promises.append(r)
        for r in async_promises:
            try:
                r.wait()
                results.append(r.get())
            except Exception as e:
                results.append(r.get())

    return results


def ensure_dir(path: path_type, recursive: bool = False, exist_ok: bool = True) -> None:
    path = Path(path)
    if not path.exists():
        # ColorPrint.print_blue(f'Creating dir: {path!r}')
        path.mkdir(parents=recursive, exist_ok=exist_ok)
    return


def read_parquet(path, **args):
    df = pd.read_parquet(path, engine='pyarrow')
    df.reset_index(inplace=True)
    if '__null_dask_index__' in df.columns:
        df.drop(['__null_dask_index__'], axis=1, inplace=True)
    if 'index' in df.columns:
        df.drop(['index'], axis=1, inplace=True)
    df.drop_duplicates(inplace=True)

    if 'index_col' in args:
        df.set_index(args['index_col'], inplace=True)
    print(f'Read {len(df):,} rows from {path.stem!r}')
    return df


def construct_abstracts(inv_abstracts):
    """"
    Construct abstracts from inverted index
    keys: words, values: list of locations
    """
    abstracts = []

    for inv_abstract_st in tqdm(inv_abstracts, desc='Constructing abstracts..'):
        inv_abstract_st = ast.literal_eval(inv_abstract_st)  # convert to python object
        if isinstance(inv_abstract_st, bytes):
            inv_abstract_st = inv_abstract_st.decode('utf-8', errors='replace')

        inv_abstract = json.loads(inv_abstract_st) if isinstance(inv_abstract_st, str) else inv_abstract_st
        abstract_dict = {}
        for word, locs in inv_abstract.items():  # invert the inversion
            for loc in locs:
                abstract_dict[loc] = word
        abstract = ' '.join(map(lambda x: x[1],  # pick the words
                                sorted(abstract_dict.items())))  # sort abstract dictionary by indices
        if len(abstract) == 0:
            abstract = None
        abstracts.append(abstract)
    return abstracts


def reconstruct_abstract_new(inv_abstract_st):
    if inv_abstract_st is None:
        return ''

    if isinstance(inv_abstract_st, bytes):
        inv_abstract_st = inv_abstract_st.decode('utf-8', errors='replace')
    # inv_abstract_st = ast.literal_eval(inv_abstract_st)  # convert to python object

    inv_abstract = json.loads(inv_abstract_st) if isinstance(inv_abstract_st, str) else inv_abstract_st
    abstract_dict = {}
    for word, locs in inv_abstract.items():  # invert the inversion
        for loc in locs:
            abstract_dict[loc] = word
    abstract = ' '.join(map(lambda x: x[1],  # pick the words
                            sorted(abstract_dict.items())))  # sort abstract dictionary by indices
    if len(abstract) == 0:
        abstract = ''
    return abstract


def reconstruct_abstract(inv_abstract_st):
    if inv_abstract_st is None:
        return ''
    inv_abstract_st = ast.literal_eval(inv_abstract_st)  # convert to python object
    if isinstance(inv_abstract_st, bytes):
        inv_abstract_st = inv_abstract_st.decode('utf-8', errors='replace')

    inv_abstract = json.loads(inv_abstract_st) if isinstance(inv_abstract_st, str) else inv_abstract_st
    abstract_dict = {}
    # print(f'{type(inv_abstract)=}')
    for word, locs in inv_abstract.items():  # invert the inversion
        for loc in locs:
            abstract_dict[loc] = word
    abstract = ' '.join(map(lambda x: x[1],  # pick the words
                            sorted(abstract_dict.items())))  # sort abstract dictionary by indices
    if len(abstract) == 0:
        abstract = ''
    return abstract


def load_pickle(path):
    with open(path, 'rb') as reader:
        return pickle.load(reader)


def dump_pickle(obj, path):
    with open(path, 'wb') as writer:
        pickle.dump(obj, writer)


if __name__ == '__main__':
    paths = Paths()
    df = pd.read_parquet(paths.processed_dir / 'authors')
    print(len(df))
