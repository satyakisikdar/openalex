import ast
import pickle
import unicodedata
from datetime import datetime
from multiprocessing import Pool
from pathlib import Path

import pandas as pd
import requests
import ujson as json
from box import Box
from tqdm import tqdm

from src.globals import path_type


class Paths:
    def __init__(self, basepath: path_type = '/N/project/openalex'):
        self.basepath: Path = Path(basepath)
        self.snapshot_dir = self.basepath / 'OpenAlex' / 'openalex-snapshot' / 'data'
        self.processed_dir = self.basepath / 'ssikdar' / 'processed'
        self.temp_dir = self.basepath / 'ssikdar' / 'temp'
        return


def convert_openalex_id_to_int(openalex_id):
    if not openalex_id:
        return None
    openalex_id = openalex_id.strip().replace('https://openalex.org/', '')
    return int(openalex_id[1:])


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
