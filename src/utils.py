import ast
import multiprocessing
import pickle
import re
import unicodedata
from collections import namedtuple
from datetime import datetime
from multiprocessing import Pool
from pathlib import Path

import numpy as np
import pandas as pd
import requests
import ujson as json
from box import Box
from seaborn._statistics import EstimateAggregator
from tqdm import tqdm
from unidecode import unidecode_expect_ascii

from src.globals import path_type


def string_to_bool(st, default_value=False):
    """
    Convert string to boolean. If NA, use default value
    """
    if isinstance(st, bool):
        return st
    elif pd.isna(st):
        return default_value
    elif isinstance(st, str):
        return eval(st)

    raise NotImplementedError(f'{st=!r} of type {type(st)} unclear.')
    return


def conf_interval(data, aggfunc='mean', errorfunc=('ci', 95), 
                  return_errors=False):
    """
    data: set of values, can be a pandas series or numpy array like  
    aggfunc: function to aggregate: mean/median 
    errorfunc: ('ci', 95), 'sd' (standard dev), 'se' (standard error)
    return_errors: return difference between the means if True, else return the absolute boundaries 
    """
    data = np.array(data)
    data = data[~np.isnan(data)]  # remove NaNs

    if len(data) == 0:
        res = namedtuple('result', 'y ymin ymax')  # to facilitate dot accessors below
        res.y = np.nan
        res.ymin = np.nan
        res.ymax = np.nan
    else:
        agg = EstimateAggregator(aggfunc, errorfunc)
        df = pd.DataFrame({'y': data})
        res = agg(df, 'y')

    result = res.y
    errorfunc_name = aggfunc + '_' + ''.join(map(str, errorfunc))

    if return_errors:
        y_error_min, y_error_max = result - res.ymin, res.ymax - result

        ser = pd.Series({aggfunc: result,
                         f'{errorfunc_name}_error_min': y_error_min,
                         f'{errorfunc_name}_error_max': y_error_max})
    else:
        ser = pd.Series({aggfunc: result,
                         f'{errorfunc_name}_min': res.ymin,
                         f'{errorfunc_name}_max': res.ymax})
    return ser 


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
    try:
        openalex_id = openalex_id.strip().replace('https://openalex.org/', '')
        id_ = int(openalex_id[1:])
    except Exception:
        id_ = None
    return id_


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


def clean_html(raw_html):
    cleanr = re.compile('<.*?>')
    try:
        cleantext = re.sub(cleanr, '', raw_html)
    except TypeError:
        cleantext = raw_html
    return cleantext


def remove_punctuation(input_string):
    # from http://stackoverflow.com/questions/265960/best-way-to-strip-punctuation-from-a-string-in-python
    no_punc = input_string
    if input_string:
        no_punc = "".join(e for e in input_string if (e.isalnum() or e.isspace()))
    return no_punc


# good for deduping strings.  warning: output removes spaces so isn't readable.
def normalize_string(text, decode=True):
    if pd.isna(text) or not text:
        return pd.NA
    response = text.lower()
    if decode:
        response = unidecode_expect_ascii(response)
    response = clean_html(response)  # has to be before remove_punctuation
    response = remove_punctuation(response)
    response = re.sub(r"\b(a|an|the)\b", "", response)
    response = re.sub(r"\b(and)\b", "", response)
    response = re.sub("\s+", "", response)
    return response


# Read file list from MANIFEST
def read_manifest(kind: str, snapshot_dir) -> Box:
    manifest_path = snapshot_dir / kind / 'manifest'
    create_date = datetime.fromtimestamp(manifest_path.stat().st_ctime).strftime("%a, %b %d %Y")

    raw_data = Box(json.load(open(manifest_path)))
    print(f'Reading {kind!r} manifest created on {create_date}. {len(raw_data.entries):,} files, '
          f'{raw_data.meta.record_count:,} records.')
    data = Box({'len': raw_data.meta.record_count})

    entries = []
    for raw_entry in raw_data.entries:
        filename = snapshot_dir / raw_entry.url.replace('s3://openalex/data/', '')
        entry = Box({'filename': filename, 'kind': kind,
                     'count': raw_entry.meta.record_count,
                     'updated_date': '_'.join(filename.parts[-2:]).replace('.gz', '')})
        if entry.count > 0:
            entries.append(entry)

    data['entries'] = sorted(entries, key=lambda x: x.count)

    return data


def parallel_async(func, args, num_workers=10, timeout=None):
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
                r.wait(timeout=timeout)
                results.append(r.get(timeout=timeout))
            except (TimeoutError, multiprocessing.context.TimeoutError) as e:
                print(f'Timeout after {timeout}s {e=}')
            except Exception as e:
                try:
                    results.append(r.get(timeout=timeout))
                except (TimeoutError, multiprocessing.context.TimeoutError) as e:
                    print(f'Timeout after {timeout}s {e=}')

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


def reconstruct_abstract(inv_abstract_st):
    if inv_abstract_st is None:
        return pd.NA

    if isinstance(inv_abstract_st, bytes):
        inv_abstract_st = inv_abstract_st.decode('utf-8', errors='replace')
    # inv_abstract_st = ast.literal_eval(inv_abstract_st)  # convert to python object
    try:
        if isinstance(inv_abstract_st, str):
            if inv_abstract_st != '':
                inv_abstract = json.loads(inv_abstract_st)
            else:
                inv_abstract = dict()
        elif isinstance(inv_abstract_st, dict):
            inv_abstract = inv_abstract_st
        else:
            raise NotImplementedError(f'Invalid {inv_abstract_st=} of type {inv_abstract_st}')
    except json.JSONDecodeError as e:
        print(f'Error {e} {inv_abstract_st=} {type(inv_abstract_st)=}')
        raise e

    abstract_dict = {}
    for word, locs in inv_abstract.items():  # invert the inversion
        for loc in locs:
            abstract_dict[loc] = word
    abstract = ' '.join(map(lambda x: x[1],  # pick the words
                            sorted(abstract_dict.items())))  # sort abstract dictionary by indices
    if len(abstract) == 0:
        abstract = pd.NA
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
