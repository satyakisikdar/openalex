import ast
import gzip
import multiprocessing
import time
from datetime import datetime
from multiprocessing import Pool
from pathlib import Path
from typing import List, Dict

import pandas as pd
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
        self.aps_parq_dir = self.basepath / 'APS' / 'new' / 'parquets'
        self.aps_csv_dir = self.basepath / 'APS' / 'new' / 'csvs'
        return


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


if __name__ == '__main__':
    paths = Paths()
    df = pd.read_parquet(paths.processed_dir / 'authors')
    print(len(df))
