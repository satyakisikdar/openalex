import multiprocessing
import threading
import time
from multiprocessing import Pool

import pandas as pd
from pathlib import Path
import gzip
from tqdm.auto import tqdm
from box import Box

from src.globals import path_type
import ujson as json
from typing import List, Dict, Union


class Paths:
    def __init__(self, basepath: path_type='/N/project/openalex'):
        self.basepath: Path = Path(basepath)
        self.snapshot_dir = self.basepath / 'OpenAlex' / 'openalex-snapshot' / 'data'
        self.processed_dir = self.basepath / 'ssikdar' / 'processed'
        self.temp_dir = self.basepath / 'ssikdar' / 'temp'
        return


# Read file list from MANIFEST
def read_manifest(kind: str, paths: Paths) -> Box:
    raw_data = Box(json.load(open(paths.snapshot_dir / kind / 'manifest')))
    print(f'Reading {kind!r} manifest: {raw_data.meta.record_count:,} records')
    data = Box({'len': raw_data.meta.record_count})

    entries = []
    for raw_entry in raw_data.entries:
        filename = paths.snapshot_dir / raw_entry.url.replace('s3://openalex/data/', '')
        entry = Box({'filename': filename, 'kind': kind,
                     'count': raw_entry.meta.record_count, 'updated_date': '_'.join(filename.parts[-2:]).replace('.gz', '')})
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


if __name__ == '__main__':
    paths = Paths()
    df = pd.read_parquet(paths.processed_dir / 'authors')
    print(len(df))

