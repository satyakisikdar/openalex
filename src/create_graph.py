"""
Constructing iGraph graphs out of parquet files using Dask
"""
from typing import Set

import dask.dataframe as dd
import pandas as pd
from dask.distributed import Client
from tqdm.auto import tqdm

from src.utils import Paths, read_parquet


class APSVenues:
    def __init__(self, paths: Paths, client: Client):
        self.paths = paths
        self.client = client
        self.aps_venue_ids = self.get_venue_ids()
        self.aps_work_ids = self.get_aps_work_ids()
        return

    def get_venue_ids(self) -> Set:
        """
        Use the Venues DF to find APS Physical Review journals
        :return:
        """
        aps_venue_id_path = self.paths.temp_dir / 'aps_venue_ids.csv'
        if aps_venue_id_path.exists():
            aps_venue_ids = set(pd.read_csv(aps_venue_id_path).id)
            return aps_venue_ids

        venue_df = read_parquet(self.paths.processed_dir / 'venues')
        keep = ['Physical Review', 'Physical Review A', 'Physical Review B', 'Physical Review C', 'Physical Review D',
                'Physical Review E', 'Physical Review (Series I)', 'Physical Review Letters', 'Physical Review X']
        keep = list(map(lambda st: st.lower(), keep))  # turn into lower case
        df = venue_df.query(f'display_name.str.lower().isin(@keep)')
        df.to_csv(aps_venue_id_path, index=False)
        aps_venues_ids = set(df.id.values)

        return aps_venues_ids

    def get_aps_work_ids(self) -> Set:
        """
        Use the APS venue_ids and work_venues table to find APS work ids
        """
        aps_work_id_path = self.paths.temp_dir / 'aps_work_ids.csv.gz'
        if aps_work_id_path.exists():
            work_ids = set(pd.read_csv(aps_work_id_path).work_id)
            return work_ids

        works_venues_df = dd.read_parquet(self.paths.processed_dir / 'works_host_venues', engine='pyarrow', )
        alt_works_venues_df = dd.read_parquet(self.paths.processed_dir / 'works_alternate_host_venues',
                                              engine='pyarrow')

        work_ids = set(works_venues_df[works_venues_df.venue_id.isin(self.aps_venue_ids)])
        alt_work_ids = set(alt_works_venues_df[alt_works_venues_df.venue_id.isin(self.aps_venue_ids)])
        work_ids = work_ids | alt_work_ids
        _df = pd.DataFrame({'work_id': list(work_ids)})
        _df.to_csv(aps_work_id_path, index=False)

        return work_ids

    def generate_works(self):
        aps_works_path = self.paths.processed_dir / 'APS/works'
        if aps_works_path.exists():
            return read_parquet(aps_works_path)

        works_df = dd.read_parquet(self.paths.processed_dir / 'works')
        aps_works_df = works_df[works_df.id.isin(self.aps_work_ids)]
        aps_works_df.to_parquet(aps_works_path, engine='pyarrow')

        return aps_works_df

    def filter_works_datasets(self):
        works_df = dd.read_parquet(self.paths.processed_dir / 'APS/works', engine='pyarrow')
        aps_work_ids = set(works_df.id)

        kinds = ['referenced_works', 'related_works', 'host_venues', '_ids', 'alternate_host_venues']

        for kind in tqdm(kinds):
            parq_path = self.paths.processed_dir / f'APS/works_{kind}'
            if parq_path.exists():
                print(f'{kind} exists!')
                continue

            overall_df = dd.read_parquet(self.paths.processed_dir / f'works_{kind}', engine='pyarrow')
            aps_df = overall_df[overall_df.work_id.isin(aps_work_ids)]
            if kind == 'referenced_works':
                aps_df = aps_df[aps_df.referenced_work_id.isin(aps_work_ids)]
            if kind == 'related_works':
                aps_df = aps_df[aps_df.related_work_id.isin(aps_work_ids)]
            aps_df.to_parquet(parq_path, engine='pyarrow')
        return


def main():
    paths = Paths()
    # cluster = LocalCluster(n_workers=1, threads_per_worker=1, processes=True, memory_limit='5GB')
    # client = Client(cluster)
    client = None
    aps = APSVenues(paths=paths, client=client)
    print(len(aps.get_aps_work_ids()))
    return


if __name__ == '__main__':
    main()
