"""
Filtering the entire dataset based on certain criteria
1. By range of years
2. Article types
3. Concepts

Essentially, read the whole CSVs in chunks, filtering them, and writing them back out again
"""
import pandas as pd
from tqdm.auto import tqdm


def process_work_chunk(df, idx, parq_path):
    """
    Process each chunked df with index idx
    """
    work_types = {'journal-article', 'proceedings-article', 'posted-content',
                  'book-chapter'}  # only keep these article types
    start_year, end_year = 2012, 2022  # year range

    (parq_path / f'_works').mkdir(exist_ok=True)  # create necessary dirs
    parq_filename = parq_path / f'_works' / f'part-{idx}.parquet'

    if parq_filename.exists():
        return

    if 'year' in df.columns.tolist():
        filt_df = (
            df
            [(df.type.isin(work_types)) & (df.year.between(start_year, end_year))]
        )
        filt_df.rename(columns={'date': 'publication_date', 'year': 'publication_year'}, inplace=True)
    else:
        filt_df = (
            df
            [(df.type.isin(work_types)) & (df.publication_year.between(start_year, end_year))]
        )
    filt_df.set_index('work_id', inplace=True)
    filt_df.to_parquet(parq_filename, engine='pyarrow')

    return


def write_filtered_works_table(csv_path, parq_path):
    """
    Write the filtered works table as a parquet
    """
    row_counts = dict(works_concepts=1174741195,
                      works_works=238504658,
                      works_authorships=598633263,
                      works_host_venues=238442535,
                      works_referenced_works=1825280059)

    chunksize = 20_000_000
    num_chunks = {kind: rows // chunksize + 1 for kind, rows in row_counts.items()}

    with pd.read_csv(csv_path / f'works.csv.gz', engine='c', chunksize=chunksize) as reader:
        # TODO: process each chunk in parallel
        for i, chunked_df in tqdm(enumerate(reader), total=num_chunks['works_works']):
            process_work_chunk(df=chunked_df, idx=i, parq_path=parq_path)

    # write a single parquet for all the parts
    split_df = pd.read_parquet(parq_path / f'_works')
    split_df.to_parquet(parq_path / f'works.parquet')

    # delete the partial parquets
    # shutil.rmtree(parq_path / f'_works')  # todo: testing needed
    return


def write_other_filtered_tables(csv_path, parq_path, work_ids):
    """
    Read tables CSVs in chunks
    """
    row_counts = dict(works_concepts=1174741195,
                      works_works=238504658,
                      works_authorships=598633263,
                      works_host_venues=238442535,
                      works_referenced_works=1825280059)
    chunksize = 20_000_000
    num_chunks = {kind: rows // chunksize + 1 for kind, rows in row_counts.items()}

    kinds = ['authorships', 'host_venues', 'referenced_works', 'concepts']
    for kind in tqdm(kinds):
        print(f'{kind=}')
        (parq_path / f'_works_{kind}').mkdir(exist_ok=True)

        with pd.read_csv(csv_path / f'works_{kind}.csv.gz', engine='c', chunksize=chunksize) as reader:
            for i, chunked_df in tqdm(enumerate(reader), total=num_chunks[f'works_{kind}']):
                parq_filename = parq_path / f'_works_{kind}' / f'part-{i}.parquet'

                if parq_filename.exists():
                    continue

                filt_df = (
                    chunked_df
                    [chunked_df.work_id.isin(work_ids)]
                )

                if kind == 'referenced_works':
                    filt_df = (
                        filt_df
                        [filt_df.referenced_work_id.isin(work_ids)]
                    )

                filt_df.rename(columns={'date': 'publication_date', 'year': 'publication_year'}, inplace=True)
                filt_df.to_parquet(parq_filename, engine='pyarrow')

        # write a single parquet for all the parts
        split_df = pd.read_parquet(parq_path / f'_works_{kind}')
        split_df.to_parquet(parq_path / f'works_{kind}.parquet')

        # delete the partial parquets
        # shutil.rmtree(parq_path / f'_works_{kind}')  # TODO: testing needed
    return


def main():
    # step 0: set paths
    csv_path = None  # set path to flattened CSV files
    parq_path = None  # set path for storing the filtered parquet files
    assert csv_path is not None and parq_path is not None, f'please set CSV and parquet directories'

    # step 1: filter the works table to contain specific article types and year ranges
    write_filtered_works_table(csv_path=csv_path, parq_path=parq_path)

    # step 2: filted the rest of the works_* tables based on the works filtered in step 1
    works_parquet_filename = parq_path / 'works.parquet'
    assert works_parquet_filename.exists(), f'Filtered works parquet does not exists!'

    work_ids = set(
        pd.read_parquet(works_parquet_filename, columns=['work_id'])  # read only the work id column
        .work_id
    )
    write_other_filtered_tables(csv_path=csv_path, parq_path=parq_path, work_ids=work_ids)
    return


if __name__ == '__main__':
    main()
