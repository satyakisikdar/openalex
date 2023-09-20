"""
Filtering the entire dataset based on certain criteria
1. By range of years
2. Article types
3. Concepts

Essentially, read the whole CSVs in chunks, filtering them, and writing them back out again
"""
import warnings

warnings.simplefilter(action='ignore', category=FutureWarning)

from pathlib import Path

import pandas as pd
from tqdm.auto import tqdm

# STRING_DTYPE = 'string[pyarrow]'  # use the more memory efficient PyArrow string datatype
STRING_DTYPE = 'string[python]'
if STRING_DTYPE == 'string[pyarrow]':
    assert pd.__version__ >= "1.3.0", f'Pandas version >1.3 needed for String[pyarrow] dtype, have {pd.__version__!r}.'

DTYPES = {
    'works': dict(
        work_id='int64', doi=STRING_DTYPE, title=STRING_DTYPE, publication_year='Int16',
        publication_date=STRING_DTYPE, type='category', type_crossref=STRING_DTYPE,
        cited_by_count='uint32', num_authors='uint16',
        language=STRING_DTYPE, has_grant_info=bool,
        num_locations='uint16', num_references='uint16',
        is_retracted=STRING_DTYPE, is_paratext=STRING_DTYPE,
        created_date=STRING_DTYPE, updated_date=STRING_DTYPE,
    ),
    'authorships': dict(
        work_id='int64', author_position='category', author_id='Int64', author_name=STRING_DTYPE,
        institution_id='Int64', institution_name=STRING_DTYPE, raw_affiliation_string=STRING_DTYPE,
        countries=STRING_DTYPE, publication_year='Int16', is_corresponding=STRING_DTYPE,
    ),
    'grants': dict(
        work_id='int64', funder_id=STRING_DTYPE, funder_name=STRING_DTYPE, award_id=STRING_DTYPE,
    ),
    'primary_location': dict(
        work_id='int64', source_id='Int64', source_name=STRING_DTYPE, source_type='category', version=STRING_DTYPE,
        license=STRING_DTYPE, is_oa=STRING_DTYPE,
    ),
    'locations': dict(
        work_id='int64', source_id='Int64', source_name=STRING_DTYPE, source_type='category', version=STRING_DTYPE,
        license=STRING_DTYPE, is_oa=STRING_DTYPE,
    ),
    'referenced_works': dict(
        work_id='int64', referenced_work_id='int64'
    ),
    'related_works': dict(
        work_id='int64', related_work_id='int64'
    ),
    'concepts': dict(
        work_id='int64', publication_year='Int16', concept_id='int64', concept_name='category', level='uint8',
        score=float
    ),
    'abstract': dict(
        work_id='int64', publication_year='Int16', title=STRING_DTYPE, abstract=STRING_DTYPE,
    ),
    'ids': dict(
        work_id='int64', openalex=STRING_DTYPE, doi=STRING_DTYPE, mag='Int64', pmid=STRING_DTYPE, pmcid=STRING_DTYPE
    ),
    'biblio': dict(
        work_id='int64', volume=STRING_DTYPE, issue=STRING_DTYPE, first_page=STRING_DTYPE, last_page=STRING_DTYPE,
    )
}

to_datetime_ags = dict(format='%Y-%m-%d', errors='coerce', )
if pd.__version__ < '2':
    to_datetime_ags.update({'infer_datetime_format': True})  # only for pandas < 2

dtypes = DTYPES  # use the dtypes from the other file


# dtypes = {
#     'works': dict(work_id='int64', doi='string', title='string', publication_year='Int16', publication_date='string',
#                   type='string', cited_by_count='uint32', is_retracted=float, is_paratext=float),
#     'authorships': dict(
#         work_id='int64', author_position='category', author_id='Int64', author_name='string',
#         institution_id='Int64', institution_name='string', raw_affiliation_string='string',
#         publication_year='Int16'),
#     'host_venues': dict(
#         work_id='int64', venue_id='Int64', venue_name='string', url='string', is_oa=float, version='string',
#         license='string'
#     ),
#     'referenced_works': dict(
#         work_id='int64', referenced_work_id='int64'
#     ),
#     'concepts': dict(
#         work_id='int64', publication_year='Int16', concept_id='int64', concept_name='category', level='uint8',
#         score=float
#     )
# }


def process_work_chunk(df, chunk_name, parq_path, year_range=(2012, 2022)):
    """
    Process each chunked df with index idx
    """
    assert len(df) > 0, f'Work df at {str(parq_path)!r} is empty'
    work_types = {'article', 'journal-article', 'proceedings-article', 'posted-content',
                  'book-chapter'}  # only keep these types
    start_year, end_year = year_range  # year range

    (parq_path / f'_works').mkdir(exist_ok=True, parents=True)  # create necessary dirs
    parq_filename = parq_path / f'_works' / f'{chunk_name}.parquet'

    if parq_filename.exists():
        return

    if 'year' in df.columns.tolist():
        df.rename(columns={'date': 'publication_date', 'year': 'publication_year'}, inplace=True)

    filt_df = (
        df[
            (df.type.isin(work_types) | df.type_crossref.isin(work_types)) &
            (df.publication_year.between(start_year, end_year))
            ]
    )

    filt_df = (
        filt_df
        .astype(dtypes['works'])
        # .astype({'cited_by_count': 'uint32', 'type': 'category', 'publication_year': 'uint32', 'is_retracted': 'bool'})
        .query('is_paratext!=1')
        .assign(publication_date=lambda df_: pd.to_datetime(df_.publication_date, **to_datetime_ags),
                doi=lambda df_: df_.doi.str.replace('https://doi.org/', '', regex=False))
        .drop(columns=['is_paratext'])
    )
    print(f'{chunk_name=} {len(filt_df)=:,}')
    filt_df.set_index('work_id', inplace=True)
    filt_df.to_parquet(parq_filename)

    return


def write_filtered_works_table_v2(whole_works_parq_path, parq_path):
    """
    Write the filtered works table as a parquet
    """
    works_parq_filename = parq_path / 'works'

    if works_parq_filename.exists():
        print(f'Filtered works parquet exists at {str(works_parq_filename)}.')
        return

    whole_work_chunks = sorted(whole_works_parq_path.glob('*.parquet'))  # sorted so pieces dont get repeated
    assert len(whole_work_chunks) > 0, f'Work chunks not found at {str(whole_work_chunks)!r}'

    for i, chunked_work_path in enumerate(tqdm(whole_work_chunks)):
        chunked_work_df = pd.read_parquet(chunked_work_path, engine='fastparquet')
        process_work_chunk(df=chunked_work_df, chunk_name=chunked_work_path.stem, parq_path=parq_path)

    # write a single parquet for all the parts
    split_df = pd.read_parquet(parq_path / f'_works')
    split_df.to_parquet(works_parq_filename)

    # delete the partial parquets
    # shutil.rmtree(parq_path / f'_works')  # todo: testing needed
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

    chunksize = 50_000_000
    num_chunks = {kind: rows // chunksize + 1 for kind, rows in row_counts.items()}
    works_parq_filename = parq_path / 'works.parquet'

    if works_parq_filename.exists():
        print(f'Filtered works parquet exists at {str(works_parq_filename)}.')
        return

    with pd.read_csv(csv_path / f'works.csv.gz', engine='c', chunksize=chunksize, dtype=dtypes['works']) as reader:
        # TODO: process each chunk in parallel
        for i, chunked_df in tqdm(enumerate(reader), total=num_chunks['works_works'], desc='Filtering works...'):
            process_work_chunk(df=chunked_df, idx=i, parq_path=parq_path)

    # write a single parquet for all the parts
    split_df = pd.read_parquet(parq_path / f'_works')
    split_df.to_parquet(works_parq_filename)

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
    chunksize = 50_000_000
    num_chunks = {kind: rows // chunksize + 1 for kind, rows in row_counts.items()}

    kinds = ['authorships', 'host_venues', 'concepts', 'referenced_works'][-1:]
    for kind in tqdm(kinds):
        print(f'{kind=}')
        final_parq_path = parq_path / f'works_{kind}.parquet'
        (parq_path / f'_works_{kind}').mkdir(exist_ok=True)

        if final_parq_path.exists():
            print(f'Filtered {kind} exists. Skipping')
            continue

        with pd.read_csv(csv_path / f'works_{kind}.csv.gz', engine='c', chunksize=chunksize,
                         dtype=dtypes[kind]) as reader:
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
        split_df.to_parquet(final_parq_path)

        # delete the partial parquets
        # shutil.rmtree(parq_path / f'_works_{kind}')  # TODO: testing needed
    return


def get_concept_workids(concept_name):
    # get concept IDs

    return


def cs_filter():
    # step 0: set paths
    csv_path = Path('/N/project/openalex/ssikdar/csv-files-new')  # set path to flattened CSV files
    parq_path = Path('/N/project/openalex/slices/CS')  # set path for storing the filtered parquet files
    assert csv_path is not None and parq_path is not None, f'please set CSV and parquet directories'

    # step 1: filter the works table to contain specific article types and year ranges
    # write_filtered_works_table(csv_path=csv_path, parq_path=parq_path)

    # step 2: filted the rest of the works_* tables based on the works filtered in step 1
    works_parquet_filename = parq_path / 'works.parquet'
    assert works_parquet_filename.exists(), f'Filtered works parquet does not exists!'

    work_ids = set(
        pd.read_parquet(works_parquet_filename, columns=['work_id'])  # read only the work id column
        .index
    )
    print(next(iter(work_ids)))
    print(f'{len(work_ids):,} work ids loaded')
    write_other_filtered_tables(csv_path=csv_path, parq_path=parq_path, work_ids=work_ids)
    return


def main():
    # step 0: set paths
    csv_path = Path('/N/project/openalex/ssikdar/csv-files-new')  # set path to flattened CSV files
    parq_path = Path('/N/project/openalex/ssikdar/filtered')  # set path for storing the filtered parquet files
    assert csv_path is not None and parq_path is not None, f'please set CSV and parquet directories'

    # step 1: filter the works table to contain specific article types and year ranges
    write_filtered_works_table(csv_path=csv_path, parq_path=parq_path)

    # step 2: filted the rest of the works_* tables based on the works filtered in step 1
    works_parquet_filename = parq_path / 'works.parquet'
    assert works_parquet_filename.exists(), f'Filtered works parquet does not exists!'

    work_ids = set(
        pd.read_parquet(works_parquet_filename, columns=['work_id'])  # read only the work id column
        .index
    )
    print(next(iter(work_ids)))
    print(f'{len(work_ids):,} work ids loaded')
    write_other_filtered_tables(csv_path=csv_path, parq_path=parq_path, work_ids=work_ids)
    return


def main_v2():
    # step 0: set paths
    whole_parq_path = Path(
        '/N/project/openalex/ssikdar/processed-snapshots/parquet-files/aug-2023')  # path to openalex parqs
    filtered_parq_path = Path('/N/project/openalex/ssikdar/filtered')

    # step 1: fitler the works table
    write_filtered_works_table_v2(parq_path=filtered_parq_path, whole_works_parq_path=whole_parq_path / 'works')
    return


if __name__ == '__main__':
    main_v2()
