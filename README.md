# OpenAlex

Updated: Feb 23, 2022

## Preprocessing
0. Create the `sci-sci` conda environment from `environment.yml`.
1. Download the OpenAlex snapshots from [this](https://docs.openalex.org/download-snapshot/download-to-your-machine)
   link to a directory of your choosing (say, `basedir`).
2. Open `preprocessing/flatten_openalex_files.py` and update the `BASEDIR` variable to the above directory.
3. Uncomment and run `flatten_<entity>` functions to generate the flattened compressed CSV files.

* The `flatten_works()` function generates CSV and Parquet files at the same time.

**Warnings**:

* flattening _authors_ and _works_ take anywhere between 15 and 30 hours. The code will cache the files, so you
  should consider running it in batches by setting the `files_to_process` variable.

### Coming Soon

* Filtering CSVs based on concepts, publication years, and venues 