# OpenAlex

Updated: Feb 23, 2022

## Preprocessing
0. Create the `sci-sci` uv environment by running `uv init` inside the `openalex` directory.
1. Download the OpenAlex snapshots from [this](https://docs.openalex.org/download-snapshot/download-to-your-machine)
   link to a directory of your choosing (say, `basedir`).
2. Open `preprocessing/flatten_openalex_files.py` and update the following variables:
   a. `BASEDIR` to the directory in Step 1.
   b. `MONTH` to the month of the snapshot, eg: `may-2025`
3. Scroll down to the `if __name__ == '__main__':` block near the end of the file.
4. Uncomment the lines one at a time and run the script `flatten_<entity>` functions to generate the flattened compressed CSV & Parquet files.
   a. Start with `flatten_merged_entries`, then
   b. Then `flatten_funders`, `flatten_concepts`, ...., `flatten_topics`. 
* The `flatten_works()` function generates CSV and Parquet files at the same time.

**Warnings**:

* flattening _authors_ and _works_ take anywhere between 15 and 30 hours. The code will cache the files, so you
  should consider running it in batches by setting the `files_to_process` variable.

### Coming Soon

* Filtering CSVs based on concepts, publication years, and venues 
