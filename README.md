# OpenAlex

Updated: Jan 16, 2026

## Preprocessing
0. Install `uv` if needed and create the `openalex` uv environment by running `uv init` inside the `openalex` directory.
1. Download the OpenAlex snapshots from [this](https://docs.openalex.org/download-all-data/download-to-your-machine)
   link to a directory of your choosing (say, `basedir`).
2. To run the flattening script, first activate the uv `openalex` environment (if needed) by running `source .venv/bin/activate` inside the directory, then execute `uv run preprocessing/flatten_openalex_files.py`. 
3. Open `preprocessing/flatten_openalex_files.py` and update the following variables:
   a. `BASEDIR` to the directory in Step 1.
   b. `MONTH` to the month of the snapshot, eg: `may-2025`
4. Scroll down to the `if __name__ == '__main__':` block near the end of the file.
5. Uncomment the lines one at a time and run the script `flatten_<entity>` functions to generate the flattened compressed CSV files.
   a. Start with `flatten_merged_entries`, then
   b. Then `flatten_funders`, `flatten_concepts`, ...., `flatten_topics`. 
6. To flatten `works`, uncomment the block of code for `flatten_works_v3`. You flatten all JSONs at once, or do it `N` files at a time by changing the `files_to_process` variable to either `all` or an integer `N`.

**Warnings**:

* flattening _authors_ and _works_ take anywhere between 15 and 30 hours. The code will cache the files, so you
  should consider running it in batches by setting the `files_to_process` variable.
