"""
Refactor SciSciNet based on Dawson's README/slides
"""
import os
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds
import pandas as pd
from tqdm import tqdm
from pathlib import Path

def str_to_int(s: str) -> int:
    """
    Strip the first letter and convert it to an integer.
    """
    return int(s[1:])


def inspect_parquet(file_path):
    file = pq.ParquetFile(file_path)
    meta = file.metadata

    print(f"--- Parquet Inspection: {os.path.basename(file_path)} ---")
    print(f"Total Rows: {meta.num_rows:,}")
    print(f"Total Columns: {meta.num_columns}")
    print(f"Number of Row Groups: {meta.num_row_groups}")
    print("-" * 30)

    # Analyze columns
    for i in range(meta.num_columns):
        col_meta = meta.row_group(0).column(i)
        print(f"Col: {col_meta.path_in_schema}")
        print(f"  - Physical Type: {col_meta.physical_type}")
        print(f"  - Logical Type:  {file.schema.column(i).logical_type}")
        print(f"  - Encoded size:  {col_meta.total_compressed_size / 1024 ** 2:.2f} MB")


def analyze_cardinality(file_path, batch_size=200_000, threshold=0.1):
    parquet_file = pq.ParquetFile(file_path)
    total_rows = parquet_file.metadata.num_rows
    columns = parquet_file.schema.names

    # Track unique values per column
    uniques = {col: set() for col in columns}
    # To keep track of columns that already failed the "categorical" test
    too_high = set()

    print(f"Analyzing cardinality for {total_rows:,} rows...")

    with tqdm(total=total_rows, desc="Streaming for Cardinality") as pbar:
        for batch in parquet_file.iter_batches(batch_size=batch_size):
            df = batch.to_pandas()

            for col in columns:
                if col in too_high:
                    continue

                # Add new unique values to the set
                uniques[col].update(df[col].unique())

                # Circuit breaker: if uniques > 10% of total rows (estimated), skip
                if len(uniques[col]) > (total_rows * threshold):
                    too_high.add(col)

            pbar.update(len(df))

    # Calculate and display results
    results = []
    for col in columns:
        unique_count = len(uniques[col])
        # If it hit the circuit breaker, we know it's "High"
        is_high = col in too_high
        ratio = unique_count / total_rows

        results.append({
            "Column": col,
            "Unique Count": f"{unique_count:,}" + ("+" if is_high else ""),
            "Ratio": f"{ratio:.2%}",
            "Recommendation": "✅ Categorize" if ratio < 0.05 else "❌ Keep as String/Int"
        })

    return pd.DataFrame(results)

def run_final_refactor(input_parquet, output_dir, batch_size=250_000):
    """
    Optimizes a giant Parquet file into a fragmented, typed, and compressed dataset.

    Args:
        input_parquet (str): Path to the source .parquet file
        output_dir (str): Destination directory for the fragmented dataset
        batch_size (int): Number of rows to process in memory at once
    """
    # 1. INITIAL INSPECTION & METADATA
    reader = pq.ParquetFile(input_parquet)
    total_rows = reader.metadata.num_rows
    print(f"🚀 Starting refactor of {total_rows:,} rows...")

    # 2. DEFINE YOUR OPTIMIZED SCHEMA
    # TODO: Update this mapping to match your specific column names and desired types
    optimized_schema = pa.schema([
        ("user_id", pa.int64()),  # Primary Key
        ("status", pa.dictionary(pa.int8(), pa.string())),  # < 128 categories
        ("region", pa.dictionary(pa.int16(), pa.string())),  # < 32k categories
        ("score", pa.int32()),  # Downcasted integer
        ("created_at", pa.timestamp('ms')),  # Millisecond precision
        ("description", pa.string())  # Regular string
    ])

    # 3. THE STREAMING GENERATOR (Pandas logic lives here)
    def stream_and_transform():
        with tqdm(total=total_rows, desc="Optimizing", unit="rows") as pbar:
            for batch in reader.iter_batches(batch_size=batch_size):
                # Convert chunk to Pandas
                df = batch.to_pandas()

                # --- APPLY PANDAS TRANSFORMATIONS ---
                # Categorical conversion
                cat_cols = [f.name for f in optimized_schema if isinstance(f.type, pa.DictionaryType)]
                for col in cat_cols:
                    df[col] = df[col].astype('category')

                # Downcast numeric columns
                df['score'] = pd.to_numeric(df['score'], downcast='integer')
                # ------------------------------------

                # Convert back to Arrow Table using strict schema
                yield pa.Table.from_pandas(df, schema=optimized_schema)
                pbar.update(len(df))

    # 4. CONFIGURE WRITE OPTIONS
    # Fragmentation: max_rows_per_file creates the directory structure
    # Row Group Size: Optimizes internal file scanning (1M is the sweet spot)
    write_options = ds.ParquetFileFormat().make_write_options(
        compression='brotli',
        compression_level=4,  # Balance of speed and size
        row_group_size=1_000_000,
        use_dictionary=True
    )

    # 5. EXECUTE THE WRITE
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    ds.write_dataset(
        stream_and_transform(),
        base_dir=output_dir,
        format='parquet',
        file_options=write_options,
        max_rows_per_file=5_000_000,  # Physical file split
        existing_data_behavior="overwrite_or_ignore"
    )

    print(f"\n✅ Success! Optimized dataset saved to: {output_dir}")


# --- EXECUTION ---
if __name__ == "__main__":
    basepath = Path('/data/shared/SciSciNet')
    refactored_path = basepath / 'refactored'
    # Update these paths to your local files
    SOURCE_FILE = "your_giant_file.parquet"
    TARGET_DIR = "optimized_dataset_v1"

    run_final_refactor(SOURCE_FILE, TARGET_DIR)