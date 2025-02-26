import os
import polars as pl
import hashlib
import time
import matplotlib.pyplot as plt
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed


def create_hash_id(owner, app, func):
    """Create a consistent hash ID for a function"""
    combined = f"{owner}_{app}_{func}"
    return hashlib.md5(combined.encode()).hexdigest()[:10]


def normalize_time_list(time_list):
    """Normalize a list of timestamps by subtracting the minimum value"""
    if not time_list:
        return []
    min_val = min(time_list)
    return [round(t - min_val, 3) for t in time_list]


def process_file(file_path, file_idx):
    """Process a single invocation file"""
    try:
        # Read the file
        df = pl.read_csv(file_path)
        base_timestamp = file_idx * 24 * 60

        # Initialize collectors for this file
        file_invocations = {}
        file_translations = []

        # Process each row with a progress bar for large files
        row_dicts = df.to_dicts()
        total_rows = len(row_dicts)

        # Only show progress for files with many rows
        if total_rows > 1000:
            row_iterator = tqdm(row_dicts, total=total_rows,
                                desc=f"  Rows in {os.path.basename(file_path)}",
                                leave=False, unit="row")
        else:
            row_iterator = row_dicts

        for row_dict in row_iterator:
            owner = row_dict['HashOwner']
            app = row_dict['HashApp']
            func = row_dict['HashFunction']
            trigger = row_dict['Trigger']

            # Create a unique identifier for this function
            func_id = create_hash_id(owner, app, func)

            # Add to translation data
            file_translations.append({
                'HashOwner': owner,
                'HashApp': app,
                'HashFunction': func,
                'Trigger': trigger,
                'original_uuid': f"{owner}_{app}_{func}",
                'new_hash_uuid': func_id
            })

            # Process each minute with invocations more efficiently
            timestamps = []
            for minute in range(1, 1441):
                minute_str = str(minute)
                if minute_str in row_dict and row_dict[minute_str] > 0:
                    timestamp = base_timestamp + minute
                    # Extend with the right number of timestamps at once
                    timestamps.extend([timestamp] * int(row_dict[minute_str]))

            if timestamps:
                file_invocations[func_id] = timestamps

        return file_invocations, file_translations

    except Exception as e:
        print(f"Error processing file {os.path.basename(file_path)}: {e}")
        return {}, []


def process_invocation_data(base_path, max_workers=4):
    """Process invocation data files to extract timestamps for each function"""
    start_time = time.time()

    # Find all invocation files
    invocation_files = [f for f in os.listdir(base_path)
                        if f.startswith('invocations_per_function_md.anon.d')
                        and f.endswith('.csv')]

    print(f"Found {len(invocation_files)} invocation files")

    # Sort files to ensure consistent processing order
    invocation_files = sorted(invocation_files)

    # Initialize collectors
    function_invocations = {}
    translation_data = []

    # Process files in parallel
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all file processing tasks
        future_to_file = {
            executor.submit(process_file, os.path.join(base_path, file), idx): file
            for idx, file in enumerate(invocation_files)
        }

        # Process results as they complete (without outer tqdm)
        print(f"Processing {len(invocation_files)} files in parallel with {max_workers} workers...")
        completed = 0
        for future in as_completed(future_to_file):
            file = future_to_file[future]
            try:
                file_invocations, file_translations = future.result()

                # Merge file results into main collectors
                for func_id, timestamps in file_invocations.items():
                    if func_id in function_invocations:
                        function_invocations[func_id].extend(timestamps)
                    else:
                        function_invocations[func_id] = timestamps

                translation_data.extend(file_translations)

                # Print progress manually instead of using tqdm
                completed += 1
                print(f"Completed {completed}/{len(invocation_files)} files. Latest: {file}")

            except Exception as e:
                print(f"Error retrieving results for {file}: {e}")

    # Convert to Polars DataFrame
    print("Creating final dataframes...")
    uuids = list(function_invocations.keys())
    times = [function_invocations[func_id] for func_id in uuids]

    # Create dataframe with normalized times and call counts in one step
    df_invocations = pl.DataFrame({
        'uuid': uuids,
        'time': times
    })

    # Normalize timelines and add call count in a single operation
    print("Normalizing timelines...")
    df_invocations = df_invocations.with_columns([
        pl.col('time').map_elements(normalize_time_list, return_dtype=pl.List(pl.Float64)),
        pl.col('time').map_elements(len, return_dtype=pl.Int64).alias('call_count')
    ])

    # Sort by call count
    df_invocations = df_invocations.sort('call_count', descending=True)

    # Create translation dataframe
    translation_df = pl.DataFrame(translation_data).unique()

    elapsed_time = time.time() - start_time
    print(f"Processing completed in {elapsed_time:.2f} seconds")

    return df_invocations, translation_df


def main():
    """Main function to process the dataset"""
    base_path = './azure2019'

    # Check if the directory exists
    if not os.path.exists(base_path):
        print(f"Error: Directory {base_path} not found. Please run the download script first.")
        return

    print("Processing Azure Functions 2019 dataset...")
    # Allow specification of CPU cores to use
    max_workers = 7
    df_invocations, translation_df = process_invocation_data(base_path, max_workers=max_workers)

    # Save intermediate results
    df_invocations.write_parquet("azure2019_invocations.parquet")
    print("Saved processed data to 'azure2019_invocations.parquet'")

    translation_df.write_parquet("azure2019_translation.parquet")
    print("Saved translation data to 'azure2019_translation.parquet'")

    # Analyze and save final results
    print("\nProcessing complete!")


if __name__ == "__main__":
    main()