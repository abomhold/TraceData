import polars as pl
import os


def process_data(csv_filename):
    """Process the trace data using Polars."""
    # Load data
    print(f"Processing {csv_filename}...")
    df = pl.read_csv(csv_filename)

    # Add computed columns
    df = df.with_columns([
        (df["end_timestamp"].cast(pl.Float64) - df["duration"].cast(pl.Float64)).alias("start"),
        (df["app"].cast(pl.Utf8) + "_" + df["func"].cast(pl.Utf8)).alias("uuid")
    ])

    # Perform aggregation
    result_df = (
        df.group_by("uuid")
        .agg([
            (pl.col("start") - pl.col("start").min()).round(3).alias("time"),
            pl.len().alias("call_count")
        ])
        .sort("call_count", descending=True)
    )

    # Convert time list to string
    result_df = result_df.with_columns(
        pl.col("time").map_elements(lambda x: ", ".join(map(str, x)), return_dtype=pl.Utf8)
    )
    print(result_df)
    # Save results
    output_file = "processed_azure_functions_trace.csv"
    result_df.write_csv(output_file)
    print(f"Results saved to {output_file}")


if __name__ == "__main__":
    filename = "AzureFunctionsInvocationTraceForTwoWeeksJan2021.txt"
    if os.path.exists(filename):
        process_data(filename)
    else:
        print(f"Error: {filename} not found")