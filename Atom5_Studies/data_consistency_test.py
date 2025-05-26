import os
import pandas as pd
import datacompy

# Define the directories
source_dir = 'azure_blob_storage'
raw_dir = 's3_raw_layer'
processed_dir = 's3_processed_layer'

# Get the list of CSV files in each directory
source_files = sorted([f for f in os.listdir(source_dir) if f.endswith('.csv')])
raw_files = sorted([f for f in os.listdir(raw_dir) if f.endswith('.csv')])
processed_files = sorted([f for f in os.listdir(processed_dir) if f.endswith('.csv')])
filesCount = len(source_files)
# Ensure there are exactly 15 files in each directory
assert len(source_files) == filesCount, f"There should be exactly {filesCount} CSV files in the source directory"
assert len(raw_files) == 20, f"There should be exactly {filesCount} CSV files in the raw directory"
assert len(processed_files) == 20, f"There should be exactly {filesCount} CSV files in the processed directory"

# Initialize the report
report = []

# Compare files
for source_file, raw_file, processed_file in zip(source_files, raw_files, processed_files):
    source_path = os.path.join(source_dir, source_file)
    raw_path = os.path.join(raw_dir, raw_file)
    processed_path = os.path.join(processed_dir, processed_file)

    # Read the CSV files
    df_source = pd.read_csv(source_path)
    df_raw = pd.read_csv(raw_path)
    df_processed = pd.read_csv(processed_path)

    # Compare source to raw
    compare_source_raw = datacompy.Compare(
        df_source,
        df_raw,
        on_index=True  # Compare based on the index
    )

    # Compare raw to processed
    compare_raw_processed = datacompy.Compare(
        df_raw,
        df_processed,
        on_index=True  # Compare based on the index
    )

    # Append the results to the report
    report.append({
        'source_filename': source_file,
        'raw_filename': raw_file,
        'processed_filename': processed_file,
        'columns_in_source': len(df_source.columns),
        'columns_in_raw': len(df_raw.columns),
        'columns_in_processed': len(df_processed.columns),
        'record_count_in_source': len(df_source),
        'record_count_in_raw': len(df_raw),
        'record_count_in_processed': len(df_processed),
        'source_to_raw_comparison': 'Pass' if compare_source_raw.matches() else 'Fail',
        'raw_to_processed_comparison': 'Pass' if compare_raw_processed.matches() else 'Fail'
    })

# Convert the report to a DataFrame and save as CSV
report_df = pd.DataFrame(report)
report_df.to_csv('reports/data_comparison_report.csv', index=False)

print("Data comparison report generated successfully.")