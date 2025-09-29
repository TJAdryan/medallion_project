import pandas as pd
import os
import pyarrow as pa
import pyarrow.parquet as pq

# Define file paths
input_dir = os.path.join("data", "bronze")
output_dir = os.path.join("data", "silver")
os.makedirs(output_dir, exist_ok=True)

input_file = os.path.join(input_dir, "gwas_catalog.tsv")
output_file = os.path.join(output_dir, "gwas_catalog_clean.parquet")

def process_gwas_data():
    """
    Reads the raw GWAS data, cleans it, and saves it as a Parquet file.
    """
    print("Reading raw data from the Bronze layer...")
    try:
        # Read the raw TSV file. The delimiter is a tab, so we use sep='\t'.
        gwas_df = pd.read_csv(input_file, sep='\t')
    except FileNotFoundError:
        print(f"Error: Raw data file not found at {input_file}. Please ensure the file is in the 'data/bronze' directory.")
        return

    print("Cleaning and structuring the data...")
    
    # We'll select a few key columns to work with
    # This also helps reduce the size of the dataset
    cleaned_df = gwas_df[[
        'DATE ADDED TO CATALOG',
        'JOURNAL',
        'DISEASE/TRAIT',
        'MAPPED_TRAIT',
        'SNPS',
        'P-VALUE',
        'OR or BETA'
    ]].copy()

    # Convert the date column to a proper datetime object
    cleaned_df['DATE ADDED TO CATALOG'] = pd.to_datetime(cleaned_df['DATE ADDED TO CATALOG'])

    # Rename columns for clarity (e.g., lowercase with underscores)
    cleaned_df.rename(columns={
        'DATE ADDED TO CATALOG': 'date_added',
        'JOURNAL': 'journal',
        'DISEASE/TRAIT': 'disease',
        'MAPPED_TRAIT': 'mapped_trait',
        'SNPS': 'snps',
        'P-VALUE': 'p_value',
        'OR or BETA': 'or_or_beta'
    }, inplace=True)

    # Convert the pandas DataFrame to a PyArrow table
    arrow_table = pa.Table.from_pandas(cleaned_df)
    
    # Save the cleaned data as a Parquet file
    print(f"Writing structured data to the Silver layer at {output_file}...")
    pq.write_table(arrow_table, output_file)
    print("Silver layer processing complete!")

# Run the function
if __name__ == "__main__":
    process_gwas_data()