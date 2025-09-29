import pandas as pd
import os
import pyarrow as pa
import pyarrow.parquet as pq

# Define file paths
silver_dir = os.path.join("data", "silver")
gold_dir = os.path.join("data", "gold")
os.makedirs(gold_dir, exist_ok=True)

input_file = os.path.join(silver_dir, "gwas_catalog_clean.parquet")
output_journal_file = os.path.join(gold_dir, "journal_study_counts.parquet")
output_disease_file = os.path.join(gold_dir, "disease_study_counts.parquet")

def aggregate_gwas_data():
    """
    Reads the Silver layer data and aggregates it for the Gold layer.
    """
    print("Reading data from the Silver layer...")
    try:
        # Read the clean Parquet file
        clean_df = pd.read_parquet(input_file)
    except FileNotFoundError:
        print(f"Error: Silver layer file not found at {input_file}. Please run the silver_process.py script first.")
        return
        
    # --- Aggregation 1: Count studies per journal ---
    print("Aggregating study counts by journal...")
    journal_counts_df = clean_df.groupby('journal').size().reset_index(name='study_count')
    journal_counts_df.sort_values(by='study_count', ascending=False, inplace=True)
    
    # Save the aggregated data
    journal_table = pa.Table.from_pandas(journal_counts_df)
    pq.write_table(journal_table, output_journal_file)
    print(f"Saved journal study counts to {output_journal_file}")

    # --- Aggregation 2: Count studies per disease/trait ---
    print("Aggregating study counts by disease/trait...")
    disease_counts_df = clean_df.groupby('mapped_trait').size().reset_index(name='study_count')
    disease_counts_df.sort_values(by='study_count', ascending=False, inplace=True)

    # Save the aggregated data
    disease_table = pa.Table.from_pandas(disease_counts_df)
    pq.write_table(disease_table, output_disease_file)
    print(f"Saved disease study counts to {output_disease_file}")

    print("\nGold layer processing complete! Final data is ready for analysis.")

# Run the function
if __name__ == "__main__":
    aggregate_gwas_data()