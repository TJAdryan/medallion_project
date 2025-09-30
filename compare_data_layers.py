import pandas as pd
import os
import pyarrow.parquet as pq

def compare_data_layers():
    """
    Loads a sample of the Bronze and Gold layer data and prints a comparison.
    """
    # Define file paths
    bronze_file = os.path.join("data", "bronze", "gwas_catalog.tsv")
    gold_file = os.path.join("data", "gold", "disease_study_counts.parquet")
    
    # --- Load and display a sample of the Bronze layer data ---
    print("--- Raw Bronze Layer Data (A single line) ---")
    try:
        # Read the raw TSV file and grab the first data line (skipping headers)
        with open(bronze_file, 'r') as f:
            for i, line in enumerate(f):
                if i == 11: # A random line with data, avoiding headers
                    print(line.strip())
                    break
    except FileNotFoundError:
        print(f"Error: Bronze layer file not found at {bronze_file}. Please run the bronze_ingest.py script first.")
        return
        
    print("\n----------------------------------------------------\n")

    # --- Load and display the final Gold layer data ---
    print("--- Polished Gold Layer Data (Top 10 rows) ---")
    try:
        # Load the final aggregated Parquet file
        gold_df = pd.read_parquet(gold_file)
        top_results = gold_df.head(10)
        print(top_results.to_string(index=False))
        
    except FileNotFoundError:
        print(f"Error: Gold layer file not found at {gold_file}. Please run the gold_transform.py script first.")
        return

if __name__ == "__main__":
    compare_data_layers()