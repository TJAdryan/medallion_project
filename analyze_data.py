import pandas as pd
import os

# Define the file path
gold_dir = os.path.join("data", "gold")
input_file = os.path.join(gold_dir, "disease_study_counts.parquet")

def analyze_gold_data():
    """
    Loads the Gold layer data and displays the top results.
    """
    print("Loading final Gold layer data for analysis...")
    try:
        # Load the Parquet file into a pandas DataFrame
        gold_df = pd.read_parquet(input_file)
        
        # Display the top 10 diseases/traits by study count
        top_results = gold_df.head(10)
        print("\nTop 10 most studied diseases/traits:")
        print(top_results.to_string(index=False))

    except FileNotFoundError:
        print(f"Error: Gold layer file not found at {input_file}. Please run the gold_transform.py script first.")
        return

# Run the function
if __name__ == "__main__":
    analyze_gold_data()