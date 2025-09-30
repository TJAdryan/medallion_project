import pandas as pd
import os
import matplotlib.pyplot as plt

gold_dir = os.path.join("data", "gold")
input_file = os.path.join(gold_dir, "disease_study_counts.parquet")

def analyze_and_visualize():
    try:
        gold_df = pd.read_parquet(input_file)
        top_results = gold_df.head(10)
        print("Top 10 most studied traits:")
        print(top_results.to_string(index=False))

        # Create a horizontal bar chart with corrected labels
        top_results.plot(kind='barh', x='mapped_trait', y='study_count', legend=False)
        plt.title('Top 10 Most Studied Traits in the GWAS Catalog')
        plt.xlabel('Study Count')
        plt.ylabel('Trait')
        plt.tight_layout()

        # Save the plot to a file
        output_image_path = os.path.join(gold_dir, "trait_counts_chart.png")
        plt.savefig(output_image_path)
        print(f"Chart saved to: {output_image_path}")

    except FileNotFoundError:
        print(f"Error: Gold layer file not found at {input_file}. Please run the gold_transform.py script first.")
        return

if __name__ == "__main__":
    analyze_and_visualize()