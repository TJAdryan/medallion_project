import gzip
import csv
import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Define input and output file paths
input_dir = os.path.join("data", "bronze")
output_dir = os.path.join("data", "silver")
os.makedirs(output_dir, exist_ok=True)

# The updated VCF filename
vcf_filename = "ALL.chr22.phase3_shapeit2_mvncall_integrated_v5b.20130502.genotypes.vcf.gz"
vcf_file = os.path.join(input_dir, vcf_filename)

# The metadata file name is unchanged
metadata_file = os.path.join(input_dir, "1000_genomes_samples.txt")
output_parquet_file = os.path.join(output_dir, "genotypes_with_metadata.parquet")

def process_and_enrich_data():
    """
    Processes the raw VCF and metadata files to create a structured Parquet file.
    """
    print("Loading metadata...")
    # Load the metadata file into a pandas DataFrame for easy lookup.
    metadata_df = pd.read_csv(metadata_file, sep='\t')
    metadata_dict = metadata_df.set_index('Sample')['Population'].to_dict()

    print("Processing VCF data...")
    all_data = []

    try:
        with gzip.open(vcf_file, 'rt') as f:
            for line in f:
                if line.startswith('##'):
                    continue
                
                if line.startswith('#'):
                    header_line = line.strip('#').strip().split('\t')
                    sample_ids = header_line[9:]
                    continue

                parts = line.strip().split('\t')
                chrom = parts[0]
                pos = int(parts[1])
                ref = parts[3]
                alt = parts[4]

                genotypes = parts[9:]
                for i, gt_data in enumerate(genotypes):
                    sample_id = sample_ids[i]
                    population = metadata_dict.get(sample_id, 'Unknown')
                    
                    record = {
                        'chromosome': chrom,
                        'position': pos,
                        'ref_allele': ref,
                        'alt_allele': alt,
                        'sample_id': sample_id,
                        'population': population,
                        'genotype': gt_data.split(':')[0]
                    }
                    all_data.append(record)

    except FileNotFoundError:
        print(f"Error: One or both of the input files not found. Make sure {vcf_file} and {metadata_file} exist.")
        return

    final_df = pd.DataFrame(all_data)
    arrow_table = pa.Table.from_pandas(final_df)

    print(f"Writing structured data to {output_parquet_file}...")
    pq.write_table(arrow_table, output_parquet_file)
    print("Silver Layer processing complete! Data saved as a Parquet file.")

# Run the function
if __name__ == "__main__":
    process_and_enrich_data()