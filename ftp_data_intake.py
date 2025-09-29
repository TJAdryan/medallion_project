import requests
import os

# Define the source URL for the GWAS Catalog
url = "https://www.ebi.ac.uk/gwas/api/search/downloads/alternative"
filename = "gwas_catalog.tsv"

# Define the Bronze layer directory
bronze_path = os.path.join("data", "bronze")
os.makedirs(bronze_path, exist_ok=True)
local_filepath = os.path.join(bronze_path, filename)

def download_file(url, local_filepath):
    """
    Downloads a file from a URL using the requests library.
    """
    if os.path.exists(local_filepath):
        print(f"File '{local_filepath}' already exists. Skipping download.")
        return

    print(f"Downloading {url} to {local_filepath}...")
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()  # Check for HTTP errors

        with open(local_filepath, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        print("Download successful!")
    except requests.exceptions.RequestException as e:
        print(f"Error during download: {e}")

# Run the download function
if __name__ == "__main__":
    download_file(url, local_filepath)