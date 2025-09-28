import ftplib
import os

# FTP server host for the 1000 Genomes Project
ftp_host = "ftp.1000genomes.ebi.ac.uk"

# Define the paths to the VCF and metadata files
# We will use the base directory for navigation
base_path = "/vol1/ftp/release/20130502/"

# Define the filenames, which are now relative to the base_path
vcf_filename = "ALL.chr22.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz"
metadata_filename = "20130502.phase3_shapeit2_mvncall_integrated_v5a.samples.txt"

def download_ftp_file(remote_path, local_filename):
    """
    Downloads a file from an FTP server by first navigating to the directory.
    """
    if os.path.exists(local_filename):
        print(f"File '{local_filename}' already exists. Skipping download.")
        return

    try:
        with ftplib.FTP(ftp_host) as ftp:
            ftp.login()
            ftp.cwd(os.path.dirname(remote_path)) # Change to the correct directory
            
            with open(local_filename, 'wb') as local_file:
                print(f"Downloading {remote_path} to {local_filename}...")
                # Now use just the filename for the download command
                ftp.retrbinary(f"RETR {os.path.basename(remote_path)}", local_file.write)
                print("Download successful!")

    except ftplib.all_errors as e:
        print(f"FTP error: {e}")

# Download the VCF file
download_ftp_file(os.path.join(base_path, vcf_filename), 'ALL.chr22.vcf.gz')

# Download the metadata file
download_ftp_file(os.path.join(base_path, metadata_filename), '1000_genomes_samples.txt')