import requests
import zipfile
import os
import io
import argparse

# Set your DuckDB version and architecture
parser = argparse.ArgumentParser(description='TPCH benchmarking script')
parser.add_argument('--duckdb_version', type=str, help='duckdb version', default='v1.3.0') # the only supported version currently 
parser.add_argument('--arch', type=str, help='arch', default='osx_amd64') # Replace with your architecture (e.g., linux_amd64, osx_amd64)
parser.add_argument('--name', type=str, help='extension name', default='lineage') # Replace with your architecture (e.g., linux_amd64, osx_amd64)
args = parser.parse_args()
duckdb_version = args.duckdb_version
arch = args.arch
name = args.name

# Construct download URL
base_url = f"https://github.com/haneensa/{name}/releases/download/v0.1.0"
filename = f"{name}-{duckdb_version}-extension-{arch}.zip"
url = f"{base_url}/{filename}"

# Target directory for extracted extension
ext_dir = os.path.expanduser(f"~/.duckdb/extensions/{duckdb_version}/{arch}/")
os.makedirs(ext_dir, exist_ok=True)

# Download and extract the extension
print("Downloading lineage extension to:", ext_dir)
response = requests.get(url)
with zipfile.ZipFile(io.BytesIO(response.content)) as z:
    z.extractall(ext_dir)

# Confirm location of the extension file
extracted_path = os.path.join(ext_dir, f"{name}.duckdb_extension")
print("Extension installed at:", extracted_path)
