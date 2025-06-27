import requests
import zipfile
import os
import io

# Set your DuckDB version and architecture
duckdb_version = "v1.3.0"        # the only supported version currently 
arch = "osx_amd64"               # Replace with your architecture (e.g., linux_amd64, osx_amd64)

# Construct download URL
base_url = "https://github.com/haneensa/fade/releases/download/v0.1.0"
filename = f"fade-{duckdb_version}-extension-{arch}.zip"
url = f"{base_url}/{filename}"

# Target directory for extracted extension
ext_dir = os.path.expanduser(f"~/.duckdb/extensions/{duckdb_version}/{arch}/")
os.makedirs(ext_dir, exist_ok=True)

# Download and extract the extension
print("Downloading FaDE extension to:", ext_dir)
response = requests.get(url)
with zipfile.ZipFile(io.BytesIO(response.content)) as z:
    z.extractall(ext_dir)

# Confirm location of the extension file
extracted_path = os.path.join(ext_dir, "fade.duckdb_extension")
print("Extension installed at:", extracted_path)
