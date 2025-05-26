import sys
from pathlib import Path
import numpy as np
import pandas as pd
import json
import urllib.request
import tarfile
from tqdm import tqdm

def download_with_progress(url, filename):
    # Create a tqdm progress bar instance
    with tqdm(unit='B', unit_scale=True, miniters=1, desc=filename.name) as t:
        def reporthook(block_num, block_size, total_size):
            if total_size is not None:
                t.total = total_size
            t.update(block_size)
        urllib.request.urlretrieve(url, filename, reporthook=reporthook)

def main():
    # Step 1: Download the dataset tarball if it doesn't exist
    download_url = ("https://g-8d6b0.fd635.8443.data.globus.org/ds131.2/"
                    "Data-Reduction-Repo/raw-data/Hurricane-ISABEL/"
                    "SDRBENCH-Hurricane-ISABEL-100x500x500.tar.gz")
    tar_file = Path("SDRBENCH-Hurricane-ISABEL-100x500x500.tar.gz")

    if not tar_file.exists():
        print("Downloading dataset...")
        download_with_progress(download_url, tar_file)
        print("Download complete.")
    else:
        print("Tar file already exists; skipping download.")

    # Step 2: Decompress the dataset if the input directory does not exist
    # Here we assume the tarball extracts a folder named "100X500X500" inside "../sdrbench/Hurricane_ISABEL"
    input_directory = Path("../sdrbench/Hurricane_ISABEL/100X500X500")
    if not input_directory.is_dir():
        print("Extracting dataset...")
        extraction_dir = input_directory.parent  # Extract into "../sdrbench/Hurricane_ISABEL"
        with tarfile.open(tar_file, "r:gz") as tar:
            tar.extractall(path=extraction_dir)
        print("Extraction complete.")
    else:
        print("Input directory already exists; skipping extraction.")

    # Step 3: Process each file in the input directory
    data = {}
    for file_path in input_directory.iterdir():
        if file_path.is_file():
            try:
                # Read the first 65,536 float32 values
                arr = np.fromfile(file_path, dtype=np.float32, count=65536)
                if arr.size < 65536:
                    print(f"Warning: {file_path.name} has less than 65,536 floats. Skipping this file.")
                    continue
                # Use the full file name (including extension) as the column name
                data[file_path.name] = arr[:65536]
                print(f"Processed {file_path.name}")
            except Exception as e:
                print(f"Error processing {file_path.name}: {e}")

    if not data:
        print("No valid data found in the provided directory.")
        sys.exit(1)

    # Step 4: Create a DataFrame and sort columns alphabetically
    df = pd.DataFrame(data)
    df = df[sorted(df.columns)]

    # Define the output directory (../sdrbench/Hurricane_ISABEL) and ensure it exists
    output_dir = Path("../sdrbench/Hurricane_ISABEL")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_csv = output_dir / "output.csv"

    # Write the DataFrame to a CSV file without a header row, using "|" as the delimiter
    df.to_csv(output_csv, index=False, header=False, sep="|")
    print(f"CSV file written to {output_csv}")

    # Step 5: Generate a JSON schema file capturing the column names and types
    sorted_columns = sorted(data.keys())
    columns_schema = []
    for idx, col in enumerate(sorted_columns):
        columns_schema.append({
            "name": col,
            "type": "float",
            "nullability": "NOT NULL",
            "index": idx
        })

    schema = {
        "table": "Bimbo_1",
        "columns": columns_schema
    }

    schema_file = output_dir / "schema.json"
    with open(schema_file, "w") as f:
        json.dump(schema, f, indent=2)
    print(f"Schema JSON written to {schema_file}")

if __name__ == "__main__":
    main()
