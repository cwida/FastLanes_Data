import os
import gdown
import subprocess
import numpy as np
import json

def download_folder(folder_url, output_dir):
    """
    Downloads an entire folder from Google Drive using gdown's command-line interface.

    Parameters:
      - folder_url: URL of the Google Drive folder.
      - output_dir: Local directory where the folder will be saved.
    """
    os.makedirs(output_dir, exist_ok=True)
    command = ["gdown", folder_url, "--folder", "-O", output_dir]
    print(f"Downloading folder from {folder_url} to {output_dir}...")
    subprocess.run(command, check=True)
    print("Folder download completed.\n")

def read_binary_files(input_dir, count=64 * 1024, dataset_prefix=""):
    """
    Reads each binary file in the input directory and returns a dictionary mapping
    the column name to its first 'count' values as a NumPy array.

    Files with '_f32' are interpreted as 32-bit floats.
    Files with '_f64' are interpreted as 64-bit floats.

    The column name is constructed as: <dataset_prefix>_<base_file_name>
    """
    files_data = {}
    for file_name in sorted(os.listdir(input_dir)):
        # Determine the data type based on the filename
        if "_f32" in file_name:
            dtype = np.float32
        elif "_f64" in file_name:
            dtype = np.float64
        else:
            print(f"Skipping {file_name}: unrecognized file type.")
            continue

        file_path = os.path.join(input_dir, file_name)
        try:
            data = np.fromfile(file_path, dtype=dtype, count=count)
            base_name, _ = os.path.splitext(file_name)
            # Prepend dataset prefix to the base name
            column_name = f"{dataset_prefix}_{base_name}" if dataset_prefix else base_name
            files_data[column_name] = data
            print(f"Read {data.size} values from {file_name} as column '{column_name}'.")
        except Exception as e:
            print(f"Error reading {file_name}: {e}")
    return files_data

def combine_and_save_csv_ordered(hpc_data, db_data, output_csv):
    """
    Combines the data from both datasets into a single CSV file.
    The HPC/TS/OBS columns (sorted alphabetically) appear first,
    followed by the DB columns (sorted alphabetically).
    Uses the pipe character '|' as the delimiter and does not write any header.
    Replaces any NaN with "Null".
    """
    hpc_keys = sorted(hpc_data.keys())
    db_keys = sorted(db_data.keys())
    combined_keys = hpc_keys + db_keys
    arrays = [hpc_data[k] for k in hpc_keys] + [db_data[k] for k in db_keys]
    combined = np.column_stack(arrays)

    # Replace any NaN with "Null" while converting numbers to strings.
    combined_str = np.where(np.isnan(combined), "Null", np.char.mod("%f", combined))

    with open(output_csv, "w") as f:
        for row in combined_str:
            f.write("|".join(row) + "\n")

    print(f"Combined CSV file saved to {output_csv}")
    return combined_keys

def generate_schema_from_columns(columns, schema_file):
    """
    Generates a JSON schema file based on the given column names.
    For each column, if its name contains '_f32', the type is set to 'float';
    if it contains '_f64', the type is set to 'double'. All columns are marked as NOT NULL.
    """
    schema = {
        "table": "CombinedData",
        "columns": []
    }
    for i, col in enumerate(columns):
        if "_f32" in col:
            col_type = "float"
        elif "_f64" in col:
            col_type = "double"
        else:
            col_type = "unknown"
        schema["columns"].append({
            "name": col,
            "type": col_type,
            "nullability": "NOT NULL",
            "index": i
        })
    with open(schema_file, "w") as f:
        json.dump(schema, f, indent=2)
    print(f"Schema file saved to {schema_file}")

if __name__ == "__main__":
    # URLs for both datasets.
    HPC_TS_OBS_url = "https://drive.google.com/drive/folders/1jdnzwvT1hya8XYdEJ7QuqUw3ALbQozc7"
    DB_url = "https://drive.google.com/drive/folders/1WKvzMErKfhqAGRkJhqXZScH15kPHUxnG"

    # Define separate directories for each dataset.
    hpc_folder = "HPC_TS_OBS"
    db_folder = "DB"

    # Download HPC_TS_OBS folder if not already present.
    if not os.path.exists(hpc_folder):
        print(f"Folder '{hpc_folder}' not found. Starting download...")
        download_folder(HPC_TS_OBS_url, hpc_folder)
    else:
        print(f"Folder '{hpc_folder}' already exists. Skipping download.\n")

    # Download DB folder if not already present.
    if not os.path.exists(db_folder):
        print(f"Folder '{db_folder}' not found. Starting download...")
        download_folder(DB_url, db_folder)
    else:
        print(f"Folder '{db_folder}' already exists. Skipping download.\n")

    # Read binary files from both folders.
    hpc_files_data = read_binary_files(hpc_folder, count=64 * 1024, dataset_prefix="HPC_TS_OBS")
    db_files_data = read_binary_files(db_folder, count=64 * 1024, dataset_prefix="DB")

    if not hpc_files_data and not db_files_data:
        print("No valid binary files found in any dataset. Exiting.")
        exit(1)

    # Combine HPC and DB files into one CSV.
    fc_bench_csv = "../fc_bench/fc_bench.csv"  # Adjust output path as needed.
    combined_columns = combine_and_save_csv_ordered(hpc_files_data, db_files_data, fc_bench_csv)

    # Generate the JSON schema based on the combined column names.
    schema_file = "../fc_bench/schema.json"  # Adjust output path as needed.
    generate_schema_from_columns(combined_columns, schema_file)
