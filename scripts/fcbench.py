import os
import gdown
import subprocess
import numpy as np

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

def process_binary_files(input_dir, output_dir):
    """
    Reads each binary file in the input directory and saves the first 64*1024 values
    to a CSV file. The file type is determined by its name:
      - Files with '_f32' are interpreted as 32-bit floats.
      - Files with '_f64' are interpreted as 64-bit floats.

    Parameters:
    - input_dir: Directory containing the binary files.
    - output_dir: Directory to store the CSV output files.
    """
    os.makedirs(output_dir, exist_ok=True)

    for file_name in os.listdir(input_dir):
        # Determine data type from the filename
        if "_f32" in file_name:
            dtype = np.float32
        elif "_f64" in file_name:
            dtype = np.float64
        else:
            print(f"Skipping {file_name}: unrecognized file type.")
            continue

        input_file = os.path.join(input_dir, file_name)

        # Read the first 64*1024 (65536) values from the binary file
        try:
            data = np.fromfile(input_file, dtype=dtype, count=64 * 1024)
        except Exception as e:
            print(f"Error reading {file_name}: {e}")
            continue

        # Generate the output CSV file name
        base_name, _ = os.path.splitext(file_name)
        output_file = os.path.join(output_dir, base_name + ".csv")

        # Save data to CSV (each value separated by a comma)
        np.savetxt(output_file, data, delimiter=",", fmt="%f")
        print(f"Saved first 64*1024 values from {file_name} to {output_file}")

if __name__ == "__main__":
    # Recommendation: We recommend using gdown to download large files from Google Drive.
    # Install it with: pip install gdown

    # Step 1: Download the HPC, TS, and OBS datasets folder if it doesn't exist.
    folder_url = "https://drive.google.com/drive/folders/1jdnzwvT1hya8XYdEJ7QuqUw3ALbQozc7"
    input_folder = "HPC_TS_OBS"
    if not os.path.exists(input_folder):
        download_folder(folder_url, input_folder)
    else:
        print(f"Folder '{input_folder}' already exists. Skipping download.\n")

    # Step 2: Process the binary files in the downloaded folder.
    csv_output_folder = "HPC_TS_OBS_csv"
    process_binary_files(input_folder, csv_output_folder)
