import os
import gdown

def download_file(file_id, file_name, output_dir):
    """
    Downloads a single file from Google Drive using its file ID if it doesn't exist already.

    Parameters:
    - file_id: The file's unique Google Drive ID.
    - file_name: The name under which the file will be saved.
    - output_dir: Local directory to save the file.
    """
    # Ensure the output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Construct the output file path
    output_path = os.path.join(output_dir, file_name)

    # Check if file already exists
    if os.path.exists(output_path):
        print(f"File '{file_name}' already exists in {output_dir}. Skipping download.\n")
        return

    # Construct the download URL
    url = f"https://drive.google.com/uc?id={file_id}"

    print(f"Downloading '{file_name}' from {url} to {output_path}...")
    # Download the file
    gdown.download(url, output_path, quiet=False)
    print(f"Download of '{file_name}' completed.\n")

if __name__ == "__main__":
    # Dictionary of HPC files with their corresponding Google Drive file IDs
    hpc_files = {
        "msg-bt": "15S7iTr_Yoo6oVv5TOemah0wP1K7VX5R1",
        "num-brain": "1D2WEJonO3GWQwAQxSokO6Pn4kffalhCy",
        "num-control": "13Lpx_S0W4K5hBMOvyW61PFUOv9BXGOMN",
        "rsim": "1C6opL2ZyJyU4074uc9T9eJBnyTvhW16-",
        "astro-mhd": "1gp2pUEtr8FP3g7hbu4EhDYtTyg2eVoBr",
        "astro-pt": "1ZI6h-8OOW2h7DG9L4P9tIGrUo_KBMH2R",
        "miranda3d": "1jTCH1i_1w_zGvfBydT1Ac-kJK-H4DZHS",
        "turbulance": "11MNFi9pGpU9IDw1QSZrA-y2oY5xPNyzs",
        "wave": "1jokNzOoEHOrXpCw8glhGzA_1lJxedKx2",
        "hurricane": "1h48gO2JNCNWMVaEPsIHPBkNllgtHDzcf"
    }

    # Set the output directory for HPC files
    output_directory = "Hpc"

    # Download each HPC file individually if it doesn't exist
    for name, file_id in hpc_files.items():
        download_file(file_id, name, output_directory)