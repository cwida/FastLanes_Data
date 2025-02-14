#!/usr/bin/env python3
import os
import bz2
import urllib.request

NUM_LINES = 64 * 1024  # 65536

# List of URLs to download
URLS = [
    "http://event.cwi.nl/da/PublicBIbenchmark/Corporations/Corporations_1.csv.bz2"
]


def download_file(url: str) -> str:
    """
    Download a file from the given URL and return the local filename.
    """
    file_name = os.path.basename(url)
    print(f"Downloading {file_name}...")
    urllib.request.urlretrieve(url, file_name)
    print(f"Downloaded {file_name}")
    return file_name


def decompress_bz2(file_name: str) -> str:
    """
    Decompress a .bz2 file and return the decompressed filename.
    The original .bz2 file is not removed here.
    """
    output_file = file_name.replace('.bz2', '')
    print(f"Decompressing {file_name} to {output_file}...")

    with bz2.open(file_name, 'rb') as fr, open(output_file, 'wb') as fw:
        # Read/decompress in chunks to avoid excessive memory usage.
        for chunk in iter(lambda: fr.read(1024 * 1024), b''):
            fw.write(chunk)

    print(f"Decompressed to {output_file}")
    return output_file


def extract_lines(input_file: str, output_file: str, num_lines: int) -> None:
    """
    Extract the first `num_lines` lines from input_file into output_file.
    """
    print(f"Extracting first {num_lines} lines from {input_file} into {output_file}...")
    with open(input_file, 'r', encoding='utf-8', errors='replace') as infile, \
            open(output_file, 'w', encoding='utf-8') as outfile:

        for i, line in enumerate(infile):
            if i >= num_lines:
                break
            outfile.write(line)

    print(f"Extracted up to {num_lines} lines into {output_file}")


def main():
    for url in URLS:
        # 1. Download
        compressed_file = download_file(url)

        # 2. Decompress
        decompressed_file = decompress_bz2(compressed_file)

        # 3. Remove the compressed file if desired
        if os.path.exists(compressed_file):
            os.remove(compressed_file)
            print(f"Removed {compressed_file}")

        # 4. Extract a sample
        sample_file = decompressed_file.replace('.csv', '_sample.csv')
        extract_lines(decompressed_file, sample_file, NUM_LINES)


if __name__ == "__main__":
    main()
