#!/usr/bin/env python3
"""
fannie_mae_extract_first_65536_no_header.py

A script to safely read a CSV file (pipe-delimited, possibly malformed or non-UTF-8)
using DuckDB, then store the first 65536 rows to ../fanniemae/2024Q3/2024Q3_first_65536.csv,
with no header line in the resulting CSV.

Usage:
    python fannie_mae_extract_first_65536_no_header.py /path/to/data.csv
"""

import sys
import os
import chardet
import duckdb

def detect_encoding(file_path):
    """
    Detect file encoding using chardet. Treat ASCII as UTF-8,
    because DuckDB doesn't support ASCII as a named encoding.
    """
    file_encoding = "utf-8"  # Default fallback
    try:
        with open(file_path, "rb") as f:
            raw = f.read(100000)  # Read up to 100KB
        guess = chardet.detect(raw)
        detected_encoding = guess["encoding"]

        if detected_encoding is None:
            print("[WARN] Could not detect encoding, falling back to UTF-8.")
        elif detected_encoding.lower() == "ascii":
            print("[INFO] ASCII detected, treating as UTF-8.")
            file_encoding = "utf-8"
        else:
            file_encoding = detected_encoding

        print(f"\n[INFO] Detected file encoding: {file_encoding} (confidence: {guess['confidence']})")
    except Exception as e:
        print(f"[WARN] Could not detect encoding automatically: {e}")

    return file_encoding


def store_first_65536_rows_no_header(file_path):
    """
    Reads the pipe-delimited CSV (with no header) using DuckDB,
    limits to the first 65536 rows, and writes to ../fanniemae/2024Q3/2024Q3_first_65536.csv
    without a header row.
    """
    file_encoding = detect_encoding(file_path)

    # Build output path (relative to this script)
    script_dir = os.path.dirname(__file__)
    output_dir = os.path.join(script_dir, "../fanniemae/2024Q3")
    output_path = os.path.join(output_dir, "2024Q3_first_65536.csv")

    # Ensure the output directory exists
    os.makedirs(output_dir, exist_ok=True)

    try:
        print("\n[INFO] Reading first 65536 rows from CSV (no header) and storing to:")
        print(output_path)

        duckdb.query(f"""
            COPY (
                SELECT *
                FROM read_csv_auto('{file_path}',
                                   delim='|',
                                   header=False,
                                   encoding='{file_encoding}')
                LIMIT 65536
            )
            TO '{output_path}'
            (DELIMITER '|', HEADER FALSE);
        """)

        print("[INFO] Successfully wrote 65536 rows (no header) to", output_path)
    except Exception as e:
        print("[ERROR] Failed to process rows with DuckDB:", e)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        # Default fallback CSV path
        input_csv = "../fanniemae/2024Q3/2024Q3.csv"
        print(f"[WARN] No CSV file provided; using default path: {input_csv}")
    else:
        input_csv = sys.argv[1]

    store_first_65536_rows_no_header(input_csv)
