#!/usr/bin/env python3
"""
fannie_mae_extract_schema_duckdb.py

A script to safely read a CSV file that uses a pipe delimiter, may have malformed lines,
or non-UTF-8 encoding. Uses DuckDB for fast processing and type inference, then outputs
schema details as JSON, saving to ../fanniemae/2024Q3/schema.json.

This version assumes the CSV file has no header row.

Usage:
    python fannie_mae_extract_schema_duckdb.py /path/to/data.csv
"""

import sys
import json
import chardet
import duckdb
import os

try:
    import ace_tools as tools
    HAVE_ACE_TOOLS = True
except ImportError:
    HAVE_ACE_TOOLS = False


def detect_encoding(file_path):
    """Detect file encoding using chardet and treat ASCII as UTF-8."""
    file_encoding = "utf-8"  # Default fallback
    try:
        with open(file_path, "rb") as f:
            raw = f.read(100000)  # Read part of the file
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


def create_temp_table_duckdb(file_path):
    """
    Creates a temporary table 'tmp' in DuckDB from the specified CSV (no header).
    Returns True on success, or False if there's an error.
    """
    encoding = detect_encoding(file_path)

    try:
        print("\n[INFO] Creating a temporary table 'tmp' with DuckDB (header=False)...")
        duckdb.query(f"""
            CREATE OR REPLACE TEMP TABLE tmp AS
            SELECT * 
            FROM read_csv_auto(
                '{file_path}',
                delim='|',
                header=False,          -- No header in the CSV
                encoding='{encoding}'
            )
        """)
        # If no error, we have a temporary table named 'tmp' in DuckDB
        print("[INFO] Successfully created temporary table 'tmp'.")
        return True
    except Exception as e:
        print(f"[ERROR] DuckDB failed to create table from CSV: {e}")
        return False


def analyze_csv_data_types(file_path):
    """
    Creates a DuckDB temporary table from the CSV (no header) and analyzes column data types,
    then outputs a JSON structure like:

    {
      "table": "2024Q3",
      "columns": [
        {
          "name": "column0",
          "type": "VARCHAR",
          "nullability": "NOT NULL" or "NULL",
          "index": 0
        },
        ...
      ]
    }

    Finally, saves it to ../fanniemae/2024Q3/schema.json.
    """
    success = create_temp_table_duckdb(file_path)
    if not success:
        print("[ERROR] Could not load the CSV into a DuckDB table.")
        return

    # Query the schema of the 'tmp' table
    query = "PRAGMA table_info('tmp')"
    type_summary = duckdb.query(query).to_df()

    # Debug: show columns returned
    print("\n[DEBUG] Columns returned by PRAGMA table_info('tmp'):")
    print(type_summary.columns.tolist())
    print(type_summary)

    # Convert to desired JSON format
    columns_info = []
    for _, row in type_summary.iterrows():
        # row['cid']   -> column index (0-based)
        # row['name']  -> column name (e.g. "column0")
        # row['type']  -> column type (DuckDB type, e.g. "VARCHAR")
        # row['notnull'] -> 0 or 1 (False/True)
        nullability = "NOT NULL" if row["notnull"] else "NULL"

        columns_info.append({
            "name": row["name"],
            "type": row["type"],
            "nullability": nullability,
            "index": int(row["cid"])
        })

    # Build the final JSON structure
    schema_json = {
        "table": "2024Q3",
        "columns": columns_info
    }

    # Print or display the JSON
    if HAVE_ACE_TOOLS:
        print("\n[INFO] Column Data Types (JSON):")
        print(json.dumps(schema_json, indent=2))
        print("\n[INFO] Column Data Types (DataFrame):")
        tools.display_dataframe_to_user(
            name="CSV Column Data Types",
            dataframe=type_summary
        )
    else:
        print("\n[INFO] Column Data Types (JSON):")
        print(json.dumps(schema_json, indent=2))

    # Save the JSON to ../fanniemae/2024Q3/schema.json (relative to the script's location)
    output_dir = os.path.join(os.path.dirname(__file__), "../fanniemae/2024Q3")
    output_path = os.path.join(output_dir, "schema.json")

    # Ensure the directory exists
    os.makedirs(output_dir, exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(schema_json, f, indent=2)

    print(f"\n[INFO] JSON schema saved to {output_path}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        input_csv = "../fanniemae/2024Q3/2024Q3_first_65536.csv"
        print(f"[WARN] No CSV file provided; using default path: {input_csv}")
    else:
        input_csv = sys.argv[1]

    analyze_csv_data_types(input_csv)
