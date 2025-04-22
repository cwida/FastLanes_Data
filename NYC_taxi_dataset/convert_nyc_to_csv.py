#!/usr/bin/env python3

import duckdb
import os
import sys
import json
import subprocess

# Hardcoded file paths
parquet_file = 'yellow_tripdata_2025-01.parquet'
csv_file = 'yellow_tripdata_2025-01.csv'
schema_txt_file = 'yellow_tripdata_2025-01.schema.txt'
schema_json_file = 'yellow_tripdata_2025-01.schema.json'

def save_schema(parquet_file, txt_file, json_file):
    try:
        result = duckdb.sql(f"DESCRIBE SELECT * FROM read_parquet('{parquet_file}')").fetchall()

        schema_lines = []
        schema_json = { "columns": [] }

        print("📄 Schema:")
        for name, dtype, *_ in result:
            line = f"{name} {dtype}"
            schema_lines.append(line)
            schema_json["columns"].append({
                "name": name,
                "type": dtype
            })
            print(f"  {line}")

        with open(txt_file, 'w') as f:
            f.write('\n'.join(schema_lines))
        with open(json_file, 'w') as f:
            json.dump(schema_json, f, indent=2)

        print(f"✅ Schema saved to '{txt_file}' and '{json_file}'")

    except Exception as e:
        print(f"❌ Failed to retrieve schema: {e}")
        sys.exit(1)

def convert_parquet_to_csv(parquet_file, csv_file):
    try:
        duckdb.sql(f"""
            COPY (
                SELECT * FROM read_parquet('{parquet_file}') LIMIT 65536
            )
            TO '{csv_file}'
            WITH (HEADER FALSE, DELIMITER ',');
        """)
        print(f"✅ Wrote first 65536 rows of '{parquet_file}' to '{csv_file}' (no header)")

        # Run wc -l to count lines
        result = subprocess.run(['wc', '-l', csv_file], capture_output=True, text=True)
        print(f"📏 Line count: {result.stdout.strip()}")

    except Exception as e:
        print(f"❌ Failed to convert: {e}")
        sys.exit(1)

def main():
    if not os.path.exists(parquet_file):
        print(f"❌ Error: File '{parquet_file}' does not exist.")
        sys.exit(1)

    save_schema(parquet_file, schema_txt_file, schema_json_file)
    convert_parquet_to_csv(parquet_file, csv_file)

if __name__ == '__main__':
    main()
