#!/usr/bin/env python3
"""
generate_csv_schemas_duckdb.py

For each tables/<name>/<name>.csv, use DuckDB's read_csv_auto to infer
column types exactly as DuckDB would, and write tables/<name>/schema.json.
Skips any table_dir that already contains schema.json.
"""

import json
from pathlib import Path

import duckdb


def introspect_csv(csv_path: Path):
    """
    Returns a list of (column_name, duckdb_type) tuples by running:
        DESCRIBE SELECT * FROM read_csv_auto(...)
    """
    con = duckdb.connect(database=':memory:')
    # DuckDB will infer the schema for us
    query = f"DESCRIBE SELECT * FROM read_csv_auto('{csv_path}', delim='|', header=True)"
    result = con.execute(query).fetchall()
    # Each row is (column_name, column_type)
    return result


def main():
    base = Path(__file__).resolve().parent / "tables"
    if not base.is_dir():
        print(f"❌ Error: '{base}' not found.")
        return

    for table_dir in sorted(base.iterdir()):
        if not table_dir.is_dir():
            continue

        schema_file = table_dir / "schema.json"
        if schema_file.exists():
            print(f"-- Skipping {table_dir.name}: schema.json already exists")
            continue

        # Find the csv
        csv_files = list(table_dir.glob("*.csv"))
        if not csv_files:
            print(f"-- Warning: no .csv found in {table_dir.name}")
            continue

        csv_path = csv_files[0]
        try:
            cols = introspect_csv(csv_path)
        except Exception as e:
            print(f"-- ❌ Failed to introspect {csv_path.name}: {e}")
            continue

        schema = {
            "table": table_dir.name,
            "columns": [
                {"name": name, "type": dtype}
                for name, dtype in cols
            ]
        }

        with schema_file.open("w", encoding="utf-8") as f:
            json.dump(schema, f, indent=2)
        print(f"-- Wrote DuckDB schema for {table_dir.name} → {schema_file.relative_to(Path.cwd())}")


if __name__ == "__main__":
    main()
