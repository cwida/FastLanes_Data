#!/usr/bin/env python3
"""
generate_csv_schemas_duckdb.py

For each tables/<name>/<name>.csv, use DuckDB's read_csv_auto to infer
column types exactly as DuckDB would, and write tables/<name>/schema.json.
Skips any table_dir that already contains schema.json or is named 'metadata'.
"""

import json
from pathlib import Path

import duckdb


def introspect_csv(csv_path: Path):
    """
    Returns DuckDB's DESCRIBE output for read_csv_auto, e.g.:
      [("col1", "VARCHAR", ...), ("col2", "BIGINT", ...), ...]
    We’ll only take the first two fields.
    """
    con = duckdb.connect(database=':memory:')
    query = (
        f"DESCRIBE SELECT * "
        f"FROM read_csv_auto('{csv_path.as_posix()}', delim='|', header=True)"
    )
    return con.execute(query).fetchall()


def main():
    base = Path(__file__).resolve().parent / "tables"
    if not base.is_dir():
        print(f"❌ Error: '{base}' not found.")
        return

    for table_dir in sorted(base.iterdir()):
        if not table_dir.is_dir():
            continue

        # Skip the metadata folder entirely
        if table_dir.name == "metadata":
            print(f"-- Skipping '{table_dir.name}' (metadata file)")
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
            rows = introspect_csv(csv_path)
        except Exception as e:
            print(f"-- ❌ Failed to introspect {csv_path.name}: {e}")
            continue

        schema = {
            "table": table_dir.name,
            "columns": []
        }
        for row in rows:
            col_name = row[0]
            col_type = row[1]
            schema["columns"].append({
                "name": col_name,
                "type": col_type
            })

        with schema_file.open("w", encoding="utf-8") as f:
            json.dump(schema, f, indent=2)
        print(f"-- Wrote DuckDB schema for {table_dir.name} → {schema_file.relative_to(Path.cwd())}")


if __name__ == "__main__":
    main()
