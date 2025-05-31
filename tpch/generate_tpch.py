#!/usr/bin/env python3
"""
Combined TPC-H workflow script. Performs the following steps in order:

1. Generate full TPC-H SF=1 tables in DuckDB and export each as a
   pipe-delimited CSV under `temp/<table>/<table>.csv`. Also write
   full CREATE TABLE statements to `temp/schema.sql`.

2. From the DuckDB tables, extract the first 65,536 rows (no header)
   of each table and write them (pipe-delimited) to `tables/<table>/<table>.csv`.

3. For each table, write its column schema as JSON to `tables/<table>/schema.json`.

4. Remove any table—both from DuckDB and its `tables/<table>/` directory—
   if it has fewer than 65,536 rows or if its `tables/<table>/` directory exceeds 100 MB.

After running, you will have:
- `tpch_sf1.duckdb` with only the remaining tables.
- A `temp/` folder containing the full table CSVs and full SQL schema.
- A `tables/` folder containing sampled CSVs and JSON schemas for tables
  that passed the row/size thresholds.
"""

import os
import shutil
import json
from pathlib import Path
import duckdb
from textwrap import indent

# ---------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------

DB_FILE        = "tpch_sf1.duckdb"         # DuckDB database file
FULL_OUT_ROOT  = Path("temp")              # Where to write full CSVs + schema.sql
SAMPLE_OUT_ROOT = Path("tables")           # Where to write sampled CSVs + JSON schemas

TPCH_TABLES    = [
    "region", "nation", "part", "supplier",
    "partsupp", "customer", "orders", "lineitem",
]

ROW_LIMIT      = 64 * 1024                 # 65,536 rows
ROW_THRESHOLD  = ROW_LIMIT                 # same threshold for cleanup
SIZE_THRESHOLD = 100 * 1024 * 1024         # 100 MB for cleanup




# ---------------------------------------------------------------------
# Helpers: Extension loading & materialization
# ---------------------------------------------------------------------

def ensure_tpch_loaded(con):
    """
    Install & load DuckDB’s bundled 'tpch' extension.
    On macOS/arm64, this provides CALL dbgen().
    """
    try:
        con.execute("INSTALL tpch;")
    except duckdb.IOException:
        # Already installed or cached: ignore
        pass
    con.execute("LOAD tpch;")


def materialise_full_tables(con):
    """
    Materialize all TPC-H SF=1 tables in the database using CALL dbgen(sf=>1).
    If they already exist, ignore the 'already exists' error.
    """
    try:
        con.execute("CALL dbgen(sf => 1);")
    except duckdb.CatalogException as e:
        if "already exists" not in str(e):
            raise


# ---------------------------------------------------------------------
# Helpers: Full CSV export + full SQL schema
# ---------------------------------------------------------------------

def export_full_tables_to_csv(con):
    """
    Export every TPCH table to FULL_OUT_ROOT/<table>/<table>.csv
    using pipe-delimited (|) format with a header row.
    """
    for tbl in TPCH_TABLES:
        out_dir = FULL_OUT_ROOT / tbl
        out_dir.mkdir(parents=True, exist_ok=True)
        csv_path = out_dir / f"{tbl}.csv"

        con.execute(
            f"""
            COPY {tbl}
            TO '{csv_path.as_posix()}'
            (HEADER, DELIMITER '|');
            """
        )
        print(f"✔  Full export {tbl:<9} → {csv_path}")


def write_full_schema_sql(con):
    """
    Write a single file FULL_OUT_ROOT/schema.sql containing the CREATE TABLE
    statements for all TPCH tables. Tries 'SHOW CREATE TABLE' (DuckDB ≥0.10),
    else falls back to a PRAGMA-based builder.
    """
    def _schema_from_show_create(table_name):
        return con.execute(f"SHOW CREATE TABLE {table_name};").fetchone()[0] + ";"

    def _schema_from_pragma(table_name):
        # Rebuild a minimal CREATE TABLE using PRAGMA table_info
        rows = con.execute(f"PRAGMA table_info('{table_name}');").fetchall()
        # Each row: (cid, name, type, notnull, dflt_value, pk)
        col_lines = []
        pk_cols = []
        for cid, name, col_type, notnull, default, pk in rows:
            parts = [f'"{name}"', col_type]
            if notnull:
                parts.append("NOT NULL")
            if default is not None:
                parts.append(f"DEFAULT {default}")
            col_lines.append(" ".join(parts))
            if pk:
                pk_cols.append(f'"{name}"')
        if pk_cols:
            col_lines.append(f"PRIMARY KEY ({', '.join(pk_cols)})")

        body = ",\n  ".join(col_lines)
        return f"CREATE TABLE {table_name} (\n  {body}\n);"

    statements = []
    for tbl in TPCH_TABLES:
        try:
            stmt = _schema_from_show_create(tbl)
        except duckdb.ParserException:
            stmt = _schema_from_pragma(tbl)
        statements.append(stmt)

    schema_path = FULL_OUT_ROOT / "schema.sql"
    schema_path.parent.mkdir(parents=True, exist_ok=True)
    schema_path.write_text("\n\n".join(statements), encoding="utf-8")
    print(f"🗒  Wrote full schema to {schema_path}")


# ---------------------------------------------------------------------
# Helpers: Sample CSV export (first ROW_LIMIT rows, no header)
# ---------------------------------------------------------------------

def export_sampled_tables_to_csv(con):
    """
    Export at most ROW_LIMIT rows from each TPCH table to
    SAMPLE_OUT_ROOT/<table>/<table>.csv (pipe-delimited, no header).
    """
    for tbl in TPCH_TABLES:
        dest_dir = SAMPLE_OUT_ROOT / tbl
        dest_dir.mkdir(parents=True, exist_ok=True)
        csv_path = dest_dir / f"{tbl}.csv"

        con.execute(
            f"""
            COPY (
                SELECT *
                FROM {tbl}
                LIMIT {ROW_LIMIT}
            )
            TO '{csv_path.as_posix()}'
            (HEADER FALSE, DELIMITER '|');
            """
        )
        print(f"✔  Sample export {tbl:<9} → {csv_path} (no header)")


# ---------------------------------------------------------------------
# Helpers: Schema JSON per table
# ---------------------------------------------------------------------

def write_table_schema_json(con):
    """
    For each TPCH table, write a JSON schema file at
    SAMPLE_OUT_ROOT/<table>/schema.json with format:
    {
        "columns": [
            {"name": "<col>", "type": "<type>", "index": <cid> },
            ...
        ]
    }
    """
    for tbl in TPCH_TABLES:
        dest_dir = SAMPLE_OUT_ROOT / tbl
        dest_dir.mkdir(parents=True, exist_ok=True)
        schema_path = dest_dir / "schema.json"

        # PRAGMA table_info returns: (cid, name, type, notnull, dflt_value, pk)
        rows = con.execute(f"PRAGMA table_info('{tbl}');").fetchall()
        columns = []
        for cid, name, col_type, notnull, default, pk in rows:
            columns.append({
                "name": name,
                "type": col_type,
                "index": cid
            })

        schema_obj = {"columns": columns}
        with open(schema_path, "w", encoding="utf-8") as f:
            json.dump(schema_obj, f, indent=4)
        print(f"✔  Wrote JSON schema {tbl:<9} → {schema_path}")


# ---------------------------------------------------------------------
# Helpers: Cleanup based on row count / sample directory size
# ---------------------------------------------------------------------

def get_row_count(con, table_name: str) -> int:
    """
    Return the total number of rows in table_name, or None if table doesn't exist.
    """
    try:
        result = con.execute(f"SELECT COUNT(*) FROM {table_name};").fetchone()
        return result[0]
    except duckdb.CatalogException:
        return None


def get_directory_size_bytes(dir_path: Path) -> int:
    """
    Recursively sum file sizes under dir_path. Return 0 if directory doesn't exist.
    """
    total = 0
    if not dir_path.exists():
        return 0
    for root, dirs, files in os.walk(dir_path):
        for f in files:
            try:
                fp = Path(root) / f
                total += fp.stat().st_size
            except (FileNotFoundError, PermissionError):
                continue
    return total


def drop_table(con, table_name: str):
    """
    Drop table_name from the DuckDB database (IF EXISTS).
    """
    con.execute(f"DROP TABLE IF EXISTS {table_name};")


def remove_directory(dir_path: Path):
    """
    Remove dir_path and all its contents, if it exists.
    """
    if dir_path.exists():
        shutil.rmtree(dir_path)


def cleanup_tables(con):
    """
    For each TPCH table, check:
      - row_count < ROW_THRESHOLD
      - OR sample directory size > SIZE_THRESHOLD
    If either condition is true, DROP the table from the database and
    delete SAMPLE_OUT_ROOT/<table>/ directory.
    """
    for tbl in TPCH_TABLES:
        row_count = get_row_count(con, tbl)
        if row_count is None:
            print(f"⚠  Table '{tbl}' does not exist; skipping cleanup.")
            continue

        tbl_dir = SAMPLE_OUT_ROOT / tbl
        dir_size = get_directory_size_bytes(tbl_dir)

        too_few_rows = row_count < ROW_THRESHOLD
        too_large = dir_size > SIZE_THRESHOLD

        if too_few_rows or too_large:
            reasons = []
            if too_few_rows:
                reasons.append(f"rows {row_count} < {ROW_THRESHOLD}")
            if too_large:
                reasons.append(f"size {dir_size / (1024*1024):.2f} MB > 100 MB")
            reason_str = " and ".join(reasons)

            drop_table(con, tbl)
            remove_directory(tbl_dir)
            print(f"→ Removed '{tbl}': {reason_str}")
        else:
            print(f"✔  Kept '{tbl}': {row_count} rows, {dir_size / (1024*1024):.2f} MB in samples")


# ---------------------------------------------------------------------
# Main orchestration
# ---------------------------------------------------------------------

def main():
    con = duckdb.connect(DB_FILE)

    # Step 1: Generate full tables + full schema in "temp/"
    print("\n=== Step 1: Generate full TPC-H tables + export to 'temp/' ===")
    ensure_tpch_loaded(con)
    materialise_full_tables(con)
    export_full_tables_to_csv(con)
    write_full_schema_sql(con)

    # Step 2: Extract first ROW_LIMIT rows into "tables/"
    print("\n=== Step 2: Export first 65,536 rows (no header) to 'tables/' ===")
    export_sampled_tables_to_csv(con)

    # Step 3: Write JSON schema for each table under "tables/<table>/schema.json"
    print("\n=== Step 3: Write JSON schema for each table ===")
    write_table_schema_json(con)

    # Step 4: Cleanup based on row count or sample directory size
    print("\n=== Step 4: Cleanup tables with <65,536 rows or >100 MB in samples ===")
    cleanup_tables(con)

    con.close()
    print("\n=== Workflow complete ===")
    print(f"- DuckDB file: {DB_FILE}")
    print(f"- Full exports: {FULL_OUT_ROOT}/")
    print(f"- Sample exports + JSON schemas: {SAMPLE_OUT_ROOT}/")
    print(f"Tables failing thresholds have been removed from both the DB and '{SAMPLE_OUT_ROOT}/'.")


if __name__ == "__main__":
    main()
