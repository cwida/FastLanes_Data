#!/usr/bin/env python3
"""
remove_small_or_oversized_tables.py

Remove any table directory under tables/ whose sample CSV has fewer
than N_ROWS data rows or whose sample CSV exceeds MAX_SIZE bytes.
Assumes sample CSVs have no header row. Skips the 'metadata' directory.
Additionally, always remove the 'github_issues' directory unconditionally.
"""

import sys
import shutil
from pathlib import Path

# minimum number of rows required to keep a table
N_ROWS = 64 * 1024
# maximum sample CSV size (in bytes) allowed to keep a table (100 MB)
MAX_SIZE = 100 * 1024 * 1024


def main():
    script_dir = Path(__file__).resolve().parent
    tables_dir = script_dir / "tables"

    if not tables_dir.is_dir():
        print(f"❌ Error: '{tables_dir}' not found", file=sys.stderr)
        sys.exit(1)

    for table_dir in sorted(tables_dir.iterdir()):
        if not table_dir.is_dir():
            continue

        # never touch the metadata directory
        if table_dir.name == "metadata":
            print(f"-- Skipping metadata directory")
            continue

        # forcibly remove the "github_issues" table unconditionally
        if table_dir.name == "github_issues":
            print(f"-- Removing {table_dir.name}: forced deletion")
            shutil.rmtree(table_dir)
            continue

        sample_csv = table_dir / f"{table_dir.name}.csv"
        if not sample_csv.exists():
            print(f"-- Warning: no sample CSV in {table_dir.name}, skipping")
            continue

        # check file size
        try:
            size_bytes = sample_csv.stat().st_size
        except Exception as e:
            print(f"-- Error getting size of {sample_csv}: {e}", file=sys.stderr)
            continue

        if size_bytes > MAX_SIZE:
            print(f"-- Removing {table_dir.name}: sample CSV size {size_bytes} bytes (> {MAX_SIZE})")
            shutil.rmtree(table_dir)
            continue

        # count the number of lines (rows) in the sample
        try:
            with sample_csv.open('r', encoding='utf-8') as f:
                row_count = sum(1 for _ in f)
        except Exception as e:
            print(f"-- Error reading {sample_csv}: {e}", file=sys.stderr)
            continue

        if row_count < N_ROWS:
            print(f"-- Removing {table_dir.name}: only {row_count} rows (< {N_ROWS})")
            shutil.rmtree(table_dir)
        else:
            print(f"-- Keeping   {table_dir.name}: {row_count} rows (≥ {N_ROWS}), size {size_bytes} bytes")

    print("✅ Done.")


if __name__ == "__main__":
    main()
