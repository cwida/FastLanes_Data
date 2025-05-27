#!/usr/bin/env python3
import os
import sys

# Resolve NextiaJD relative to this script’s location,
# then make it absolute so os.walk can find it.
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
ROOT_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, "..", "NextiaJD","tables"))

# Sanity check: does the directory exist?
if not os.path.isdir(ROOT_DIR):
    sys.stderr.write(f"Error: ROOT_DIR '{ROOT_DIR}' does not exist or is not a directory\n")
    sys.exit(1)

def report_csv_sizes(root_dir):
    """
    Walk through root_dir, find all .csv files, and print their
    table_name, version, and file_size in CSV format.
    """
    print("table_name,version,file_size")
    for dirpath, dirnames, filenames in os.walk(root_dir):
        for fn in filenames:
            if fn.lower().endswith(".csv"):
                full_path = os.path.join(dirpath, fn)
                try:
                    size = os.path.getsize(full_path)
                except OSError as e:
                    print(f"# could not get size for {full_path}: {e}", file=sys.stderr)
                    continue

                table_name = os.path.splitext(fn)[0]
                version = "csv"
                print(f"{table_name},{version},{size}")

if __name__ == "__main__":
    print(f"# Scanning: {ROOT_DIR}", file=sys.stderr)
    report_csv_sizes(ROOT_DIR)
