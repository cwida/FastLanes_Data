#!/usr/bin/env python3
"""
extract_samples.py

For each .csv file in NextiaJD/temp/, take the first 64*1024 bytes of data
(ignoring the header row), re-serialize using '|' as delimiter and '\n' line
breaks, and write into tables/<csv_name>/<csv_name>.csv.  If the target file
already exists, it’s skipped.

This version reads the delimiter for each file from temp/metadata.csv.
"""

import sys
import csv
import bz2
from pathlib import Path
from io import StringIO

# how many bytes of output to accumulate
BYTE_LIMIT = 64 * 1024

def load_delimiters(metadata_path: Path):
    """Return a dict mapping filename → delimiter (interpreting '\t')."""
    delim_map = {}
    with metadata_path.open(newline='', encoding='utf-8-sig') as mf:
        reader = csv.DictReader(mf)
        for row in reader:
            fname = (row.get('filename') or '').strip()
            raw = (row.get('delimiter') or '').strip()
            if not fname:
                continue
            # decode escape sequences like '\t'
            try:
                delim = raw.encode('utf-8').decode('unicode_escape')
            except Exception:
                delim = raw
            if delim == '':
                delim = ','  # default fallback
            delim_map[fname] = delim
    return delim_map

def extract_sample(in_path: Path, out_path: Path, delim: str):
    if out_path.exists():
        print(f"-- Skipping {in_path.name}: sample already exists")
        return

    out_path.parent.mkdir(parents=True, exist_ok=True)
    bytes_written = 0

    with in_path.open(newline='', encoding='utf-8') as src, \
            out_path.open('w', newline='', encoding='utf-8') as dst:

        reader = csv.reader(src, delimiter=delim)
        writer = csv.writer(dst, delimiter='|', lineterminator='\n')

        # skip header
        try:
            next(reader)
        except StopIteration:
            print(f"-- Warning: {in_path.name} is empty, skipping")
            return

        for row in reader:
            # serialize one row to measure byte length
            buf = StringIO()
            tmp = csv.writer(buf, delimiter='|', lineterminator='\n')
            tmp.writerow(row)
            row_str = buf.getvalue()
            row_bytes = row_str.encode('utf-8')

            if bytes_written + len(row_bytes) > BYTE_LIMIT:
                break

            dst.write(row_str)
            bytes_written += len(row_bytes)

    print(f"-- Wrote {bytes_written} bytes to {out_path.relative_to(Path.cwd())}")

def main():
    script_dir = Path(__file__).resolve().parent
    input_dir  = script_dir / 'temp'
    metadata_path = input_dir / 'metadata.csv'
    output_root = script_dir / 'tables'

    if not input_dir.is_dir():
        print(f"❌ Error: input directory {input_dir} not found", file=sys.stderr)
        sys.exit(1)
    if not metadata_path.is_file():
        print(f"❌ Error: metadata file {metadata_path} not found", file=sys.stderr)
        sys.exit(1)

    # load per-file delimiters
    delim_map = load_delimiters(metadata_path)

    # process every .csv in temp/
    for csv_file in sorted(input_dir.glob('*.csv')):
        name = csv_file.stem
        out_dir  = output_root / name
        out_file = out_dir / f"{name}.csv"
        delim = delim_map.get(csv_file.name, ',')
        extract_sample(csv_file, out_file, delim)

if __name__ == '__main__':
    main()
