#!/usr/bin/env python3
"""
extract_samples.py

For each .csv file in NextiaJD/temp/, take the first 64*1024 bytes of data
(ignoring the header row), re-serialize using '|' as delimiter and '\n' line
breaks, and write into tables/<csv_name>/<csv_name>.csv.  If the target file
already exists, it’s skipped.
"""

import sys
import csv
from pathlib import Path
from io import StringIO

# how many bytes of output to accumulate
BYTE_LIMIT = 64 * 1024


def extract_sample(in_path: Path, out_path: Path):
    if out_path.exists():
        print(f"-- Skipping {in_path.name}: sample already exists")
        return

    out_path.parent.mkdir(parents=True, exist_ok=True)

    bytes_written = 0
    with in_path.open(newline='', encoding='utf-8') as src, \
            out_path.open('w', newline='', encoding='utf-8') as dst:

        reader = csv.reader(src)  # assume comma-delim input
        writer = csv.writer(dst,
                            delimiter='|',
                            lineterminator='\n')

        # skip header
        try:
            next(reader)
        except StopIteration:
            print(f"-- Warning: {in_path.name} is empty, skipping")
            return

        for row in reader:
            # serialize one row to a string buffer to measure bytes
            buf = StringIO()
            w = csv.writer(buf,
                           delimiter='|',
                           lineterminator='\n')
            w.writerow(row)
            row_str = buf.getvalue()
            row_bytes = row_str.encode('utf-8')

            if bytes_written + len(row_bytes) > BYTE_LIMIT:
                # if adding this row would exceed limit, stop
                break

            dst.write(row_str)
            bytes_written += len(row_bytes)

    print(f"-- Wrote {bytes_written} bytes to {out_path.relative_to(Path.cwd())}")


def main():
    script_dir = Path(__file__).resolve().parent
    input_dir = script_dir / 'temp'
    output_root = script_dir / 'tables'

    if not input_dir.is_dir():
        print(f"❌ Error: input directory {input_dir} not found", file=sys.stderr)
        sys.exit(1)

    # process every .csv in temp/
    for csv_file in sorted(input_dir.glob('*.csv')):
        name = csv_file.stem  # e.g. "freecodecamp_casual_chatroom"
        out_dir = output_root / name
        out_file = out_dir / f"{name}.csv"
        extract_sample(csv_file, out_file)


if __name__ == '__main__':
    main()
