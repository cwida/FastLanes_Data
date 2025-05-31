#!/usr/bin/env python3
"""
extract_samples.py

For each .csv file in NextiaJD/temp/, take the first N_ROWS of data
(ignoring the header row), re-serialize using '|' as delimiter and '\n'
line breaks, and write into tables/<csv_name>/<csv_name>.csv. If the
target file already exists, it’s skipped.

This version reads the delimiter for each file from temp/metadata.csv
and strips out any NUL bytes before parsing, and replaces empty values with 'null'.
"""

import sys
import csv
from pathlib import Path

# how many data rows (excluding header) to extract
N_ROWS = 64 * 1024


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
                delim = ','  # fallback
            delim_map[fname] = delim
    return delim_map


def extract_sample(in_path: Path, out_path: Path, delim: str):
    if out_path.exists():
        print(f"-- Skipping {in_path.name}: sample already exists")
        return

    out_path.parent.mkdir(parents=True, exist_ok=True)

    with in_path.open('r', newline='', encoding='utf-8', errors='replace') as src, \
            out_path.open('w', newline='', encoding='utf-8') as dst:

        # Wrap src lines to strip any NUL bytes before parsing:
        def nul_stripped_lines():
            for line in src:
                yield line.replace('\x00', '')

        reader = csv.reader(nul_stripped_lines(), delimiter=delim)
        writer = csv.writer(dst, delimiter='|', lineterminator='\n')

        # skip header
        try:
            next(reader)
        except StopIteration:
            print(f"-- Warning: {in_path.name} is empty, skipping")
            return

        row_count = 0
        for row in reader:
            if row_count >= N_ROWS:
                break
            # replace empty fields with 'null'
            cleaned_row = [cell if cell != '' else 'null' for cell in row]
            writer.writerow(cleaned_row)
            row_count += 1

    print(f"-- Wrote {row_count} rows to {out_path.relative_to(Path.cwd())}")


def main():
    script_dir = Path(__file__).resolve().parent
    input_dir = script_dir / 'temp'
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
        out_dir = output_root / name
        out_file = out_dir / f"{name}.csv"
        delim = delim_map.get(csv_file.name, ',')
        extract_sample(csv_file, out_file, delim)


if __name__ == '__main__':
    main()
