#!/usr/bin/env python3
"""
check_metadata.py

Verify that every filename listed in metadata.csv exists
(as either .csv or .csv.bz2) in the <script_dir>/temp/ folder.
Outputs colored, prefixed lines and exits with error if any missing.
"""

import sys
import csv
from pathlib import Path

# ANSI color codes
GREEN = '\033[32m'
RED = '\033[31m'
RESET = '\033[0m'


def main():
    script_dir = Path(__file__).resolve().parent
    base_dir = script_dir / 'temp'

    if not base_dir.is_dir():
        print(f"{RED}-- Error: download directory '{base_dir}' not found.{RESET}", file=sys.stderr)
        sys.exit(1)

    meta_file = base_dir / 'metadata.csv'
    if not meta_file.is_file():
        print(f"{RED}-- Error: metadata file '{meta_file}' not found.{RESET}", file=sys.stderr)
        sys.exit(1)

    missing = []
    with meta_file.open(newline='', encoding='utf-8-sig') as mf:
        reader = csv.DictReader(mf)
        for row in reader:
            fname = (row.get('filename') or '').strip()
            if not fname:
                continue

            csv_path = base_dir / fname
            bz2_path = base_dir / (fname + '.bz2')

            if csv_path.exists():
                print(f"-- {GREEN}{fname}: ✅ FOUND ({csv_path.name}){RESET}")
            elif bz2_path.exists():
                print(f"-- {GREEN}{fname}: ✅ FOUND ({bz2_path.name}){RESET}")
            else:
                print(f"-- {RED}{fname}: ❌ MISSING{RESET}")
                missing.append(fname)

    if missing:
        print(f"-- {RED}{len(missing)} file(s) missing.{RESET}")
        sys.exit(1)
    else:
        print(f"-- {GREEN}All files present.{RESET}")
        sys.exit(0)


if __name__ == '__main__':
    main()
