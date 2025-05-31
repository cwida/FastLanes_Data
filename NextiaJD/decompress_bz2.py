#!/usr/bin/env python3
"""
decompress_bz2.py

Decompress all .csv.bz2 files in NextiaJD/temp (relative to this script).

Usage:
    # from anywhere
    python3 path/to/NextiaJD/decompress_bz2.py
"""

import sys
import bz2
from pathlib import Path

def main():
    # Always use the temp/ folder next to this script
    script_dir = Path(__file__).resolve().parent
    target_dir = script_dir / 'temp'

    if not target_dir.is_dir():
        print(f"❌ Error: '{target_dir}' not found.", file=sys.stderr)
        sys.exit(1)

    bz2_files = sorted(target_dir.glob('*.csv.bz2'))
    if not bz2_files:
        print("ℹ️  No .csv.bz2 files found in temp/.")
        return

    for bz2_path in bz2_files:
        csv_path = bz2_path.with_suffix('')  # drop the .bz2
        if csv_path.exists():
            print(f"-- Skipping {bz2_path.name}: {csv_path.name} already exists")
            continue

        print(f"-- Decompressing {bz2_path.name} → {csv_path.name}")
        try:
            with bz2.open(bz2_path, 'rb') as src, open(csv_path, 'wb') as dst:
                for chunk in iter(lambda: src.read(1024*1024), b''):
                    dst.write(chunk)
        except Exception as e:
            print(f"❌ Error decompressing {bz2_path.name}: {e}", file=sys.stderr)

    print("✅ Done.")

if __name__ == '__main__':
    import sys
    main()
