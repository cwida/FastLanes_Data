#!/usr/bin/env python3
import pandas as pd
import os
import argparse
import tempfile
import shutil

def reformat_csv_in_place(path):
    """
    Read a '|'-delimited CSV with no header, skip bad lines,
    then write it back in place with:
      - sep="|"
      - quotechar='"'
      - header=False
      - index=False
      - na_rep="NULL"
    """
    # read
    df = pd.read_csv(
        path,
        sep="|",
        quotechar='"',
        header=None,
        on_bad_lines="skip",
        low_memory=False
    )

    # write to a temp file in the same dir, then move over original
    dir_, name = os.path.split(path)
    fd, tmp = tempfile.mkstemp(dir=dir_, prefix=name, suffix=".tmp")
    os.close(fd)

    df.to_csv(
        tmp,
        sep="|",
        quotechar='"',
        header=False,
        index=False,
        na_rep="NULL"
    )
    shutil.move(tmp, path)

def main(root_dir):
    for dirpath, dirnames, filenames in os.walk(root_dir):
        for fn in filenames:
            if fn.lower().endswith(".csv"):
                full = os.path.join(dirpath, fn)
                print(f"Reformatting {full}…")
                try:
                    reformat_csv_in_place(full)
                except Exception as e:
                    print(f"  ✗ failed: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Re-format every CSV under NextiaJD to use pandas' default float formatting, drop "
                    "the index, and write NULL for empty fields."
    )
    parser.add_argument(
        "nextia_dir",
        help="Path to your NextiaJD directory (e.g. /…/FastLanes_Data/NextiaJD)"
    )
    args = parser.parse_args()
    main(args.nextia_dir)
