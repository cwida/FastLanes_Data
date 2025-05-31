# NextiaJD Data Preparation Pipeline

This document describes the full NextiaJD dataset prepration pipeline, including special cases requiring manual attention.

## Table of Contents

1. [Overview](#overview)
2. [Repository Structure](#repository-structure)
3. [Prerequisites & Setup](#prerequisites--setup)
4. [Usage](#usage)

    * [Run All Steps](#run-all-steps)
    * [Individual Scripts](#individual-scripts)
5. [Script Details](#script-details)
6. [Special Table Handling](#special-table-handling)
7. [Customization](#customization)
8. [License](#license)

---

## Overview

A set of Python scripts to:

1. Download compressed datasets from a remote directory.
2. Decompress `.bz2` archives to CSV.
3. Extract fixed-size samples from each CSV.
4. Generate JSON schemas via DuckDB introspection.
5. Verify metadata presence.
6. Remove any tables below a minimum row threshold.

All scripts can be orchestrated via `prepare.py`.

---

## Repository Structure

```plaintext
NextiaJD/
├── download.py               # Downloads .bz2 files into temp/
├── decompress_bz2.py         # Decompresses .bz2 → .csv in temp/
├── extract_samples.py        # Writes fixed-row samples
├── generate_csv_schemas.py   # Introspects CSVs to JSON schemas
├── check_metadata.py         # Validates metadata entries
├── remove_small_tables.py    # Deletes tables < MIN_ROWS
├── prepare.py                # Runs all steps in order
├── temp/                     # (Auto) Download & decompression staging
└── tables/                   # (Auto) Schema & metadata output
```

---

## Usage

### Run All Steps

```bash
# Make sure prepare.py is executable
chmod +x prepare.py
./prepare.py
```

This executes:

1. `download.py`
2. `decompress_bz2.py`
3. `extract_samples.py`
4. `generate_csv_schemas.py`
5. `check_metadata.py`
6. `remove_small_tables.py`

### Individual Scripts

Run manually in sequence if desired:

```bash
python download.py
python decompress_bz2.py
python extract_samples.py
python generate_csv_schemas.py
python check_metadata.py
python remove_small_tables.py
```

---

## Script Details

### `download.py`

* **Function**: Crawl `BASE_URL`, download `.bz2` files into `temp/`. Skips files matching remote sizes.
* **Key Constants**:

    * `BASE_URL`: source directory URL
    * `dest_dir`: defaults to `temp/` relative to script

### `decompress_bz2.py`

* **Function**: Decompress every `.bz2` in `temp/` to `.csv`, skipping if target exists.
* **Output**: `.csv` files alongside `.bz2` files in `temp/`.

### `extract_samples.py`

* **Function**: Read each CSV and write a sample of `SAMPLE_SIZE` rows to `temp/samples/`.
* **Default Sample Size**: 65,536 rows.

### `generate_csv_schemas.py`

* **Function**: Use DuckDB’s `read_csv_auto` to infer column types/dialects, write each schema to
  `tables/<name>/schema.json`.
* **Failure Handling**: Logs errors when sniffing fails, skips those tables.

### `check_metadata.py`

* **Function**: Confirm each CSV has a corresponding metadata entry; prints ✅ or ❌.

### `remove_small_tables.py`

* **Function**: Count rows in each table; remove directories for tables with fewer than `MIN_ROWS` rows (default
  65,536).

---

## Special Table Handling

The following three tables require manual attention:

### 1. `comments_negative.csv`

* **Status**: No `schema.json` generated.
* **Issue**: CSV sniffing failed (likely pipe-delimited with irregular quoting).
* **Action**:

    1. Inspect the first lines for delimiter/quote/escape settings.
    2. In DuckDB:

       ```sql
       CREATE TABLE tmp AS SELECT * FROM read_csv_auto(
         'tables/comments_negative/comments_negative.csv',
         delim='|', quote='"', escape='\\', strict_mode=false
       );
       PRAGMA write_json_schema('tmp', 'tables/comments_negative/schema.json');
       ```
    3. Commit the generated `schema.json`.

### 2. `comments_positive.csv`

* **Status**: No `schema.json` generated.
* **Issue**: Same sniffing failure as above.
* **Action**: Follow identical steps to produce and commit `tables/comments_positive/schema.json`.

### 3. `glassdoor.csv`

* **Status**: Removed by cleanup (0 rows).
* **Issue**: `remove_small_tables.py` deletes tables <65,536 rows.
* **Action**:

    1. Verify `temp/glassdoor.csv.bz2` contains data.
    2. Re-download or regenerate if missing.
    3. Adjust `MIN_ROWS` threshold or add exception in `remove_small_tables.py`.
    4. Re-run pipeline to restore `tables/glassdoor/`.

---