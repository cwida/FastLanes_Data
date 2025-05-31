# Makefile for FastLanes Data workflows

# Force all recipes to run under Bash (so "set -o pipefail" works)
SHELL := /bin/bash

PYTHON              := python3
VENV_DIR            := venv
ENV_SCRIPT          := export_fastlanes_data_dir.sh

# Existing scripts
SCRIPT              := public_bi_extract_schemas.py
REFORMAT            := reformat_csvs.py
CSV_SIZE_REPORT     := csv_size_report.py

# TPCH-related script and outputs
TPCH_DIR            := tpch
TPCH_SCRIPT         := generate_tpch.py
TPCH_DB             := tpch/tpch_sf1.duckdb
TPCH_FULL_OUT       := tpch/temp
TPCH_SAMPLE_OUT     := tpch/tables

.PHONY: all env install get_public_bi_schemas reformat_csvs check_metadata \
        prepare_nextiajd csv_size_report prepare_tpch clean_tpch clean

# Default: load env, create venv, and run schema extraction
all: install get_public_bi_schemas

# Set up (if needed) and install into virtual environment
install:
	@if [ ! -d $(VENV_DIR) ]; then \
		echo "Creating virtual environment..."; \
		$(PYTHON) -m venv $(VENV_DIR); \
	fi
	@echo "Upgrading pip..."
	. $(VENV_DIR)/bin/activate && pip install --upgrade pip
	@echo "Installing required Python packages..."
	. $(VENV_DIR)/bin/activate && pip install pyyaml pandas beautifulsoup4 requests duckdb
	@if [ -f requirements.txt ]; then \
		echo "Installing dependencies from requirements.txt..."; \
		. $(VENV_DIR)/bin/activate && pip install -r requirements.txt; \
	fi

# Run the BI schema extraction script
get_public_bi_schemas: install
	@echo "Extracting public BI schemas..."
	cd scripts && . ../$(VENV_DIR)/bin/activate && $(PYTHON) $(SCRIPT)

# Re-format all CSV files under NextiaJD
reformat_csvs: install
	@echo "Re-formatting all CSV files under NextiaJD..."
	. $(VENV_DIR)/bin/activate && \
		$(PYTHON) scripts/$(REFORMAT) $(FASTLANES_DATA_DIR)/NextiaJD

# --------------------------------------------------------------------
# Verify presence of all files listed in metadata.csv
check_metadata:
	@echo "Verifying NextiaJD downloads against metadata..."
	. $(VENV_DIR)/bin/activate && \
		$(PYTHON) NextiaJD/check_metadata.py

# --------------------------------------------------------------------
# Run the full NextiaJD data preparation pipeline
prepare_nextiajd: install
	@echo "Running full NextiaJD pipeline via prepare.py..."
	# Change directory into NextiaJD/ so that each script is found
	cd NextiaJD && . ../venv/bin/activate && $(PYTHON) prepare.py

# --------------------------------------------------------------------
# Run the CSV‐size report script and save to csv_sizes_report.csv
csv_size_report: install
	@echo "Generating CSV size report..."
	. $(VENV_DIR)/bin/activate && \
		$(PYTHON) scripts/$(CSV_SIZE_REPORT) > csv_sizes_report.csv
	@echo "→ csv_sizes_report.csv created."

# --------------------------------------------------------------------
# TPCH targets

# Generate full TPC-H SF=1 tables and exports inside tpch/
prepare_tpch: install
	@echo "Generating TPC-H SF=1 tables and exporting CSVs/schema under tpch/ ..."
	@cd $(TPCH_DIR) && . ../$(VENV_DIR)/bin/activate && $(PYTHON) $(TPCH_SCRIPT)

# Remove TPCH database and full exports only (preserve sampled tables)
clean_tpch:
	@echo "Cleaning up TPCH artifacts (excluding sampled tables)..."
	@rm -f $(TPCH_DB)
	@rm -rf $(TPCH_FULL_OUT)
	@echo "Removed $(TPCH_DB) and $(TPCH_FULL_OUT)."

# --------------------------------------------------------------------
# Clean up generated files and virtual environment
clean: clean_tpch
	@echo "Cleaning up global artifacts..."
	@rm -rf $(VENV_DIR) public_bi_benchmark ../public_bi/tables csv_sizes_report.csv
	@rm -rf NextiaJD/temp
	@echo "Cleanup complete."
