# Makefile for FastLanes Data workflows

PYTHON       := python3
SCRIPT       := public_bi_extract_schemas.py
VENV_DIR     := venv
ENV_SCRIPT   := export_fastlanes_data_dir.sh
REFORMAT     := reformat_csvs.py

.PHONY: all env install get_public_bi_schemas reformat_csvs clean

# Default: load env, create venv, and run schema extraction
all: env install get_public_bi_schemas

# Load FASTLANES_DATA_DIR and other env vars
env:
	@echo "Loading environment variables..."
	. $(ENV_SCRIPT)

# Set up (if needed) and install into virtual environment
install:
	@if [ ! -d $(VENV_DIR) ]; then \
		echo "Creating virtual environment..."; \
		$(PYTHON) -m venv $(VENV_DIR); \
	fi
	@echo "Upgrading pip..."
	. $(VENV_DIR)/bin/activate && pip install --upgrade pip
	@echo "Installing required Python packages..."
	. $(VENV_DIR)/bin/activate && pip install pyyaml pandas
	@if [ -f requirements.txt ]; then \
		echo "Installing dependencies from requirements.txt..."; \
		. $(VENV_DIR)/bin/activate && pip install -r requirements.txt; \
	fi

# Run the BI schema extraction script
get_public_bi_schemas: install env
	@echo "Extracting public BI schemas..."
	cd scripts && . ../$(VENV_DIR)/bin/activate && $(PYTHON) $(SCRIPT)

# Re-format all CSV files under NextiaJD
reformat_csvs: install env
	@echo "Re-formatting all CSV files under NextiaJD..."
	. $(VENV_DIR)/bin/activate && \
		$(PYTHON) scripts/$(REFORMAT) $(FASTLANES_DATA_DIR)/NextiaJD

# Clean up generated files and virtual environment
clean:
	@echo "Cleaning up..."
	rm -rf $(VENV_DIR) public_bi_benchmark ../public_bi/tables
