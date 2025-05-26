PYTHON := python3
SCRIPT := public_bi_extract_schemas.py
VENV_DIR := venv
ENV_SCRIPT := export_fastlanes_data_dir.sh

.PHONY: all env install get_public_bi_schemas clean

# Default: load env, create venv, and run schema extraction
all: env install get_public_bi_schemas

# Load FASTLANES_DATA_DIR and other env vars
env:
	@echo "Loading environment variables..."
	. $(ENV_SCRIPT)

# Set up and activate virtual environment
install: $(VENV_DIR)

$(VENV_DIR):
	@echo "Creating virtual environment..."
	$(PYTHON) -m venv $(VENV_DIR)
	@echo "Upgrading pip..."
	. $(VENV_DIR)/bin/activate && pip install --upgrade pip
	@echo "Installing required Python packages..."
	. $(VENV_DIR)/bin/activate && pip install pyyaml
	@if [ -f requirements.txt ]; then \
		echo "Installing dependencies from requirements.txt..."; \
		. $(VENV_DIR)/bin/activate && pip install -r requirements.txt; \
	fi

# Run the BI schema extraction script
get_public_bi_schemas: install env
	@echo "Extracting public BI schemas..."
	cd scripts && . ../$(VENV_DIR)/bin/activate && $(PYTHON) $(SCRIPT)

# Clean up generated files and virtual environment
clean:
	@echo "Cleaning up..."
	rm -rf $(VENV_DIR) public_bi_benchmark ../public_bi/tables
