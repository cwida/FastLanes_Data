# Variables
PYTHON := python3
SCRIPT := public_bi_extract_schemas.py
VENV_DIR := venv

# Targets
.PHONY: all clean get_public_bi_schemas install

# Default target: install and run the script
all: install get_public_bi_schemas

# Set up the virtual environment
install: $(VENV_DIR)

$(VENV_DIR):
	@echo "Creating virtual environment..."
	$(PYTHON) -m venv $(VENV_DIR)
	@echo "Activating virtual environment and upgrading pip..."
	. $(VENV_DIR)/bin/activate && pip install --upgrade pip
	@if [ -f requirements.txt ]; then \
	    echo "Installing dependencies from requirements.txt..."; \
	    . $(VENV_DIR)/bin/activate && pip install -r requirements.txt; \
	else \
	    echo "No requirements.txt found. Skipping dependency installation."; \
	fi

# Run the Python script from the scripts directory
get_public_bi_schemas: $(VENV_DIR)
	@echo "Running the Python script from the scripts directory..."
	cd scripts && . ../$(VENV_DIR)/bin/activate && $(PYTHON) $(SCRIPT)

# Clean up generated files and directories
clean:
	@echo "Cleaning up..."
	rm -rf $(VENV_DIR)
	rm -rf ./public_bi_benchmark
	rm -rf ../public_bi/tables
