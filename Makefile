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

# ────────────────────────────────────────────────────────────────
# Makefile for FastLanes (with FASTLANES_DATA_DIR setup)
# ────────────────────────────────────────────────────────────────

# Path to the helper script
ENV_SCRIPT := ./export_fastlanes_data_dir.sh

# Build directory
BUILD_DIR := build

# Default target
.PHONY: all
all: configure build

# Source env script so FASTLANES_DATA_DIR is set for later targets
# Note: each make recipe runs in its own shell, so we re-source in each.
.PHONY: env
env:
	@. $(ENV_SCRIPT)

# Run CMake configure
.PHONY: configure
configure: env
	@echo "Configuring in $(BUILD_DIR)..."
	@cmake -B $(BUILD_DIR)

# Build the project
.PHONY: build
build: env
	@echo "Building in $(BUILD_DIR)..."
	@cmake --build $(BUILD_DIR)

# Clean out the build directory
.PHONY: clean
clean:
	@echo "Removing $(BUILD_DIR)..."
	@rm -rf $(BUILD_DIR)
