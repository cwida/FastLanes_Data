#!/usr/bin/env python3
"""
Orchestrator script to run dataset preparation steps in order.

Usage:
    ./run_all.py
"""
import subprocess
import sys


def run_step(script_name):
    """Run a Python script by name and exit on failure."""
    print(f"\n=== Running {script_name} ===")
    try:
        # Use the same Python interpreter
        subprocess.run([sys.executable, script_name], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error: {script_name} failed with exit code {e.returncode}")
        sys.exit(e.returncode)


def main():
    steps = [
        "download.py",
        "decompress_bz2.py",
        "extract_samples.py",
        "generate_csv_schemas.py",
        "check_metadata.py",
        "remove_small_tables.py",
    ]

    for step in steps:
        run_step(step)

    print("\nAll steps completed successfully!")


if __name__ == "__main__":
    main()
