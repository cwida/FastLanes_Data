#!/usr/bin/env bash
# export_fastlanes_data_dir.sh — export FASTLANES_DATA_DIR for local FastLanes_Data usage

# Resolve this script’s directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Figure out the “default” FastLanes_Data path:
#  • if there's a ./data subfolder, assume SCRIPT_DIR is your FastLanes root
#    and data lives in SCRIPT_DIR/data
#  • otherwise assume SCRIPT_DIR *is* the FastLanes_Data clone itself
if [ -d "${SCRIPT_DIR}/data" ]; then
    DEFAULT_DIR="${SCRIPT_DIR}/data"
else
    DEFAULT_DIR="${SCRIPT_DIR}"
fi

# Let the user override by pre-setting FASTLANES_DATA_DIR, otherwise pick DEFAULT_DIR
DATA_DIR="${FASTLANES_DATA_DIR:-$DEFAULT_DIR}"

# If it still doesn’t exist, bail with actionable error
if [ ! -d "${DATA_DIR}" ]; then
    cat <<EOF >&2
Error: directory '${DATA_DIR}' not found.

Options:
  1) Clone FastLanes_Data into the expected place:
       git clone https://github.com/cwida/FastLanes_Data.git ${SCRIPT_DIR}/data
  2) Or point to any other copy by doing:
       export FASTLANES_DATA_DIR=/path/to/your/FastLanes_Data

Then re-run:
    source export_fastlanes_data_dir.sh
EOF
    return 1
fi

# Export & confirm
export FASTLANES_DATA_DIR="${DATA_DIR}"
echo "FASTLANES_DATA_DIR set to '${FASTLANES_DATA_DIR}'"
