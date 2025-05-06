import numpy as np
import csv
import os

def convert_binary_to_csv(input_path, output_dir, output_filename="data.csv"):
    # Ensure the output directory exists
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, output_filename)

    # Read all float32 values from the binary file
    with open(input_path, "rb") as f:
        data = np.fromfile(f, dtype=np.float32)

    # Write all values to a CSV file
    with open(output_path, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        for value in data:
            writer.writerow([value])  # One float per line

    print(f"Wrote {len(data)} float values to '{output_path}'.")

if __name__ == "__main__":
    base_path = "/Users/azim/CLionProjects/FastLanes_Data/issues/cwida/alp/37"

    convert_binary_to_csv(
        input_path=f"{base_path}/kv_cache_original.txt",
        output_dir=f"{base_path}/kv_cache_original"
    )

    convert_binary_to_csv(
        input_path=f"{base_path}/diff_data.txt",
        output_dir=f"{base_path}/diff_data"
    )
