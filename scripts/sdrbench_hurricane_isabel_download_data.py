import sys
from pathlib import Path
import numpy as np
import pandas as pd
import json

def main():
    # Convert the input directory string to a Path object
    input_directory = Path("../sdrbench/Hurricane_ISABEL/100X500X500")
    if not input_directory.is_dir():
        print(f"{input_directory} is not a valid directory.")
        sys.exit(1)

    # Dictionary to hold data from each file
    data = {}

    # Process each file in the input directory
    for file_path in input_directory.iterdir():
        if file_path.is_file():
            try:
                # Read the first 65,536 float32 values
                arr = np.fromfile(file_path, dtype=np.float32, count=65536)
                if arr.size < 65536:
                    print(f"Warning: {file_path.name} has less than 65,536 floats. Skipping this file.")
                    continue
                # Use the full file name (including extension) as the column name
                data[file_path.name] = arr[:65536]
                print(f"Processed {file_path.name}")
            except Exception as e:
                print(f"Error processing {file_path.name}: {e}")

    if not data:
        print("No valid data found in the provided directory.")
        sys.exit(1)

    # Create a DataFrame from the data dictionary
    df = pd.DataFrame(data)
    # Reorder the DataFrame columns sorted alphabetically
    df = df[sorted(df.columns)]

    # Define the output directory and ensure it exists
    output_dir = Path("sdrbench/Hurricane_ISABEL")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_csv = output_dir / "output.csv"

    # Write the DataFrame to a CSV file
    df.to_csv(output_csv, index=False)
    print(f"CSV file written to {output_csv}")

    # Create a JSON schema file capturing the column names and types
    # Using the sorted column names from the DataFrame
    sorted_columns = sorted(data.keys())
    columns_schema = []
    for idx, col in enumerate(sorted_columns):
        columns_schema.append({
            "name": col,
            "type": "float",
            "nullability": "NOT NULL",
            "index": idx
        })

    schema = {
        "table": "Bimbo_1",
        "columns": columns_schema
    }

    # Write the schema to a file named schema.json in the output directory
    schema_file = output_dir / "schema.json"
    with open(schema_file, "w") as f:
        json.dump(schema, f, indent=2)
    print(f"Schema JSON written to {schema_file}")

if __name__ == "__main__":
    main()
