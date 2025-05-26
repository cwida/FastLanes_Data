import os
import subprocess

def find_csv_files(root_dir):
    csv_files = []
    for root, _, files in os.walk(root_dir):
        for file in files:
            if file.endswith(".csv"):
                csv_files.append(os.path.join(root, file))
    return csv_files


def count_csv_rows(csv_file):
    try:
        result = subprocess.run(["wc", "-l", csv_file], capture_output=True, text=True)
        row_count = int(result.stdout.split()[0])
        return row_count
    except Exception as e:
        print(f"⚠️ Error reading {csv_file}: {e}")
        return None


def process_csv_files(csv_files):
    results = []
    for csv_file in csv_files:
        row_count = count_csv_rows(csv_file)
        if row_count is not None:
            results.append({"file": csv_file, "rows": row_count})
    return sorted(results, key=lambda x: x["file"])  # Sort results alphabetically by file name


def main():
    root_dir = "../public_bi/tables"  # Change to the correct directory path

    # Find CSV files
    csv_files = find_csv_files(root_dir)
    print(f"📂 Found {len(csv_files)} CSV files.")

    # Process CSV files
    results = process_csv_files(csv_files)

    # Display results
    for result in results:
        print(f"📊 File: {result['file']} - Rows: {result['rows']}")


if __name__ == "__main__":
    main()
