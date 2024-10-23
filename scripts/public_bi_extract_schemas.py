import os
import subprocess
import json
import re


# Step 1: Clone the GitHub repository
def clone_repo(repo_url, clone_dir):
    if not os.path.exists(clone_dir):
        print(f"Cloning repository from {repo_url}...")
        subprocess.run(["git", "clone", repo_url, clone_dir], check=True)
    else:
        print(f"Directory '{clone_dir}' already exists. Skipping clone.")


# Step 2: Walk through directories to find SQL files that contain "1" in their name
def find_sql_files(root_dir):
    sql_files = []
    for root, dirs, files in os.walk(root_dir):
        for file in files:
            if file.endswith(".sql") and "1" in file:
                sql_files.append(os.path.join(root, file))
    return sql_files


# Step 3: Extract field name, type, and nullability from SQL definition
def extract_field_info(line):
    # Regular expression to match the field definition in SQL
    match = re.match(r'^\s*"(.+)"\s+([a-zA-Z]+(?:\(\d+(?:,\s*\d+)?\))?)\s*(NOT\sNULL|NULL)?', line)
    if match:
        field_name = match.group(1)
        field_type = match.group(2)
        nullability = "NOT NULL" if match.group(3) == "NOT NULL" else "NULL"
        return {
            "name": field_name,
            "type": field_type,
            "nullability": nullability
        }
    return None


# Step 4: Convert the SQL table definition to JSON
def convert_sql_to_json(sql_text):
    lines = sql_text.splitlines()
    columns = []
    inside_create_table = False
    table_name = None

    for line in lines:
        # Start capturing when we hit CREATE TABLE
        if 'CREATE TABLE' in line and not inside_create_table:
            inside_create_table = True
            table_name_match = re.search(r'CREATE TABLE\s+"?(\w+)"?', line)
            table_name = table_name_match.group(1) if table_name_match else "UnknownTable"
            continue

        # If we reach the end of the table definition, stop capturing
        if inside_create_table and line.strip() == ");":
            inside_create_table = False
            break

        # Extract column information if inside the CREATE TABLE block
        if inside_create_table:
            field_info = extract_field_info(line)
            if field_info:
                columns.append(field_info)

    if columns and table_name:
        return {"table": table_name, "columns": columns}
    return None


# Step 5: Write the JSON schema to the appropriate directory
def write_json_to_file(json_data, table_name):
    # Create directory structure: ../public_bi/tables/X/X_1/schema.json
    base_table_name = table_name.split("_")[0]  # Extract base table name (e.g., "Arade" from "Arade_1")
    output_dir = f"../public_bi/tables/{base_table_name}/{table_name}/"
    os.makedirs(output_dir, exist_ok=True)

    # Write JSON to schema.json file
    output_file = os.path.join(output_dir, "schema.json")
    with open(output_file, 'w') as json_file:
        json.dump(json_data, json_file, indent=2)
    print(f"Written JSON schema to {output_file}")


# Step 6: Process each SQL file
def process_sql_files(sql_files):
    for sql_file in sql_files:
        with open(sql_file, 'r') as f:
            sql_text = f.read()

            # Only process if the SQL starts with "CREATE TABLE"
            if sql_text.strip().upper().startswith("CREATE TABLE"):
                json_schema = convert_sql_to_json(sql_text)
                if json_schema:
                    table_name = json_schema['table']
                    write_json_to_file(json_schema, table_name)


# Main function to execute the tasks
def main():
    repo_url = "https://github.com/cwida/public_bi_benchmark"
    clone_dir = "./public_bi_benchmark"

    # Clone the repository
    clone_repo(repo_url, clone_dir)

    # Find SQL files in the cloned directory that contain "1" in their name
    sql_files = find_sql_files(clone_dir)
    print(f"Found {len(sql_files)} SQL files with '1' in their name.")

    # Process and convert each SQL file to JSON and write it to the desired path
    process_sql_files(sql_files)


if __name__ == "__main__":
    main()
