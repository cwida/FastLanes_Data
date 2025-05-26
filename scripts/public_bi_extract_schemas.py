import os
import shutil
import subprocess
import json
import yaml
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


# Helper function: Map SQL data types to YAML-friendly types
def map_sql_to_yaml_type(sql_type):
    sql_type = sql_type.lower()
    if "int" in sql_type:
        return "integer"
    elif "varchar" in sql_type or "char" in sql_type or "text" in sql_type:
        return "string"
    elif "decimal" in sql_type or "double" in sql_type or "float" in sql_type:
        return "double"
    elif "timestamp" in sql_type or "date" in sql_type:
        return "string"
    else:
        return "string"  # Default to string


# Step 4: Convert the SQL table definition to JSON and YAML
def convert_sql_to_json_and_yaml(sql_text):
    lines = sql_text.splitlines()
    columns = []
    inside_create_table = False
    table_name = None

    column_index = 0  # Initialize column index

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
                # Add column index to the field information
                field_info['index'] = column_index
                columns.append(field_info)
                column_index += 1  # Increment the column index

    if columns and table_name:
        json_schema = {"table": table_name, "columns": columns}

        # Convert JSON schema to YAML format
        yaml_schema = {"columns": [{"name": col["name"], "type": map_sql_to_yaml_type(col["type"])} for col in columns]}

        return json_schema, yaml_schema
    return None, None


# Step 5: Write JSON and YAML schema to the appropriate directory, using CRLF line endings
def write_schema_files(json_data, yaml_data, table_name):
    base_table_name = table_name.split("_")[0]
    output_dir = f"../public_bi/tables/{base_table_name}/{table_name}/"
    os.makedirs(output_dir, exist_ok=True)

    json_output_file = os.path.join(output_dir, "schema.json")
    yaml_output_file = os.path.join(output_dir, "schema.yaml")

    # Open with newline='\r\n' so that every '\n' in your data is written as CRLF
    with open(json_output_file, 'w', newline='\r\n', encoding='utf-8') as json_file:
        json.dump(json_data, json_file, indent=2)
    print(f"✅ JSON schema written to {json_output_file} (CRLF endings)")

    with open(yaml_output_file, 'w', newline='\r\n', encoding='utf-8') as yaml_file:
        yaml.dump(yaml_data, yaml_file, default_flow_style=False)
    print(f"✅ YAML schema written to {yaml_output_file} (CRLF endings)")


# Step 6: Process each SQL file for JSON & YAML
def process_sql_files(sql_files):
    schemas = []
    for sql_file in sql_files:
        with open(sql_file, 'r') as f:
            sql_text = f.read()

            if sql_text.strip().upper().startswith("CREATE TABLE"):
                json_schema, yaml_schema = convert_sql_to_json_and_yaml(sql_text)
                if json_schema and yaml_schema:
                    schemas.append(json_schema)
                    write_schema_files(json_schema, yaml_schema, json_schema["table"])

    return schemas


# Step 7: Summarize column types
def summarize_column_types(schemas):
    total_columns = 0
    type_counts = {}

    for json_schema in schemas:
        table_name = json_schema.get('table', 'UnknownTable')
        print(f"📌 Table: {table_name}")

        for column in json_schema.get('columns', []):
            total_columns += 1
            column_type = column['type'].lower()
            if column_type not in type_counts:
                type_counts[column_type] = 0
            type_counts[column_type] += 1

    print(f"\n🔹 Summary:")
    print(f"Total columns: {total_columns}")

    for column_type, count in type_counts.items():
        percentage = (count / total_columns) * 100
        print(f"{column_type}: {count} ({percentage:.2f}%)")


# Step 8: Remove the cloned directory
def cleanup_directory(clone_dir):
    if os.path.exists(clone_dir):
        print(f"Removing cloned directory: {clone_dir}")
        shutil.rmtree(clone_dir)
        print("Directory removed successfully.")
    else:
        print(f"Directory '{clone_dir}' does not exist. No cleanup required.")


# Step 9: Main function
def main():
    repo_url = "https://github.com/cwida/public_bi_benchmark"
    clone_dir = "./public_bi_benchmark"

    # Clone the repository
    clone_repo(repo_url, clone_dir)

    # Find SQL files
    sql_files = find_sql_files(clone_dir)
    print(f"📂 Found {len(sql_files)} SQL files with '1' in their name.")

    # Process SQL files into JSON & YAML
    schemas = process_sql_files(sql_files)

    # Summarize column types
    summarize_column_types(schemas)

    # Remove the cloned directory
    cleanup_directory(clone_dir)


if __name__ == "__main__":
    main()
