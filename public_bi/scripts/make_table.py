import os
import yaml
import numpy as np
import pandas as pd
import dask.dataframe as dd

TABLES_PATH = os.path.dirname(os.getcwd()) + '/tables'
SAVE_PATH = os.path.dirname(os.getcwd()) + '/'


def get_table_columns():
    """
    Get all the table columns and table columns that are integers from the schema.yaml files

    :return: table_columns: Dictionary with table names as keys and list of column names as values.
             table_columns_int: Dictionary with table names as keys and list of integer column names as values.
    """
    table_columns, table_columns_int = {}, {}
    for table in os.listdir(TABLES_PATH):
        if os.path.isdir(os.path.join(TABLES_PATH, table)):
            table_columns[table], table_columns_int[table] = [], []
            for row in os.listdir(os.path.join(TABLES_PATH, table, table + "_1")):
                if row.endswith(".yaml"):
                    with open(os.path.join(TABLES_PATH, table, table + "_1", row)) as f:
                        schema = yaml.load(f, Loader=yaml.FullLoader)
                        for column in schema["columns"]:
                            table_columns[table].append(column["name"])
                            if column["type"] == "integer":
                                table_columns_int[table].append(column["name"])
    return table_columns, table_columns_int


def load_tables(table_columns, table_columns_int):
    """
    Load all the tables from the csv files. Make a dictionary with the table name as key and the table as value

    :param table_columns: Dictionary with table names as keys and list of column names as values.
    :param table_columns_int: Dictionary with table names as keys and list of integer column names as values.
    :return: tables: Dictionary with table names as keys and DataFrames as values.
    """
    tables = {}
    for table in table_columns:
        tables[table] = pd.read_csv(os.path.join(TABLES_PATH, table, table + "_1", table + "_1.csv"), delimiter="|",
                                    names=table_columns[table], low_memory=False)
        tables[table] = tables[table][table_columns_int[table]]
        # Convert all columns to integers in case they are ints written down as doubles
        for column in table_columns_int[table]:
            tables[table][column] = pd.to_numeric(tables[table][column], errors="raise").astype("Int64")

    # Identify and remove empty columns
    for table, df in tables.items():
        empty_columns = [column for column in df.columns if df[column].isna().all()]
        tables[table] = df.drop(columns=empty_columns)

    return tables


def get_table_stats(tables):
    """
    Calculate min, max, and unique values for each column in the given tables. Also, find a value that is not present
    in the column and is not below the minimum or above the maximum value to use it later.

    :return: Dictionary with table statistics.
    """
    # For each table and column, find min/max and unique values. Convert all to integers
    table_stats = {}
    for table in tables:
        table_stats[table] = {}
        for column in tables[table]:
            table_stats[table][column] = {}
            table_stats[table][column]["min"] = tables[table][column].min()
            table_stats[table][column]["max"] = tables[table][column].max()
            table_stats[table][column]["unique"] = tables[table][column].unique()

    # For each table and column, generate a value that is within min and max boundary, but not in the unique values, if
    # possible. Write it down as a new row. This is useful for the = 0 selectivity.
    for table in tables:
        found_value = False
        for column in tables[table]:
            # First check if unique is not empty
            if table_stats[table][column]["unique"].isna().all():
                continue

            min_val = table_stats[table][column]["min"]
            max_val = table_stats[table][column]["max"]
            unique_values = set(table_stats[table][column]["unique"])

            if len(unique_values) < (max_val - min_val + 1):
                all_values = set(range(min_val, max_val + 1))
                available_values = all_values - unique_values

                if available_values:
                    found_value = available_values.pop()
                    if "= 0 selectivity" not in table_stats[table]:
                        table_stats[table]["= 0 selectivity"] = {}
                    table_stats[table]["= 0 selectivity"]["value"] = found_value
                    table_stats[table]["= 0 selectivity"]["column"] = column
                    break  # Exit the column loop if a value is found
        if found_value:
            continue  # Move to the next table if a value is found

    return table_stats


def generate_equality_selectivity_rows(table_stats):
    """
    Generate rows for predicates with selectivity 0 and 1 for '=' and '≠'.

    :param table_stats: Dictionary with table statistics.
    :return: List of rows with selectivity information.
    """
    rows = []
    for table, stats in table_stats.items():
        if "= 0 selectivity" in stats:
            value = stats["= 0 selectivity"]["value"]
            column = stats["= 0 selectivity"]["column"]
            rows.append({
                "Predicate": "=",
                "Selectivity": 0,
                "Filter": value,
                "Table": table,
                "Column": column,
                "Query": f'SELECT * FROM {table} WHERE "{column}" = {value}'
            })
            rows.append({
                "Predicate": "≠",
                "Selectivity": 1,
                "Filter": value,
                "Table": table,
                "Column": column,
                "Query": f'SELECT * FROM {table} WHERE "{column}" != {value}'
            })

        # Add rows for columns with only one unique value.
        # This is a special case where the selectivity is 0 for '≠' and 1 for '='
        for column, col_stats in stats.items():
            unique_values = col_stats.get("unique", [])
            if len(unique_values) == 1:
                unique_value = unique_values[0]
                rows.append({
                    "Predicate": "≠",
                    "Selectivity": 0,
                    "Filter": unique_value,
                    "Table": table,
                    "Column": column,
                    "Query": f'SELECT * FROM {table} WHERE "{column}" != {unique_value}'
                })
                rows.append({
                    "Predicate": "=",
                    "Selectivity": 1,
                    "Filter": unique_value,
                    "Table": table,
                    "Column": column,
                    "Query": f'SELECT * FROM {table} WHERE "{column}" = {unique_value}'
                })
    return rows


def generate_inequality_selectivity_rows(table_stats):
    """
    Generate rows for inequality predicates with selectivity 0 and 1.

    :param table_stats: Dictionary with table statistics.
    :return: List of rows with selectivity information.
    """
    rows = []
    for table, stats in table_stats.items():
        for column, col_stats in stats.items():
            if "min" in col_stats and "max" in col_stats:
                min_val, max_val = col_stats["min"], col_stats["max"]

                predicate_rows = [
                    {"Predicate": "<", "Selectivity": 0, "Filter": min_val, "Table": table, "Column": column,
                     "Query": f'SELECT * FROM "{table}" WHERE {column} < {min_val}'},
                    {"Predicate": "≤", "Selectivity": 0, "Filter": min_val - 1, "Table": table, "Column": column,
                     "Query": f'SELECT * FROM "{table}" WHERE {column} <= {min_val - 1}'},
                    {"Predicate": ">", "Selectivity": 0, "Filter": max_val, "Table": table, "Column": column,
                     "Query": f'SELECT * FROM "{table}" WHERE {column} > {max_val}'},
                    {"Predicate": "≥", "Selectivity": 0, "Filter": max_val + 1, "Table": table, "Column": column,
                     "Query": f'SELECT * FROM "{table}" WHERE {column} >= {max_val + 1}'},
                    {"Predicate": "<", "Selectivity": 1, "Filter": max_val + 1, "Table": table, "Column": column,
                     "Query": f'SELECT * FROM "{table}" WHERE {column} < {max_val + 1}'},
                    {"Predicate": "≤", "Selectivity": 1, "Filter": max_val, "Table": table, "Column": column,
                     "Query": f'SELECT * FROM "{table}" WHERE {column} <= {max_val}'},
                    {"Predicate": ">", "Selectivity": 1, "Filter": min_val - 1, "Table": table, "Column": column,
                     "Query": f'SELECT * FROM "{table}" WHERE {column} > {min_val - 1}'},
                    {"Predicate": "≥", "Selectivity": 1, "Filter": min_val, "Table": table, "Column": column,
                     "Query": f'SELECT * FROM "{table}" WHERE {column} >= {min_val}'}
                ]

                rows.extend(predicate_rows)
    return rows


def calculate_selectivity(predicate, value, value_counts, cumulative_dist):
    """
    Calculate the selectivity for a given predicate and value. The selectivity is calculated based on the value counts
    and cumulative distribution of the column.
    """
    if predicate == "=":
        return value_counts.get(value, 0)
    elif predicate == "<":
        return cumulative_dist.get(value, 0) - value_counts.get(value, 0)
    elif predicate == "≤":
        return cumulative_dist.get(value, 0)
    elif predicate == ">":
        return 1 - cumulative_dist.get(value, 0)
    elif predicate == "≥":
        return 1 - cumulative_dist.get(value, 0) + value_counts.get(value, 0)
    elif predicate == "≠":
        return 1 - value_counts.get(value, 0)
    else:
        raise ValueError(f"Invalid predicate: {predicate}")


def generate_all_predicates_rows(tables, table_stats):
    """
    Generate rows for all predicates with their respective selectivities for selectivities 0 < s < 1.

    :param tables: Dictionary with table names as keys and DataFrame objects as values.
    :param table_stats: Dictionary with table statistics.
    :return: List of rows with selectivity information.
    """
    rows = []
    for table in tables:
        print('Processing table:', table + '...')
        for column in tables[table]:
            col_data = tables[table][column]
            unique_values = table_stats[table][column]["unique"]
            value_counts = col_data.value_counts(normalize=True).sort_index()
            cumulative_dist = value_counts.cumsum().to_dict()
            value_counts = value_counts.to_dict()

            # If there is 1 unique value, selectivity is either 0 or 1. Both cases we handle separately
            if len(unique_values) == 1:
                continue

            for unique_value in unique_values:
                for predicate in ["=", "<", "≤", ">", "≥", "≠"]:
                    selectivity = calculate_selectivity(predicate, unique_value, value_counts, cumulative_dist)
                    query = f'SELECT * FROM {table} WHERE "{column}" {predicate} {unique_value}'
                    row = {
                        "Predicate": predicate,
                        "Selectivity": selectivity,
                        "Filter": unique_value,
                        "Table": table,
                        "Column": column,
                        "Query": query
                    }
                    rows.append(row)
    return rows


def generate_selectivity_rows(tables):
    """
    Generate all rows for selectivity calculations and queries.

    :param tables: Dictionary with table names as keys and DataFrame objects as values.
    :return: List of rows with selectivity information.
    """
    table_stats = get_table_stats(tables)
    rows = []
    rows.extend(generate_equality_selectivity_rows(table_stats))
    rows.extend(generate_inequality_selectivity_rows(table_stats))
    rows.extend(generate_all_predicates_rows(tables, table_stats))
    return rows


def generate_selectivity_df(rows):
    """
    Generate the final DataFrame with selectivity information.
    """
    final = pd.DataFrame(rows, columns=["Predicate", "Selectivity", "Filter", "Table", "Column", "Query"])
    final = final.drop_duplicates()
    # Sort final by predicate, and within a predicate sort it  by selectivity
    final = final.sort_values(by=["Predicate", "Selectivity"], ascending=[True, True])
    # Since calculations are very precise, sometimes we can get tiny negative numbers or numbers slightly above 1.
    # We'll round the selectivity to 10 decimal places to avoid this artifact
    final["Selectivity"] = final["Selectivity"].round(10)
    # To avoid -0.0
    final["Selectivity"] = final["Selectivity"].abs()
    return final


def find_closest_idx(series, target):
    """
    Find the index of the closest value to the target in the series. If there are two values with the same distance,
    the one before is returned. If the target is smaller than the first value, 0 is returned. If the target is larger
    than the last value, the last index is returned. The series must be sorted in ascending order.
    """
    pos = np.searchsorted(series, target, side='left')
    if pos == 0:
        return 0
    if pos == len(series):
        return len(series) - 1
    before = pos - 1
    after = pos
    if abs(series[after] - target) < abs(series[before] - target):
        return after
    else:
        return before


def generate_mini_selectivity_df(df):
    """
    Generate a mini DataFrame with the closest selectivity to 0.000, 0.0001, 0.0002, ..., 1.0000 for each predicate.
    This is useful for most applications as the full selectivity DataFrame is too large to be used in most cases.
    """
    df_mini = pd.DataFrame()
    # Iterate over each unique predicate
    for predicate in df["Predicate"].unique():
        print(predicate)
        predicate_rows = df[df["Predicate"] == predicate]

        # Extract the selectivity column for binary search
        selectivities = predicate_rows["Selectivity"].values

        for selectivity in range(0, 10001):
            target_selectivity = selectivity / 10000

            # Use binary search to find the index of the closest selectivity
            closest_idx = find_closest_idx(selectivities, target_selectivity)
            closest_row = predicate_rows.iloc[closest_idx]

            # Add the closest row to df_mini
            df_mini = pd.concat([df_mini, closest_row.to_frame().T])

    df_mini = df_mini.sort_values(by=["Predicate", "Selectivity"], ascending=[True, True])
    # Remove duplicates
    df_mini = df_mini.drop_duplicates()
    # Make all selectivities positive to avoid -0.0
    df_mini["Selectivity"] = df_mini["Selectivity"].abs()

    print(f"Total rows in df_mini: {len(df_mini)}")

    return df_mini


def save_csv(df, folder):
    """
    Save a DataFrame to a CSV file using Dask to speed up the process.
    """
    ddf = dd.from_pandas(df, npartitions=6)
    ddf.to_csv(folder, single_file=True)


if __name__ == '__main__':
    print("Loading the columns and tables...")
    table_columns, table_columns_int = get_table_columns()
    tables = load_tables(table_columns, table_columns_int)
    print("Generating selectivity rows...")
    rows = generate_selectivity_rows(tables)
    print("Generating a large selectivity DataFrame...")
    df = generate_selectivity_df(rows)
    print("Generating a mini selectivity DataFrame...")
    df_mini = generate_mini_selectivity_df(df)

    print("Saving the DataFrames...")
    save_csv(df, SAVE_PATH + "selectivity.csv")
    save_csv(df_mini, SAVE_PATH + "selectivity_mini.csv")
    print("Done!")
