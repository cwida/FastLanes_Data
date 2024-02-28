import functools
import json
import re
import sys
from pathlib import Path

import boto3
import duckdb
from botocore import UNSIGNED
from botocore.client import Config

WORKDIR = Path.cwd()
ROW_LIMIT = 64 * 1024

print = functools.partial(print, flush=True)


def convert_schema(nested_cols, is_root=True):
    columns = []
    for (colname, coltype) in nested_cols:
        col = {
            'name': colname,
            'type': coltype.id
        }

        try:
            if coltype.id != 'decimal':
                col['children'] = convert_schema(coltype.children, False)
        except duckdb.InvalidInputException:
            pass

        columns.append(col)

    return {'columns': columns} if is_root else columns


def read_and_export_parquet(url, table_key, filename, row_limit, data):
    try:
        print("Reading {}".format(url))
        try:
            columns = duckdb.sql("DESCRIBE SELECT * FROM read_parquet('{}')".format(url)).fetchnumpy()
        except Exception as e:
            print("Could not read {}".format(url), e, file=sys.stderr)
            return

        nested_cols = []
        for colname, typestr in zip(columns["column_name"], columns["column_type"]):
            type = duckdb.type(typestr)
            try:
                _ = type.children
                if type.id != 'decimal':
                    nested_cols.append((colname, type))
            except duckdb.InvalidInputException:
                pass

        if nested_cols:
            schema = convert_schema(nested_cols)

            filepath = f'tables/{table_key}/{filename}'
            return {'rowcount': export_to_json(url, schema, filepath, row_limit, data), 'schema': schema}
    except Exception as e:
        print("Unexpected error for {}".format(url), e, file=sys.stderr)


def export_to_json(url, schema, export_path, row_limit, data):
    full_path = WORKDIR / export_path
    print("Exporting {} to {}".format(url, full_path))
    select_fields = ', '.join(['"' + col['name'] + '"' for col in schema['columns']])
    try:
        full_path.parent.mkdir(exist_ok=True, parents=True)
        duckdb.sql(f"COPY (SELECT {select_fields} FROM read_parquet('{url}') LIMIT {row_limit}) TO '{full_path}'")
        rowcount = duckdb.sql(f"SELECT COUNT(*) FROM '{full_path}'").fetchone()[0]
        print(f"Exported {rowcount} rows")

        with open(full_path, 'r') as f:
            data.append(f.read())

        full_path.unlink()
        return rowcount
    except Exception as e:
        print("Could not export to {}".format(full_path), e, file=sys.stderr)


def main():
    with open(WORKDIR / 'metadata.json', 'r') as f:
        datasets = json.load(f)

    s3 = boto3.resource("s3", config=Config(signature_version=UNSIGNED))
    for dataset_name in datasets:
        print("Handling dataset {}".format(dataset_name))
        bucket = s3.Bucket(dataset_name)
        tables = datasets[dataset_name]

        for table in tables:
            table['url_regex'] = re.compile(table['url_regex'])

        table_files = {}
        for url in bucket.objects.all():
            parquet_url = "s3://{}/{}".format(url.bucket_name, url.key)
            for table in tables:
                match = table['url_regex'].fullmatch(parquet_url)
                if match:
                    table_key = f"{dataset_name}-{table['table_key_format'].format(*match.groups())}"
                    if table_key not in table_files:
                        table_files[table_key] = []

                    table_files[table_key].append((parquet_url,
                                                   table_key,
                                                   table['filename_format'].format(*match.groups())))
                    break
        print("Done traversing URLs in bucket")

        for key in table_files:
            schema = []
            total_rows = 0
            table_dir = WORKDIR / f'tables/{key}'

            data = []
            for file in reversed(table_files[key]):
                result = read_and_export_parquet(*file, ROW_LIMIT - total_rows, data)
                if result is None:
                    continue

                schema = result['schema']
                total_rows += result['rowcount']
                if total_rows >= ROW_LIMIT:
                    break

                print(f"Total rows so far: {total_rows}")

            if total_rows == 0:
                continue

            if total_rows < ROW_LIMIT:
                print(f"WARNING: Table {key} has {total_rows} rows (<{ROW_LIMIT})", file=sys.stderr)

            with open(table_dir / 'data.jsonl', 'w') as f:
                for item in reversed(data):
                    f.write(item)

            with open(table_dir / 'schema.json', 'w') as f:
                json.dump(schema, f)

    print("Done!")


if __name__ == '__main__':
    main()
