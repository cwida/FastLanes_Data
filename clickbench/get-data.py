#!/usr/bin/env python3

import duckdb


# FROM https://github.com/ClickHouse/ClickBench/blob/main/duckdb/benchmark.sh
#  download wget --no-verbose --continue 'https://datasets.clickhouse.com/hits_compatible/hits.csv.gz'

def load():
    con = duckdb.connect()

    # enable the progress bar
    con.execute('PRAGMA enable_progress_bar')
    con.execute('PRAGMA enable_print_progress_bar;')
    con.execute("SET preserve_insertion_order=true")

    # perform the actual load
    print("Will load the data")
    con.execute(open("create.sql").read())
    con.execute("CREATE TABLE new_tbl AS SELECT * FROM read_csv('hits.tsv.gz') LIMIT 65536;")
    con.execute("COPY new_tbl TO 'hits/hits.csv' (HEADER  false, DELIMITER '|');")


if __name__ == '__main__':
    load()
