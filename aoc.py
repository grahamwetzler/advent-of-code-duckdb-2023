from pip_wtf import pip_wtf

pip_wtf("duckdb click")

import time
from pathlib import Path

import click
from duckdb import sql

import udf


@click.command()
@click.argument("day")
def run(day):
    day = day.zfill(2)

    sql_files = sorted(Path(f"{day}").glob("*.sql"))

    for sql_file in sql_files:
        start_time = time.time()
        sql_text = sql_file.read_text()
        base_dir = sql_file.parents[0]
        sql(f"set file_search_path = '{base_dir}'")
        sql(sql_text).show()
        execute_time = time.time() - start_time
        print(f"{execute_time:.2}s")


if __name__ == "__main__":
    run()
