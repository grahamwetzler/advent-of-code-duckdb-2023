from pip_wtf import pip_wtf

pip_wtf("duckdb click")

from pathlib import Path

import click
from duckdb import sql


@click.command()
@click.argument("day")
def run(day):
    day = day.zfill(2)

    sql_files = sorted(Path(f"{day}").glob("*.sql"))

    for sql_file in sql_files:
        sql_text = sql_file.read_text()
        base_dir = sql_file.parents[0]
        sql(f"set file_search_path = '{base_dir}'")
        sql(sql_text).show()


if __name__ == "__main__":
    run()
