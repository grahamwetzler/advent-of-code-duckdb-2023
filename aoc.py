def pip_wtf(command):
    """https://pip.wtf/"""
    import os
    import os.path
    import sys

    t = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        ".pip_wtf." + os.path.basename(__file__),
    )
    sys.path = [p for p in sys.path if "-packages" not in p] + [t]
    os.environ["PATH"] += os.pathsep + t + os.path.sep + "bin"
    os.environ["PYTHONPATH"] = os.pathsep.join(sys.path)
    if os.path.exists(t):
        return
    os.system(" ".join([sys.executable, "-m", "pip", "install", "-t", t, command]))


pip_wtf("duckdb click")

from datetime import datetime
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
