import duckdb
from duckdb.typing import VARCHAR


def trim_null(string: str) -> VARCHAR:
    return None if string == "" else string.strip()


funcs = [
    dict(name="trim_null", function=trim_null),
]

for func in funcs:
    duckdb.create_function(**func)
