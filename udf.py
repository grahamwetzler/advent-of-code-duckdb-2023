import duckdb
from duckdb.typing import VARCHAR


def string_clean(string) -> VARCHAR:
    return None if string == "" else string.strip()


funcs = [
    dict(
        name="string_clean",
        function=string_clean,
    )
]

for func in funcs:
    duckdb.create_function(**func)
