import os

current_folder = os.path.curdir
data_folder = os.path.join(current_folder, "Data", "transformed")
entries: list[str] = os.listdir(data_folder)
queries: list[str] = []

for entry in entries:
    if entry.endswith(".parquet"):
        path: str = os.path.join(data_folder, entry)
        queries.append(f"SELECT * FROM read_parquet('{path}')")

query: str = "\nUNION ALL\n".join(queries) + ";"

with open(os.path.join("src", "query.sql"), "w") as fd:
    fd.write(query)
