import os
import sys


def get_files_path(folder_path: str, file_extension: str = ".parquet") -> list[str]:
    """Explore the folder and it's subfolder to construct the list of files path with the file_extension"""
    entries: list[str] = os.listdir(folder_path)
    files_path: list[str] = []

    for entry in entries:
        path: str = os.path.join(folder_path, entry)

        if entry.endswith(file_extension):
            files_path.append(path)
        elif os.path.isdir(path):
            # Recursive call to retrieve all the files
            files_path.extend(get_files_path(path))

    return files_path


def generate_duckdb_query(
    files_path: list[str], load_function: str = "read_parquet"
) -> str:
    """Generate the DuckDB query to load all the files as a table."""
    queries: list[str] = [
        f"SELECT * FROM {load_function}('{path}')" for path in files_path
    ]
    return "\nUNION ALL\n".join(queries) + ";"


if __name__ == "__main__":
    args = sys.argv[1:]
    files_path: list[str] = []

    if len(args) == 0:
        current_folder = os.path.curdir
        data_folder = os.path.join(current_folder, "Data", "transformed")
        files_path = get_files_path(data_folder, ".parquet")
    else:
        path: str = args[0]

        if os.path.exists(path):
            files_path = get_files_path(path, ".parquet")
        else:
            raise ValueError(f"ERROR : Invalid path '{path}'")

    query: str = generate_duckdb_query(files_path, "read_parquet")

    with open(os.path.join("src", "query.sql"), "w") as fd:
        fd.write(query)
