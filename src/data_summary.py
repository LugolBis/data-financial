import duckdb
from generate_query import get_files_path, generate_duckdb_query
import os

def stat_groupby(subquery: str) -> None:
    query: str = \
        f"""SELECT payment_currency as Currency, MEAN(exchange), MIN(exchange), MAX(exchange) \
            FROM ({subquery}) \
            GROUP BY payment_currency;
        """
    
    duckdb.sql(query).show()

if __name__ == "__main__":
    current_folder = os.path.curdir
    data_folder = os.path.join(current_folder, "Data", "transformed")

    files_path = get_files_path(data_folder, ".parquet")
    subquery: str = generate_duckdb_query(files_path, "read_parquet")[:-1]
    
    print("\nIn all the following data the column 'exchange' is the exchange between the currencies and the US Dollar.")
    print("Moreover, keep in mind that the transactions are extracted from a Dataset of Transactions for AML.\n")

    stat_groupby(subquery)