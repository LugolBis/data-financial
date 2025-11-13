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

def stat_currencies(subquery: str) -> None:
    query: str = \
        f"""WITH ratio as (
                SELECT payment_currency, date_trans, amount_paid, (amount_paid - exchange) as ratio
                FROM ({subquery})
            ),
            computed as (
                SELECT s.payment_currency as Currency, MEAN(r.ratio), MIN(r.ratio), MAX(r.ratio), SUM(r.ratio)
                FROM ({subquery}) s INNER JOIN ratio r 
                    ON r.payment_currency = s.payment_currency
                    AND r.date_trans = s.date_trans
                    AND r.amount_paid = s.amount_paid
                GROUP BY s.payment_currency
            )

            SELECT *
            FROM computed;
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
    stat_currencies(subquery)