import os

import duckdb

from generate_query import generate_duckdb_query, get_files_path


def stat_summary(folder: str, subquery: str) -> None:
    path: str = os.path.join(folder, "summary.csv")
    query: str = f"""COPY (SELECT * FROM (SUMMARIZE {subquery})) TO '{path}' (HEADER, DELIMITER ';');"""

    duckdb.execute(query)


def stat_currencies(folder: str, subquery: str) -> None:
    path: str = os.path.join(folder, "ratio.csv")
    query: str = f"""COPY (
                WITH subquery as (
                    {subquery}
                ),
                ratio as (
                    SELECT payment_currency, date_trans, amount_paid, (amount_paid - exchange) as ratio
                    FROM subquery
                ),
                computed as (
                    SELECT
                        s.payment_currency as currency,
                        MEAN(r.ratio) as mean_ratio,
                        MIN(r.ratio) as min_ratio,
                        MAX(r.ratio) as max_ratio,
                        SUM(r.ratio) as sum_ratio
                    FROM subquery s INNER JOIN ratio r 
                        ON r.payment_currency = s.payment_currency
                        AND r.date_trans = s.date_trans
                        AND r.amount_paid = s.amount_paid
                    GROUP BY s.payment_currency
                )
            
                SELECT * FROM computed
            ) TO '{path}' (HEADER, DELIMITER ';');
        """

    duckdb.execute(query)


if __name__ == "__main__":
    current_folder = os.path.curdir
    data_folder = os.path.join(current_folder, "Data", "transformed")

    files_path = get_files_path(data_folder, ".parquet")
    subquery: str = generate_duckdb_query(files_path, "read_parquet")[:-1]

    print(
        "\nIn all the following data the column 'exchange' is the exchange between the currencies and the US Dollar."
    )
    print(
        "Moreover, keep in mind that the transactions are extracted from a Dataset of Transactions for AML.\n"
    )

    # stat_summary(data_folder, subquery)
    stat_currencies(data_folder, subquery)
