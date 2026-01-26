import os
import sys

import polars as pl
from polars import DataFrame, LazyFrame

CURRENCIES: dict[str, str] = {
    "JPY": "Yen",
    "GBP": "UK Pound",
    "AUD": "Australian Dollar",
    "MXN": "Mexican Peso",
    "ILS": "Shekel",
    "CNY": "Yuan",
    "CAD": "Canadian Dollar",
    "EUR": "Euro",
    "INR": "Rupee",
    "CHF": "Swiss Franc",
    "USD": "US Dollar",
    "BRL": "Brazil Real",
    "RUB": "Ruble",
}


def load_transactions(file_path: str) -> LazyFrame:
    lf: LazyFrame = pl.scan_csv(
        source=file_path,
        has_header=True,
        infer_schema=True,
        row_index_name="rowid",
        try_parse_dates=True,
    )

    lf_renamed: LazyFrame = lf.rename(
        {
            "From Bank": "from_bank",
            "To Bank": "to_bank",
            "Amount Received": "amount_received",
            "Receiving Currency": "receiving_currency",
            "Amount Paid": "amount_paid",
            "Payment Currency": "payment_currency",
            "Payment Format": "payment_format",
            "Timestamp": "date_trans",
            "Account": "account_to",
            "Account_duplicated_0": "account_for",
            "Is Laundering": "is_laundering",
        }
    )

    return lf_renamed


def load_exchanges(file_path: str) -> LazyFrame:
    lf: LazyFrame = pl.scan_csv(
        source=file_path,
        has_header=True,
        infer_schema=True,
        try_parse_dates=True,
    )

    target_columns: list[str] = [v for v in CURRENCIES.keys()]
    target_columns.append("Date")

    lf_selected: LazyFrame = lf.select(target_columns)

    lf_renamed: LazyFrame = lf_selected.rename(CURRENCIES)

    return lf_renamed


def main(folder_path: str) -> None:
    raw_folder: str = os.path.join(folder_path, "raw")
    transformed_folder: str = os.path.join(folder_path, "transformed")

    if not os.path.exists(transformed_folder):
        os.mkdir(transformed_folder)

    lf_transactions: LazyFrame = load_transactions(
        file_path=os.path.join(raw_folder, "HI-Medium_Trans.csv")
    )

    lf_exchanges: LazyFrame = load_exchanges(
        file_path=os.path.join(raw_folder, "currency_exchange_rates.csv")
    )

    row_count = 32_000_000
    batch_size = 250_000
    batches = [i * batch_size for i in range(0, (row_count // batch_size) + 1)]

    case_expr = pl.coalesce(
        *[
            pl.when(pl.col("payment_currency") == col_name)
            .then(pl.col(col_name).cast(pl.Float64))
            .otherwise(None)
            for col_name in CURRENCIES.values()
        ]
    )

    for index, start_id in enumerate(batches):
        end_id = start_id + batch_size

        lf_batch: LazyFrame = lf_transactions.filter(
            (pl.col("rowid") >= start_id) & (pl.col("rowid") < end_id)
        )

        lf_joined: LazyFrame = lf_batch.join_where(
            lf_exchanges, pl.col("date_trans") <= pl.col("Date")
        )

        lf_partitioned: LazyFrame = (
            lf_joined.group_by(["rowid"]).first().drop("Date").drop("rowid")
        )

        lf_computed: LazyFrame = lf_partitioned.with_columns(
            (pl.col("amount_paid") * case_expr).alias("exchange")
        )

        lf_selected: LazyFrame = lf_computed.select(
            "date_trans",
            "from_bank",
            "account_to",
            "account_for",
            "to_bank",
            "amount_paid",
            "payment_currency",
            "exchange",
        )

        df: DataFrame = lf_selected.collect(engine="streaming")

        df.write_parquet(
            file=os.path.join(transformed_folder, f"partition_{index}.parquet")
        )

        print(f"Successfully write the partition n°{index}")


if __name__ == "__main__":
    args: list[str] = sys.argv[1:]

    if len(args) < 1:
        print("Usage : main.py <FOLDER_PATH>")
    else:
        main(args[0])
