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
            "Account2": "account_to",
            "Account4": "account_for",
            "Is Laundering": "is_laundering",
        }
    )

    # lf_date: LazyFrame = lf_renamed.with_columns(pl.col("date_trans").dt.date())

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

    case_expr = pl.coalesce(
        *[
            pl.when(pl.col("payment_currency") == col_name)
            .then(pl.col(col_name))
            .otherwise(None)
            for col_name in CURRENCIES.values()
        ]
    )

    lf_joined: LazyFrame = lf_transactions.join(
        lf_exchanges,
        pl.col("date_trans") <= pl.col("Date"),
        how="left",
        maintain_order="left",
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

    df: DataFrame = lf_selected.collect()
    df.write_parquet(file=os.path.join(transformed_folder), row_group_size=500_000)


if __name__ == "__main__":
    args: list[str] = sys.argv[1:]

    if len(args) < 1:
        print("Usage : main.py <FOLDER_PATH>")
    else:
        main(args[0])
