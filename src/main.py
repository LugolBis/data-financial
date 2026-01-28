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

TRANSACTIONS_COLUMNS: list[str] = [
    "rowid",
    "date_trans",
    "from_bank",
    "account_to",
    "account_for",
    "to_bank",
    "amount_paid",
    "payment_currency",
    "exchange",
]


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


def load_exchanges(file_path: str, df_meta: DataFrame) -> LazyFrame:
    lf: LazyFrame = pl.scan_csv(
        source=file_path,
        has_header=True,
        infer_schema=True,
        try_parse_dates=True,
    )

    date_min, date_max = df_meta.select("date_min", "date_max").row(0)

    lf_filtered: LazyFrame = (
        lf.filter(
            pl.col("Date").is_between(
                date_min,
                date_max,
            )
        )
        .group_by(pl.col("Date"))
        .first()
    )

    target_columns: list[str] = [v for v in CURRENCIES.keys()]
    target_columns.append("Date")

    lf_selected: LazyFrame = lf_filtered.select(target_columns)

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

    df_meta: DataFrame = lf_transactions.select(
        pl.len().alias("row_count"),
        pl.col("date_trans").min().cast(pl.Date).alias("date_min"),
        pl.col("date_trans").max().cast(pl.Date).alias("date_max"),
    ).collect(engine="streaming")

    lf_exchanges: LazyFrame = load_exchanges(
        file_path=os.path.join(raw_folder, "currency_exchange_rates.csv"),
        df_meta=df_meta,
    )

    row_count = df_meta["row_count"][0]
    batch_size = 500_000
    batches = [i * batch_size for i in range(0, (row_count // batch_size) + 1)]

    for index, start_id in enumerate(batches):
        end_id = start_id + batch_size

        lf_batch: LazyFrame = lf_transactions.filter(
            (pl.col("rowid") >= start_id) & (pl.col("rowid") < end_id)
        )

        lf_joined: LazyFrame = lf_batch.join_where(
            lf_exchanges, pl.col("date_trans") <= pl.col("Date")
        )

        lf_partitioned: LazyFrame = lf_joined.group_by(["rowid"]).first().drop("Date")

        lf_unpivoted: LazyFrame = (
            lf_partitioned.unpivot(
                on=list(CURRENCIES.values()),
                index=["rowid", "payment_currency", "amount_paid"],
                variable_name="target_currency",
                value_name="exchange_rate",
            )
            .filter(pl.col("payment_currency") == pl.col("target_currency"))
            .group_by(["rowid"])
            .first()
            .with_columns(
                (
                    pl.col("exchange_rate").cast(pl.Float64) * pl.col("amount_paid")
                ).alias("exchange")
            )
            .select(["rowid", "exchange"])
        )

        lf_computed: LazyFrame = lf_partitioned.join(
            lf_unpivoted, on="rowid", how="left"
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

        del df

        print(f"Successfully write the partition n°{index}")


if __name__ == "__main__":
    args: list[str] = sys.argv[1:]

    if len(args) < 1:
        print("Usage : main.py <FOLDER_PATH>")
    else:
        main(args[0])
