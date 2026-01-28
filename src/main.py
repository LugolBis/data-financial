import datetime as dt
import os
import sys

import numpy as np
import pandas as pd
from pandas import DataFrame

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


def load_transactions(file_path: str) -> DataFrame:
    df: DataFrame = pd.read_csv(
        file_path, parse_dates=["Timestamp"], dtype={"Amount Paid": "float64"}
    )

    df_renamed: DataFrame = df.rename(
        columns={
            "From Bank": "from_bank",
            "To Bank": "to_bank",
            "Amount Received": "amount_received",
            "Receiving Currency": "receiving_currency",
            "Amount Paid": "amount_paid",
            "Payment Currency": "payment_currency",
            "Payment Format": "payment_format",
            "Timestamp": "date_trans",
            "Account": "account_to",
            "Account.1": "account_for",
            "Is Laundering": "is_laundering",
        }
    )

    del df

    df_renamed["date_trans"] = df_renamed["date_trans"].dt.floor("D")  # pyright: ignore[reportAttributeAccessIssue]
    df_renamed["rowid"] = df_renamed.index

    return df_renamed


def load_exchanges(file_path: str, date_min: dt.date, date_max: dt.date) -> DataFrame:
    df: DataFrame = pd.read_csv(file_path, parse_dates=["Date"])

    df_partitioned: DataFrame = df.groupby("Date", as_index=False).first()

    del df

    df_filtered: DataFrame = df_partitioned[
        (df_partitioned["Date"] >= date_min) & (df_partitioned["Date"] <= date_max)
    ]

    target_columns: list[str] = [v for v in CURRENCIES.keys()]
    target_columns.append("Date")

    df_selected: DataFrame = df_filtered[target_columns]

    del df_filtered

    df_renamed: DataFrame = df_selected.rename(columns=CURRENCIES)

    return df_renamed


def main(folder_path: str) -> None:
    raw_folder: str = os.path.join(folder_path, "raw")
    transformed_folder: str = os.path.join(folder_path, "transformed")

    if not os.path.exists(transformed_folder):
        os.mkdir(transformed_folder)

    df_transactions: DataFrame = load_transactions(
        file_path=os.path.join(raw_folder, "HI-Medium_Trans.csv")
    )

    date_min, date_max = df_transactions["date_trans"].agg(["min", "max"])

    df_exchanges: DataFrame = load_exchanges(
        file_path=os.path.join(raw_folder, "currency_exchange_rates.csv"),
        date_min=date_min,
        date_max=date_max,
    )

    row_count = len(df_transactions)
    batch_size = 500_000
    batches = [i * batch_size for i in range(0, (row_count // batch_size) + 1)]

    for index, start_id in enumerate(batches):
        end_id = start_id + batch_size

        df_batch: DataFrame = df_transactions[
            (df_transactions["rowid"] >= start_id) & (df_transactions["rowid"] < end_id)
        ]

        df_joined: DataFrame = df_batch.merge(df_exchanges, how="cross")

        del df_batch

        df_partitioned: DataFrame = df_joined.groupby("rowid", as_index=False).first()

        del df_joined

        series = df_partitioned.apply(
            lambda row: row[row["payment_currency"]] * row["amount_paid"]
            if row["payment_currency"] in df_partitioned.columns
            else np.nan,
            axis=1,
        )

        df_partitioned["exchange"] = series

        df_selected: DataFrame = df_partitioned[
            [
                "date_trans",
                "from_bank",
                "account_to",
                "account_for",
                "to_bank",
                "amount_paid",
                "payment_currency",
                "exchange",
            ]
        ]

        del df_partitioned

        df_selected.to_parquet(
            path=os.path.join(transformed_folder, f"partition_{index}.parquet"),
            engine="pyarrow",
        )

        del df_selected

        print(f"Successfully write the partition n°{index}")


if __name__ == "__main__":
    args: list[str] = sys.argv[1:]

    if len(args) < 1:
        print("Usage : main.py <FOLDER_PATH>")
    else:
        main(args[0])
