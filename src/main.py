import sys
import os
from typing import Optional
import dask.dataframe as dd
from dask.dataframe import DataFrame
from dask.distributed import Client
from datetime import datetime

CURRENCIES: dict[str, str] = {
    'JPY': 'Yen', 'GBP': 'UK Pound', 'AUD': 'Australian Dollar', 'MXN': 'Mexican Peso',
    'ILS': 'Shekel', 'CNY': 'Yuan', 'CAD': 'Canadian Dollar', 'EUR': 'Euro',
    'INR': 'Rupee', 'CHF': 'Swiss Franc', 'USD': 'US Dollar', 'BRL': 'Brazil Real',
    'RUB': 'Ruble'
}

def load_transactions(file_path: str) -> DataFrame:
    df: DataFrame = dd.read_csv(urlpath=file_path, assume_missing=True)
    
    df_renamed: DataFrame = df.rename(columns={
        "From Bank": "from_bank", "To Bank": "to_bank",
        "Amount Received": "amount_received", "Receiving Currency": "receiving_currency",
        "Amount Paid": "amount_paid", "Payment Currency": "payment_currency",
        "Payment Format": "payment_format", "Timestamp": "date_trans",
        "Account": "account_to", "Account.1": "account_for", "Is Laundering": "is_laundering"
    })

    df_renamed["date_trans"] = dd.to_datetime(df_renamed["date_trans"], format="%Y/%m/%d %H:%M") \
        .dt.normalize()
    
    df_rowid: DataFrame = df_renamed.reset_index().rename(columns={"index": "rowid"})
    
    return df_rowid

def load_exchanges(file_path: str, min_date: datetime, max_date: datetime) -> DataFrame:
    df: DataFrame = dd.read_csv(file_path, assume_missing=True)
    
    df["Date"] = dd.to_datetime(df["Date"])
    
    df_filtered: DataFrame = df[(df["Date"] >= min_date) & (df["Date"] <= max_date)]
    
    target_columns = list(CURRENCIES.keys()) + ["Date"]
    df_selected = df_filtered[target_columns]
    
    df_renamed = df_selected.rename(columns=CURRENCIES)
    
    return df_renamed

def config_dask() -> Client:
    """Configuration of Dask client"""
    client: Client = Client(
        n_workers=4,
        memory_limit="8GB",
        threads_per_worker=2
    )

    return client

def calculate_exchange(row) -> Optional[float]:
    payment_currency = row["payment_currency"]
    amount_paid = row["amount_paid"]

    if payment_currency in row.index:
        exchange_rate = row[payment_currency]
        return amount_paid * exchange_rate
    else:
        return None

def main(folder_path: str) -> None:
    raw_folder: str = os.path.join(folder_path, "raw")
    transformed_folder: str = os.path.join(folder_path, "transformed")

    if os.path.exists(transformed_folder) == False:
        os.mkdir(transformed_folder)

    client = config_dask()
    print(f"Dask Dashboard : {client.dashboard_link}")

    df_transactions: DataFrame = load_transactions(
        file_path=os.path.join(raw_folder, "HI-Medium_Trans.csv")
    )

    min_max_dates: DataFrame = df_transactions[["date_trans"]].compute()
    min_date: datetime = min_max_dates["date_trans"].min()
    max_date: datetime = min_max_dates["date_trans"].max()

    df_exchanges: DataFrame = load_exchanges(
        file_path=os.path.join(raw_folder, "currency_exchange_rates.csv"),
        min_date=min_date,
        max_date=max_date
    )

    n_partitions: int = df_transactions.npartitions
    selected_columns = [
        "date_trans", "from_bank", "account_to", "account_for", 
        "to_bank", "amount_paid", "payment_currency", "exchange"
    ]
    
    for index in range(n_partitions):
        print(f"Process the partition n°{index+1}/{n_partitions}")
        
        df_batch: DataFrame = df_transactions.get_partition(index)
        
        df_joined: DataFrame = dd.merge(
            df_batch,
            df_exchanges,
            left_on="date_trans",
            right_on="Date",
            how="left"
        )
    
        df_filtered: DataFrame = df_joined.drop_duplicates(subset=["rowid"], keep="first")

        df_computed: DataFrame = df_filtered.assign(
            exchange=df_filtered.apply(calculate_exchange, axis=1, meta=("exchange", "float64"))
        )

        df_selected: DataFrame = df_computed[selected_columns]

        output_path = os.path.join(transformed_folder, f"partition_{index}")
        df_selected.to_parquet(
            output_path,
            engine="pyarrow",
            write_index=False,
            overwrite=True
        )

    client.close()

if __name__ == "__main__":
    args: list[str] = sys.argv[1:]
    
    if len(args) < 1:
        print("Usage : main.py <FOLDER_PATH>")
    else:
        main(args[0])