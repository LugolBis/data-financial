import sys
import os
import shutil
from datetime import datetime
import datafusion
import pandas as pd
from datafusion import RuntimeEnvBuilder, SessionConfig, SessionContext, DataFrame
from datafusion import lit, functions as F

CURRENCIES: dict[str, str] = {
    'JPY': 'Yen', 'GBP': 'UK Pound', 'AUD': 'Australian Dollar', 'MXN': 'Mexican Peso',
    'ILS': 'Shekel', 'CNY': 'Yuan', 'CAD': 'Canadian Dollar', 'EUR': 'Euro',
    'INR': 'Rupee', 'CHF': 'Swiss Franc', 'USD': 'US Dollar', 'BRL': 'Brazil Real',
    'RUB': 'Ruble'
}

TRANSACTIONS_HEADERS: list[str] = [
    "Timestamp",
    "from_bank",
    "account_to",
    "to_bank",
    "account_for",
    "amount_received",
    "receiving_currency",
    "amount_paid",
    "payment_currency",
    "payment_format",
    "is_laundering"
]

def rewrite_csv_headers(file_path: str, headers: list[str]):
    tmp = "tmp.csv"
    new_header = ",".join(headers) + "\n"

    with open(file_path, "rb") as fin, open(tmp, "wb") as fout:
        fin.readline()
        fout.write(new_header.encode())

        shutil.copyfileobj(fin, fout, length=1024*1024)

    os.replace(tmp, file_path)

def load_transactions(file_path: str, ctx: SessionContext) -> DataFrame:
    rewrite_csv_headers(file_path, TRANSACTIONS_HEADERS)
    df: DataFrame = ctx.read_csv(file_path, has_header=True, delimiter=",")

    df_date: DataFrame = df.with_column(
        "date_trans",
        F.date_trunc(lit("day"), F.to_timestamp(F.col("Timestamp"), lit("%Y/%m/%d %H:%M")))
    )

    df_rowid: DataFrame = df_date.with_column(
        "rowid",
        F.row_number()
    )
    
    return df_rowid

def load_exchanges(
    file_path: str, ctx: SessionContext, min_date: datetime, max_date: datetime
) -> DataFrame:
    df: DataFrame = ctx.read_csv(file_path, has_header=True)

    df_filtered: DataFrame = df.filter(
        (F.col("Date") >= lit(min_date.strftime("%Y/%m/%d"))) &
        (F.col("Date") <= lit(max_date.strftime("%Y/%m/%d")))
    )

    target_columns: list[datafusion.Expr] = [F.col(k).alias(v) for k, v in CURRENCIES.items()]
    target_columns.append(F.col("Date"))
    
    df_selected: DataFrame = df_filtered.select(*target_columns)
    
    return df_selected

def config_datafusion() -> SessionContext:
    """Configuration de DataFusion"""
    config: SessionConfig = SessionConfig() \
        .with_batch_size(4096) \
        .with_target_partitions(4) \
        .with_parquet_pruning(True)

    runtime: RuntimeEnvBuilder = RuntimeEnvBuilder() \
        .with_greedy_memory_pool(8 * 1024 * 1024 * 1024) \
        .with_fair_spill_pool(1610612736) \
        .with_disk_manager_os()

    ctx: SessionContext = SessionContext(config, runtime)
    
    return ctx

def main(folder_path: str) -> None:
    raw_folder: str = os.path.join(folder_path, "raw")
    transformed_folder: str = os.path.join(folder_path, "transformed")

    if not os.path.exists(transformed_folder):
        os.makedirs(transformed_folder)

    ctx: SessionContext = config_datafusion()

    df_transactions: DataFrame = load_transactions(
        file_path=os.path.join(raw_folder, "HI-Medium_Trans.csv"),
        ctx=ctx
    )

    min_max_df: DataFrame = df_transactions.aggregate(
        [],
        [
            F.min(F.col("date_trans")).alias("min_date"),
            F.max(F.col("date_trans")).alias("max_date")
        ]
    ).select(F.col("min_date"), F.col("max_date"))

    min_max_pandas: pd.DataFrame = min_max_df.to_pandas()
    min_date = min_max_pandas["min_date"].iloc[0].to_pydatetime()
    max_date = min_max_pandas["max_date"].iloc[0].to_pydatetime()
    
    df_exchanges = load_exchanges(
        file_path=os.path.join(raw_folder, "currency_exchange_rates.csv"),
        ctx=ctx,
        min_date=min_date,
        max_date=max_date
    )

    batch_size: int = 500_000
    total_count: int = df_transactions.count()
    batches: range = range(0, total_count, batch_size)

    case_expr: datafusion.Expr = F.coalesce(
        *[
            F.when(F.col("payment_currency") == lit(value), F.col("payment_currency"))
                .otherwise(lit(None))
            for value in CURRENCIES.values()
        ],
        lit(None)
    )

    for index, start_id in enumerate(batches):
        end_id: int = start_id + batch_size
        
        df_batch: DataFrame = df_transactions.filter(
            (F.col("rowid") >= lit(start_id)) & 
            (F.col("rowid") < lit(end_id))
        )

        df_joined: DataFrame = df_batch.join_on(
            df_exchanges,
            F.col("date_trans") <= F.col("Date"),
            how="left"
        )

        df_row_number: DataFrame = df_joined.with_column(
            "rn",
            F.row_number(
                partition_by=F.col("rowid"),
                order_by=F.col("Date")
            )
        )
        
        df_filtered: DataFrame = df_row_number.filter(F.col("rn") == 1) \
            .drop("rn")
        
        df_computed: DataFrame = df_filtered.with_column(
            "exchange",
            F.col("amount_paid") * case_expr
        )

        df_selected: DataFrame = df_computed.select(
            "date_trans", "from_bank", "account_to", "account_for", 
            "to_bank", "amount_paid", "payment_currency", "exchange"
        )

        df_selected.write_parquet(
            os.path.join(transformed_folder, str(index)),
            compression="snappy"
        )

if __name__ == "__main__":
    args: list[str] = sys.argv[1:]
    
    if len(args) < 1:
        print("Usage : main.py <FOLDER_PATH>")
    else:
        main(args[0])