import sys
from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window, WindowSpec
from pyspark.sql import Row
from pyspark.sql.dataframe import DataFrame
import os

CURRENCIES: dict[str, Optional[str]] = {
    "Yen": "JPY", "UK Pound": "GBP", "Australian Dollar": "AUD",
    "Saudi Riyal": None, "Mexican Peso": "MXN", "Shekel": "ILS",
    "Yuan": "CNY", "Canadian Dollar": "CAD", "Euro": "EUR",
    "Rupee": "INR", "Swiss Franc": "CHF", "US Dollar": "USD",
    "Brazil Real": "BRL", "Bitcoin": None, "Ruble": "RUB"
}

def load_transactions(file_path: str, spark: SparkSession) -> DataFrame:
    df: DataFrame = spark.read.csv(path=file_path, header=True, inferSchema=True)

    df_renamed: DataFrame = df.withColumnsRenamed({
        "From Bank": "from_bank", "To Bank": "to_bank",
        "Amount Received": "amount_received", "Receiving Currency": "receiving_currency",
        "Amount Paid": "amount_paid", "Payment Currency": "payment_currency",
        "Payment Format": "payment_format", "Timestamp": "date_trans",
        "Account2": "account_to", "Account4": "account_for"
    })

    df_date: DataFrame = df_renamed.withColumn("date_trans", F.to_date("date_trans", "yyyy/MM/dd HH:mm"))

    return df_date

def load_exchanges(file_path: str, spark: SparkSession, row: Row) -> DataFrame:
    df: DataFrame = spark.read.csv(path=file_path, header=True, inferSchema=True)

    target_columns: list[str] = [v for v in CURRENCIES.values() if v != None]
    target_columns.append("Date")

    df_selected: DataFrame = df.filter((df["Date"] >= row["min_date"]) & (df["Date"] <= row["max_date"])).select(target_columns)

    return df_selected
    
def main(folder_path: str) -> None:
    raw_folder: str = os.path.join(folder_path, "raw")

    # Create a Spark session
    spark: SparkSession = SparkSession.builder \
        .appName("data-financial") \
        .config("spark.driver.memory", "8g") \
        .config("spark.executor.memory", "8g") \
        .config("spark.memory.fraction", "0.6") \
        .config("spark.memory.offHeap.enabled", "true") \
        .config("spark.memory.offHeap.size", "7.5g") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skew.enabled", "true") \
        .config("spark.sql.autoBroadcastJoinThreshold", "10MB") \
        .config("spark.sql.shuffle.partitions", "100") \
        .getOrCreate()
    
    df_transactions: DataFrame = load_transactions(
        file_path=os.path.join(raw_folder, "HI-Medium_Trans.csv"),
        spark=spark
    )

    df_exchanges: DataFrame = load_exchanges(
        file_path=os.path.join(raw_folder, "currency_exchange_rates.csv"),
        spark=spark,
        row=df_transactions.select(
            F.min("date_trans").alias("min_date"),
            F.max("date_trans").alias("max_date")
        ).collect()[0]
    )

    df_joined: DataFrame = df_transactions.join(
        df_exchanges,
        df_transactions["date_trans"] <= df_exchanges["Date"],
        "left"
    )

    window_spec: WindowSpec = Window.partitionBy("date_trans", "account_to", "account_for").orderBy("Date")
    df_partioned: DataFrame = df_joined.withColumn("rn", F.row_number().over(window_spec)) \
        .filter(F.col("rn") == 1) \
        .drop("rn")
    
    df_partioned.show(5)

if __name__ == "__main__":
    args: list[str] = sys.argv[1:]
    
    if len(args) < 1:
        print("Usage : main.py <FOLDER_PATH>")
    else:
        main(args[0])
