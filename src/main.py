import sys
from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame
import os

CURRENCIES: dict[str, Optional[str]] = {
    "Yen": "JPY", "UK Pound": "GBP", "Australian Dollar": "AUD",
    "Saudi Riyal": None, "Mexican Peso": "MXN", "Shekel": "ILS",
    "Yuan": "CNY", "Canadian Dollar": "CAD", "Euro": "EUR",
    "Rupee": "INR", "Swiss Franc": "CHF", "US Dollar": "USD",
    "Brazil Real": "BRL", "Bitcoin": None, "Ruble": "RUB"
}

def load_exchanges(file_path: str, spark: SparkSession) -> DataFrame:
    df: DataFrame = spark.read.csv(path=file_path, header=True, inferSchema=True)

    target_columns: list[str] = [v for v in CURRENCIES.values() if v != None]
    target_columns.append("Date")

    df_selected: DataFrame = df.select(target_columns)

    return df_selected

def load_transactions(file_path: str, spark: SparkSession) -> DataFrame:
    df: DataFrame = spark.read.csv(path=file_path, header=True, inferSchema=True)

    df_renamed: DataFrame = df.withColumnsRenamed({
        "From Bank": "from_bank", "To Bank": "to_bank",
        "Amount Received": "amount_received", "Receiving Currency": "receiving_currency",
        "Amount Paid": "amount_paid", "Payment Currency": "payment_currency",
        "Payment Format": "payment_format"
    })

    return df_renamed

def main(folder_path: str) -> None:
    raw_folder: str = os.path.join(folder_path, "raw")

    # Create a Spark session
    spark: SparkSession = SparkSession.builder \
        .appName("data-financial") \
        .getOrCreate()
    
    df_transactions: DataFrame = load_transactions(
        file_path=os.path.join(raw_folder, "HI-Medium_Trans.csv"),
        spark=spark
    )

    df_exchanges: DataFrame = load_exchanges(
        file_path=os.path.join(raw_folder, "currency_exchange_rates.csv"),
        spark=spark
    )
    
    df_transactions.show(40)
    # For basic RDD operations, you might also need SparkContext
    # sc = spark.sparkContext

if __name__ == "__main__":
    args: list[str] = sys.argv[1:]
    
    if len(args) < 1:
        print("Usage : main.py <FOLDER_PATH>")
    else:
        main(args[0])
