import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window, WindowSpec
from pyspark.sql import Row
from pyspark.sql.dataframe import DataFrame
import os

CURRENCIES: dict[str, str] = {
    'JPY': 'Yen', 'GBP': 'UK Pound', 'AUD': 'Australian Dollar', 'MXN': 'Mexican Peso',
    'ILS': 'Shekel', 'CNY': 'Yuan', 'CAD': 'Canadian Dollar', 'EUR': 'Euro',
    'INR': 'Rupee', 'CHF': 'Swiss Franc', 'USD': 'US Dollar', 'BRL': 'Brazil Real',
    'RUB': 'Ruble'
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

    df_rowid: DataFrame = df_date.withColumn("rowid", F.monotonically_increasing_id())

    return df_rowid

def load_exchanges(file_path: str, spark: SparkSession, row: Row) -> DataFrame:
    df: DataFrame = spark.read.csv(path=file_path, header=True, inferSchema=True)

    target_columns: list[str] = [v for v in CURRENCIES.keys()]
    target_columns.append("Date")

    df_selected: DataFrame = df.filter((df["Date"] >= row["min_date"]) & (df["Date"] <= row["max_date"])).select(target_columns)

    df_renamed: DataFrame = df_selected.withColumnsRenamed(CURRENCIES)

    return df_renamed
    
def main(folder_path: str) -> None:
    raw_folder: str = os.path.join(folder_path, "raw")
    transformed_folder: str = os.path.join(folder_path, "transformed")

    if os.path.exists(transformed_folder) == False:
        os.mkdir(transformed_folder)

    # Create a Spark session
    spark: SparkSession = SparkSession.builder \
        .appName("data-financial") \
        .config("spark.driver.memory", "8g") \
        .config("spark.executor.memory", "8g") \
        .config("spark.memory.fraction", "0.6") \
        .config("spark.memory.offHeap.enabled", "true") \
        .config("spark.memory.offHeap.size", "7g") \
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

    batch_size = 500_000
    batches = [i * batch_size for i in range(0, (df_transactions.count() // batch_size) + 1)]

    window_spec: WindowSpec = Window.partitionBy("date_trans", "account_to", "account_for").orderBy("Date")
    case_expr = F.coalesce(*[
        F.when(F.col("payment_currency") == col_name, F.col(col_name))
        for col_name in CURRENCIES.values()
    ])

    for start_id in batches:
        end_id = start_id + batch_size

        df_batch = df_transactions.filter(
            (df_transactions["rowid"] >= start_id) & (df_transactions["rowid"] < end_id)
        ).drop("rowid")

        df_joined: DataFrame = df_batch.join(
            df_exchanges,
            df_batch["date_trans"] <= df_exchanges["Date"],
            "left"
        )

        df_partitioned: DataFrame = df_joined.withColumn("rn", F.row_number().over(window_spec)) \
            .filter(F.col("rn") == 1) \
            .drop("rn") \
            .drop("Date")

        df_computed: DataFrame = df_partitioned.withColumn(
            "exchange",
            F.col("amount_paid") * case_expr
        ).drop(*[column for column in CURRENCIES.values()])

        df_computed.write \
            .mode("append") \
            .parquet(transformed_folder)
        
        df_computed.unpersist()

if __name__ == "__main__":
    args: list[str] = sys.argv[1:]
    
    if len(args) < 1:
        print("Usage : main.py <FOLDER_PATH>")
    else:
        main(args[0])
