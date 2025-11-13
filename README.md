# data-financial

__data-financial__ is an optimized data pipeline designed to process large datasets on modest hardware — _“toasters”_ — using __Spark__.
For this purpose, I selected a dataset of financial transactions (__32 million rows__) and combined it with another dataset containing historical currency exchange rates.
The goal was to perform large-scale operations such as cross joins, partitioning, and other transformations efficiently.

## 🚀​ Project Overview

This project leverages PySpark and its lazy evaluation model to process a large dataset of over 32 million financial transactions in batch mode. For each record, the pipeline computes the transaction amount converted into US Dollars (USD) using historical exchange rates.

To handle such a large volume of data on a machine with limited hardware resources, the dataset is split into batches. Each batch is processed sequentially — the results are written to disk before moving to the next batch, allowing memory to be freed efficiently between operations.

At the end of the processing stage, the aggregated results are analyzed using DuckDB, which provides fast, in-memory analytics on the processed data. This combination of PySpark for distributed batch computation and DuckDB for analytical queries ensures both scalability and performance, even on modest hardware.

## 🛠️​ Tech Stack
- Python
- Spark
- DuckDB
