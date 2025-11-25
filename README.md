# data-financial

__data-financial__ is an optimized data pipeline designed to process large datasets on modest hardware — _“toasters”_ — using __Dask__.
For this purpose, I selected a dataset of financial transactions (__32 million rows__) and combined it with another dataset containing historical currency exchange rates.
The goal was to perform large-scale operations such as cross joins, partitioning, and other transformations efficiently.

## 🔍​ Schema of the pipeline

```mermaid
flowchart LR
    A[**Financial transactions** Dataset] -->|Streaming| C[**Python** <br>--- <br> **Dask**]
    B[**Currencies exchange** Dataset] -->|Streaming| C[**Python** <br>--- <br> **Dask**]
    C -->|Saving batch| D0[Parquet file]
    C -->|Saving batch| D1[**.**<br>**.**<br> **.**]
    C -->|Saving batch| D2[Parquet file]
    D0 -->|Loading| E[(**DuckDB**)]
    D1 -->|Loading| E[(**DuckDB**)]
    D2 -->|Loading| E[(**DuckDB**)]
    E -->|Saving reports| F[**Reports** files]

    subgraph Extraction [Storage]
        A
        B
    end

    subgraph Processing [Memory]
        C
    end

    subgraph Save [Storage]
        D0
        D1
        D2
    end

    subgraph DBInMemory [Memory]
        E
    end

    subgraph Reports [Storage]
        F
    end
    
    style A fill:#018f4a,stroke:#000000,color:#000000,stroke-width:1px
    style B fill:#018f4a,stroke:#000000,color:#000000,stroke-width:1px
    style C fill:#fc6e6b,stroke:#000000,color:#000000,stroke-width:1px
    style D0 fill:#55acee,stroke:#000000,color:#000000,stroke-width:1px
    style D1 fill:#55acee,stroke:#000000,color:#000000,stroke-width:1px
    style D2 fill:#55acee,stroke:#000000,color:#000000,stroke-width:1px
    style E fill:#fef242,stroke:#000000,color:#000000,stroke-width:1px
    style F fill:#018f4a,stroke:#000000,color:#000000,stroke-width:1px
```

## 🚀​ Project Overview

This project leverages Dask and its lazy evaluation model to process a large dataset of over 32 million financial transactions in batch mode. For each record, the pipeline computes the transaction amount converted into US Dollars (USD) using historical exchange rates.

To handle such a large volume of data on a machine with limited hardware resources, the dataset is split into batches. Each batch is processed sequentially — the results are written to disk before moving to the next batch, allowing memory to be freed efficiently between operations.

At the end of the processing stage, the aggregated results are analyzed using DuckDB, which provides fast, in-memory analytics on the processed data. This combination of Dask for distributed batch computation and DuckDB for analytical queries ensures both scalability and performance, even on modest hardware.

## 🛠️​ Tech Stack
- Python → Offers great flexibility and simplicity for scripting, making it ideal for building and automating data workflows.
- Dask → Used to handle and process large-scale streaming and batch data efficiently across distributed systems.
- DuckDB → Enables fast in-memory analytics directly on top of Parquet files, allowing high-performance querying without heavy infrastructure.

## 📚​ Data Sources
I used Datasets from **Kaggle**.

- [IBM Transactions for Anti Money Laundering (AML)](https://www.kaggle.com/datasets/ealtman2019/ibm-transactions-for-anti-money-laundering-aml)
- [Major Currency Exchange Rates](https://www.kaggle.com/datasets/weirdanalyst/currency-exchange-rates-since-2000-01-03)

