# Databricks notebook source
# Move files
dbutils.fs.mv(
    'abfss://datalake@stdemdsai.dfs.core.windows.net/raw/airflow/G6/Wine_Sales_Orders_20250709.csv',
    'abfss://datalake@stdemdsai.dfs.core.windows.net/raw/airflow/G6/sales/'
    )

# COMMAND ----------

# Move files
dbutils.fs.mv(
    'abfss://datalake@stdemdsai.dfs.core.windows.net/raw/airflow/G6/winemag-data-130k-v2.json',
    'abfss://datalake@stdemdsai.dfs.core.windows.net/raw/airflow/G6/marketing/'
    )

# COMMAND ----------

