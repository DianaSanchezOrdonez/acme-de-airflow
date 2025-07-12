# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook will run
# MAGIC ## Capa Bronce

# COMMAND ----------

from pyspark.sql.functions import current_date, lit, current_timestamp
from pyspark.sql.types import StructType, StructField, TimestampType, StringType, IntegerType, DoubleType, DateType

# COMMAND ----------

# table_input = "g2_log_stock.bronze.dgr_inventory_stock"

table_input = dbutils.widgets.get("TABLE_INPUT")

# COMMAND ----------

base_path = "abfss://datalake@stdemdsai.dfs.core.windows.net/raw/airflow/G6/logistic"

source_path = f"{base_path}/Wine_Inventory_Stock_20250709.csv"

inventory_stock_schema = StructType([
    StructField("sku", StringType(), True),
    StructField("title", StringType(), True),
    StructField("stock", IntegerType(), True),
    StructField("country", StringType(), True),
    StructField("date_stock_updated", DateType(), True)
])


# COMMAND ----------

# spark.sql(f'DROP TABLE IF EXISTS {table_input}')

# COMMAND ----------

df_input = spark.read \
    .option("header", True) \
    .schema(inventory_stock_schema) \
    .csv(source_path)

# COMMAND ----------

df_input = df_input.withColumn("inserted_at", current_timestamp())

# COMMAND ----------

df_input.write.format("delta").mode("overwrite").saveAsTable(table_input)

# COMMAND ----------

spark.sql(f'SELECT * FROM {table_input}').display()

# COMMAND ----------

