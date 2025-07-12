# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook will run in Pipeline
# MAGIC - Need to create table `DGR_SALES_ORDERS` with databricks autoloader

# COMMAND ----------

from pyspark.sql.functions import current_date, lit, current_timestamp
from pyspark.sql.types import StructType, StructField, TimestampType, StringType, IntegerType, DoubleType, DateType

# COMMAND ----------

# table_input = "g2_sal_orders.bronze.dgr_sales_orders"
table_input = dbutils.widgets.get("TABLE_INPUT")

# COMMAND ----------

base_path = "abfss://datalake@stdemdsai.dfs.core.windows.net/raw/airflow/G6/sales"

source_path = f"{base_path}/Wine_Sales_Orders_20250709.csv"

sales_orders_schema = StructType([
    StructField("sku", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("order_date", DateType(), True),
    StructField("ordered_units", IntegerType(), True),
    StructField("customer", StringType(), True),
    StructField("country_of_origin", StringType(), True)
])


# COMMAND ----------

# spark.sql(f'DROP TABLE IF EXISTS {table_input}')

# COMMAND ----------

df_input = spark.read \
    .option("header", True) \
    .schema(sales_orders_schema) \
    .csv(source_path)

# COMMAND ----------

df_input = df_input.withColumn("inserted_at", current_timestamp())

# COMMAND ----------

df_input.write.format("delta").mode("overwrite").saveAsTable(table_input)

# COMMAND ----------

spark.sql(f'SELECT * FROM {table_input}').display()

# COMMAND ----------

