# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook will run in Pipeline
# MAGIC ## Capa Oro

# COMMAND ----------

from pyspark.sql.functions import col, count, sum, year, month, date_format

# COMMAND ----------

# table_transform = 'g2_log_stock.silver.dgr_inventory_stock'
# table_gold = 'g2_log_stock.gold.dgr_stock_summary'

table_transform = dbutils.widgets.get("TABLE_TRANSFORM")
table_gold = dbutils.widgets.get("TABLE_GOLD")

# COMMAND ----------

df_transform = spark.table(table_transform)
df_gold = df_transform.withColumn("stock_month", date_format("date_stock_updated", "yyyy-MM"))
df_gold = df_gold.groupBy("sku", "title", "stock_month").agg(
    sum("stock").alias("total_stock")
)

# COMMAND ----------

df_gold.write.format("delta").mode("overwrite").saveAsTable(table_gold)

# COMMAND ----------

