# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook will run in Pipeline

# COMMAND ----------

from pyspark.sql.functions import col, count, sum, current_date, year, month, date_format

# COMMAND ----------

# table_transform = 'g2_sal_orders.silver.dgr_sales_orders'
# table_gold = 'g2_sal_orders.gold.dgr_sales_orders_summary'

table_transform = dbutils.widgets.get("TABLE_TRANSFORM")
table_gold = dbutils.widgets.get("TABLE_GOLD")

# COMMAND ----------

# MAGIC %md
# MAGIC Incrementando una columna de `order_month` para guardar solo el mes de la venta

# COMMAND ----------

df_transform = spark.table(table_transform)
df_gold = df_transform.withColumn("order_month", date_format("order_date", "yyyy-MM"))

# COMMAND ----------

# MAGIC %md
# MAGIC Sumarizando: agrupando por `sku` y `order_month`

# COMMAND ----------


df_gold = df_gold.groupBy("sku","order_month").agg(
    sum("ordered_units").alias("total_units"),
    count("order_id").alias("total_orders"),
)



# COMMAND ----------

df_gold.write.format("delta").mode("overwrite").saveAsTable(table_gold)

# COMMAND ----------

