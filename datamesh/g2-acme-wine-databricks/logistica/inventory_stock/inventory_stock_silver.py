# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook will run in Pipeline

# COMMAND ----------

from pyspark.sql.functions import col, when

# COMMAND ----------


# table_input = "g2_log_stock.bronze.dgr_inventory_stock"
# table_transform = "g2_log_stock.silver.dgr_inventory_stock"

table_input = dbutils.widgets.get("TABLE_INPUT")
table_transform = dbutils.widgets.get("TABLE_TRANSFORM")

# COMMAND ----------

df_input = spark.table(table_input)
# df_input.display(10)

# COMMAND ----------

# MAGIC %md
# MAGIC Creando el campo categórico "stock_status" para indicar si el stock está en `out_of_stock`, `low` o `medium`

# COMMAND ----------


df_input = df_input.withColumn("stock_status", when(col("stock") == 0, "out_of_stock")
                                   .when(col("stock") < 20, "low")
                                   .when(col("stock") < 100, "medium")
                                   .otherwise("high"))
df_input.write.format("delta").mode("overwrite").saveAsTable(table_transform)

# COMMAND ----------

