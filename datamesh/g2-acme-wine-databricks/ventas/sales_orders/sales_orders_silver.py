# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook will run in Pipeline
# MAGIC - Need to create table `SILVER.DGR_SALES_ORDERS` with databricks autoloader

# COMMAND ----------

from pyspark.sql.functions import col, when, date_format

# COMMAND ----------

# table_input = "g2_sal_orders.bronze.dgr_sales_orders"
# table_transform = "g2_sal_orders.silver.dgr_sales_orders"

table_input = dbutils.widgets.get("TABLE_INPUT")
table_transform = dbutils.widgets.get("TABLE_TRANSFORM") 

# COMMAND ----------

df_input = spark.table(table_input)

# COMMAND ----------

# MAGIC %md
# MAGIC Remover duplicados y aplicar validaciones

# COMMAND ----------

df_input = df_input.dropDuplicates(["order_id"])
df_input = df_input.filter(
    (col("sku").isNotNull()) &
    (col("order_id").isNotNull()) &
    (col("ordered_units").isNotNull())
)

df_input = df_input.filter(col("ordered_units") > 0)


# COMMAND ----------

# MAGIC %md
# MAGIC Escribir en la tabla

# COMMAND ----------

df_input.write.format("delta").mode("overwrite").saveAsTable(table_transform)

# COMMAND ----------

