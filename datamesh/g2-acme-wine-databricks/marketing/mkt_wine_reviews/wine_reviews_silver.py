# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook will run in Pipeline
# MAGIC ## Capa Plata 

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

# table_input = "g2_mkt_reviews.bronze.dgr_wine_reviews"
# table_transform = "g2_mkt_reviews.silver.dgr_wine_reviews"

table_input = dbutils.widgets.get("TABLE_INPUT")
table_transform = dbutils.widgets.get("TABLE_TRANSFORM")


# COMMAND ----------

df_input = spark.table(table_input)
df_input = df_input.dropDuplicates(["title"])
df_input = df_input.filter(col("description").isNotNull())
df_input.write.format("delta").mode("overwrite").saveAsTable(table_transform)

# COMMAND ----------

