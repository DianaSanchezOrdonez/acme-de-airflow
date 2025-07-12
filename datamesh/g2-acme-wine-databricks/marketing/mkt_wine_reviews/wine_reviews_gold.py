# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook will run in Pipeline

# COMMAND ----------

from pyspark.sql.functions import col, count, avg, when

# COMMAND ----------

# table_transform = 'g2_mkt_reviews.silver.dgr_wine_reviews'
# table_gold = 'g2_mkt_reviews.gold.dgr_reviews_points_by_title'

table_transform = dbutils.widgets.get("TABLE_TRANSFORM")
table_gold = dbutils.widgets.get("TABLE_GOLD")

# COMMAND ----------

# MAGIC %md
# MAGIC Sumarizar la tabla promediando los points y contando los reviews

# COMMAND ----------

df_transform = spark.table(table_transform)
df_gold = df_transform.groupBy("title").agg(
  avg("points").alias("avg_points"),
  count("*").alias("num_reviews")
)

# COMMAND ----------

# MAGIC %md
# MAGIC Enriquecer la tabla definiendo categorías según el score

# COMMAND ----------

df_gold_enrich = df_gold.withColumn(
    "quality_points",
    when(col("avg_points") >= 95, "Exceptional")
    .when(col("avg_points") >= 90, "Excellent")
    .when(col("avg_points") >= 85, "Good")
    .otherwise("Regular")
)

# COMMAND ----------

df_gold_enrich.write.format("delta").mode("overwrite").saveAsTable(table_gold)

# COMMAND ----------

