# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook will run in Pipeline
# MAGIC - Need to create table `DGR_WINE_REVIEWS` with databricks autoloader

# COMMAND ----------

from pyspark.sql.functions import current_date, lit, current_timestamp
from pyspark.sql.types import StructType, StructField, TimestampType, StringType, IntegerType, DoubleType

# COMMAND ----------

# table_input = "g2_mkt_reviews.bronze.dgr_wine_reviews"
table_input = dbutils.widgets.get("TABLE_INPUT")

# COMMAND ----------

base_path = "abfss://datalake@stdemdsai.dfs.core.windows.net/raw/airflow/G6/marketing"

source_path = f"{base_path}/winemag-data-130k-v2.json"

wine_reviews_schema = StructType([
    StructField("points", StringType(), True),
    StructField("title", StringType(), True),
    StructField("description", StringType(), True),
    StructField("taster_name", StringType(), True),
    StructField("taster_twitter_handle", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("designation", StringType(), True),
    StructField("variety", StringType(), True),
    StructField("region_1", StringType(), True),
    StructField("region_2", StringType(), True),
    StructField("province", StringType(), True),
    StructField("country", StringType(), True),
    StructField("winery", StringType(), True)
])



# COMMAND ----------

# spark.sql(f'DROP TABLE IF EXISTS {table_input}')

# COMMAND ----------

df_input = spark.read \
    .option("multiLine", "true") \
    .schema(wine_reviews_schema) \
    .json(source_path)

# COMMAND ----------

df_input = df_input.withColumn("inserted_at", current_timestamp())

# COMMAND ----------

df_input.write.format("delta").mode("overwrite").saveAsTable(table_input)

# COMMAND ----------

spark.sql(f'SELECT * FROM {table_input}').display()

# COMMAND ----------

