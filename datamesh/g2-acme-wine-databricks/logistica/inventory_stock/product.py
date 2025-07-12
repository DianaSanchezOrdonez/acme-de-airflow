# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook will run in Pipeline

# COMMAND ----------

from pyspark.sql.functions import col, date_format, current_date

# COMMAND ----------

current_month = date_format(current_date(), "yyyy-MM")

# COMMAND ----------

reviews_table_name = f"g2_mkt_reviews.gold.dgr_reviews_points_by_title"
sales_table_name = f"g2_sal_orders.gold.dgr_sales_orders_summary"
stock_table_name = f"g2_log_stock.gold.dgr_stock_summary"

wine_stock_scores = f"g2_log_product.gold.dgr_wine_stock_scores"

# COMMAND ----------

# dbutils.widgets.text('widget_mv_stock_scores', mv_stock_scores)
# widget_mv_stock_scores = dbutils.widgets.get("widget_mv_stock_scores")
# print(widget_mv_stock_scores)

# COMMAND ----------

df_reviews = spark.table(reviews_table_name)
df_sales = spark.table(sales_table_name)
df_stock = spark.table(stock_table_name)

# COMMAND ----------

df_stock_with_reviews = df_stock.alias("s") \
    .join(df_reviews.alias("r"), col("s.title") == col("r.title"), "inner") \
    .join(df_sales.alias("o"), (col("s.sku") == col("o.sku")) & (col("s.stock_month") == col("o.order_month")), "inner")

df_stock_with_reviews = df_stock_with_reviews.filter(col("stock_month") == current_month)

df_result = df_stock_with_reviews.select(
    col("s.sku"),
    col("s.title").alias("wine"),
    col("s.total_stock"),
    col("o.total_orders"),
    (col("s.total_stock") - col("o.total_orders")).alias("current_stock"),
    col("r.avg_points").alias("score")
)

# COMMAND ----------

df_result.display()

# COMMAND ----------

df_result.write.format("delta").mode("overwrite").saveAsTable(wine_stock_scores)