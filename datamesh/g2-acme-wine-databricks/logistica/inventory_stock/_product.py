# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook will NOT run in Pipeline

# COMMAND ----------

# dbutils.widgets.text('widget_df_result', df_result)
# widget_df_result = dbutils.widgets.get("widget_df_result")

# COMMAND ----------

spark.sql(
f"""
CREATE OR REPLACE VIEW g2_log_product.gold.vw_wine_stock_scores
(
  sku COMMENT 'Identificador único del vino (SKU)',
  wine COMMENT 'Nombre completo del vino',
  total_stock COMMENT 'Unidades totales disponibles en inventario para el mes actual',
  total_orders COMMENT 'Número total de órdenes realizadas en el mes actual para este SKU',
  current_stock COMMENT 'Stock disponible actual (total_stock - total_orders)',
  score COMMENT 'Puntuación promedio del vino según reviews (escala de 1 a 100)'
)
COMMENT 'Vista que consolida el estado actual del inventario de vinos, incluyendo compras y scoring'
AS
SELECT
  sku,
  wine,
  total_stock,
  total_orders,
  current_stock,
  score
FROM
  g2_log_product.gold.dgr_wine_stock_scores
"""
)

# COMMAND ----------

