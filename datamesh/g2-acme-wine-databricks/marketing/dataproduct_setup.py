# Databricks notebook source
CATALOG = 'g2_mkt_reviews'
BRONZE_SCHEMA = f'{CATALOG}.bronze'
SILVER_SCHEMA = f'{CATALOG}.silver'
GOLD_SCHEMA = f'{CATALOG}.gold'

# COMMAND ----------

spark.sql(f'DROP SCHEMA IF EXISTS {BRONZE_SCHEMA} CASCADE')
spark.sql(f'DROP SCHEMA IF EXISTS {SILVER_SCHEMA} CASCADE')
spark.sql(f'DROP SCHEMA IF EXISTS {GOLD_SCHEMA} CASCADE')

# COMMAND ----------

spark.sql(f'CREATE DATABASE {BRONZE_SCHEMA}')
spark.sql(f'CREATE DATABASE {SILVER_SCHEMA}')
spark.sql(f'CREATE DATABASE {GOLD_SCHEMA}')

# COMMAND ----------

