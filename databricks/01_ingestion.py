# Databricks notebook source
df = spark.read.parquet(
    "/Volumes/workspace/default/nyc_taxi_raw/yellow_tripdata_2026-01.parquet"
)
df.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW VOLUMES IN workspace.default;
# MAGIC

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /Volumes/workspace/default/nyc_taxi_raw

# COMMAND ----------

df_bronze = spark.read.parquet("/Volumes/workspace/default/nyc_taxi_raw/bronze/yellow_tripdata_2026-01.parquet")
df_bronze.show(5)

# COMMAND ----------

df_bronze.printSchema()
df_bronze.count()

# COMMAND ----------

