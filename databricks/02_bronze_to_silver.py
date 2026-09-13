# Databricks notebook source
df_bronze = spark.read.parquet("/Volumes/workspace/default/nyc_taxi_raw/bronze/yellow_tripdata_2026-01.parquet")
df_bronze.show()

# COMMAND ----------

df_clean = df_bronze.filter(
    (df_bronze.fare_amount >= 0) &
    (df_bronze.passenger_count > 0) &
    (df_bronze.tpep_pickup_datetime <= df_bronze.tpep_dropoff_datetime)
)

df_clean.show()
df_clean = df_clean.dropDuplicates()
df_clean.count()




# COMMAND ----------

from pyspark.sql.functions import col, sum
null_count = df_bronze.select([
    sum(col(c).isNull().cast("int")).alias(c)
    for c in df_bronze.columns
])
null_count.show()
null_count.count()

# COMMAND ----------

df_clean = df_bronze.dropna(
    subset=["tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "fare_amount",
        "trip_distance",
        "PULocationID",
        "DOLocationID"]
)



# COMMAND ----------

from pyspark.sql.functions import col

df_clean = df_clean.withColumn(
    "fare_amount",
    col("fare_amount").cast("double")
)

# COMMAND ----------

df_clean.count()

# COMMAND ----------

print("Before:", df_bronze.count())
print("After NULL removal:", df_clean.count())

# COMMAND ----------

silver_path = "/Volumes/workspace/default/nyc_taxi_raw/silver/"

dbutils.fs.mkdirs(silver_path)

df_clean.write.mode("overwrite").parquet(silver_path)

print("Silver data saved successfully")

# COMMAND ----------

# MAGIC %md   
# MAGIC ### overall code in correct form 
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col, sum

# 1. Load Bronze
df_bronze = spark.read.parquet(
    "/Volumes/workspace/default/nyc_taxi_raw/bronze/yellow_tripdata_2026-01.parquet"
)

# 2. NULL values check
null_count = df_bronze.select([
    sum(col(c).isNull().cast("int")).alias(c)
    for c in df_bronze.columns
])

null_count.show()

# 3. Remove NULLs
df_clean = df_bronze.dropna(
    subset=[
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "fare_amount",
        "trip_distance",
        "PULocationID",
        "DOLocationID"
    ]
)

# 4. Remove invalid rows
df_clean = df_clean.filter(
    (df_clean.fare_amount >= 0) &
    (df_clean.passenger_count > 0) &
    (df_clean.trip_distance > 0) &
    (df_clean.tpep_pickup_datetime <= df_clean.tpep_dropoff_datetime)
)

# 5. Remove duplicates
df_clean = df_clean.dropDuplicates()

# 6. Fix datatype
df_clean = df_clean.withColumn(
    "fare_amount",
    col("fare_amount").cast("double")
)

# 7. Before vs After
print("Before:", df_bronze.count())
print("After:", df_clean.count())

# 8. Save Silver
silver_path = "/Volumes/workspace/default/nyc_taxi_raw/silver/"

dbutils.fs.mkdirs(silver_path)

df_clean.write.mode("overwrite").parquet(silver_path)

print("Silver data saved successfully")

# COMMAND ----------

from pyspark.sql.functions import col , to_date 
silver_path = "/Volumes/workspace/default/nyc_taxi_raw/silver/"
df_silver = spark.read.parquet(silver_path)
df_silver.show(5)
df_silver.count()

# COMMAND ----------

df_silver = df_silver.withColumn("pickup_date", to_date(col("tpep_pickup_datetime")))

# COMMAND ----------

df_silver.select("tpep_pickup_datetime", "pickup_date").show(5)

# COMMAND ----------

partitioned_path = "/Volumes/workspace/default/nyc_taxi_raw/silver_partitioned/"
df_silver.write \
    .mode("overwrite") \
    .partitionBy("pickup_date") \
    .parquet(partitioned_path)

# COMMAND ----------

# MAGIC %fs ls /Volumes/workspace/default/nyc_taxi_raw/silver_partitioned/
# MAGIC

# COMMAND ----------

df_partitioned = spark.read.parquet(partitioned_path)
df_partitioned.filter(col("pickup_date") == "2026-01-15").show(10)

# COMMAND ----------

df_partitioned = spark.read.parquet(partitioned_path)
df_partitioned.filter(col("pickup_date") == "2026-01-15").count()

# COMMAND ----------

df_partitioned = spark.read.parquet(partitioned_path)
df_partitioned.filter(col("pickup_date") == "2026-01-15").explain(True)

# COMMAND ----------

