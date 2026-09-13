# Databricks notebook source
from pyspark.sql.functions import hour, avg , col ,  sum
silver_path = "/Volumes/workspace/default/nyc_taxi_raw/silver/"
df_silver = spark.read.parquet(silver_path)
df_silver.show(1)
df_silver.count()

df_hourly = df_silver.withColumn(
    "hour",
    hour(col("tpep_pickup_datetime")) 
   )
df_hourly = df_hourly.groupBy("hour").agg(avg("fare_amount").alias("avg_fare"))
df_hourly.orderBy("hour").show()

# COMMAND ----------

gold_path = "/Volumes/workspace/default/nyc_taxi_raw/gold/hourly_avg_fare/"

df_hourly.write \
    .mode("overwrite") \
    .parquet(gold_path)

# COMMAND ----------

from pyspark.sql.functions import *
df_zone_trip = df_silver.groupBy("PULocationID").agg(count("*").alias("total_trips"))
df_zone_trip.orderBy(col("total_trips").desc()).show()


# COMMAND ----------

gold_path = "/Volumes/workspace/default/nyc_taxi_raw/gold/zone_trip_count/"

df_zone_trip.write \
    .mode("overwrite") \
    .parquet(gold_path)

# COMMAND ----------

from pyspark.sql.functions import dayofweek, avg, when, col

df_day_tip = df_silver.withColumn(
    "day_of_week",
    dayofweek(col("tpep_pickup_datetime"))
)

df_day_tip = df_day_tip.groupBy(
    "day_of_week"
).agg(
    avg(
        when(
            col("fare_amount") > 0,
            (col("tip_amount") / col("fare_amount")) * 100
        )
    ).alias("avg_tip_percentage")
)

df_day_tip.orderBy("day_of_week").show()

# COMMAND ----------

gold_path = "/Volumes/workspace/default/nyc_taxi_raw/gold/daily_avg_tip_percentage/"

df_day_tip.write \
    .mode("overwrite") \
    .parquet(gold_path)

# COMMAND ----------

df_peak_hours = df_silver.withColumn(
    "hour",
    hour(col("tpep_pickup_datetime"))
)

df_peak_hours = df_peak_hours.groupBy(
    "hour"
).agg(
    count("*").alias("total_trips")
)


df_peak_hours = df_peak_hours.orderBy(
    col("total_trips").desc()
)

df_peak_hours.show()

# COMMAND ----------

gold_path = "/Volumes/workspace/default/nyc_taxi_raw/gold/peak_demand_hours/"

df_peak_hours.write \
    .mode("overwrite") \
    .parquet(gold_path)

# COMMAND ----------

