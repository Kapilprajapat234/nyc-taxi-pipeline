# Databricks notebook source
hourly_avg_fare_path = "/Volumes/workspace/default/nyc_taxi_raw/gold/hourly_avg_fare/"
zone_trip_count_path = "/Volumes/workspace/default/nyc_taxi_raw/gold/zone_trip_count/"
daily_tip_path = "/Volumes/workspace/default/nyc_taxi_raw/gold/daily_avg_tip_percentage/"
peak_hours_path = "/Volumes/workspace/default/nyc_taxi_raw/gold/peak_demand_hours/"

df_hourly = spark.read.parquet(hourly_avg_fare_path)
df_zone = spark.read.parquet(zone_trip_count_path)

df_tip = spark.read.parquet(daily_tip_path)
df_peak = spark.read.parquet(peak_hours_path)

df_hourly.createOrReplaceTempView("hourly_avg_fare")
df_zone.createOrReplaceTempView("zone_trip_count")
df_tip.createOrReplaceTempView("daily_avg_tip")
df_peak.createOrReplaceTempView("peak_demand_hours")

# COMMAND ----------

spark.sql(""" 
        select  
        hour , 
        total_trips 
        from peak_demand_hours
        order By total_trips desc
        limit 5 
         """).show()

# COMMAND ----------

spark.sql("SELECT * FROM hourly_avg_fare").show()
spark.sql("SELECT * FROM zone_trip_count").show()

# COMMAND ----------

silver_path = "/Volumes/workspace/default/nyc_taxi_raw/silver/"

df_silver = spark.read.parquet(silver_path)

from pyspark.sql.functions import to_date, col

df_daily = df_silver.withColumn(
    "pickup_date",
    to_date(col("tpep_pickup_datetime"))
)

df_daily = df_daily.groupBy(
    "pickup_date"
).count()

df_daily.createOrReplaceTempView("daily_trips")

df_daily.orderBy("pickup_date").show()

# COMMAND ----------

spark.sql("""
SELECT
    pickup_date,
    count AS daily_trips,
    SUM(count) OVER (
        ORDER BY pickup_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_total_trips
FROM daily_trips
ORDER BY pickup_date
""").show()

# COMMAND ----------

spark.sql("""
SELECT
    PULocationID,
    total_trips,
    RANK() OVER (
        ORDER BY total_trips DESC
    ) AS demand_rank
FROM zone_trip_count
ORDER BY demand_rank
""").show()

# COMMAND ----------

from pyspark.sql.functions import col, count

df_silver = spark.read.parquet(
    "/Volumes/workspace/default/nyc_taxi_raw/silver/"
)

query = df_silver.filter(
    col("trip_distance") > 10
).groupBy(
    "PULocationID"
).agg(
    count("*").alias("total_trips")
)

query.show()

# COMMAND ----------

query.explain(True)

# COMMAND ----------

import time
from pyspark.sql.functions import col

df_normal = spark.read.parquet(
    "/Volumes/workspace/default/nyc_taxi_raw/silver/"
)

start = time.time()

df_normal.filter(
    col("tpep_pickup_datetime").cast("date") == "2026-01-15"
).count()

end = time.time()

print("Without partition:", end - start, "seconds")

# COMMAND ----------

import time
from pyspark.sql.functions import col

df_partitioned = spark.read.parquet(
    "/Volumes/workspace/default/nyc_taxi_raw/silver_partitioned/"
)

start = time.time()

df_partitioned.filter(
    col("pickup_date") == "2026-01-15"
).count()

end = time.time()

print("With partition:", end - start, "seconds")

# COMMAND ----------

df_partitioned.filter(
    col("pickup_date") == "2026-01-15"
).explain(True)

# COMMAND ----------

