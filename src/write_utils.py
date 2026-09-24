def enable_dynamic_partition_overwrite(spark):
    spark.conf.set(
        "spark.sql.sources.partitionOverwriteMode",
        "dynamic"
    )


def write_silver(df, path):
    df.write \
        .mode("overwrite") \
        .partitionBy("year_month") \
        .parquet(path)