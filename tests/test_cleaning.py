
import sys
import os
import shutil
from datetime import datetime

import pytest
from pyspark.sql import SparkSession

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from cleaning import clean_trips
from write_utils import enable_dynamic_partition_overwrite, write_silver

COLUMNS = ["tpep_pickup_datetime", "tpep_dropoff_datetime", "fare_amount", "trip_distance"]
SILVER_PATH = "/tmp/nyc_taxi_test_silver"


def make_row(pickup, dropoff, fare, distance):
    return (
        datetime.fromisoformat(pickup),
        datetime.fromisoformat(dropoff),
        fare,
        distance,
    )


@pytest.fixture(scope="module")
def spark():
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("test-idempotent-pipeline")
        .getOrCreate()
    )
    enable_dynamic_partition_overwrite(spark)
    yield spark
    spark.stop()


@pytest.fixture(autouse=True)
def clean_silver_dir():
    if os.path.exists(SILVER_PATH):
        shutil.rmtree(SILVER_PATH)
    yield
    if os.path.exists(SILVER_PATH):
        shutil.rmtree(SILVER_PATH)


def january_batch(spark, fare_for_trip_1=12.5):
    rows = [
        make_row("2026-01-05T08:00:00", "2026-01-05T08:15:00", fare_for_trip_1, 3.2),
        make_row("2026-01-10T09:00:00", "2026-01-10T09:20:00", 15.0, 4.1),
    ]
    return spark.createDataFrame(rows, COLUMNS)


def february_batch(spark):
    rows = [
        make_row("2026-02-02T07:00:00", "2026-02-02T07:25:00", 22.0, 6.0),
        make_row("2026-02-20T18:00:00", "2026-02-20T18:10:00", 9.5, 1.8),
    ]
    return spark.createDataFrame(rows, COLUMNS)


def test_jan_then_feb_then_rerun_feb_then_late_correction(spark):
    # 1. Load January
    jan_clean = clean_trips(january_batch(spark))
    write_silver(jan_clean, SILVER_PATH)

    result = spark.read.parquet(SILVER_PATH)
    assert result.count() == 2
    assert set(r["year_month"] for r in result.collect()) == {"2026-01"}

    # 2. Load February
    feb_clean = clean_trips(february_batch(spark))
    write_silver(feb_clean, SILVER_PATH)

    result = spark.read.parquet(SILVER_PATH)
    assert result.count() == 4, "January should still be there alongside February"
    months = sorted(r["year_month"] for r in result.collect())
    assert months == ["2026-01", "2026-01", "2026-02", "2026-02"]

    # 3. Rerun February (same data again)
    feb_clean_rerun = clean_trips(february_batch(spark))
    write_silver(feb_clean_rerun, SILVER_PATH)

    result = spark.read.parquet(SILVER_PATH)
    assert result.count() == 4, "Rerunning February must not create duplicates"
    jan_rows = result.filter(result.year_month == "2026-01").count()
    feb_rows = result.filter(result.year_month == "2026-02").count()
    assert jan_rows == 2, "January must be untouched by a February rerun"
    assert feb_rows == 2, "February must still have exactly its 2 rows, not 4"

    # 4. Late correction to January (trip 1's fare was recorded wrong, fix it)
    jan_corrected = clean_trips(january_batch(spark, fare_for_trip_1=13.75))
    write_silver(jan_corrected, SILVER_PATH)

    result = spark.read.parquet(SILVER_PATH)
    assert result.count() == 4, "Correction must replace, not add to, January"
    corrected_row = result.filter(
        (result.year_month == "2026-01") & (result.trip_distance == 3.2)
    ).collect()[0]
    assert corrected_row["fare_amount"] == 13.75, "January correction should be visible"
    feb_rows_after = result.filter(result.year_month == "2026-02").count()
    assert feb_rows_after == 2, "February must be unaffected by a January correction"