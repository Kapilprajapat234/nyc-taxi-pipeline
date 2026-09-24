

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def clean_trips(
    df: DataFrame,
    pickup_col: str = "tpep_pickup_datetime",
    dropoff_col: str = "tpep_dropoff_datetime",
    fare_col: str = "fare_amount",
    distance_col: str = "trip_distance",
) -> DataFrame:
    required_cols = [pickup_col, dropoff_col, fare_col, distance_col]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"clean_trips: missing expected column(s): {missing}")

    cleaned = df.dropDuplicates()

    cleaned = cleaned.dropna(subset=required_cols)

    cleaned = cleaned.filter(
        (F.col(fare_col) > 0)
        & (F.col(distance_col) > 0)
        & (F.col(dropoff_col) > F.col(pickup_col))
    )

    cleaned = cleaned.withColumn(
        "year_month", F.date_format(F.col(pickup_col), "yyyy-MM")
    )

    return cleaned