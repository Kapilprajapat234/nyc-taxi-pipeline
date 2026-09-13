-- 1. Top 5 busiest hours

SELECT
    hour,
    total_trips
FROM peak_demand_hours
ORDER BY total_trips DESC
LIMIT 5;


-- 2. Running total of trips by date

SELECT
    pickup_date,
    count AS daily_trips,
    SUM(count) OVER (
        ORDER BY pickup_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_total_trips
FROM daily_trips
ORDER BY pickup_date;


-- 3. Zone-wise rank by demand

SELECT
    PULocationID,
    total_trips,
    RANK() OVER (
        ORDER BY total_trips DESC
    ) AS demand_rank
FROM zone_trip_count
ORDER BY demand_rank;