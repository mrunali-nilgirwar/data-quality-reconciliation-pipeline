-- Part 2: Redshift Serverless load + cross-system reconciliation
-- Source: s3://mrunali-dqe-project-20260913/flights_raw/ (JSON Lines, us-east-2)
-- Target: Redshift Serverless workgroup dqe-flights-wg (us-east-1), database dev

-- 1. Create target table
--    Nullable numeric columns use DOUBLE PRECISION because pandas stores
--    columns containing missing values as floats (e.g. 2354.0), which
--    Redshift would reject for an INTEGER column.
CREATE TABLE public.flights_source (
    year                 INTEGER,
    month                INTEGER,
    day                  INTEGER,
    day_of_week          INTEGER,
    airline              VARCHAR(10),
    flight_number        INTEGER,
    tail_number          VARCHAR(20),
    origin_airport       VARCHAR(10),
    destination_airport  VARCHAR(10),
    scheduled_departure  INTEGER,
    departure_time       DOUBLE PRECISION,
    departure_delay      DOUBLE PRECISION,
    taxi_out             DOUBLE PRECISION,
    wheels_off           DOUBLE PRECISION,
    scheduled_time       DOUBLE PRECISION,
    elapsed_time         DOUBLE PRECISION,
    air_time             DOUBLE PRECISION,
    distance             INTEGER,
    wheels_on            DOUBLE PRECISION,
    taxi_in              DOUBLE PRECISION,
    scheduled_arrival    INTEGER,
    arrival_time         DOUBLE PRECISION,
    arrival_delay        DOUBLE PRECISION,
    diverted             INTEGER,
    cancelled            INTEGER,
    cancellation_reason  VARCHAR(5),
    air_system_delay     DOUBLE PRECISION,
    security_delay       DOUBLE PRECISION,
    airline_delay        DOUBLE PRECISION,
    late_aircraft_delay  DOUBLE PRECISION,
    weather_delay        DOUBLE PRECISION
);

-- 2. Load from S3
--    REGION is required because the bucket (us-east-2) is in a different
--    region than the workgroup (us-east-1); without it COPY fails with a
--    301 PermanentRedirect. IAM role is scoped to this single bucket.
COPY public.flights_source
FROM 's3://mrunali-dqe-project-20260913/flights_raw/'
IAM_ROLE default
FORMAT AS JSON 'auto ignorecase'
REGION 'us-east-2';
-- Result: 285,368 records loaded (matches pandas source count)

-- 3. Schema check: non-standard airport codes
SELECT
    month,
    COUNT(*) AS total_rows,
    SUM(CASE WHEN LENGTH(origin_airport) != 3
              OR LENGTH(destination_airport) != 3
             THEN 1 ELSE 0 END) AS nonstandard_codes
FROM public.flights_source
GROUP BY month;
-- Result: month 6 | 285,368 | 0

-- 4. Duplicate check (origin + destination included, since flight
--    numbers operate round-trip legs on the same day)
SELECT
    year, month, day, airline, flight_number,
    origin_airport, destination_airport,
    COUNT(*) AS cnt
FROM public.flights_source
GROUP BY year, month, day, airline, flight_number,
         origin_airport, destination_airport
HAVING COUNT(*) > 1;
-- Result: 0 rows

-- 5. Null check on delay-reason columns (verifies NaN -> null fix end to end)
SELECT
    COUNT(*) - COUNT(weather_delay)       AS weather_nulls,
    COUNT(*) - COUNT(airline_delay)       AS airline_nulls,
    COUNT(*) - COUNT(air_system_delay)    AS air_system_nulls,
    COUNT(*) - COUNT(security_delay)      AS security_nulls,
    COUNT(*) - COUNT(late_aircraft_delay) AS late_aircraft_nulls
FROM public.flights_source;
-- Result: 217,877 in all five columns (matches pandas)