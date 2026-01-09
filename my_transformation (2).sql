-- =============================================================================
-- Databricks Delta Live Tables (DLT) Pipeline
-- Pipeline Name : dlt_airline_pipeline_sql
-- Description   : Airlines data processing
--
-- Author        : Raveendra Reddy
-- Created On    : 2025-12-05
-- Last Modified : 2025-12-05
-- Version       : 1.0
--
-- Notes:
-- - Designed for Medallion Architecture (Landing → Bronze → Silver → Gold)
-- =============================================================================


-- =============================================================================
-- SECTION 1: Landing → Bronze (Raw Ingestion with Metadata & Corruption Handling)
-- =============================================================================

-- Streaming read from all CSV files in the ASA airlines dataset
-- Auto-detect schema, handle corrupt records via "PERMISSIVE" + rescue column
CREATE OR REFRESH STREAMING  TABLE bronze_airlines_raw
COMMENT "Raw immutable ingestion of ASA airlines CSV files with full fidelity"
TBLPROPERTIES ("quality" = "bronze", "pipeline.medallion" = "bronze")
AS SELECT 
    current_timestamp() AS ingestion_timestamp,
    _metadata.file_path AS source_file,
    *
  FROM cloud_files(
    "/databricks-datasets/asa/airlines/*.csv",
    "csv",
    map(
      "header", "true",
      "inferSchema", "true",
      "cloudFiles.inferColumnTypes", "true",
      "badRecordsPath", "/dlt/quarantine/asa_airlines/bad_records",
      "cloudFiles.schemaEvolutionMode", "rescue"
    )
  );

-- Bronze Delta Table: Clean raw data (excludes rescued corrupt columns)
CREATE OR REFRESH LIVE TABLE bronze_airlines
COMMENT "Bronze layer: Immutable raw data preserving original structure + metadata"
TBLPROPERTIES ("quality" = "bronze", "pipeline.medallion" = "bronze")
AS SELECT
    ingestion_timestamp,
    source_file,
    -- Explicitly list known columns to ensure stability (schema evolution safe)
    Year, Month, DayofMonth, DayOfWeek, DepTime, CRSDepTime, ArrTime, CRSArrTime,
    UniqueCarrier, FlightNum, TailNum, ActualElapsedTime, CRSElapsedTime,
    AirTime, ArrDelay, DepDelay, Origin, Dest, Distance, TaxiIn, TaxiOut,
    Cancelled, CancellationCode, Diverted, CarrierDelay, WeatherDelay,
    NASDelay, SecurityDelay, LateAircraftDelay
  FROM live.bronze_airlines_raw
  WHERE _rescued_data IS NULL;  -- Exclude fully corrupt records

-- Optional: Quarantine table for fully malformed records
CREATE OR REFRESH LIVE TABLE bronze_airlines_quarantine
COMMENT "Records that could not be parsed at all (rescued JSON blob)"
TBLPROPERTIES ("quality" = "quarantine")
AS SELECT
    current_timestamp() AS quarantine_timestamp,
    _rescued_data AS raw_json
  FROM live.bronze_airlines_raw
  WHERE _rescued_data IS NOT NULL;


-- =============================================================================
-- SECTION 2: Bronze → Silver (Cleaning, Standardization, Validation)
-- =============================================================================

CREATE OR REFRESH LIVE TABLE silver_airlines_clean
COMMENT "Silver layer: Cleaned, standardized, deduplicated, validated airline performance data"
TBLPROPERTIES ("quality" = "silver", "pipeline.medallion" = "silver")
AS
WITH cleaned AS (
  SELECT
    -- Metadata
    ingestion_timestamp,
    source_file,

    -- Standardized snake_case columns with proper types
    CAST(Year AS INT) AS flight_year,
    CAST(Month AS INT) AS flight_month,
    CAST(DayofMonth AS INT) AS flight_day_of_month,
    CAST(DayOfWeek AS INT) AS flight_day_of_week,

    -- Flight date reconstruction
    make_date(CAST(Year AS INT), CAST(Month AS INT), CAST(DayofMonth AS INT)) AS flight_date,

    -- Time-related fields (convert to proper formats)
    TRY_CAST(DepTime AS DOUBLE) AS dep_time,
    TRY_CAST(CRSDepTime AS DOUBLE) AS crs_dep_time,
    TRY_CAST(ArrTime AS DOUBLE) AS arr_time,
    TRY_CAST(CRSArrTime AS DOUBLE) AS crs_arr_time,

    -- Identifiers
    UniqueCarrier AS carrier_code,
    FlightNum AS flight_number,
    TailNum AS tail_number,

    -- Airport codes
    Origin AS origin_airport,
    Dest AS dest_airport,

    -- Durations & distances
    TRY_CAST(ActualElapsedTime AS DOUBLE) AS actual_elapsed_time_min,
    TRY_CAST(CRSElapsedTime AS DOUBLE) AS crs_elapsed_time_min,
    TRY_CAST(AirTime AS DOUBLE) AS air_time_min,
    TRY_CAST(Distance AS DOUBLE) AS distance_miles,
    TRY_CAST(TaxiIn AS DOUBLE) AS taxi_in_min,
    TRY_CAST(TaxiOut AS DOUBLE) AS taxi_out_min,

    -- Delays (cast safely)
    TRY_CAST(ArrDelay AS DOUBLE) AS arrival_delay_min,
    TRY_CAST(DepDelay AS DOUBLE) AS departure_delay_min,
    TRY_CAST(CarrierDelay AS DOUBLE) AS carrier_delay_min,
    TRY_CAST(WeatherDelay AS DOUBLE) AS weather_delay_min,
    TRY_CAST(NASDelay AS DOUBLE) AS nas_delay_min,
    TRY_CAST(SecurityDelay AS DOUBLE) AS security_delay_min,
    TRY_CAST(LateAircraftDelay AS DOUBLE) AS late_aircraft_delay_min,

    -- Flags
    CAST(Cancelled AS INT) AS is_cancelled,
    CancellationCode AS cancellation_reason,
    CAST(Diverted AS INT) AS is_diverted
  FROM live.bronze_airlines
),

deduped AS (
  SELECT *,
    row_number() OVER (
      PARTITION BY flight_date, carrier_code, flight_number, origin_airport, dest_airport, crs_dep_time
      ORDER BY ingestion_timestamp DESC
    ) AS rn
  FROM cleaned
)

SELECT * EXCEPT(rn)
FROM deduped
WHERE rn = 1

  -- Data Quality Expectations (will drop or quarantine violating rows depending on config)
  AND flight_date IS NOT NULL
  AND flight_date BETWEEN '1987-01-01' AND current_date()

  -- Key business rules
  AND (is_cancelled = 0 OR is_cancelled = 1)
  AND (is_diverted = 0 OR is_diverted = 1)
  AND (arrival_delay_min IS NULL OR arrival_delay_min >= -120)   -- Reasonable early arrival
  AND (departure_delay_min IS NULL OR departure_delay_min >= -120);

-- Expectations for monitoring (do not drop rows, just report)
--@dlt.expect("valid_flight_date", "flight_date IS NOT NULL AND flight_date >= '1987-01-01'")
--@dlt.expect("non_negative_arrival_delay", "arrival_delay_min IS NULL OR arrival_delay_min >= -120")
--@dlt.expect("non_negative_departure_delay", "departure_delay_min IS NULL OR departure_delay_min >= -120")
--@dlt.expect_or_drop("valid_carrier_code", "carrier_code IS NOT NULL AND length(carrier_code) = 2")
--@dlt.expect_or_drop("valid_distance", "distance_miles > 0")
--@dlt.expect_or_fail("valid_flight_date_range", "flight_date <= current_date()")


-- =============================================================================
-- SECTION 3: Silver → Gold (Business-Level Aggregated Tables)
-- =============================================================================

-- GOLD TABLE 1: Monthly On-Time Performance by Airline
-- Using MATERIALIZED VIEW for incremental refresh (serverless optimization)
CREATE OR REFRESH MATERIALIZED VIEW gold_monthly_airline_otp
COMMENT "Gold: Monthly on-time performance summary per carrier (dashboard ready)"
TBLPROPERTIES ("quality" = "gold", "pipeline.medallion" = "gold")
AS
SELECT
  flight_year,
  flight_month,
  carrier_code,
  COUNT(*) AS total_flights,
  COUNT_IF(is_cancelled = 1) AS cancelled_flights,
  COUNT_IF(is_diverted = 1) AS diverted_flights,
  COUNT_IF(arrival_delay_min <= 15 AND is_cancelled = 0 AND is_diverted = 0) AS on_time_flights,
  
  ROUND(AVG(arrival_delay_min), 2) AS avg_arrival_delay_min,
  ROUND(AVG(departure_delay_min), 2) AS avg_departure_delay_min,

  ROUND(100.0 * COUNT_IF(is_cancelled = 1) / COUNT(*), 2) AS cancellation_rate_pct,
  ROUND(100.0 * COUNT_IF(is_diverted = 1) / COUNT(*), 2) AS diversion_rate_pct,
  ROUND(100.0 * COUNT_IF(arrival_delay_min <= 15 AND is_cancelled = 0 AND is_diverted = 0) / COUNT(*), 2) AS on_time_percentage
FROM live.silver_airlines_clean
WHERE is_cancelled = 0 AND is_diverted = 0
GROUP BY flight_year, flight_month, carrier_code
ORDER BY flight_year DESC, flight_month DESC, cancellation_rate_pct DESC;


-- GOLD TABLE 2: Delay Reason Breakdown (National Level)
-- Using MATERIALIZED VIEW for incremental refresh (serverless optimization)
CREATE OR REFRESH MATERIALIZED VIEW gold_delay_reason_analytics
COMMENT "Gold: Breakdown of delay minutes by reason (carrier, weather, NAS, etc.)"
TBLPROPERTIES ("quality" = "gold", "pipeline.medallion" = "gold")
AS
SELECT
  flight_year,
  flight_month,
  carrier_code,
  COUNT(*) AS flights_with_delay_data,
  
  ROUND(SUM(carrier_delay_min), 0) AS total_carrier_delay_min,
  ROUND(SUM(weather_delay_min), 0) AS total_weather_delay_min,
  ROUND(SUM(nas_delay_min), 0) AS total_nas_delay_min,
  ROUND(SUM(security_delay_min), 0) AS total_security_delay_min,
  ROUND(SUM(late_aircraft_delay_min), 0) AS total_late_aircraft_delay_min,

  ROUND(AVG(carrier_delay_min), 2) AS avg_carrier_delay_when_attributed,
  ROUND(AVG(weather_delay_min), 2) AS avg_weather_delay_when_attributed
FROM live.silver_airlines_clean
WHERE arrival_delay_min > 15
GROUP BY flight_year, flight_month, carrier_code
HAVING flights_with_delay_data > 10
ORDER BY total_carrier_delay_min DESC;


-- GOLD TABLE 3: Airport-Level KPI Dashboard Dataset
-- Using MATERIALIZED VIEW for incremental refresh (serverless optimization)
CREATE OR REFRESH MATERIALIZED VIEW gold_airport_kpis
COMMENT "Gold: Airport-level performance metrics for BI dashboards"
TBLPROPERTIES ("quality" = "gold", "pipeline.medallion" = "gold")
AS
WITH departures AS (
  SELECT
    origin_airport AS airport,
    'departure' AS flow_type,
    COUNT(*) AS total_ops,
    AVG(departure_delay_min) AS avg_delay_min,
    COUNT_IF(departure_delay_min > 15) AS delayed_ops,
    COUNT_IF(is_cancelled = 1) AS cancelled_ops
  FROM live.silver_airlines_clean
  GROUP BY origin_airport
),
arrivals AS (
  SELECT
    dest_airport AS airport,
    'arrival' AS flow_type,
    COUNT(*) AS total_ops,
    AVG(arrival_delay_min) AS avg_delay_min,
    COUNT_IF(arrival_delay_min > 15) AS delayed_ops,
    COUNT_IF(is_cancelled = 1) AS cancelled_ops
  FROM live.silver_airlines_clean
  GROUP BY dest_airport
),
combined AS (
  SELECT * FROM departures
  UNION ALL
  SELECT * FROM arrivals
)
SELECT
  airport,
  COUNT(*) AS total_operations,
  ROUND(AVG(avg_delay_min), 2) AS avg_delay_all_flights,
  ROUND(100.0 * SUM(delayed_ops) / SUM(total_ops), 2) AS delay_rate_pct,
  ROUND(100.0 * SUM(cancelled_ops) / SUM(total_ops), 2) AS cancellation_rate_pct,
  SUM(total_ops) AS total_flights
FROM combined
GROUP BY airport
HAVING total_flights >= 1000  -- Focus on major airports
ORDER BY total_operations DESC;

-- =============================================================================
-- END OF PIPELINE
-- This file can be saved as airlines_medallion_dlt.sql and attached to a serverless DLT pipeline
-- in Databricks with target catalog/schema (e.g., `main.airlines_dlt`).
-- Benefits of serverless: No cluster config needed, auto-scaling, incremental MV refreshes,
-- rapid startup, and pay-per-use billing. Ensure workspace is Unity Catalog-enabled.
-- =============================================================================