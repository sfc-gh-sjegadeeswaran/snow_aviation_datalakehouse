"""
QAR Flight Analytics — Gold Layer Aggregation (Incremental)
===========================================================
PySpark job that reads cleaned telemetry from the Iceberg Silver table,
computes per-flight-phase aggregations (engine, aero, structural, weather),
and appends only **new** (not-yet-processed) flights to the Iceberg Gold table.

Pipeline position:
    Bronze (raw HDF5 ingest)  →  Silver (cleaned parquet/Iceberg)  →  **Gold (this script)**

Incremental strategy:
    1.  Read distinct flight_id values from both Silver and Gold.
    2.  Left-anti join to isolate flights present in Silver but absent from Gold.
    3.  Filter Silver rows to only those new flight_ids, then aggregate and append.

Environment variables (required):
    SNOWFLAKE_DATABASE   Target Snowflake database
    SNOWFLAKE_SCHEMA     Target Snowflake schema

Runs on Snowpark Container Services via `snowpark-submit` (Spark Connect).
"""

import logging
import os
import sys
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("qar_flight_analytics")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Iceberg table names (within the configured database.schema)
SILVER_TABLE_NAME = "FLIGHT_QAR_ICEBERG_SILVER"
GOLD_TABLE_NAME = "FLIGHT_QAR_ICEBERG_AGGR_GOLD"

# Silver table is recorded at 4 Hz; used to convert row counts → seconds.
SAMPLE_RATE_HZ = 4.0


# ---------------------------------------------------------------------------
# Core aggregation logic
# Compute per-flight, per-phase aggregations across 10 metric groups:
# ---------------------------------------------------------------------------
def compute_aggregations(raw):
    # Headwind component: positive = headwind, negative = tailwind
    wind_hw = F.col("wind_speed") * F.cos(
        F.toRadians(F.col("wind_direction") - F.col("heading_true"))
    )

    return (
        raw
        .groupBy("flight_id", "flight_phase")
        .agg(
            # -- 1. Duration ------------------------------------------------
            (F.count("*") / F.lit(SAMPLE_RATE_HZ)).alias("duration_sec"),

            # -- 2. Fuel ----------------------------------------------------
            F.mean("fuel_flow_total").alias("fuel_mean_flow_kg_hr"),

            # -- 3. Engine N1 -----------------------------------------------
            F.mean("engine_n1_1").alias("n1_mean_eng1_pct"),
            F.mean("engine_n1_2").alias("n1_mean_eng2_pct"),
            F.max(F.abs(F.col("engine_n1_1") - F.col("engine_n1_2"))).alias("n1_max_imbalance_pct"),
            F.mean(F.abs(F.col("engine_n1_1") - F.col("engine_n1_2"))).alias("n1_mean_imbalance_pct"),

            # -- 4. Vertical acceleration (turbulence / structural loads) ----
            F.mean("vertical_acceleration").alias("vrtg_mean_g"),
            F.max("vertical_acceleration").alias("vrtg_max_g"),
            F.min("vertical_acceleration").alias("vrtg_min_g"),
            F.stddev("vertical_acceleration").alias("vrtg_std_g"),
            F.sum(F.when(F.abs(F.col("vertical_acceleration")) > 1.5, 1).otherwise(0)).alias("vrtg_exceedances_1p5g"),

            # -- 5. Indicated airspeed (IAS) --------------------------------
            F.min("airspeed").alias("ias_min_kts"),
            F.mean("airspeed").alias("ias_mean_kts"),
            F.max("airspeed").alias("ias_max_kts"),
            F.stddev("airspeed").alias("ias_std_kts"),

            # -- 6. Barometric altitude -------------------------------------
            F.min("baro_altitude").alias("alt_min_ft"),
            F.mean("baro_altitude").alias("alt_mean_ft"),
            F.max("baro_altitude").alias("alt_max_ft"),
            (F.max("baro_altitude") - F.min("baro_altitude")).alias("alt_range_ft"),

            # -- 7. Wind ----------------------------------------------------
            F.mean("wind_speed").alias("wind_mean_spd_kts"),
            F.mean(wind_hw).alias("wind_mean_hw_comp_kts"),
            F.max(wind_hw).alias("wind_max_headwind_kts"),
            F.min(wind_hw).alias("wind_max_tailwind_kts"),

            # -- 8. Roll ----------------------------------------------------
            F.max(F.abs(F.col("roll_angle"))).alias("roll_max_deg"),
            F.sqrt(F.mean(F.col("roll_angle") ** 2)).alias("roll_rms_deg"),
            F.sum(F.when(F.abs(F.col("roll_angle")) > 30, 1).otherwise(0)).alias("roll_exceed_30deg"),

            # -- 9. Pitch rate ----------------------------------------------
            F.mean("pitch_rate").alias("pitch_rate_mean_dps"),
            F.sqrt(F.mean(F.col("pitch_rate") ** 2)).alias("pitch_rate_rms_dps"),
            F.max(F.abs(F.col("pitch_rate"))).alias("pitch_rate_peak_dps"),
            F.stddev("pitch_rate").alias("pitch_rate_std_dps"),

            # -- 10. Angle of attack ----------------------------------------
            F.mean("attack_angle").alias("aoa_mean_deg"),
            F.max("attack_angle").alias("aoa_max_deg"),
            F.stddev("attack_angle").alias("aoa_std_deg"),
            F.sum(F.when(F.col("attack_angle") > 10, 1).otherwise(0)).alias("aoa_events_gt10deg"),

            # -- 11. Lateral acceleration -----------------------------------
            F.mean("lateral_acceleration").alias("latg_mean_g"),
            F.sqrt(F.mean(F.col("lateral_acceleration") ** 2)).alias("latg_rms_g"),
            F.max(F.abs(F.col("lateral_acceleration"))).alias("latg_peak_g"),
            F.sum(F.when(F.abs(F.col("lateral_acceleration")) > 0.1, 1).otherwise(0)).alias("latg_events_gt0p1g"),
        )
    )


# ---------------------------------------------------------------------------
# Post-aggregation: derive fuel total, map phase labels, add audit columns
# ---------------------------------------------------------------------------
def build_gold_dataframe(agg_df, now_ts):
    """
    Transform the raw aggregation output into the final Gold schema:
    """
    return (
        agg_df
        # Fuel burned (kg) = mean flow (kg/hr) × duration (hr)
        .withColumn("fuel_total_kg",
                     F.col("fuel_mean_flow_kg_hr") * (F.col("duration_sec") / 3600.0))
        # Audit / lineage timestamps
        .withColumn("record_created_at", F.lit(now_ts))
        .withColumn("record_updated_at", F.lit(now_ts))
        .select(
            "flight_id", "flight_phase", "duration_sec",
            "fuel_mean_flow_kg_hr", "fuel_total_kg",
            "n1_mean_eng1_pct", "n1_mean_eng2_pct",
            "n1_max_imbalance_pct", "n1_mean_imbalance_pct",
            "vrtg_mean_g", "vrtg_max_g", "vrtg_min_g", "vrtg_std_g",
            "vrtg_exceedances_1p5g",
            "ias_min_kts", "ias_mean_kts", "ias_max_kts", "ias_std_kts",
            "alt_min_ft", "alt_mean_ft", "alt_max_ft", "alt_range_ft",
            "wind_mean_spd_kts", "wind_mean_hw_comp_kts",
            "wind_max_headwind_kts", "wind_max_tailwind_kts",
            "roll_max_deg", "roll_rms_deg", "roll_exceed_30deg",
            "pitch_rate_mean_dps", "pitch_rate_rms_dps",
            "pitch_rate_peak_dps", "pitch_rate_std_dps",
            "aoa_mean_deg", "aoa_max_deg", "aoa_std_deg", "aoa_events_gt10deg",
            "latg_mean_g", "latg_rms_g", "latg_peak_g", "latg_events_gt0p1g",
            "record_created_at", "record_updated_at",
        )
    )


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
def get_config():
    """Read database/schema from environment variables set by entrypoint.sh."""
    database = os.environ.get("SNOWFLAKE_DATABASE")
    schema = os.environ.get("SNOWFLAKE_SCHEMA")
    if not database or not schema:
        logger.error("SNOWFLAKE_DATABASE and SNOWFLAKE_SCHEMA env vars are required")
        sys.exit(1)
    return database, schema


# ---------------------------------------------------------------------------
# Incremental detection
# ---------------------------------------------------------------------------
def get_incremental_flights(spark, silver_tbl, gold_tbl):
    """
    Return a DataFrame of flight_id values that exist in Silver but NOT in Gold.

    Uses a left-anti join so only unprocessed flights are selected.
    If the Gold table does not yet exist (first run), all Silver flights
    are treated as new.
    """
    silver_flights = spark.table(silver_tbl).select("flight_id").distinct()

    try:
        gold_flights = spark.table(gold_tbl).select("flight_id").distinct()
        new_flights = silver_flights.join(gold_flights, on="flight_id", how="left_anti")
    except Exception:
        logger.info("Gold table %s does not exist or is empty — treating all silver flights as new", gold_tbl)
        new_flights = silver_flights

    return new_flights


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main():
    database, schema = get_config()
    silver_tbl = f"{database}.{schema}.{SILVER_TABLE_NAME}"
    gold_tbl = f"{database}.{schema}.{GOLD_TABLE_NAME}"

    spark = SparkSession.builder.appName("QAR_Flight_Analytics").getOrCreate()

    try:
        now_ts = datetime.now(timezone.utc)

        # Step 1: Identify which flights in Silver have not been aggregated yet
        logger.info("Identifying incremental flights from %s", silver_tbl)
        new_flights = get_incremental_flights(spark, silver_tbl, gold_tbl)
        new_flight_count = new_flights.count()

        if new_flight_count == 0:
            logger.info("No new flights to process — gold table is up to date")
            return

        logger.info("Found %d new flight(s) to process", new_flight_count)

        # Step 2: Read only incremental Silver rows (inner join on new flight_ids)
        raw = spark.table(silver_tbl).join(new_flights, on="flight_id", how="inner")
        raw_count = raw.count()
        logger.info("Incremental silver row count: %d", raw_count)

        # Step 3: Compute per-flight, per-phase aggregations
        agg_df = compute_aggregations(raw)
        gold_df = build_gold_dataframe(agg_df, now_ts)

        # Step 4: Append new aggregations to the Gold Iceberg table
        logger.info("Appending gold aggregations to %s", gold_tbl)
        gold_df.write.mode("append").format("iceberg").saveAsTable(gold_tbl)

        # Verify by reading back from gold (Spark Connect invalidates the
        # DataFrame plan after saveAsTable, so gold_df.count() would return 0).
        new_ids = [row.flight_id for row in new_flights.collect()]
        written = spark.table(gold_tbl).filter(F.col("flight_id").isin(new_ids)).count()
        logger.info("Wrote %d phase-aggregation rows for %d new flight(s) to %s",
                    written, new_flight_count, gold_tbl)

    except Exception:
        logger.exception("QAR analytics job failed")
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
