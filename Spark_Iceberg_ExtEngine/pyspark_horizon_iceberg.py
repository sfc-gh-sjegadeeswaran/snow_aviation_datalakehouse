"""
PySpark script to read/write Snowflake-managed Iceberg tables via Horizon Iceberg REST Catalog.

Prerequisites:
  - PySpark 3.5.x installed (tested with 3.5.5 via Homebrew)
  - Iceberg JARs cached at ~/.ivy2/jars/:
      - iceberg-spark-runtime-3.5_2.12-1.9.1.jar
      - iceberg-azure-bundle-1.9.1.jar
    If not cached, replace spark.jars with spark.jars.packages:
      spark.jars.packages = org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.1,org.apache.iceberg:iceberg-azure-bundle:1.9.1

Usage:
  export HORIZON_PAT="<your-pat-token>"
  python3 pyspark_horizon_iceberg.py

  Or with explicit SPARK_HOME:
  export SPARK_HOME=/opt/homebrew/opt/apache-spark/libexec
  export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip
  export HORIZON_PAT="<your-pat-token>"
  python3 pyspark_horizon_iceberg.py
"""

import os
import sys
import logging
from datetime import datetime

SPARK_HOME = os.environ.get("SPARK_HOME", "/opt/homebrew/opt/apache-spark/libexec")
os.environ["SPARK_HOME"] = SPARK_HOME
sys.path.insert(0, os.path.join(SPARK_HOME, "python"))
sys.path.insert(0, os.path.join(SPARK_HOME, "python", "lib", "py4j-0.10.9.7-src.zip"))

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ─── Configuration ────────────────────────────────────────────────────────────
PAT_TOKEN = os.environ.get("HORIZON_PAT", "")
if not PAT_TOKEN:
    logger.error("HORIZON_PAT environment variable is not set. Exiting.")
    sys.exit(1)

ACCOUNT_LOCATOR_URL = "https://<org>-<acc>.azure.snowflakecomputing.com" # replace with your Horizon account locator URL
HORIZON_URI = f"{ACCOUNT_LOCATOR_URL}/polaris/api/catalog"
CATALOG_NAME = "SNOWPARK_TRIAL"
SCHEMA_NAME = "SNOWPARK_SC"
TABLE_NAME = "PREPROCESS_H5FILE_STATUS"
FULL_TABLE = f"{SCHEMA_NAME}.{TABLE_NAME}"

ICEBERG_RUNTIME_JAR = os.path.expanduser(
    "~/.ivy2/jars/org.apache.iceberg_iceberg-spark-runtime-3.5_2.12-1.9.1.jar"
)
ICEBERG_AZURE_JAR = os.path.expanduser(
    "~/.ivy2/jars/org.apache.iceberg_iceberg-azure-bundle-1.9.1.jar"
)


# ─── SparkSession Builder ────────────────────────────────────────────────────
def create_spark_session() -> SparkSession:
    spark = SparkSession.builder \
        .appName("Horizon-Iceberg-ReadWrite") \
        .master("local[*]") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.jars", f"{ICEBERG_RUNTIME_JAR},{ICEBERG_AZURE_JAR}") \
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.defaultCatalog", CATALOG_NAME) \
        .config(f"spark.sql.catalog.{CATALOG_NAME}",
                "org.apache.iceberg.spark.SparkCatalog") \
        .config(f"spark.sql.catalog.{CATALOG_NAME}.type", "rest") \
        .config(f"spark.sql.catalog.{CATALOG_NAME}.uri", HORIZON_URI) \
        .config(f"spark.sql.catalog.{CATALOG_NAME}.warehouse", CATALOG_NAME) \
        .config(f"spark.sql.catalog.{CATALOG_NAME}.credential", PAT_TOKEN) \
        .config(f"spark.sql.catalog.{CATALOG_NAME}.scope",
                "session:role:HORIZON_ICEBERG_ROLE") \
        .config(f"spark.sql.catalog.{CATALOG_NAME}.header.X-Iceberg-Access-Delegation",
                "vended-credentials") \
        .config("spark.sql.iceberg.vectorization.enabled", "false") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    return spark


# ─── READ Operations ─────────────────────────────────────────────────────────
def read_all_rows(spark: SparkSession):
    """Read all rows from PREPROCESS_H5FILE_STATUS."""
    logger.info(f"Reading all rows from {FULL_TABLE}...")
    df = spark.table(FULL_TABLE)
    df.show(truncate=80)
    logger.info(f"Total rows: {df.count()}")
    logger.info(f"Schema: {df.columns}")
    return df


def read_with_filter(spark: SparkSession, status_filter: str = "Completed"):
    """Read rows with a specific status filter."""
    logger.info(f"Reading rows where STATUS = '{status_filter}'...")
    df = spark.table(FULL_TABLE).filter(F.col("STATUS") == status_filter)
    df.show(truncate=80)
    logger.info(f"Filtered row count: {df.count()}")
    return df


def read_with_sql(spark: SparkSession):
    """Read using Spark SQL syntax."""
    logger.info("Reading via Spark SQL...")
    df = spark.sql(f"""
        SELECT
            INPUT_FILE_NAME,
            STATUS,
            FILE_PROCESSED_DT
        FROM {FULL_TABLE}
        ORDER BY FILE_PROCESSED_DT DESC
    """)
    df.show(truncate=80)
    return df


# ─── WRITE Operations ────────────────────────────────────────────────────────
def insert_new_row(spark: SparkSession):
    """Insert a new row into PREPROCESS_H5FILE_STATUS using SQL INSERT."""
    logger.info("Inserting a new row...")

    spark.sql(f"""
        INSERT INTO {FULL_TABLE} VALUES (
            '/test/pyspark_test_file.h5',
            'pyspark_test_file.h5',
            'abc123def456',
            '@snow_int_stage_h5data/output-files/pyspark_test.parquet',
            'Pending',
            'Inserted via PySpark Horizon REST Catalog',
            current_timestamp()
        )
    """)
    logger.info("Row inserted successfully.")


def insert_via_sql(spark: SparkSession):
    """Insert a new row using Spark SQL INSERT INTO."""
    logger.info("Inserting via Spark SQL...")

    spark.sql(f"""
        INSERT INTO {FULL_TABLE} VALUES (
            '/test/pyspark_sql_insert.h5',
            'pyspark_sql_insert.h5',
            'hash_sql_insert_001',
            '@snow_int_stage_h5data/output-files/pyspark_sql_insert.parquet',
            'Pending',
            'Inserted via PySpark SQL',
            current_timestamp()
        )
    """)
    logger.info("SQL insert completed.")


def update_status(spark: SparkSession, file_name: str, new_status: str, status_msg: str):
    """Update the STATUS and STATUS_MSG for a specific file using MERGE."""
    logger.info(f"Updating status for '{file_name}' to '{new_status}'...")

    spark.sql(f"""
        MERGE INTO {FULL_TABLE} t
        USING (
            SELECT
                '{file_name}' AS INPUT_FILE_NAME,
                '{new_status}' AS STATUS,
                '{status_msg}' AS STATUS_MSG,
                current_timestamp() AS FILE_PROCESSED_DT
        ) s
        ON t.INPUT_FILE_NAME = s.INPUT_FILE_NAME
        WHEN MATCHED THEN UPDATE SET
            t.STATUS = s.STATUS,
            t.STATUS_MSG = s.STATUS_MSG,
            t.FILE_PROCESSED_DT = s.FILE_PROCESSED_DT
    """)
    logger.info(f"Status updated for '{file_name}'.")


def delete_test_rows(spark: SparkSession):
    """Delete test rows inserted by this script."""
    logger.info("Deleting test rows...")
    logger.info(
        "NOTE: DELETE rewrites data files (succeeds) but Horizon vended credentials "
        "lack Azure ADLS delete permission. Orphaned parquet files are cleaned up by "
        "Snowflake's internal table maintenance. Any 'Failed to delete path' warnings "
        "from ADLSFileIO are expected and harmless."
    )
    spark.sql(f"""
        DELETE FROM {FULL_TABLE}
        WHERE INPUT_FILE_NAME LIKE 'pyspark_%'
    """)
    logger.info("Test rows deleted.")


# ─── Main ────────────────────────────────────────────────────────────────────
def main():
    spark = create_spark_session()

    try:
        logger.info("=" * 60)
        logger.info("SECTION 1: READ OPERATIONS")
        logger.info("=" * 60)

        read_all_rows(spark)
        read_with_filter(spark, "Completed")
        read_with_sql(spark)

        logger.info("=" * 60)
        logger.info("SECTION 2: WRITE OPERATIONS")
        logger.info("=" * 60)

        insert_new_row(spark)
        insert_via_sql(spark)

        logger.info("Verifying inserts...")
        spark.sql(f"""
            SELECT * FROM {FULL_TABLE}
            WHERE INPUT_FILE_NAME LIKE 'pyspark_%'
        """).show(truncate=80)

        logger.info("=" * 60)
        logger.info("SECTION 3: UPDATE OPERATIONS (MERGE)")
        logger.info("=" * 60)

        update_status(
            spark,
            file_name="pyspark_test_file.h5",
            new_status="Completed",
            status_msg="Updated via PySpark MERGE"
        )

        logger.info("Verifying update...")
        spark.sql(f"""
            SELECT INPUT_FILE_NAME, STATUS, STATUS_MSG, FILE_PROCESSED_DT
            FROM {FULL_TABLE}
            WHERE INPUT_FILE_NAME = 'pyspark_test_file.h5'
        """).show(truncate=80)

        logger.info("=" * 60)
        logger.info("SECTION 4: CLEANUP (DELETE test rows)")
        logger.info("=" * 60)

        delete_test_rows(spark)

        logger.info("Verifying cleanup...")
        remaining = spark.sql(f"""
            SELECT COUNT(*) AS cnt FROM {FULL_TABLE}
            WHERE INPUT_FILE_NAME LIKE 'pyspark_%'
        """).collect()[0]["cnt"]
        logger.info(f"Test rows remaining: {remaining}")

        logger.info("=" * 60)
        logger.info("ALL OPERATIONS COMPLETED SUCCESSFULLY")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
