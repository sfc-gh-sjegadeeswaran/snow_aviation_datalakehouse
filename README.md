# Aviation Data Lake on Snowflake

End-to-end pipeline for ingesting, preprocessing, and analyzing Quick Access Recorder (QAR) flight data using Snowflake's native capabilities: Snowpark Python, Iceberg Tables, Snowpark Container Services (SPCS), and Task DAGs.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        Azure ADLS (External Storage)                     │
│   ┌──────────────┐                          ┌────────────────────────┐  │
│   │  HDF5 Files  │                          │  Iceberg Table Storage │  │
│   └──────┬───────┘                          └────────────┬───────────┘  │
└──────────┼───────────────────────────────────────────────┼──────────────┘
           │                                               │
           ▼                                               ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                              Snowflake                                    │
│                                                                          │
│  ┌─────────────────┐    ┌──────────────────┐    ┌────────────────────┐  │
│  │  External Stage  │───▶│ Snowpark Python  │───▶│  Internal Stage    │  │
│  │  (HDF5 source)   │    │  Stored Proc     │    │  (Parquet output)  │  │
│  └─────────────────┘    │  (Bronze→Silver)  │    └────────┬───────────┘  │
│                          └──────────────────┘             │              │
│                                                           ▼              │
│  ┌───────────────────────────────────────────────────────────────────┐  │
│  │                     Iceberg Tables (Medallion)                     │  │
│  │  ┌──────────┐     ┌─────────────────────┐     ┌──────────────┐   │  │
│  │  │  Bronze  │     │       Silver         │     │     Gold     │   │  │
│  │  │ Metadata │     │  Flight Telemetry    │────▶│ Phase Aggr.  │   │  │
│  │  └──────────┘     │  (1Hz normalized)    │     │ (PySpark/    │   │  │
│  │                    └─────────────────────┘     │  SPCS)       │   │  │
│  │                                                 └──────────────┘   │  │
│  └───────────────────────────────────────────────────────────────────┘  │
│                                                                          │
│  ┌───────────────────────────────────────────────────────────────────┐  │
│  │  Task DAG (Orchestration)                                         │  │
│  │  Root Task → Preprocess SP → Load Parquet → SPCS Analytics Job    │  │
│  └───────────────────────────────────────────────────────────────────┘  │
│                                                                          │
│  ┌───────────────────────────────────────────────────────────────────┐  │
│  │  External Engine Access (Horizon Iceberg REST Catalog)            │  │
│  │  PySpark ←→ Snowflake-managed Iceberg via PAT + vended creds     │  │
│  └───────────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────────────┘
```

## Pipeline Flow

| Layer | Table | Description |
|-------|-------|-------------|
| Bronze | `flight_h5_metadata` | Flight metadata extracted from HDF5 attributes |
| Silver | `FLIGHT_QAR_ICEBERG_SILVER` | Normalized 1Hz telemetry (30+ parameters) |
| Gold | `FLIGHT_QAR_ICEBERG_AGGR_GOLD` | Per-flight, per-phase aggregations (engine, aero, structural, weather) |

## File Descriptions

| File | Purpose |
|------|---------|
| `01. Setup Objects.sql` | Creates storage integrations, stages, Iceberg tables, sequences, and SPCS image repository |
| `02. Preprocess-H5-StoredProc.sql` | Snowpark Python stored procedure that reads HDF5 files, extracts/normalizes QAR signals, and writes Parquet |
| `03. Tasks DAG Parallel.sql` | Snowflake Task DAG for automated orchestration with async parallel file processing |
| `04. External Engine - Iceberg RW Setup.sql` | Creates a service user and role for external Spark access to Iceberg tables via Horizon Catalog |
| `Snowpark_Job_SPCS/` | Containerized PySpark job (Gold-layer analytics) running on Snowpark Container Services |
| `Spark_Iceberg/pyspark_horizon_iceberg.py` | Standalone PySpark script demonstrating external R/W to Snowflake-managed Iceberg tables via REST Catalog |
| `article_assets/` | Sample HDF5 data file for testing the pipeline |

## Prerequisites

- Snowflake account with:
  - ACCOUNTADMIN and SYSADMIN roles
  - Snowpark Container Services enabled
  - External volume support (for Iceberg tables)
- Azure ADLS Gen2 storage account with two containers:
  - `input-files` — source HDF5 files
  - `output-files` — Iceberg table data
- PySpark 3.5.x (for external engine demo only)
- Docker (for building the SPCS container image)

## Setup Instructions

### 1. Configure Placeholders

Before running the scripts, replace the following placeholders with your values:

| Placeholder | Location | Replace With |
|-------------|----------|--------------|
| `<>` (AZURE_TENANT_ID) | `01. Setup Objects.sql` lines 16, 87 | Your Azure AD tenant ID |
| `<org>-<acc>` | `03. Tasks DAG Parallel.sql` line 103 | Your Snowflake account identifier |
| `<org>-<acc>` | `Spark_Iceberg/pyspark_horizon_iceberg.py` line 48 | Your Snowflake account locator URL |

### 2. Run Setup Scripts (in order)

```sql
-- Step 1: Create infrastructure (storage integration, stages, tables)
-- Execute: 01. Setup Objects.sql

-- Step 2: Create the preprocessing stored procedure
-- Execute: 02. Preprocess-H5-StoredProc.sql

-- Step 3: Create and resume the Task DAG
-- Execute: 03. Tasks DAG Parallel.sql

-- Step 4 (optional): Set up external engine access
-- Execute: 04. External Engine - Iceberg RW Setup.sql
```

### 3. Build and Push SPCS Container Image

```bash
cd Snowpark_Job_SPCS

# Build the Docker image
docker build -t qar_spark_analytics:latest .

# Tag for your Snowflake image repository
docker tag qar_spark_analytics:latest \
  <org>-<acc>.registry.snowflakecomputing.com/snowpark_trial/snowpark_sc/spcs_images/qar_spark_analytics:latest

# Login and push
docker login <org>-<acc>.registry.snowflakecomputing.com
docker push <org>-<acc>.registry.snowflakecomputing.com/snowpark_trial/snowpark_sc/spcs_images/qar_spark_analytics:latest
```

### 4. External Spark Access (Optional)

```bash
export HORIZON_PAT="<your-programmatic-access-token>"
cd Spark_Iceberg
python3 pyspark_horizon_iceberg.py
```

## Sample Data

The `article_assets/` directory contains a sample HDF5 file (`SNW-01_SNOW-048_LAX_ICN_SHORT.h5`) representing a single flight recording (LAX → ICN). Upload it to your external stage to test the pipeline end-to-end.

## Key Snowflake Features Demonstrated

- **Snowpark Python** — Stored procedures for HDF5 preprocessing with h5py/pandas/pyarrow
- **Async Stored Procedure Calls** — Parallel file processing within a task using `ASYNC (CALL ...)`
- **Managed Iceberg Tables** — Open table format with external volume on Azure ADLS
- **Snowpark Container Services (SPCS)** — PySpark batch jobs via `EXECUTE JOB SERVICE`
- **Snowflake Task DAGs** — Multi-step orchestration with stream-based conditional execution
- **Horizon Iceberg REST Catalog** — External engine access with PAT and vended credentials
- **Stage Streams** — Change detection on internal stage directory tables
