/**********************************************************************************************************************
 Use Case: To demonstrate the preprocessing of semi-structured files (.HDF5) and convert it to Snowflake-compatible format
 Setup script to create Storage Integration, Stage, External Volume, Iceberg Tables and SPCS Image Repository
***********************************************************************************************************************/


/****************** Create Storage Integration for Azure ADLS ******************/

USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE STORAGE INTEGRATION snowpark_az_eastus2_storage_integ
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'AZURE'
    ENABLED = TRUE
    AZURE_TENANT_ID = '<>' -- Replace with your Azure Tenant ID
    STORAGE_ALLOWED_LOCATIONS = (
        'azure://demoicebergadls.blob.core.windows.net/input-files'
    );

DESC STORAGE INTEGRATION snowpark_az_eastus2_storage_integ;

GRANT USAGE ON INTEGRATION snowpark_az_eastus2_storage_integ TO ROLE SYSADMIN;


/****************** Create Database, Schema and External Stage ******************/

USE ROLE SYSADMIN;

CREATE DATABASE IF NOT EXISTS SNOWPARK_TRIAL;
CREATE SCHEMA IF NOT EXISTS SNOWPARK_TRIAL.SNOWPARK_SC;

USE SCHEMA SNOWPARK_TRIAL.SNOWPARK_SC;

-- External stage pointing to HDF5 source files on Azure ADLS
CREATE OR REPLACE STAGE snow_ext_stage_h5data
    STORAGE_INTEGRATION = snowpark_az_eastus2_storage_integ
    URL = 'azure://demoicebergadls.blob.core.windows.net/input-files/h5-files'
    DIRECTORY = (ENABLE = TRUE);

LIST @snow_ext_stage_h5data;
ALTER STAGE snow_ext_stage_h5data REFRESH;
SELECT * FROM DIRECTORY(@snow_ext_stage_h5data);


/****************** Create Internal Stage for Preprocessed Output ******************/

CREATE OR ALTER STAGE snow_int_stage_h5data
    DIRECTORY = (ENABLE = TRUE);

LIST @snow_int_stage_h5data;

SHOW STAGES;


/****************** Create a View on External Stage Directory Table ******************/

CREATE OR REPLACE VIEW vw_stage_h5_dir_tbl
AS
SELECT
    relative_path,
    file_url,
    last_modified,
    etag,
    MD5(relative_path)      AS file_hash_relpath,
    MD5(file_url)           AS file_hash_fileurl,
    size,
    'snow_int_stage_h5data' AS target_stage,
    'snow_ext_stage_h5data' AS source_stage
FROM DIRECTORY(@snow_ext_stage_h5data)
WHERE size > 0;

SELECT * FROM vw_stage_h5_dir_tbl;


/****************** Create External Volume for Iceberg Tables ******************/

USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE EXTERNAL VOLUME snowpark_az_eastus2_iceberg_ext_vol
    STORAGE_LOCATIONS = (
        (
            NAME             = 'azure-adls-eastus2_iceberg'
            STORAGE_PROVIDER = 'AZURE'
            STORAGE_BASE_URL = 'azure://demoicebergadls.dfs.core.windows.net/output-files/h5-iceberg-data/'
            AZURE_TENANT_ID  = '<>' -- Replace with your Azure Tenant ID
        )
    );

DESC EXTERNAL VOLUME snowpark_az_eastus2_iceberg_ext_vol;
SELECT SYSTEM$VERIFY_EXTERNAL_VOLUME('snowpark_az_eastus2_iceberg_ext_vol');

GRANT USAGE ON EXTERNAL VOLUME snowpark_az_eastus2_iceberg_ext_vol TO ROLE SYSADMIN;


/****************** Create Managed Iceberg Tables ******************/

USE ROLE SYSADMIN;
USE SCHEMA SNOWPARK_TRIAL.SNOWPARK_SC;

-- Preprocessing status tracking table
CREATE OR REPLACE ICEBERG TABLE preprocess_h5file_status (
    file_relative_path      VARCHAR,
    input_file_name         VARCHAR,
    input_file_hash_rel     VARCHAR,
    output_file_path        VARCHAR,
    status                  VARCHAR,
    status_msg              VARCHAR,
    file_processed_dt       TIMESTAMP_NTZ
)
    CATALOG = 'SNOWFLAKE'
    EXTERNAL_VOLUME = 'SNOWPARK_AZ_EASTUS2_ICEBERG_EXT_VOL'
    BASE_LOCATION = 'iceberg-preprocess-h5-status/';

SELECT * FROM preprocess_h5file_status;


-- Flight metadata table (Bronze)
CREATE OR REPLACE ICEBERG TABLE flight_h5_metadata (
    flight_id                   INT PRIMARY KEY,
    file_name                   VARCHAR,
    flight_number               VARCHAR,
    aircraft_tail               VARCHAR,
    aircraft_type               VARCHAR,
    departure_airport           VARCHAR,
    arrival_airport             VARCHAR,
    flight_date                 DATE,
    flight_departure_utc        TIMESTAMP_NTZ,
    qar_recorded_data_seconds   INT,
    created_date                TIMESTAMP_NTZ,
    updated_date                TIMESTAMP_NTZ
)
    CATALOG = 'SNOWFLAKE'
    EXTERNAL_VOLUME = 'SNOWPARK_AZ_EASTUS2_ICEBERG_EXT_VOL'
    BASE_LOCATION = 'iceberg-flight-h5-metadata/';

SELECT * FROM flight_h5_metadata;


-- Flight telemetry table (Silver) — loaded from preprocessed Parquet files
CREATE OR REPLACE ICEBERG TABLE FLIGHT_QAR_ICEBERG_SILVER (
    flight_id                    INT 
                FOREIGN KEY REFERENCES flight_h5_metadata(flight_id),
    flight_date                  VARCHAR,
    flight_number                VARCHAR,
    aircraft_tail                VARCHAR,
    aircraft_type                VARCHAR,
    arrival_airport              VARCHAR,
    departure_airport            VARCHAR,
    air_temperature              FLOAT,
    airspeed                     FLOAT,
    attack_angle                 FLOAT,
    baro_altitude                FLOAT,
    engine_n1_1                  FLOAT,
    engine_n1_2                  FLOAT,
    flight_phase                 INT,
    fuel_flow_total              FLOAT,
    ground_speed                 FLOAT,
    heading_magnetic             FLOAT,
    heading_true                 FLOAT,
    lateral_acceleration         FLOAT,
    latitude                     FLOAT,
    longitude                    FLOAT,
    longitudinal_acceleration    FLOAT,
    mach_number                  FLOAT,
    pitch_angle                  FLOAT,
    pitch_rate                   FLOAT,
    radio_altitude               FLOAT,
    roll_angle                   FLOAT,
    total_air_temperature        FLOAT,
    vertical_acceleration        FLOAT,
    vertical_speed               FLOAT,
    wind_direction               FLOAT,
    wind_speed                   FLOAT,
    yaw_rate                     FLOAT,
    flight_departure_dttm        LONG
)
    CATALOG = 'SNOWFLAKE'
    EXTERNAL_VOLUME = 'SNOWPARK_AZ_EASTUS2_ICEBERG_EXT_VOL'
    BASE_LOCATION = 'iceberg-flight-qar-raw/';


-- Flight phase aggregations table (Gold) — computed by PySpark SPCS job
CREATE OR REPLACE ICEBERG TABLE FLIGHT_QAR_ICEBERG_AGGR_GOLD (
    flight_id                   INT 
        FOREIGN KEY REFERENCES flight_h5_metadata(flight_id),
    flight_phase                INT,
    duration_sec                FLOAT,
    fuel_mean_flow_kg_hr        FLOAT,
    fuel_total_kg               FLOAT,
    n1_mean_eng1_pct            FLOAT,
    n1_mean_eng2_pct            FLOAT,
    n1_max_imbalance_pct        FLOAT,
    n1_mean_imbalance_pct       FLOAT,
    vrtg_mean_g                 FLOAT,
    vrtg_max_g                  FLOAT,
    vrtg_min_g                  FLOAT,
    vrtg_std_g                  FLOAT,
    vrtg_exceedances_1p5g       FLOAT,
    ias_min_kts                 FLOAT,
    ias_mean_kts                FLOAT,
    ias_max_kts                 FLOAT,
    ias_std_kts                 FLOAT,
    alt_min_ft                  FLOAT,
    alt_mean_ft                 FLOAT,
    alt_max_ft                  FLOAT,
    alt_range_ft                FLOAT,
    wind_mean_spd_kts           FLOAT,
    wind_mean_hw_comp_kts       FLOAT,
    wind_max_headwind_kts       FLOAT,
    wind_max_tailwind_kts       FLOAT,
    roll_max_deg                FLOAT,
    roll_rms_deg                FLOAT,
    roll_exceed_30deg           INT,
    pitch_rate_mean_dps         FLOAT,
    pitch_rate_rms_dps          FLOAT,
    pitch_rate_peak_dps         FLOAT,
    pitch_rate_std_dps          FLOAT,
    aoa_mean_deg                FLOAT,
    aoa_max_deg                 FLOAT,
    aoa_std_deg                 FLOAT,
    aoa_events_gt10deg          INT,
    latg_mean_g                 FLOAT,
    latg_rms_g                  FLOAT,
    latg_peak_g                 FLOAT,
    latg_events_gt0p1g          INT,
    record_created_at           TIMESTAMP_NTZ,
    record_updated_at           TIMESTAMP_NTZ
)
    CATALOG = 'SNOWFLAKE'
    EXTERNAL_VOLUME = 'SNOWPARK_AZ_EASTUS2_ICEBERG_EXT_VOL'
    BASE_LOCATION = 'qar_flight_analytics_gold/';

SHOW TABLES;


/****************** Create Sequence for Flight ID Generation ******************/

CREATE OR REPLACE SEQUENCE flight_id_seq START = 1 INCREMENT = 1;


/****************** Create SPCS Image Repository ******************/

CREATE IMAGE REPOSITORY IF NOT EXISTS SNOWPARK_TRIAL.SNOWPARK_SC.SPCS_IMAGES;

SHOW IMAGE REPOSITORIES IN SCHEMA SNOWPARK_TRIAL.SNOWPARK_SC;

SHOW IMAGES IN IMAGE REPOSITORY SNOWPARK_TRIAL.SNOWPARK_SC.SPCS_IMAGES;
