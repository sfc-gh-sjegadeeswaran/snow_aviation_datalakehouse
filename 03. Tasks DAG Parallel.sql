/**************************************************************************************************************************
 Use Case: To demonstrate the preprocessing of semi-structured files (.HDF5) and convert it to Snowflake-compatible format
 Tasks DAG to preprocess files, ingest them into stage and load the file data into Snowflake Managed Iceberg tables
***************************************************************************************************************************/

USE ROLE SYSADMIN;
USE SCHEMA SNOWPARK_TRIAL.SNOWPARK_SC;


/****** Create a Root Task to refresh the directory table associated with external stage ******/

CREATE OR REPLACE TASK preprocess_h5_root_task
    WAREHOUSE = SNOWPRO_WH
    SCHEDULE = '5 MINUTES'
AS
    ALTER STAGE SNOW_EXT_STAGE_H5DATA REFRESH;


/****** Create a Child Task to call Stored Procedure to preprocess .h5 files ******/

CREATE OR REPLACE TASK preprocess_h5_call_sp
    WAREHOUSE = SNOWPRO_WH
    AFTER preprocess_h5_root_task
AS
DECLARE
    src_stage VARCHAR;
    tgt_stage VARCHAR;
    source_relative_path VARCHAR;
    file_hash_relpath VARCHAR;
    ret_val VARCHAR;
    file_count INT DEFAULT 0;
BEGIN
    LET res RESULTSET :=
        (SELECT
            dt.relative_path,
            file_hash_relpath,
            source_stage,
            target_stage
         FROM SNOWPARK_TRIAL.SNOWPARK_SC.vw_stage_h5_dir_tbl dt
         LEFT JOIN SNOWPARK_TRIAL.SNOWPARK_SC.preprocess_h5file_status jbs
            ON md5(dt.relative_path) = jbs.input_file_hash_rel
         WHERE jbs.file_relative_path IS NULL
           AND dt.size > 0
        );
    FOR RECORD IN res DO
        src_stage := RECORD.source_stage;
        tgt_stage := RECORD.target_stage;
        source_relative_path := RECORD.relative_path;
        file_hash_relpath := RECORD.file_hash_relpath;

        ASYNC (CALL SNOWPARK_TRIAL.SNOWPARK_SC.preprocess_h5_sp(
            :source_relative_path,
            :file_hash_relpath,
            :src_stage,
            :tgt_stage,
            BUILD_SCOPED_FILE_URL(@SNOWPARK_TRIAL.SNOWPARK_SC.SNOW_EXT_STAGE_H5DATA, :source_relative_path)
        ));
        file_count := file_count + 1;
        SYSTEM$LOG_INFO('Processing submitted for ' || source_relative_path);
    END FOR;
    AWAIT ALL;
    ALTER STAGE SNOW_INT_STAGE_H5DATA REFRESH;
    RETURN 'H5 Preprocessing completed using Async. Files processed: ' || file_count::VARCHAR;
END;


/****** Create a Child Task to load Parquet files into Silver Iceberg Table ******/

-- Create a stream on the internal stage's directory table
CREATE OR REPLACE STREAM snow_int_h5data_stage_stream
    ON STAGE SNOW_INT_STAGE_H5DATA;

-- Task runs only when the stream (Stage) has new data
CREATE OR REPLACE TASK load_parquet_to_iceberg
    WAREHOUSE = SNOWPRO_WH
    AFTER preprocess_h5_call_sp
    WHEN SYSTEM$STREAM_HAS_DATA('snow_int_h5data_stage_stream')
AS
    COPY INTO FLIGHT_QAR_ICEBERG_SILVER
    FROM @SNOW_INT_STAGE_H5DATA/output-files/
    FILE_FORMAT = (TYPE = PARQUET)
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
    PATTERN = '.*\.parquet';


/****** Create a Child Task to run PySpark analytics via SPCS batch job ******/

CREATE OR REPLACE TASK run_qar_analytics_gold
    WAREHOUSE = SNOWPRO_WH
    AFTER load_parquet_to_iceberg
AS
BEGIN
    DROP SERVICE IF EXISTS SNOWPARK_TRIAL.SNOWPARK_SC.QAR_ANALYTICS_GOLD_JOB;
    EXECUTE JOB SERVICE
        IN COMPUTE POOL SPCS_COMPUTE_POOL
        NAME = SNOWPARK_TRIAL.SNOWPARK_SC.QAR_ANALYTICS_GOLD_JOB
        FROM SPECIFICATION $$
        spec:
          container:
          - name: submitter
            image: /SNOWPARK_TRIAL/SNOWPARK_SC/SPCS_IMAGES/qar_spark_analytics:latest
            env:
              SNOWFLAKE_HOST: <org>-<acc>.azure.snowflakecomputing.com  # replace with your Horizon account locator URL
              SNOWFLAKE_ACCOUNT: <org>-<acc>  # replace with your Horizon account identifier
              SNOWFLAKE_WAREHOUSE: SNOWPRO_WH
              SNOWFLAKE_DATABASE: SNOWPARK_TRIAL
              SNOWFLAKE_SCHEMA: SNOWPARK_SC
              SNOWFLAKE_ROLE: SYSADMIN
              COMPUTE_POOL: SPCS_COMPUTE_POOL
            resources:
              requests:
                cpu: 0.5
                memory: 1G
          logExporters:
            eventTableConfig:
              logLevel: INFO
        $$;
END;

/****** Manage Tasks ******/

DROP SERVICE IF EXISTS SNOWPARK_TRIAL.SNOWPARK_SC.QAR_ANALYTICS_GOLD_JOB;

-- Resume tasks (leaf-first order)
ALTER TASK run_qar_analytics_gold RESUME;
ALTER TASK load_parquet_to_iceberg RESUME;
ALTER TASK preprocess_h5_call_sp RESUME;
ALTER TASK preprocess_h5_root_task RESUME;

-- Suspend tasks (root-first order)
ALTER TASK preprocess_h5_root_task SUSPEND;
ALTER TASK preprocess_h5_call_sp SUSPEND;
ALTER TASK load_parquet_to_iceberg SUSPEND;
ALTER TASK run_qar_analytics_gold SUSPEND;

EXECUTE TASK preprocess_h5_root_task;


/****** Validate loaded data in tables ******/

SELECT * FROM flight_h5_metadata;
SELECT * FROM preprocess_h5file_status;
SELECT * FROM FLIGHT_QAR_ICEBERG_SILVER;
SELECT * FROM FLIGHT_QAR_ICEBERG_AGGR_GOLD;
SHOW TABLES;
