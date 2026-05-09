/**************************************************************************************************************************
 Use Case: To demonstrate the preprocessing of semi-structured files (.HDF5) and convert it to Snowflake-compatible format
 Script to create a role and user for external engine to R/W Iceberg tables managed by Snowflake Horizon Catalog
***************************************************************************************************************************/

/****** Create Role and User for Iceberg R/W from external query engine ******/

USE ROLE SECURITYADMIN;

CREATE ROLE IF NOT EXISTS HORIZON_ICEBERG_ROLE;

CREATE USER IF NOT EXISTS SPARK_HORIZON_SVC
    TYPE = SERVICE
    DEFAULT_ROLE = HORIZON_ICEBERG_ROLE;

GRANT ROLE HORIZON_ICEBERG_ROLE TO USER SPARK_HORIZON_SVC;


/****** Grant Usage on External Volume and Generate PAT for the Iceberg User ******/

USE ROLE ACCOUNTADMIN;

GRANT USAGE ON EXTERNAL VOLUME SNOWPARK_AZ_EASTUS2_ICEBERG_EXT_VOL
    TO ROLE HORIZON_ICEBERG_ROLE;

ALTER USER SPARK_HORIZON_SVC
    ADD PROGRAMMATIC ACCESS TOKEN SPARK_HORIZON_PAT
    DAYS_TO_EXPIRY = 30
    ROLE_RESTRICTION = 'HORIZON_ICEBERG_ROLE'
    COMMENT = 'PAT for Spark Horizon Iceberg REST Catalog';


/****** Grant Usage on Iceberg Tables to Iceberg Role ******/

USE ROLE SYSADMIN;

GRANT USAGE ON DATABASE SNOWPARK_TRIAL TO ROLE HORIZON_ICEBERG_ROLE;

GRANT USAGE ON SCHEMA SNOWPARK_TRIAL.SNOWPARK_SC TO ROLE HORIZON_ICEBERG_ROLE;

GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE
    ON TABLE SNOWPARK_TRIAL.SNOWPARK_SC.PREPROCESS_H5FILE_STATUS
    TO ROLE HORIZON_ICEBERG_ROLE;