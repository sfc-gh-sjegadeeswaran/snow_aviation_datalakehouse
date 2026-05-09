/**************************************************************************************************************************
 Use Case: To demonstrate the preprocessing of semi-structured files (.HDF5) and convert it to Snowflake-compatible format
 Snowpark script to create a Python Stored Procedure to preprocess .HDF5 file and convert it to Parquet in parallel mode
***************************************************************************************************************************/

USE ROLE SECURITYADMIN;
-- This is required to access PyPi repo
GRANT DATABASE ROLE SNOWFLAKE.PYPI_REPOSITORY_USER TO ROLE SYSADMIN;

USE ROLE SYSADMIN;
USE SCHEMA SNOWPARK_TRIAL.SNOWPARK_SC;

/****************** Create a Stored Procedure using Snowpark SP to perform Pre-Processing ****************/

CREATE OR REPLACE PROCEDURE preprocess_h5_sp(src_file_rel_path STRING, file_hash_relpath STRING, src_stage STRING, tgt_stage STRING, file_bsurl STRING)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python', 'h5py', 'numpy', 'pandas', 'pyarrow')
HANDLER = 'main'
AS
$$
import io
import os
import re
import h5py
import numpy as np
import pandas as pd
from datetime import datetime, timezone
from snowflake.snowpark.files import SnowflakeFile
import logging

logger = logging.getLogger("H5_Preprocess_Log_Events")

H5_SIGNAL_TO_COLUMN = {
    'FlightDate': 'flight_date',
    'FlightNumber': 'flight_number',
    'AircraftTail': 'aircraft_tail',
    'AircraftType': 'aircraft_type',
    'ArrivalAirport': 'arrival_airport',
    'DepartureAirport': 'departure_airport',
    'AirTemperature': 'air_temperature',
    'Airspeed': 'airspeed',
    'AttackAngle': 'attack_angle',
    'BaroAltitude': 'baro_altitude',
    'EngineN1_1': 'engine_n1_1',
    'EngineN1_2': 'engine_n1_2',
    'FlightPhase': 'flight_phase',
    'FuelFlowTotal': 'fuel_flow_total',
    'GroundSpeed': 'ground_speed',
    'HeadingMagnetic': 'heading_magnetic',
    'HeadingTrue': 'heading_true',
    'LateralAcceleration': 'lateral_acceleration',
    'Latitude': 'latitude',
    'Longitude': 'longitude',
    'LongitudinalAcceleration': 'longitudinal_acceleration',
    'MachNumber': 'mach_number',
    'PitchAngle': 'pitch_angle',
    'PitchRate': 'pitch_rate',
    'RadioAltitude': 'radio_altitude',
    'RollAngle': 'roll_angle',
    'TotalAirTemperature': 'total_air_temperature',
    'VerticalAcceleration': 'vertical_acceleration',
    'VerticalSpeed': 'vertical_speed',
    'WindDirection': 'wind_direction',
    'WindSpeed': 'wind_speed',
    'YawRate': 'yaw_rate',
}

def extract_metadata(attrs, file_name):
    return {
        'file_name': file_name,
        'flight_number': str(attrs.get('FlightNumber', '')),
        'aircraft_tail': str(attrs.get('AircraftTail', '')),
        'aircraft_type': str(attrs.get('AircraftType', '')),
        'departure_airport': str(attrs.get('DepartureAirport', '')),
        'arrival_airport': str(attrs.get('ArrivalAirport', '')),
        'flight_date': datetime.fromtimestamp(attrs.get('FlightDepartureUTC_epoch', 0), tz=timezone.utc).strftime('%Y-%m-%d'),
        'flight_departure_utc': datetime.fromtimestamp(attrs.get('FlightDepartureUTC_epoch', 0), tz=timezone.utc).replace(tzinfo=None),
        'qar_recorded_data_seconds': int(attrs.get('RecordedDataSeconds', 0))
    }

def extract_signals(data_series, base_frequency):
    h5_df = {}
    for param in data_series.keys():
        value = data_series[param + '/data'][()]
        freq = len(value) / base_frequency

        if not np.issubdtype(value.dtype, np.number):
            decoded = [re.sub(r'[^A-Z0-9-]', '', x.decode('utf-8')) for x in value]
            signal = [max(decoded)] * base_frequency
        elif freq > 1:
            f = int(freq)
            signal = np.mean(value[:f * base_frequency].reshape(-1, f), axis=1)
        elif 0 < freq < 1:
            signal = np.repeat(value, round(1 / freq))[:base_frequency]
        else:
            signal = value[:base_frequency]

        signal_name = H5_SIGNAL_TO_COLUMN.get(param, param)
        h5_df[signal_name] = signal
    return h5_df

def build_output_filename(metadata_dct):
    dep_dt = metadata_dct['flight_departure_utc'].strftime('%Y%m%d%H%M%S')
    parts = [
        metadata_dct['aircraft_tail'],
        metadata_dct['flight_number'],
        metadata_dct['departure_airport'],
        metadata_dct['arrival_airport'],
        dep_dt
    ]
    return '_'.join(parts) + '.parquet'

def write_parquet(session, df, output_filename, tgt_stage):
    local_tgt_dir = '/tmp/output/'
    os.makedirs(local_tgt_dir, exist_ok=True)
    local_path = os.path.join(local_tgt_dir, output_filename)
    df.to_parquet(local_path, index=False)
    output_stage_path = f'@{tgt_stage}/output-files'
    put_result = session.file.put(
        local_file_name=local_path,
        stage_location=output_stage_path,
        auto_compress=False,
        source_compression='NONE',
        overwrite=True
    )
    os.unlink(local_path)
    return str(put_result), f"{output_stage_path}/{output_filename}"

def write_metadata(session, flight_id, metadata_dct):
    current_time = datetime.strftime(datetime.now(), format='%Y-%m-%d %H:%M:%S')
    sql_stmt = """INSERT INTO flight_h5_metadata (flight_id, file_name, flight_number, aircraft_tail, aircraft_type,
                  departure_airport, arrival_airport, flight_date, flight_departure_utc,
                  qar_recorded_data_seconds, created_date, updated_date) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)"""
    session.sql(sql_stmt, params=[
        flight_id,
        metadata_dct['file_name'],
        metadata_dct['flight_number'],
        metadata_dct['aircraft_tail'],
        metadata_dct['aircraft_type'],
        metadata_dct['departure_airport'],
        metadata_dct['arrival_airport'],
        metadata_dct['flight_date'],
        str(metadata_dct['flight_departure_utc']),
        metadata_dct['qar_recorded_data_seconds'],
        current_time,
        current_time
    ]).collect()

def write_status(session, src_file_rel_path, file_name, file_hash_relpath, output_file_path, status, status_msg):
    current_time = datetime.strftime(datetime.now(), format='%Y-%m-%d %H:%M:%S')
    sql_stmt = """INSERT INTO preprocess_h5file_status (file_relative_path, input_file_name, input_file_hash_rel,
                  output_file_path, file_processed_dt, status, status_msg) VALUES (?,?,?,?,?,?,?)"""
    session.sql(sql_stmt, params=[
        src_file_rel_path, file_name, file_hash_relpath,
        output_file_path, current_time, status, status_msg
    ]).collect()

def main(session, src_file_rel_path, file_hash_relpath, src_stage, tgt_stage, file_bsurl):
    src_filename = src_file_rel_path.split('/')[-1]
    src_file_path = f'@{src_stage}/{src_file_rel_path}'
    row_count = 0
    output_file_path = ""
    ret_str = ""

    try:
        flight_id = session.sql("SELECT flight_id_seq.NEXTVAL AS id").collect()[0]['ID']

        with SnowflakeFile.open(file_bsurl, 'rb') as src_file:
            buffer = io.BytesIO(src_file.read())
            buffer.seek(0)
            h5file = h5py.File(buffer, 'r')

            attrs = dict(h5file.attrs)
            metadata_dct = extract_metadata(attrs, src_filename)
            data_series = h5file['qarsignals']
            base_frequency = len(data_series['FlightPhase/data'][()])

            h5_df = extract_signals(data_series, base_frequency)
            h5_df['flight_id'] = [flight_id] * base_frequency
            h5_df['flight_departure_dttm'] = [metadata_dct['flight_departure_utc']] * base_frequency

            h5file.close()

        df = pd.DataFrame(h5_df)
        row_count = df.shape[0]

        output_filename = build_output_filename(metadata_dct)
        put_result, output_file_path = write_parquet(session, df, output_filename, tgt_stage)
        logger.info(f"Parquet written: {output_file_path}")

        write_metadata(session, flight_id, metadata_dct)
        logger.info(f"Metadata written for {src_filename}")

        write_status(session, src_file_rel_path, src_filename, file_hash_relpath, output_file_path, 'Completed', f'Processed {row_count} rows | {put_result}')
        ret_str = f"SUCCESS: {src_filename} -> {output_filename} | Rows: {row_count} | Params: {df.shape[1]}"
        logger.info(ret_str)

    except Exception as e:
        error_msg = str(e)[:500]
        write_status(session, src_file_rel_path, src_filename, file_hash_relpath, output_file_path, 'Failed', error_msg)
        ret_str = f"FAILED: {src_filename} | Error: {error_msg}"
        logger.error(ret_str)

    return ret_str
$$;


/****************** Testing Block ******************/

USE ROLE SYSADMIN;
USE SCHEMA SNOWPARK_TRIAL.SNOWPARK_SC;

CALL preprocess_h5_sp(
    'SNW-02_SNOW-038_SEA_ICN.h5',
    MD5('SNW-02_SNOW-038_SEA_ICN.h5'),
    'SNOW_EXT_STAGE_H5DATA',
    'SNOW_INT_STAGE_H5DATA',
    BUILD_SCOPED_FILE_URL(@SNOW_EXT_STAGE_H5DATA, 'SNW-02_SNOW-038_SEA_ICN.h5')
);

SELECT * FROM preprocess_h5file_status;
SELECT * FROM flight_h5_metadata;


/****************** Bulk Testing Block ******************/

USE ROLE SYSADMIN;
USE SCHEMA SNOWPARK_TRIAL.SNOWPARK_SC;

DECLARE
    src_stage VARCHAR;
    tgt_stage VARCHAR;
    source_relative_path VARCHAR;
    file_hash_relpath VARCHAR;
    ret_val VARCHAR;
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
        SYSTEM$LOG_INFO('Processing submitted for ' || source_relative_path);
    END FOR;
    AWAIT ALL;
    RETURN 'H5 Preprocessing completed using Async';
END;
