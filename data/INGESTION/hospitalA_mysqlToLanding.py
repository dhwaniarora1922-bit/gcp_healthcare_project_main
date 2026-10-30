#!/usr/bin/env python
# coding: utf-8

# In[2]:


# Hospital A ingestion pipeline code 

from google.cloud import storage, bigquery
import pandas as pd
from pyspark.sql import SparkSession
import datetime
import json

##-----------------------------------------------------------------------------------------------------------------##
# intialize GCS and BQ clients
storage_client = storage.Client()
bq_client = bigquery.Client()

##-----------------------------------------------------------------------------------------------------------------##

#initialize spark session
spark = SparkSession.builder.appName("HospitalAdbToLanding").getOrCreate()

##-----------------------------------------------------------------------------------------------------------------##

# Google Cloud Storage (GCS) Configuration
GCS_BUCKET = "bucket-healthcare-04082026"
HOSPITAL_NAME = "Hospital-a"
LANDING_PATH = f"gs://{GCS_BUCKET}/landing/{HOSPITAL_NAME}/"
ARCHIVE_PATH = f"gs://{GCS_BUCKET}/landing/{HOSPITAL_NAME}/archive/"
CONFIG_FILE_PATH = f"gs://{GCS_BUCKET}/configs/load_config.csv"
# ONE-TIME RECOVERY MODE
RECOVERY_FULL_LOAD = False

##-----------------------------------------------------------------------------------------------------------------##

# BQ configuration
BQ_PROJECT = "healthcare-rcm-project-504309"
BQ_AUDIT_TABLE = f"{BQ_PROJECT}.temp_dataset.audit_log"
BQ_LOG_TABLE = f"{BQ_PROJECT}.temp_dataset.pipeline_logs"
BQ_TEMP_PATH = f"{GCS_BUCKET}/temp/"

##-----------------------------------------------------------------------------------------------------------------##
#MySql configuration
MYSQL_CONFIG = {
    "url": "jdbc:mysql://34.133.28.245:3306/hospital-a-mysql-db?sslMode=REQUIRED&allowPublicKeyRetrieval=true&zeroDateTimeBehavior=CONVERT_TO_NULL",
    "driver" : "com.mysql.cj.jdbc.Driver",
    "user" : "myuser",
    "password" : "Mypass@22"
}

##-----------------------------------------------------------------------------------------------------------------##

# Logging mechanism
log_entries = [] #Stores logs before writing to GCS

def log_event(event_type, message, table=None):
    """log an event and store it in the log list"""
    log_entry = {
        "timestamp" : datetime.datetime.now().isoformat(),
        "event_type" : event_type,
        "message" : message,
        "table" : table
    }
    log_entries.append(log_entry)
    print(f"[{log_entry['timestamp']}] {event_type} - {message}") #print for visibility  


def save_logs_to_gcs():
    """Save logs to a JSON file and upload to GCS"""
    log_filename = f"pipeline_log_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}.json"
    log_filepath = f"temp/pipeline_logs/{log_filename}"

    json_data = json.dumps(log_entries, indent=4)

    # Get GCS bucket
    bucket = storage_client.bucket(GCS_BUCKET)
    blob = bucket.blob(log_filepath)

    # upload JSON data as a file
    blob.upload_from_string(json_data, content_type="application/json")

    print(f" logs Successfully saved to GCS at gs://{GCS_BUCKET}/{log_filepath}")


def save_logs_to_bigquery():
    """Save logs to Bigquery"""
    if log_entries:
        from pyspark.sql.types import StructType, StructField, StringType

        log_schema = StructType([
            StructField("timestamp", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("message", StringType(), True),
            StructField("table", StringType(), True)
        ])

        log_df = spark.createDataFrame(log_entries, schema=log_schema)

        log_df.write.format("bigquery")\
            .option("table", BQ_LOG_TABLE)\
            .option("temporaryGcsBucket", BQ_TEMP_PATH)\
            .mode("append")\
            .save()

        print("Logs stored in BigQuery for future analysis")


##-----------------------------------------------------------------------------------------------------------------##

# function to move existing files to archive 
def move_existing_files_to_archive(table):
    """Archive only direct-child JSON files in the table's active landing folder."""
    prefix = f"landing/Hospital-a/{table}/"
    blobs = list(storage_client.bucket(GCS_BUCKET).list_blobs(prefix=prefix))

    # Do not accidentally re-archive nested folders or anything under archive/.
    existing_files = [
        blob.name for blob in blobs
        if blob.name.endswith(".json")
        and "/" not in blob.name[len(prefix):]
    ]

    if not existing_files:
        log_event("INFO", f"no existing active files for table {table}", table=table)
        return

    bucket = storage_client.bucket(GCS_BUCKET)
    for file in existing_files:
        source_blob = bucket.blob(file)
        filename = file.split("/")[-1]
        date_part = filename.rsplit("_", 1)[-1].rsplit(".", 1)[0]

        try:
            file_date = datetime.datetime.strptime(date_part, "%d%m%Y")
        except ValueError:
            # Handle legacy/manual files such as *_fixed.json without guessing a date.
            file_date = datetime.datetime.now()
            log_event(
                "WARNING",
                f"Filename {filename} has no DDMMYYYY suffix; archiving under today's date",
                table=table
            )

        archive_path = (
            f"landing/Hospital-a/archive/{table}/"
            f"{file_date.strftime('%Y')}/{file_date.strftime('%m')}/"
            f"{file_date.strftime('%d')}/{filename}"
        )
        bucket.copy_blob(source_blob, bucket, archive_path)
        source_blob.delete()
        log_event("INFO", f"moved {file} to {archive_path}", table=table)


##-----------------------------------------------------------------------------------------------------------------##

# function to get the latest watermark from BQ audit table
def get_latest_watermark(table_name):
    """Read the last successfully processed source watermark from GCS."""
    watermark_path = f"configs/watermarks/Hospital-a/{table_name}.txt"
    blob = storage_client.bucket(GCS_BUCKET).blob(watermark_path)
    if not blob.exists():
        return "1900-01-01 00:00:00"
    value = blob.download_as_text().strip()
    return value if value else "1900-01-01 00:00:00"


def save_latest_watermark(table_name, watermark_value):
    """Persist the source watermark after landing and audit writes succeed."""
    if watermark_value is None:
        return
    watermark_path = f"configs/watermarks/Hospital-a/{table_name}.txt"
    storage_client.bucket(GCS_BUCKET).blob(watermark_path).upload_from_string(
        str(watermark_value), content_type="text/plain"
    )


##-----------------------------------------------------------------------------------------------------------------##


##-----------------------------------------------------------------------------------------------------------------##

# function to extract data from MySQL and save to GCS
def extract_and_save_to_landing(table, load_type, watermark_col):
    """Extract, validate, stage and publish MySQL data to GCS."""

    try:
        # ONE-TIME RECOVERY:
        # True = read all rows, ignoring the saved incremental watermark.
        # This allows previously skipped incremental records to be recovered.
        RECOVERY_FULL_LOAD = True

        is_incremental = load_type.lower() == "incremental"
        last_watermark = get_latest_watermark(table) if is_incremental else None

        log_event(
            "INFO",
            f"Recovery mode enabled; previous watermark for {table}: {last_watermark}",
            table=table
        )

        # Build the source query.
        # During recovery, read every row from the source table.
        if is_incremental and not RECOVERY_FULL_LOAD:
            if not watermark_col:
                raise ValueError(f"Missing watermark column for {table}")

            query = (
                f"(SELECT * FROM `{table}` "
                f"WHERE `{watermark_col}` > '{last_watermark}') AS t"
            )
        else:
            query = f"(SELECT * FROM `{table}`) AS t"

        # Read data from MySQL using the existing Spark JDBC configuration.
        df = (
            spark.read.format("jdbc")
            .option("url", MYSQL_CONFIG["url"])
            .option("user", MYSQL_CONFIG["user"])
            .option("password", MYSQL_CONFIG["password"])
            .option("driver", MYSQL_CONFIG["driver"])
            .option("dbtable", query)
            .load()
        )

        row_count = df.count()

        log_event(
            "INFO",
            f"Extracted {row_count} rows from MySQL for {table}",
            table=table
        )

        # IMPORTANT:
        # Never archive existing files or publish JSON when no rows were extracted.
        if row_count == 0:
            log_event(
                "WARNING",
                f"No rows extracted for {table}; landing files and watermark unchanged",
                table=table
            )
            return

        # Calculate the new source watermark without using the audit timestamp.
        new_watermark = None

        if is_incremental:
            if not watermark_col or watermark_col not in df.columns:
                raise ValueError(
                    f"Configured watermark column '{watermark_col}' "
                    f"missing from '{table}'"
                )

            from pyspark.sql import functions as F

            watermark_value = (
                df.agg(F.max(F.col(watermark_col)).alias("max_watermark"))
                .first()["max_watermark"]
            )

            if watermark_value is not None:
                new_watermark = str(watermark_value)

        # Convert to JSON and reject empty output before changing landing files.
        json_payload = df.toPandas().to_json(
            orient="records",
            lines=True,
            date_format="iso"
        )

        if not json_payload or not json_payload.strip():
            raise ValueError(
                f"Empty JSON payload rejected for table {table}"
            )

        # Create the date-based active landing path: YYYY/MM/DD.
        now = datetime.datetime.now()
        date_folder = now.strftime("%Y/%m/%d")
        date_filename = now.strftime("%d%m%Y")

        json_file_path = (
            f"landing/Hospital-a/{table}/{date_folder}/"
            f"{table}_{date_filename}.json"
        )

        # Stage the JSON before touching existing active files.
        staging_path = (
            f"temp/ingestion_staging/Hospital-a/{table}_"
            f"{now.strftime('%Y%m%d%H%M%S%f')}.json"
        )

        bucket = storage_client.bucket(GCS_BUCKET)
        staging_blob = bucket.blob(staging_path)

        staging_blob.upload_from_string(
            json_payload,
            content_type="application/json"
        )

        try:
            # Archive existing direct-child active files only after
            # a non-empty replacement has been staged successfully.
            move_existing_files_to_archive(table)

            # Publish the new JSON to the date-based landing folder.
            bucket.copy_blob(
                staging_blob,
                bucket,
                json_file_path
            )

        finally:
            # Remove the temporary staging object.
            if staging_blob.exists():
                staging_blob.delete()

        log_event(
            "SUCCESS",
            f"Published {row_count} rows to gs://{GCS_BUCKET}/{json_file_path}",
            table=table
        )

        # Write the audit record using the existing audit table schema.
        audit_df = spark.createDataFrame(
            [(
                "hospital-a-mysql-db",
                table,
                load_type,
                row_count,
                datetime.datetime.now(),
                "SUCCESS"
            )],
            [
                "data_source",
                "tablename",
                "load_type",
                "record_count",
                "load_timestamp",
                "status"
            ]
        )

        (
            audit_df.write.format("bigquery")
            .option("table", BQ_AUDIT_TABLE)
            .option("temporaryGcsBucket", GCS_BUCKET)
            .mode("append")
            .save()
        )

        # Save the source watermark only after publishing and audit succeed.
        if is_incremental and new_watermark is not None:
            save_latest_watermark(table, new_watermark)

        log_event(
            "SUCCESS",
            f"Audit log and watermark updated for {table}",
            table=table
        )

    except Exception as e:
        log_event(
            "ERROR",
            f"Error processing {table}: {str(e)}",
            table=table
        )
        raise


##-----------------------------------------------------------------------------------------------------------------##



##

# Function to read config file from GCS
def read_config_file():

    df = spark.read.csv(
        CONFIG_FILE_PATH,
        header=True
    )

    log_event("INFO", " Successfully read the config file")

    return df


##-----------------------------------------------------------------------------------------------------------------##

# reading config file 
config_df = read_config_file()

for row in config_df.collect():

    if row["is_active"] == '1' and row["datasource"] == 'hospital_a_db':

        db, src, table, load_type, watermark, _, targetpath = row

        # Archiving is handled inside extract_and_save_to_landing only after
        # a non-empty extract has been safely staged. Do not archive here.
        extract_and_save_to_landing(
            table,
            load_type,
            watermark
        )

save_logs_to_gcs()
save_logs_to_bigquery()


# In[ ]:




