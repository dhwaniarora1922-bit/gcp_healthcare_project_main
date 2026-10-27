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

##-----------------------------------------------------------------------------------------------------------------##

# BQ configuration
BQ_PROJECT = "healthcare-rcm-project-504309"
BQ_AUDIT_TABLE = f"{BQ_PROJECT}.temp_dataset.audit_log"
BQ_LOG_TABLE = f"{BQ_PROJECT}.temp_dataset.pipeline_logs"
BQ_TEMP_PATH = f"{GCS_BUCKET}/temp/"

##-----------------------------------------------------------------------------------------------------------------##
#MySql configuration
MYSQL_CONFIG = {
    "url" : "jdbc:mysql://34.133.28.245:3306/hospital-a-mysql-db?sslMode=REQUIRED&allowPublicKeyRetrieval=true",
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

    blobs = list(
        storage_client.bucket(GCS_BUCKET).list_blobs(
            prefix=f"landing/{HOSPITAL_NAME}/{table}/"
        )
    )

    existing_files = [
        blob.name for blob in blobs if blob.name.endswith(".json")
    ]

    if not existing_files:
        log_event("INFO", f"no existing files for table {table}")
        return

    for file in existing_files:
        source_blob = storage_client.bucket(GCS_BUCKET).blob(file)

        #extract date from file name
        date_part = file.split("_")[-1].split(".")[0]
        year, month, day = date_part[-4:], date_part[2:4], date_part[:2]

        #move to archive 
        archive_path = f"landing/{HOSPITAL_NAME}/archive/{table}/{year}/{month}/{day}/{file.split('/')[-1]}"
        destination_blob = storage_client.bucket(GCS_BUCKET).blob(archive_path)

        # copy file to archive and delete the original
        storage_client.bucket(GCS_BUCKET).copy_blob(
            source_blob,
            storage_client.bucket(GCS_BUCKET),
            destination_blob.name
        )

        source_blob.delete()

        log_event(
            "INFO",
            f"moved {file} to {archive_path}",
            table=table
        )


##-----------------------------------------------------------------------------------------------------------------##

# function to get the latest watermark from BQ audit table
def get_latest_watermark(table_name):

    query = f"""
        SELECT MAX (load_timestamp) AS latest_timestamp
        FROM `{BQ_AUDIT_TABLE}`
        WHERE tablename = '{table_name}' and data_source = "hospital-a-mysql-db"
    """

    query_job = bq_client.query(query)
    result = query_job.result()

    for row in result:
        return row.latest_timestamp if row.latest_timestamp else "1900-01-01 00:00:00"

    return "1900-01-01 00:00:00"


##-----------------------------------------------------------------------------------------------------------------##

# function to extract data from MySQL and save to GCS
def extract_and_save_to_landing(table, load_type, watermark_col):

    try:

        last_watermark = (
            get_latest_watermark(table)
            if load_type.lower() == "incremental"
            else None
        )

        log_event(
            "INFO",
            f" latest watermark for {table}: {last_watermark}",
            table=table
        )

        query = (
            f"(SELECT * FROM `{table}`) AS t"
            if load_type.lower() == "full"
            else
            f"(Select * From {table} WHERE {watermark_col} > '{last_watermark}') AS t"
        )

        df = (
            spark.read.format("jdbc")
            .option("url", MYSQL_CONFIG["url"])
            .option("user", MYSQL_CONFIG["user"])
            .option("password", MYSQL_CONFIG["password"])
            .option("driver", MYSQL_CONFIG["driver"])
            .option("dbtable", query)
            .load()
        )

        log_event(
            "SUCCESS",
            f"Successfully extracted data from {table}",
            table=table
        )

        today = datetime.datetime.today().strftime('%d%m%Y')
        JSON_FILE_PATH = f"landing/{HOSPITAL_NAME}/{table}/{table}_{today}.json"

        bucket = storage_client.bucket(GCS_BUCKET)
        blob = bucket.blob(JSON_FILE_PATH)

        blob.upload_from_string(
            df.toPandas().to_json(
                orient="records",
                lines=True
            ),
            content_type="application/json"
        )

        log_event(
            "SUCCESS",
            f" JSON file successfully written to gs://{GCS_BUCKET}/{JSON_FILE_PATH}",
            table=table
        )

        #insert audit entry 
        audit_df = spark.createDataFrame(
            [
                (
                    "hospital-a-mysql-db",
                    table,
                    load_type,
                    df.count(),
                    datetime.datetime.now(),
                    "SUCCESS"
                )
            ],
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

        log_event(
            "SUCCESS",
            f" Audit log updated for {table}",
            table=table
        )

    except Exception as e:

        log_event(
            "Error",
            f"Error processing {table}: {str(e)}",
            table=table
        )


##-----------------------------------------------------------------------------------------------------------------##

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

        move_existing_files_to_archive(table)

        extract_and_save_to_landing(
            table,
            load_type,
            watermark
        )

save_logs_to_gcs()
save_logs_to_bigquery()


# In[ ]:




