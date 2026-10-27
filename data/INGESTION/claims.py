#!/usr/bin/env python
# coding: utf-8

# In[1]:


# load claims data from the landing layer to the bronze dataset

from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, when

# create spark session
spark = SparkSession.builder \
    .appName("Healthcare Claims Ingestion") \
    .getOrCreate()

# All the variables are configured
BUCKET_NAME = "bucket-healthcare-04082026"
CLAIMS_BUCKET_PATH = f"gs://{BUCKET_NAME}/landing/claims/*.csv"
BQ_TABLE = "healthcare-rcm-project-504309.bronze_dataset.claims"
TEMP_GCS_BUCKET = BUCKET_NAME

# Read data from claims source file
claims_df = spark.read.csv(CLAIMS_BUCKET_PATH, header=True)

# adding datasource column in the claims file for future reference

claims_df = (
    claims_df
        .withColumn(
            "datasource",
            when(input_file_name().contains("hospital2"), "hosb")
            .when(input_file_name().contains("hospital1"), "hosa")
            .otherwise("None")
        )
)

# dropping duplicates if any
claims_df = claims_df.dropDuplicates()

claims_df.write \
    .format("bigquery") \
    .option("table", BQ_TABLE) \
    .option("temporaryGcsBucket", TEMP_GCS_BUCKET) \
    .mode("overwrite") \
    .save()

# only to see the data in the notebook
claims_df.show()


# In[ ]:




