#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# loading cpt codes file data from landing to bronze dataset 
from pyspark.sql import SparkSession

# creating a spark session
spark = SparkSession.builder \
                    .appName("CPT Codes Ingestion") \
                    .getOrCreate()

# configured the variables 
BUCKET_NAME = "bucket-healthcare-04082026"
CPT_BUCKET_PATH = f"gs://{BUCKET_NAME}/landing/cptcodes/*.csv" 
BQ_TABLE = "healthcare-rcm-project-504309.bronze_dataset.cpt_codes"
TEMP_GCS_BUCKET = f"{BUCKET_NAME}/temp/"                    

# read data from cpt codes file
cptcodes_df = spark.read.csv(CPT_BUCKET_PATH, header=True)

# replacing the column name spaces with underscore
for col in cptcodes_df.columns:
    new_col = col.replace(" ", "_").lower()
    cptcodes_df = cptcodes_df.withColumnRenamed(col, new_col) 
    
# now writing this to BQ
cptcodes_df.write \
        .format("bigquery") \
        .option("table", BQ_TABLE) \
        .option("temporaryGcsBucket", TEMP_GCS_BUCKET) \
        .mode("overwrite") \
        .save()

