-- description: create external tables for bronze dataset in BQ
-- do not forget to replace the bucket path
-- for hospital a


CREATE EXTERNAL TABLE IF NOT EXISTS `healthcare-rcm-project-504309.bronze_dataset.departments_ha`
OPTIONS(
   FORMAT = "JSON",
   URIS = ["gs://bucket-healthcare-04082026/landing/Hospital-a/departments/*.json"]
);

CREATE EXTERNAL TABLE IF NOT EXISTS `healthcare-rcm-project-504309.bronze_dataset.encounters_ha`
OPTIONS(
   FORMAT = "JSON",
   URIS = ["gs://bucket-healthcare-04082026/landing/Hospital-a/encounters/*.json"]
);

CREATE EXTERNAL TABLE IF NOT EXISTS `healthcare-rcm-project-504309.bronze_dataset.patients_ha`
OPTIONS(
   FORMAT = "JSON",
   URIS = ["gs://bucket-healthcare-04082026/landing/Hospital-a/patients/*.json"]
);

CREATE EXTERNAL TABLE IF NOT EXISTS `healthcare-rcm-project-504309.bronze_dataset.providers_ha`
OPTIONS(
   FORMAT = "JSON",
   URIS = ["gs://bucket-healthcare-04082026/landing/Hospital-a/providers/*.json"]
);

CREATE EXTERNAL TABLE IF NOT EXISTS `healthcare-rcm-project-504309.bronze_dataset.transactions_ha`
OPTIONS(
   FORMAT = "JSON",
   URIS = ["gs://bucket-healthcare-04082026/landing/Hospital-a/transactions/*.json"]
);

---------------------------------------------------------------------------------------------------------------------------

-- description: create external tables for bronze dataset in BQ
-- do not forget to replace the bucket path
-- for hospital b


CREATE EXTERNAL TABLE IF NOT EXISTS `healthcare-rcm-project-504309.bronze_dataset.departments_hb`
OPTIONS(
   FORMAT = "JSON",
   URIS = ["gs://bucket-healthcare-04082026/landing/Hospital-b/departments/*.json"]
);

CREATE EXTERNAL TABLE IF NOT EXISTS `healthcare-rcm-project-504309.bronze_dataset.encounters_hb`
OPTIONS(
   FORMAT = "JSON",
   URIS = ["gs://bucket-healthcare-04082026/landing/Hospital-b/encounters/*.json"]
);

CREATE EXTERNAL TABLE IF NOT EXISTS `healthcare-rcm-project-504309.bronze_dataset.patients_hb`
OPTIONS(
   FORMAT = "JSON",
   URIS = ["gs://bucket-healthcare-04082026/landing/Hospital-b/patients/*.json"]
);

CREATE EXTERNAL TABLE IF NOT EXISTS `healthcare-rcm-project-504309.bronze_dataset.providers_hb`
OPTIONS(
   FORMAT = "JSON",
   URIS = ["gs://bucket-healthcare-04082026/landing/Hospital-b/providers/*.json"]
);

CREATE EXTERNAL TABLE IF NOT EXISTS `healthcare-rcm-project-504309.bronze_dataset.transactions_hb`
OPTIONS(
   FORMAT = "JSON",
   URIS = ["gs://bucket-healthcare-04082026/landing/Hospital-b/transactions/*.json"]
);
---------------------------------------------------------------------------------------------------------------------------