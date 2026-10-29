-- Create or refresh external tables for Bronze dataset
-- Project: healthcare-rcm-project-504309
-- Bucket: bucket-healthcare-04082026

-- =========================
-- HOSPITAL A
-- =========================

CREATE OR REPLACE EXTERNAL TABLE
`healthcare-rcm-project-504309.bronze_dataset.departments_ha`
OPTIONS (
  format = 'JSON',
  uris = ['gs://bucket-healthcare-04082026/landing/Hospital-a/departments/*.json']
);

CREATE OR REPLACE EXTERNAL TABLE
`healthcare-rcm-project-504309.bronze_dataset.encounters_ha`
OPTIONS (
  format = 'JSON',
  uris = ['gs://bucket-healthcare-04082026/landing/Hospital-a/encounters/*.json']
);

CREATE OR REPLACE EXTERNAL TABLE
`healthcare-rcm-project-504309.bronze_dataset.patients_ha`
OPTIONS (
  format = 'JSON',
  uris = ['gs://bucket-healthcare-04082026/landing/Hospital-a/patients/*.json']
);

CREATE OR REPLACE EXTERNAL TABLE
`healthcare-rcm-project-504309.bronze_dataset.providers_ha`
OPTIONS (
  format = 'JSON',
  uris = ['gs://bucket-healthcare-04082026/landing/Hospital-a/providers/*.json']
);

CREATE OR REPLACE EXTERNAL TABLE
`healthcare-rcm-project-504309.bronze_dataset.transactions_ha`
OPTIONS (
  format = 'JSON',
  uris = ['gs://bucket-healthcare-04082026/landing/Hospital-a/transactions/*.json']
);

-- =========================
-- HOSPITAL B
-- =========================

CREATE OR REPLACE EXTERNAL TABLE
`healthcare-rcm-project-504309.bronze_dataset.departments_hb`
OPTIONS (
  format = 'JSON',
  uris = ['gs://bucket-healthcare-04082026/landing/Hospital-b/departments/*.json']
);

CREATE OR REPLACE EXTERNAL TABLE
`healthcare-rcm-project-504309.bronze_dataset.encounters_hb`
OPTIONS (
  format = 'JSON',
  uris = ['gs://bucket-healthcare-04082026/landing/Hospital-b/encounters/*.json']
);

CREATE OR REPLACE EXTERNAL TABLE
`healthcare-rcm-project-504309.bronze_dataset.patients_hb`
OPTIONS (
  format = 'JSON',
  uris = ['gs://bucket-healthcare-04082026/landing/Hospital-b/patients/*.json']
);

CREATE OR REPLACE EXTERNAL TABLE
`healthcare-rcm-project-504309.bronze_dataset.providers_hb`
OPTIONS (
  format = 'JSON',
  uris = ['gs://bucket-healthcare-04082026/landing/Hospital-b/providers/*.json']
);

CREATE OR REPLACE EXTERNAL TABLE
`healthcare-rcm-project-504309.bronze_dataset.transactions_hb`
OPTIONS (
  format = 'JSON',
  uris = ['gs://bucket-healthcare-04082026/landing/Hospital-b/transactions/*.json']
);---------------------------------------------------------------------------------------------------------------------------