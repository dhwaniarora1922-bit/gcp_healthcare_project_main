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
  uris = ['gs://bucket-healthcare-04082026/landing/Hospital-a/departments/2026/*.json']
);

CREATE OR REPLACE EXTERNAL TABLE
`healthcare-rcm-project-504309.bronze_dataset.encounters_ha`
OPTIONS (
  format = 'JSON',
uris = ['gs://bucket-healthcare-04082026/landing/Hospital-a/encounters/2026/*.json']
);

CREATE OR REPLACE EXTERNAL TABLE
`healthcare-rcm-project-504309.bronze_dataset.patients_ha`
OPTIONS (
  format = 'JSON',
  uris = ['gs://bucket-healthcare-04082026/landing/Hospital-a/patients/2026/*.json']
);

CREATE OR REPLACE EXTERNAL TABLE
`healthcare-rcm-project-504309.bronze_dataset.providers_ha`
OPTIONS (
  format = 'JSON',
  uris = ['gs://bucket-healthcare-04082026/landing/Hospital-a/providers/2026/*.json']
);

CREATE OR REPLACE EXTERNAL TABLE
`healthcare-rcm-project-504309.bronze_dataset.transactions_ha`
OPTIONS (
  format = 'JSON',
  uris = ['gs://bucket-healthcare-04082026/landing/Hospital-a/transactions/2026/*.json']
);

-- =========================
-- HOSPITAL B
-- =========================

CREATE OR REPLACE EXTERNAL TABLE
`healthcare-rcm-project-504309.bronze_dataset.departments_hb`
OPTIONS (
  format = 'JSON',
  uris = ['gs://bucket-healthcare-04082026/landing/Hospital-b/departments/2026/*.json']
);

CREATE OR REPLACE EXTERNAL TABLE
`healthcare-rcm-project-504309.bronze_dataset.encounters_hb`
OPTIONS (
  format = 'JSON',
  uris = ['gs://bucket-healthcare-04082026/landing/Hospital-b/encounters/2026/*.json']
);

CREATE OR REPLACE EXTERNAL TABLE
`healthcare-rcm-project-504309.bronze_dataset.patients_hb`
OPTIONS (
  format = 'JSON',
  uris = ['gs://bucket-healthcare-04082026/landing/Hospital-b/patients/2026/*.json']
);

CREATE OR REPLACE EXTERNAL TABLE
`healthcare-rcm-project-504309.bronze_dataset.providers_hb`
OPTIONS (
  format = 'JSON',
  uris = ['gs://bucket-healthcare-04082026/landing/Hospital-b/providers/2026/*.json']
);

CREATE OR REPLACE EXTERNAL TABLE
`healthcare-rcm-project-504309.bronze_dataset.transactions_hb`
OPTIONS (
  format = 'JSON',
  uris = ['gs://bucket-healthcare-04082026/landing/Hospital-b/transactions/2026/*.json']
);---------------------------------------------------------------------------------------------------------------------------