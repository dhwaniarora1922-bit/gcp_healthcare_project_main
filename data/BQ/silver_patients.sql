
-- PATIENTS: corrected SCD Type 2 logic
-- Existing target table is preserved.

CREATE TABLE IF NOT EXISTS
`healthcare-rcm-project-504309.silver_dataset.patients` (
  Patient_Key STRING,
  SRC_PatientID STRING,
  FirstName STRING,
  LastName STRING,
  MiddleName STRING,
  SSN STRING,
  PhoneNumber STRING,
  Gender STRING,
  DOB INT64,
  Address STRING,
  SRC_ModifiedDate INT64,
  datasource STRING,
  is_quarantined BOOL,
  inserted_date TIMESTAMP,
  modified_date TIMESTAMP,
  is_current BOOL
);

-- 1. Build a cleaned, source-aware staging table
CREATE OR REPLACE TABLE
`healthcare-rcm-project-504309.silver_dataset.quality_checks_patients` AS
WITH combined AS (
  SELECT
    PatientID AS SRC_PatientID,
    FirstName,
    LastName,
    MiddleName,
    SSN,
    PhoneNumber,
    Gender,
    DOB,
    Address,
    ModifiedDate AS SRC_ModifiedDate,
    'hosa' AS datasource
  FROM `healthcare-rcm-project-504309.bronze_dataset.patients_ha`

  UNION ALL

  SELECT
    ID AS SRC_PatientID,
    F_Name AS FirstName,
    L_Name AS LastName,
    M_Name AS MiddleName,
    SSN,
    PhoneNumber,
    Gender,
    DOB,
    Address,
    ModifiedDate AS SRC_ModifiedDate,
    'hosb' AS datasource
  FROM `healthcare-rcm-project-504309.bronze_dataset.patients_hb`
),
ranked AS (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY SRC_PatientID, datasource
      ORDER BY SRC_ModifiedDate DESC
    ) AS rn
  FROM combined
)
SELECT
  CONCAT(SRC_PatientID, '-', datasource) AS Patient_Key,
  SRC_PatientID,
  FirstName,
  LastName,
  MiddleName,
  SSN,
  PhoneNumber,
  Gender,
  DOB,
  Address,
  SRC_ModifiedDate,
  datasource,
  CASE
    WHEN SRC_PatientID IS NULL
      OR DOB IS NULL
      OR FirstName IS NULL
      OR LOWER(FirstName) = 'null'
    THEN TRUE
    ELSE FALSE
  END AS is_quarantined
FROM ranked
WHERE rn = 1;

-- 2. Expire the previous current version when data changes.
-- IS DISTINCT FROM correctly detects changes involving NULLs.
MERGE INTO
`healthcare-rcm-project-504309.silver_dataset.patients` AS target
USING
`healthcare-rcm-project-504309.silver_dataset.quality_checks_patients` AS source
ON target.Patient_Key = source.Patient_Key
AND target.is_current = TRUE

WHEN MATCHED AND (
  target.SRC_PatientID IS DISTINCT FROM source.SRC_PatientID OR
  target.FirstName IS DISTINCT FROM source.FirstName OR
  target.LastName IS DISTINCT FROM source.LastName OR
  target.MiddleName IS DISTINCT FROM source.MiddleName OR
  target.SSN IS DISTINCT FROM source.SSN OR
  target.PhoneNumber IS DISTINCT FROM source.PhoneNumber OR
  target.Gender IS DISTINCT FROM source.Gender OR
  target.DOB IS DISTINCT FROM source.DOB OR
  target.Address IS DISTINCT FROM source.Address OR
  target.SRC_ModifiedDate IS DISTINCT FROM source.SRC_ModifiedDate OR
  target.datasource IS DISTINCT FROM source.datasource OR
  target.is_quarantined IS DISTINCT FROM source.is_quarantined
)
THEN UPDATE SET
  target.is_current = FALSE,
  target.modified_date = CURRENT_TIMESTAMP();

-- 3. Insert new patients and new versions of changed patients.
INSERT INTO
`healthcare-rcm-project-504309.silver_dataset.patients` (
  Patient_Key,
  SRC_PatientID,
  FirstName,
  LastName,
  MiddleName,
  SSN,
  PhoneNumber,
  Gender,
  DOB,
  Address,
  SRC_ModifiedDate,
  datasource,
  is_quarantined,
  inserted_date,
  modified_date,
  is_current
)
SELECT
  source.Patient_Key,
  source.SRC_PatientID,
  source.FirstName,
  source.LastName,
  source.MiddleName,
  source.SSN,
  source.PhoneNumber,
  source.Gender,
  source.DOB,
  source.Address,
  source.SRC_ModifiedDate,
  source.datasource,
  source.is_quarantined,
  CURRENT_TIMESTAMP(),
  CURRENT_TIMESTAMP(),
  TRUE
FROM `healthcare-rcm-project-504309.silver_dataset.quality_checks_patients` AS source
WHERE NOT EXISTS (
  SELECT 1
  FROM `healthcare-rcm-project-504309.silver_dataset.patients` AS target
  WHERE target.Patient_Key = source.Patient_Key
    AND target.is_current = TRUE
);

-- 4. Remove the temporary staging table
DROP TABLE IF EXISTS
`healthcare-rcm-project-504309.silver_dataset.quality_checks_patients`;
