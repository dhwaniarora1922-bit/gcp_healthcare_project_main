
-- ENCOUNTERS: corrected SCD Type 2 logic
-- Preserve the existing target table and history.

CREATE TABLE IF NOT EXISTS
`healthcare-rcm-project-504309.silver_dataset.encounters` (
  Encounter_Key STRING,
  SRC_EncounterID STRING,
  PatientID STRING,
  ProviderID STRING,
  DepartmentID STRING,
  EncounterDate INT64,
  EncounterType STRING,
  ProcedureCode INT64,
  SRC_ModifiedDate INT64,
  datasource STRING,
  is_quarantined BOOL,
  inserted_date TIMESTAMP,
  modified_date TIMESTAMP,
  is_current BOOL
);

-- 1. Combine both hospitals into a staging table.
CREATE OR REPLACE TABLE
`healthcare-rcm-project-504309.silver_dataset.quality_checks_encounters` AS
WITH combined AS (
  SELECT
    EncounterID AS SRC_EncounterID,
    PatientID,
    ProviderID,
    DepartmentID,
    EncounterDate,
    EncounterType,
    ProcedureCode,
    ModifiedDate AS SRC_ModifiedDate,
    'hosa' AS datasource
  FROM `healthcare-rcm-project-504309.bronze_dataset.encounters_ha`

  UNION ALL

  SELECT
    EncounterID AS SRC_EncounterID,
    PatientID,
    ProviderID,
    DepartmentID,
    EncounterDate,
    EncounterType,
    ProcedureCode,
    ModifiedDate AS SRC_ModifiedDate,
    'hosb' AS datasource
  FROM `healthcare-rcm-project-504309.bronze_dataset.encounters_hb`
),
ranked AS (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY SRC_EncounterID, datasource
      ORDER BY SRC_ModifiedDate DESC
    ) AS rn
  FROM combined
)
SELECT
  CONCAT(SRC_EncounterID, '-', datasource) AS Encounter_Key,
  SRC_EncounterID,
  PatientID,
  ProviderID,
  DepartmentID,
  EncounterDate,
  EncounterType,
  ProcedureCode,
  SRC_ModifiedDate,
  datasource,
  CASE
    WHEN SRC_EncounterID IS NULL
      OR PatientID IS NULL
      OR EncounterDate IS NULL
      OR EncounterType IS NULL
      OR LOWER(EncounterType) = 'null'
    THEN TRUE
    ELSE FALSE
  END AS is_quarantined
FROM ranked
WHERE rn = 1;

-- 2. Expire the old current version when a record changes.
MERGE INTO
`healthcare-rcm-project-504309.silver_dataset.encounters` AS target
USING
`healthcare-rcm-project-504309.silver_dataset.quality_checks_encounters` AS source
ON target.Encounter_Key = source.Encounter_Key
AND target.is_current = TRUE

WHEN MATCHED AND (
  target.SRC_EncounterID IS DISTINCT FROM source.SRC_EncounterID OR
  target.PatientID IS DISTINCT FROM source.PatientID OR
  target.ProviderID IS DISTINCT FROM source.ProviderID OR
  target.DepartmentID IS DISTINCT FROM source.DepartmentID OR
  target.EncounterDate IS DISTINCT FROM source.EncounterDate OR
  target.EncounterType IS DISTINCT FROM source.EncounterType OR
  target.ProcedureCode IS DISTINCT FROM source.ProcedureCode OR
  target.SRC_ModifiedDate IS DISTINCT FROM source.SRC_ModifiedDate OR
  target.datasource IS DISTINCT FROM source.datasource OR
  target.is_quarantined IS DISTINCT FROM source.is_quarantined
)
THEN UPDATE SET
  target.is_current = FALSE,
  target.modified_date = CURRENT_TIMESTAMP();

-- 3. Insert new encounters and new versions of changed encounters.
INSERT INTO
`healthcare-rcm-project-504309.silver_dataset.encounters` (
  Encounter_Key,
  SRC_EncounterID,
  PatientID,
  ProviderID,
  DepartmentID,
  EncounterDate,
  EncounterType,
  ProcedureCode,
  SRC_ModifiedDate,
  datasource,
  is_quarantined,
  inserted_date,
  modified_date,
  is_current
)
SELECT
  source.Encounter_Key,
  source.SRC_EncounterID,
  source.PatientID,
  source.ProviderID,
  source.DepartmentID,
  source.EncounterDate,
  source.EncounterType,
  source.ProcedureCode,
  source.SRC_ModifiedDate,
  source.datasource,
  source.is_quarantined,
  CURRENT_TIMESTAMP(),
  CURRENT_TIMESTAMP(),
  TRUE
FROM
`healthcare-rcm-project-504309.silver_dataset.quality_checks_encounters` AS source
WHERE NOT EXISTS (
  SELECT 1
  FROM `healthcare-rcm-project-504309.silver_dataset.encounters` AS target
  WHERE target.Encounter_Key = source.Encounter_Key
    AND target.is_current = TRUE
);

-- 4. Remove the staging table.
DROP TABLE IF EXISTS
`healthcare-rcm-project-504309.silver_dataset.quality_checks_encounters`;
