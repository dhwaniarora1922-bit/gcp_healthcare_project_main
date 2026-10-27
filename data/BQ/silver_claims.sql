
-- CLAIMS: corrected SCD Type 2 logic
-- Preserve the existing target table and its history.

CREATE TABLE IF NOT EXISTS
`healthcare-rcm-project-504309.silver_dataset.claims` (
  Claim_Key STRING,
  SRC_ClaimID STRING,
  TransactionID STRING,
  PatientID STRING,
  EncounterID STRING,
  ProviderID STRING,
  DeptID STRING,
  ServiceDate STRING,
  ClaimDate STRING,
  PayorID STRING,
  ClaimAmount STRING,
  PaidAmount STRING,
  ClaimStatus STRING,
  PayorType STRING,
  Deductible STRING,
  Coinsurance STRING,
  Copay STRING,
  SRC_InsertDate STRING,
  SRC_ModifiedDate STRING,
  datasource STRING,
  is_quarantined BOOL,
  inserted_date TIMESTAMP,
  modified_date TIMESTAMP,
  is_current BOOL
);

-- 1. Build the staging table from the combined Bronze claims table.
CREATE OR REPLACE TABLE
`healthcare-rcm-project-504309.silver_dataset.quality_checks_claims` AS
WITH combined AS (
  SELECT
    ClaimID AS SRC_ClaimID,
    TransactionID,
    PatientID,
    EncounterID,
    ProviderID,
    DeptID,
    ServiceDate,
    ClaimDate,
    PayorID,
    ClaimAmount,
    PaidAmount,
    ClaimStatus,
    PayorType,
    Deductible,
    Coinsurance,
    Copay,
    InsertDate AS SRC_InsertDate,
    ModifiedDate AS SRC_ModifiedDate,
    datasource
  FROM `healthcare-rcm-project-504309.bronze_dataset.claims`
),
ranked AS (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY SRC_ClaimID, datasource
      ORDER BY SRC_ModifiedDate DESC, SRC_InsertDate DESC
    ) AS rn
  FROM combined
)
SELECT
  CONCAT(SRC_ClaimID, '-', datasource) AS Claim_Key,
  SRC_ClaimID,
  TransactionID,
  PatientID,
  EncounterID,
  ProviderID,
  DeptID,
  ServiceDate,
  ClaimDate,
  PayorID,
  ClaimAmount,
  PaidAmount,
  ClaimStatus,
  PayorType,
  Deductible,
  Coinsurance,
  Copay,
  SRC_InsertDate,
  SRC_ModifiedDate,
  datasource,
  CASE
    WHEN SRC_ClaimID IS NULL
      OR PatientID IS NULL
      OR TransactionID IS NULL
      OR ClaimStatus IS NULL
      OR LOWER(ClaimStatus) = 'null'
    THEN TRUE
    ELSE FALSE
  END AS is_quarantined
FROM ranked
WHERE rn = 1;

-- 2. Expire the existing current version if any tracked value changed.
MERGE INTO
`healthcare-rcm-project-504309.silver_dataset.claims` AS target
USING
`healthcare-rcm-project-504309.silver_dataset.quality_checks_claims` AS source
ON target.Claim_Key = source.Claim_Key
AND target.is_current = TRUE

WHEN MATCHED AND (
  target.SRC_ClaimID IS DISTINCT FROM source.SRC_ClaimID OR
  target.TransactionID IS DISTINCT FROM source.TransactionID OR
  target.PatientID IS DISTINCT FROM source.PatientID OR
  target.EncounterID IS DISTINCT FROM source.EncounterID OR
  target.ProviderID IS DISTINCT FROM source.ProviderID OR
  target.DeptID IS DISTINCT FROM source.DeptID OR
  target.ServiceDate IS DISTINCT FROM source.ServiceDate OR
  target.ClaimDate IS DISTINCT FROM source.ClaimDate OR
  target.PayorID IS DISTINCT FROM source.PayorID OR
  target.ClaimAmount IS DISTINCT FROM source.ClaimAmount OR
  target.PaidAmount IS DISTINCT FROM source.PaidAmount OR
  target.ClaimStatus IS DISTINCT FROM source.ClaimStatus OR
  target.PayorType IS DISTINCT FROM source.PayorType OR
  target.Deductible IS DISTINCT FROM source.Deductible OR
  target.Coinsurance IS DISTINCT FROM source.Coinsurance OR
  target.Copay IS DISTINCT FROM source.Copay OR
  target.SRC_InsertDate IS DISTINCT FROM source.SRC_InsertDate OR
  target.SRC_ModifiedDate IS DISTINCT FROM source.SRC_ModifiedDate OR
  target.datasource IS DISTINCT FROM source.datasource OR
  target.is_quarantined IS DISTINCT FROM source.is_quarantined
)
THEN UPDATE SET
  target.is_current = FALSE,
  target.modified_date = CURRENT_TIMESTAMP();

-- 3. Insert brand-new claims and new versions of changed claims.
INSERT INTO
`healthcare-rcm-project-504309.silver_dataset.claims` (
  Claim_Key,
  SRC_ClaimID,
  TransactionID,
  PatientID,
  EncounterID,
  ProviderID,
  DeptID,
  ServiceDate,
  ClaimDate,
  PayorID,
  ClaimAmount,
  PaidAmount,
  ClaimStatus,
  PayorType,
  Deductible,
  Coinsurance,
  Copay,
  SRC_InsertDate,
  SRC_ModifiedDate,
  datasource,
  is_quarantined,
  inserted_date,
  modified_date,
  is_current
)
SELECT
  source.Claim_Key,
  source.SRC_ClaimID,
  source.TransactionID,
  source.PatientID,
  source.EncounterID,
  source.ProviderID,
  source.DeptID,
  source.ServiceDate,
  source.ClaimDate,
  source.PayorID,
  source.ClaimAmount,
  source.PaidAmount,
  source.ClaimStatus,
  source.PayorType,
  source.Deductible,
  source.Coinsurance,
  source.Copay,
  source.SRC_InsertDate,
  source.SRC_ModifiedDate,
  source.datasource,
  source.is_quarantined,
  CURRENT_TIMESTAMP(),
  CURRENT_TIMESTAMP(),
  TRUE
FROM
`healthcare-rcm-project-504309.silver_dataset.quality_checks_claims` AS source
WHERE NOT EXISTS (
  SELECT 1
  FROM `healthcare-rcm-project-504309.silver_dataset.claims` AS target
  WHERE target.Claim_Key = source.Claim_Key
    AND target.is_current = TRUE
);

-- 4. Remove the temporary staging table.
DROP TABLE IF EXISTS
`healthcare-rcm-project-504309.silver_dataset.quality_checks_claims`;
