
-- TRANSACTIONS: corrected SCD Type 2 logic
-- Preserve the existing target table and its history.

CREATE TABLE IF NOT EXISTS
`healthcare-rcm-project-504309.silver_dataset.transactions` (
  Transaction_Key STRING,
  SRC_TransactionID STRING,
  EncounterID STRING,
  PatientID STRING,
  ProviderID STRING,
  DeptID STRING,
  VisitDate INT64,
  ServiceDate INT64,
  PaidDate INT64,
  VisitType STRING,
  Amount FLOAT64,
  AmountType STRING,
  PaidAmount FLOAT64,
  ClaimID STRING,
  PayorID STRING,
  ProcedureCode INT64,
  ICDCode STRING,
  LineOfBusiness STRING,
  MedicaidID STRING,
  MedicareID STRING,
  SRC_InsertedDate INT64,
  SRC_ModifiedDate INT64,
  datasource STRING,
  is_quarantined BOOL,
  inserted_date TIMESTAMP,
  modified_date TIMESTAMP,
  is_current BOOL
);

-- 1. Combine Hospital A and Hospital B into a staging table.
CREATE OR REPLACE TABLE
`healthcare-rcm-project-504309.silver_dataset.quality_checks_transactions` AS
WITH combined AS (
  SELECT *, 'hosa' AS datasource
  FROM `healthcare-rcm-project-504309.bronze_dataset.transactions_ha`

  UNION ALL

  SELECT *, 'hosb' AS datasource
  FROM `healthcare-rcm-project-504309.bronze_dataset.transactions_hb`
),
ranked AS (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY TransactionID, datasource
      ORDER BY ModifiedDate DESC, InsertDate DESC
    ) AS rn
  FROM combined
)
SELECT
  CONCAT(TransactionID, '-', datasource) AS Transaction_Key,
  TransactionID AS SRC_TransactionID,
  EncounterID,
  PatientID,
  ProviderID,
  DeptID,
  VisitDate,
  ServiceDate,
  PaidDate,
  VisitType,
  Amount,
  AmountType,
  PaidAmount,
  ClaimID,
  PayorID,
  ProcedureCode,
  ICDCode,
  LineOfBusiness,
  MedicaidID,
  MedicareID,
  InsertDate AS SRC_InsertedDate,
  ModifiedDate AS SRC_ModifiedDate,
  datasource,
  CASE
    WHEN TransactionID IS NULL
      OR EncounterID IS NULL
      OR PatientID IS NULL
      OR VisitDate IS NULL
    THEN TRUE
    ELSE FALSE
  END AS is_quarantined
FROM ranked
WHERE rn = 1;

-- 2. Expire the old current version when a record changes.
MERGE INTO
`healthcare-rcm-project-504309.silver_dataset.transactions` AS target
USING
`healthcare-rcm-project-504309.silver_dataset.quality_checks_transactions` AS source
ON target.Transaction_Key = source.Transaction_Key
AND target.is_current = TRUE

WHEN MATCHED AND (
  target.SRC_TransactionID IS DISTINCT FROM source.SRC_TransactionID OR
  target.EncounterID IS DISTINCT FROM source.EncounterID OR
  target.PatientID IS DISTINCT FROM source.PatientID OR
  target.ProviderID IS DISTINCT FROM source.ProviderID OR
  target.DeptID IS DISTINCT FROM source.DeptID OR
  target.VisitDate IS DISTINCT FROM source.VisitDate OR
  target.ServiceDate IS DISTINCT FROM source.ServiceDate OR
  target.PaidDate IS DISTINCT FROM source.PaidDate OR
  target.VisitType IS DISTINCT FROM source.VisitType OR
  target.Amount IS DISTINCT FROM source.Amount OR
  target.AmountType IS DISTINCT FROM source.AmountType OR
  target.PaidAmount IS DISTINCT FROM source.PaidAmount OR
  target.ClaimID IS DISTINCT FROM source.ClaimID OR
  target.PayorID IS DISTINCT FROM source.PayorID OR
  target.ProcedureCode IS DISTINCT FROM source.ProcedureCode OR
  target.ICDCode IS DISTINCT FROM source.ICDCode OR
  target.LineOfBusiness IS DISTINCT FROM source.LineOfBusiness OR
  target.MedicaidID IS DISTINCT FROM source.MedicaidID OR
  target.MedicareID IS DISTINCT FROM source.MedicareID OR
  target.SRC_InsertedDate IS DISTINCT FROM source.SRC_InsertedDate OR
  target.SRC_ModifiedDate IS DISTINCT FROM source.SRC_ModifiedDate OR
  target.datasource IS DISTINCT FROM source.datasource OR
  target.is_quarantined IS DISTINCT FROM source.is_quarantined
)
THEN UPDATE SET
  target.is_current = FALSE,
  target.modified_date = CURRENT_TIMESTAMP();

-- 3. Insert brand-new transactions and new versions of changed ones.
INSERT INTO
`healthcare-rcm-project-504309.silver_dataset.transactions` (
  Transaction_Key,
  SRC_TransactionID,
  EncounterID,
  PatientID,
  ProviderID,
  DeptID,
  VisitDate,
  ServiceDate,
  PaidDate,
  VisitType,
  Amount,
  AmountType,
  PaidAmount,
  ClaimID,
  PayorID,
  ProcedureCode,
  ICDCode,
  LineOfBusiness,
  MedicaidID,
  MedicareID,
  SRC_InsertedDate,
  SRC_ModifiedDate,
  datasource,
  is_quarantined,
  inserted_date,
  modified_date,
  is_current
)
SELECT
  source.Transaction_Key,
  source.SRC_TransactionID,
  source.EncounterID,
  source.PatientID,
  source.ProviderID,
  source.DeptID,
  source.VisitDate,
  source.ServiceDate,
  source.PaidDate,
  source.VisitType,
  source.Amount,
  source.AmountType,
  source.PaidAmount,
  source.ClaimID,
  source.PayorID,
  source.ProcedureCode,
  source.ICDCode,
  source.LineOfBusiness,
  source.MedicaidID,
  source.MedicareID,
  source.SRC_InsertedDate,
  source.SRC_ModifiedDate,
  source.datasource,
  source.is_quarantined,
  CURRENT_TIMESTAMP(),
  CURRENT_TIMESTAMP(),
  TRUE
FROM
`healthcare-rcm-project-504309.silver_dataset.quality_checks_transactions` AS source
WHERE NOT EXISTS (
  SELECT 1
  FROM `healthcare-rcm-project-504309.silver_dataset.transactions` AS target
  WHERE target.Transaction_Key = source.Transaction_Key
    AND target.is_current = TRUE
);

-- 4. Remove the temporary staging table.
DROP TABLE IF EXISTS
`healthcare-rcm-project-504309.silver_dataset.quality_checks_transactions`;
