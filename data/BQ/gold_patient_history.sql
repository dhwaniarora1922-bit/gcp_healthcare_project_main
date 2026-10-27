
CREATE OR REPLACE TABLE
  `healthcare-rcm-project-504309.gold_dataset.patient_history` AS

SELECT
    p.Patient_Key,
    p.FirstName,
    p.LastName,
    p.Gender,
    p.DOB,
    p.Address,
    e.EncounterDate,
    e.EncounterType,
    t.Transaction_Key,
    t.VisitDate,
    c.ServiceDate,
    SAFE_CAST(t.Amount AS FLOAT64) AS BilledAmount,
    SAFE_CAST(t.PaidAmount AS FLOAT64) AS PaidAmount,
    c.ClaimStatus,
    SAFE_CAST(c.ClaimAmount AS FLOAT64) AS ClaimAmount,
    SAFE_CAST(c.PaidAmount AS FLOAT64) AS ClaimPaidAmount,
    c.PayorType

FROM
  `healthcare-rcm-project-504309.silver_dataset.patients` AS p

LEFT JOIN
  `healthcare-rcm-project-504309.silver_dataset.transactions` AS t
  ON t.PatientID = p.SRC_PatientID
  AND t.datasource = p.datasource
  AND t.is_current = TRUE
  AND t.is_quarantined = FALSE

LEFT JOIN
  `healthcare-rcm-project-504309.silver_dataset.encounters` AS e
  ON e.SRC_EncounterID = t.EncounterID
  AND e.datasource = t.datasource
  AND e.is_current = TRUE
  AND e.is_quarantined = FALSE

LEFT JOIN
  `healthcare-rcm-project-504309.silver_dataset.claims` AS c
  ON c.TransactionID = t.SRC_TransactionID
  AND c.datasource = t.datasource
  AND c.is_current = TRUE
  AND c.is_quarantined = FALSE

WHERE
  p.is_current = TRUE
  AND p.is_quarantined = FALSE;
