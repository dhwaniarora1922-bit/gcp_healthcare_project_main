CREATE OR REPLACE TABLE
  `healthcare-rcm-project-504309.silver_dataset.providers`
(
  ProviderID STRING,
  FirstName STRING,
  LastName STRING,
  Specialization STRING,
  DeptID STRING,
  NPI INT64,
  datasource STRING,
  is_quarantined BOOL
);

INSERT INTO
  `healthcare-rcm-project-504309.silver_dataset.providers`
  (ProviderID, FirstName, LastName, Specialization, DeptID, NPI, datasource, is_quarantined)
WITH source_providers AS (
  SELECT
    ProviderID,
    FirstName,
    LastName,
    Specialization,
    DeptID,
    SAFE_CAST(NPI AS INT64) AS NPI,
    'hosa' AS datasource
  FROM `healthcare-rcm-project-504309.bronze_dataset.providers_ha`

  UNION ALL

  SELECT
    ProviderID,
    FirstName,
    LastName,
    Specialization,
    DeptID,
    SAFE_CAST(NPI AS INT64) AS NPI,
    'hosb' AS datasource
  FROM `healthcare-rcm-project-504309.bronze_dataset.providers_hb`
)
SELECT DISTINCT
  ProviderID,
  FirstName,
  LastName,
  Specialization,
  DeptID,
  NPI,
  datasource,
  ProviderID IS NULL OR DeptID IS NULL AS is_quarantined
FROM source_providers;