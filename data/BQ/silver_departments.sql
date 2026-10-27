
-- 1. Create the departments table
CREATE TABLE IF NOT EXISTS
  `healthcare-rcm-project-504309.silver_dataset.departments` (
    Dept_Id STRING,
    SRC_Dept_Id STRING,
    Name STRING,
    datasource STRING,
    is_quarantined BOOL
  );

-- 2. Full refresh
TRUNCATE TABLE
  `healthcare-rcm-project-504309.silver_dataset.departments`;

-- 3. Load departments from both hospitals
INSERT INTO
  `healthcare-rcm-project-504309.silver_dataset.departments`
  (Dept_Id, SRC_Dept_Id, Name, datasource, is_quarantined)
WITH source_departments AS (
  SELECT
    CAST(DeptID AS STRING) AS SRC_Dept_Id,
    CAST(Name AS STRING) AS Name,
    'hosa' AS datasource
  FROM `healthcare-rcm-project-504309.bronze_dataset.departments_ha`

  UNION ALL

  SELECT
    CAST(DeptID AS STRING) AS SRC_Dept_Id,
    CAST(Name AS STRING) AS Name,
    'hosb' AS datasource
  FROM `healthcare-rcm-project-504309.bronze_dataset.departments_hb`
)
SELECT DISTINCT
  CONCAT(SRC_Dept_Id, '-', datasource) AS Dept_Id,
  SRC_Dept_Id,
  Name,
  datasource,
  SRC_Dept_Id IS NULL OR Name IS NULL AS is_quarantined
FROM source_departments;
