
-- CPT Codes: Full load into Silver from Bronze

CREATE TABLE IF NOT EXISTS
`healthcare-rcm-project-504309.silver_dataset.cpt_codes` (
    CP_Code_Key STRING,
    procedure_code_category STRING,
    cpt_codes STRING,
    procedure_code_descriptions STRING,
    code_status STRING,
    datasource STRING,
    is_quarantined BOOLEAN,
    inserted_date TIMESTAMP,
    modified_date TIMESTAMP,
    is_current BOOLEAN
);

-- Refresh the existing Silver table
TRUNCATE TABLE
`healthcare-rcm-project-504309.silver_dataset.cpt_codes`;

INSERT INTO
`healthcare-rcm-project-504309.silver_dataset.cpt_codes`
(
    CP_Code_Key,
    procedure_code_category,
    cpt_codes,
    procedure_code_descriptions,
    code_status,
    datasource,
    is_quarantined,
    inserted_date,
    modified_date,
    is_current
)
SELECT
    CONCAT(
        COALESCE(cpt_codes, 'UNKNOWN'),
        '-cpt'
    ) AS CP_Code_Key,
    procedure_code_category,
    cpt_codes,
    procedure_code_descriptions,
    code_status,
    'reference' AS datasource,
    CASE
        WHEN cpt_codes IS NULL
          OR procedure_code_descriptions IS NULL
        THEN TRUE
        ELSE FALSE
    END AS is_quarantined,
    CURRENT_TIMESTAMP() AS inserted_date,
    CURRENT_TIMESTAMP() AS modified_date,
    TRUE AS is_current
FROM `healthcare-rcm-project-504309.bronze_dataset.cpt_codes`;
