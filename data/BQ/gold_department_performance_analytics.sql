-- 4. Department Performance Analytics (Gold): Provides insights into department-level efficiency, revenue, and patient volume.

# CREATE TABLE
CREATE TABLE IF NOT EXISTS `healthcare-rcm-project-504309.gold_dataset.department_performance` (
    Dept_Id STRING,
    DepartmentName STRING,
    TotalEncounters INT64,
    TotalTransactions INT64,
    TotalBilledAmount FLOAT64,
    TotalPaidAmount FLOAT64,
    AvgPaymentPerTransaction FLOAT64
);

# TRUNCATE TABLE
TRUNCATE TABLE `healthcare-rcm-project-504309.gold_dataset.department_performance`;

# INSERT DATA
INSERT INTO `healthcare-rcm-project-504309.gold_dataset.department_performance`
SELECT
    d.Dept_Id,
    d.Name AS DepartmentName,
    COUNT(DISTINCT e.Encounter_Key) AS TotalEncounters,
    COUNT(DISTINCT t.Transaction_Key) AS TotalTransactions,
    SUM(t.Amount) AS TotalBilledAmount,
    SUM(t.PaidAmount) AS TotalPaidAmount,
    AVG(t.PaidAmount) AS AvgPaymentPerTransaction
FROM `healthcare-rcm-project-504309.silver_dataset.departments` d
LEFT JOIN `healthcare-rcm-project-504309.silver_dataset.encounters` e
    ON SPLIT(d.Dept_Id, "-")[SAFE_OFFSET(0)] = e.DepartmentID
LEFT JOIN `healthcare-rcm-project-504309.silver_dataset.transactions` t
    ON SPLIT(d.Dept_Id, "-")[SAFE_OFFSET(0)] = t.DeptID
WHERE d.is_quarantined = FALSE
GROUP BY d.Dept_Id, d.Name;