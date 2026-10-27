-- 3. Provider Performance Summary (Gold) : This table summarizes provider activity, including the number of encounters, total billed amount, and claim success rate.

# CREATE TABLE
CREATE TABLE IF NOT EXISTS `healthcare-rcm-project-504309.gold_dataset.provider_performance` (
    ProviderID STRING,
    FirstName STRING,
    LastName STRING,
    Specialization STRING,
    TotalEncounters INT64,
    TotalTransactions INT64,
    TotalBilledAmount FLOAT64,
    TotalPaidAmount FLOAT64,
    ApprovedClaims INT64,
    TotalClaims INT64,
    ClaimApprovalRate FLOAT64
);

# TRUNCATE TABLE
TRUNCATE TABLE `healthcare-rcm-project-504309.gold_dataset.provider_performance`;

# INSERT DATA
INSERT INTO `healthcare-rcm-project-504309.gold_dataset.provider_performance`
SELECT
    pr.ProviderID,
    pr.FirstName,
    pr.LastName,
    pr.Specialization,
    COUNT(DISTINCT e.Encounter_Key) AS TotalEncounters,
    COUNT(DISTINCT t.Transaction_Key) AS TotalTransactions,
    SUM(t.Amount) AS TotalBilledAmount,
    SUM(t.PaidAmount) AS TotalPaidAmount,
    COUNT(DISTINCT CASE WHEN c.ClaimStatus = 'Approved' THEN c.Claim_Key END) AS ApprovedClaims,
    COUNT(DISTINCT c.Claim_Key) AS TotalClaims,
    ROUND((COUNT(DISTINCT CASE WHEN c.ClaimStatus = 'Approved' THEN c.Claim_Key END) / NULLIF(COUNT(DISTINCT c.Claim_Key), 0)) * 100, 2) AS ClaimApprovalRate
FROM `healthcare-rcm-project-504309.silver_dataset.providers` pr
LEFT JOIN `healthcare-rcm-project-504309.silver_dataset.encounters` e
    ON SPLIT(pr.ProviderID, "-")[SAFE_OFFSET(1)] = e.ProviderID
LEFT JOIN `healthcare-rcm-project-504309.silver_dataset.transactions` t
    ON SPLIT(pr.ProviderID, "-")[SAFE_OFFSET(1)] = t.ProviderID
LEFT JOIN `healthcare-rcm-project-504309.silver_dataset.claims` c
    ON t.SRC_TransactionID = c.TransactionID
GROUP BY pr.ProviderID, pr.FirstName, pr.LastName, pr.Specialization;