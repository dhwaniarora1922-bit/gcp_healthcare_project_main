-- 4. Financial Metrics (Gold) : Aggregates financial KPIs, such as total revenue, claim success rate, and outstanding balances.

CREATE TABLE IF NOT EXISTS `healthcare-rcm-project-504309.gold_dataset.financial_metrics` AS
SELECT
    COUNT(DISTINCT t.Transaction_Key) AS TotalTransactions,
    SUM(t.Amount) AS TotalBilledAmount,
    SUM(t.PaidAmount) AS TotalPaidAmount,
    SUM(t.Amount) - SUM(t.PaidAmount) AS OutstandingBalance,
    COUNT(DISTINCT CASE WHEN c.ClaimStatus = 'Approved' THEN c.Claim_Key END) AS ApprovedClaims,
    COUNT(DISTINCT c.Claim_Key) AS TotalClaims,
    ROUND((COUNT(DISTINCT CASE WHEN c.ClaimStatus = 'Approved' THEN c.Claim_Key END) / NULLIF(COUNT(DISTINCT c.Claim_Key), 0)) * 100, 2) AS ClaimApprovalRate
FROM `healthcare-rcm-project-504309.silver_dataset.transactions` t
LEFT JOIN `healthcare-rcm-project-504309.silver_dataset.claims` c
    ON t.SRC_TransactionID = c.TransactionID
WHERE t.is_current = TRUE;