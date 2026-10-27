--5. Payor Performance & Claims Summary (Gold): This table tracks the performance of insurance payors, focusing on claim approval rates, payout amounts, and processing efficiency.

CREATE TABLE IF NOT EXISTS `healthcare-rcm-project-504309.gold_dataset.payor_performance` AS
SELECT
    c.PayorID,
    c.PayorType,
    COUNT(DISTINCT c.Claim_Key) AS TotalClaims,
    COUNT(DISTINCT CASE WHEN c.ClaimStatus = 'Approved' THEN c.Claim_Key END) AS ApprovedClaims,
    COUNT(DISTINCT CASE WHEN c.ClaimStatus = 'Denied' THEN c.Claim_Key END) AS DeniedClaims,
    COUNT(DISTINCT CASE WHEN c.ClaimStatus = 'Pending' THEN c.Claim_Key END) AS PendingClaims,
    ROUND((COUNT(DISTINCT CASE WHEN c.ClaimStatus = 'Approved' THEN c.Claim_Key END) / NULLIF(COUNT(DISTINCT c.Claim_Key), 0)) * 100, 2) AS ClaimApprovalRate,
    SUM(CAST(c.ClaimAmount AS FLOAT64)) AS TotalClaimAmount,
    SUM(CAST(c.PaidAmount AS FLOAT64)) AS TotalPaidAmount,
    SUM(CAST(c.ClaimAmount AS FLOAT64)) - SUM(CAST(c.PaidAmount AS FLOAT64)) AS OutstandingAmount
FROM `healthcare-rcm-project-504309.silver_dataset.claims` c
WHERE c.is_current = TRUE
GROUP BY c.PayorID, c.PayorType;