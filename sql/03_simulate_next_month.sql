INSERT INTO insurance_dev.dm_analytics.dm_policy_performance BY NAME
SELECT * EXCEPT (policy_status, data_as_of_date),
       CASE WHEN policy_id IN ('POL0000002', 'POL0000005', 'POL0000012') THEN 'LAPSED'
            ELSE policy_status END AS policy_status,
       DATE'2026-05-01' AS data_as_of_date
FROM insurance_dev.dm_analytics.dm_policy_performance
WHERE data_as_of_date = DATE'2026-04-01'
  AND policy_id NOT IN ('POL0000003', 'POL0000013');
 
INSERT INTO insurance_dev.dm_analytics.dm_claim_detail BY NAME
SELECT * EXCEPT (data_as_of_date), DATE'2026-05-01' AS data_as_of_date
FROM insurance_dev.dm_analytics.dm_claim_detail
WHERE data_as_of_date = DATE'2026-04-01';