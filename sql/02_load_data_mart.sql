CREATE OR REPLACE TABLE insurance_dev.dm_analytics.dm_policy_performance AS
SELECT
  POLICY_ID                                                        AS policy_id,
  CAST(try_to_timestamp(POLICY_START_DATE, 'M/d/yyyy') AS DATE)    AS policy_start_date,
  CAST(try_to_timestamp(POLICY_END_DATE, 'M/d/yyyy') AS DATE)      AS policy_end_date,
  PRODUCT_NAME AS product_name,  PRODUCT_LINE AS product_line,
  AGENT_NAME AS agent_name,      AGENT_CHANNEL AS agent_channel,
  POLICYHOLDER_NAME AS policyholder_name, POLICYHOLDER_CITY AS policyholder_city,
  CUSTOMER_SEGMENT AS customer_segment,
  try_cast(SUM_INSURED AS DOUBLE)            AS sum_insured,
  try_cast(ANNUAL_PREMIUM AS DOUBLE)         AS annual_premium,
  POLICY_STATUS AS policy_status,
  IS_ACTIVE_POLICY AS is_active_policy, IS_LAPSED_POLICY AS is_lapsed_policy,
  IS_CANCELLED_POLICY AS is_cancelled_policy,
  CAST(try_to_timestamp(DATA_AS_OF_DATE, 'M/d/yyyy') AS DATE)      AS data_as_of_date,
  try_cast(INSTALLMENTS_PAID AS INT)         AS installments_paid,
  try_cast(LATE_INSTALLMENTS AS INT)         AS late_installments,
  HAS_LATE_PAYMENT AS has_late_payment,      LATE_PAYMENT_BUCKET AS late_payment_bucket,
  try_cast(CLAIM_COUNT AS INT)               AS claim_count,
  try_cast(APPROVED_CLAIM_COUNT AS INT)      AS approved_claim_count,
  try_cast(OPEN_CLAIM_COUNT AS INT)          AS open_claim_count,
  try_cast(OPEN_CLAIMED_AMOUNT AS DOUBLE)    AS open_claimed_amount,
  try_cast(TOTAL_CLAIMED_AMOUNT AS DOUBLE)   AS total_claimed_amount,
  try_cast(TOTAL_APPROVED_AMOUNT AS DOUBLE)  AS total_approved_amount
FROM read_files('/Volumes/insurance_dev/landing/raw_files/DM_POLICY_PERFORMENCE.csv',
                format => 'csv', header => true, inferSchema => false);
 
CREATE OR REPLACE TABLE insurance_dev.dm_analytics.dm_claim_detail AS
SELECT
  CLAIM_ID AS claim_id, POLICY_ID AS policy_id,
  CAST(try_to_timestamp(CLAIM_DATE, 'M/d/yyyy') AS DATE)       AS claim_date,
  CAST(try_to_timestamp(LOSS_DATE, 'M/d/yyyy') AS DATE)        AS loss_date,
  CAST(try_to_timestamp(SETTLED_DATE, 'M/d/yyyy') AS DATE)     AS settled_date,
  PRODUCT_NAME AS product_name,  PRODUCT_LINE AS product_line,
  AGENT_NAME AS agent_name,      AGENT_CHANNEL AS agent_channel,
  POLICYHOLDER_NAME AS policyholder_name, POLICYHOLDER_CITY AS policyholder_city,
  CUSTOMER_SEGMENT AS customer_segment,   CLAIM_TYPE AS claim_type,
  try_cast(CLAIMED_AMOUNT AS DOUBLE)   AS claimed_amount,
  try_cast(APPROVED_AMOUNT AS DOUBLE)  AS approved_amount,
  CLAIM_STATUS AS claim_status, DOCUMENT_STATUS AS document_status, CLAIM_STAGE AS claim_stage,
  CAST(try_to_timestamp(DATA_AS_OF_DATE, 'M/d/yyyy') AS DATE) AS data_as_of_date,
  IS_OPEN_CLAIM AS is_open_claim,
  try_cast(SETTLEMENT_TIME_DAYS AS INT) AS settlement_time_days,
  try_cast(DAYS_OPEN AS INT)            AS days_open,
  BACKLOG_AGING_BUCKET AS backlog_aging_bucket,
  IS_SLA_BREACH AS is_sla_breach
FROM read_files('/Volumes/insurance_dev/landing/raw_files/DM_CLAIM_DETAIL.csv',
                format => 'csv', header => true, inferSchema => false);