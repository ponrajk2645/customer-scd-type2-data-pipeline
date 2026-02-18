/*
====================================================================================================
Script: scd2_customer_load.sql
====================================================================================================
Script Purpose:
    This script implements the Slowly Changing Dimension Type 2 (SCD Type 2) logic
    to load customer data from the staging table into the dimension table dim_customer_scd2.

    It ensures historical tracking of customer changes by:
        - Inserting new customer records
        - Expiring existing records when changes are detected
        - Creating new records with updated information
        - Maintaining effective_date, end_date, and active_flag columns

    This allows full history tracking of customer attribute changes over time.

Source:
    staging_customer

Target:
    dim_customer_scd2

Usage:
    - Run this script after load_to_staging.py
    - Ensures dimension table maintains accurate historical customer records
====================================================================================================
*/

START TRANSACTION;

-- -----------------------------------------------------
-- 1. EXPIRE CURRENT RECORDS WHEN DATA CHANGES
-- -----------------------------------------------------
UPDATE dim_customer_scd2 d
JOIN stg_customer s
  ON d.customer_id = s.customer_id
SET
    d.effective_end_date = DATE_SUB(s.event_date, INTERVAL 1 DAY),
    d.is_current = 0
WHERE d.is_current = 1
  AND d.effective_start_date < s.event_date
  AND (
        IFNULL(d.first_name,'')          <> IFNULL(s.first_name,'') OR
        IFNULL(d.last_name,'')           <> IFNULL(s.last_name,'') OR
        IFNULL(d.email,'')               <> IFNULL(s.email,'') OR
        IFNULL(d.phone_number,'')        <> IFNULL(s.phone_number,'') OR
        IFNULL(d.email_verified,0)       <> IFNULL(s.email_verified,0) OR
        IFNULL(d.city,'')                <> IFNULL(s.city,'') OR
        IFNULL(d.plan,'')                <> IFNULL(s.plan,'') OR
        IFNULL(d.billing_cycle,'')       <> IFNULL(s.billing_cycle,'') OR
        IFNULL(d.subscription_status,'') <> IFNULL(s.subscription_status,'') OR
        IFNULL(d.source_system,'')       <> IFNULL(s.source_system,'')
      );

-- -----------------------------------------------------
-- 2. INSERT NEW RECORDS
-- -----------------------------------------------------
INSERT INTO dim_customer_scd2 (
    customer_id,
    first_name,
    last_name,
    email,
    phone_number,
    email_verified,
    city,
    plan,
    billing_cycle,
    subscription_status,
    source_system,
    effective_start_date,
    effective_end_date,
    is_current
)
SELECT
    s.customer_id,
    s.first_name,
    s.last_name,
    s.email,
    s.phone_number,
    s.email_verified,
    s.city,
    s.plan,
    s.billing_cycle,
    s.subscription_status,
    s.source_system,
    s.event_date,           -- effective_start_date
    '9999-12-31',           -- effective_end_date
    1                       -- is_current
FROM stg_customer s
LEFT JOIN dim_customer_scd2 d
  ON s.customer_id = d.customer_id
 AND d.is_current = 1
WHERE d.customer_id IS NULL
   OR (
        IFNULL(d.first_name,'')          <> IFNULL(s.first_name,'') OR
        IFNULL(d.last_name,'')           <> IFNULL(s.last_name,'') OR
        IFNULL(d.email,'')               <> IFNULL(s.email,'') OR
        IFNULL(d.phone_number,'')        <> IFNULL(s.phone_number,'') OR
        IFNULL(d.email_verified,0)       <> IFNULL(s.email_verified,0) OR
        IFNULL(d.city,'')                <> IFNULL(s.city,'') OR
        IFNULL(d.plan,'')                <> IFNULL(s.plan,'') OR
        IFNULL(d.billing_cycle,'')       <> IFNULL(s.billing_cycle,'') OR
        IFNULL(d.subscription_status,'') <> IFNULL(s.subscription_status,'') OR
        IFNULL(d.source_system,'')       <> IFNULL(s.source_system,'')
      );

COMMIT;
