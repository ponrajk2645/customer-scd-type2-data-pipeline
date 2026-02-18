/*
====================================================================================================
Script Name: create_dim_customer_scd2.sql
====================================================================================================
Script Purpose:
    Creates the dimension table to store customer data using Slowly Changing Dimension Type 2 (SCD2).

Description:
    - Stores complete historical customer records
    - Tracks changes using effective_start_date and effective_end_date
    - Uses is_current flag to identify active record
    - Uses surrogate_key as primary key for uniqueness

Source:
    stg_customer (Staging Table)

Target:
    dim_customer_scd2 (Dimension Table)

Execution:
    Executed once during initial database setup or schema initialization.

====================================================================================================
*/

CREATE TABLE IF NOT EXISTS dim_customer_scd2 (
    surrogate_key BIGINT AUTO_INCREMENT PRIMARY KEY,
    customer_id INT NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(150),
    phone_number VARCHAR(20),
    email_verified TINYINT(1),
    city VARCHAR(50),
    plan VARCHAR(50),
    billing_cycle VARCHAR(50),
    subscription_status VARCHAR(50),
    source_system VARCHAR(50),
    effective_start_date DATE NOT NULL,
    effective_end_date DATE NOT NULL,
    is_current TINYINT(1) NOT NULL,
    record_created_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    record_updated_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_customer_id (customer_id),
    INDEX idx_current_flag (is_current)
);

