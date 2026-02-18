/*
====================================================================================================
Script Name: create_staging_customer.sql
====================================================================================================
Script Purpose:
    Creates the staging table for customer data if it does not exist.

Description:
    - Stores raw customer data before applying SCD Type 2 logic
    - Acts as an intermediate layer between source and dimension table

Source:
    CSV file / Source system

Target:
    stg_customer table

Execution:
    Executed as part of the ETL pipeline.
====================================================================================================
*/

CREATE TABLE IF NOT EXISTS stg_customer (
    customer_id INT NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(150),
    phone_number VARCHAR(20),
    email_verified TINYINT(1),
    city VARCHAR(50),
    plan VARCHAR(50),
    subscription_status VARCHAR(50),
    billing_cycle VARCHAR(50),
    event_date DATE NOT NULL,
    source_system VARCHAR(50),
    PRIMARY KEY (customer_id, event_date)
);
