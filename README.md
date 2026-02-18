# Customer SCD Type 2 Data Pipeline using Airflow and MySQL

## Overview

This project implements an end-to-end automated data pipeline to track customer data history using Slowly Changing Dimension Type 2 (SCD Type 2). The pipeline generates monthly customer data, cleans and processes it, loads it into a MySQL data warehouse, and maintains historical records using SCD Type 2 logic.

Apache Airflow is used to orchestrate and automate the entire ETL pipeline.

---

## Problem Statement

In real-world business systems, customer data changes over time (plan changes, subscription status updates, billing cycle modifications, etc.). A simple update overwrites the old data and loses historical information.

Businesses need to preserve full history to support:

* Historical reporting
* Customer behavior analysis
* Audit requirements
* Business intelligence and analytics

This project solves that problem using SCD Type 2 methodology.

---

## Project Objective

The main objectives of this project are:

* Generate monthly customer data automatically
* Clean and standardize raw data
* Load data into staging tables
* Apply SCD Type 2 logic
* Maintain full customer history
* Automate pipeline execution using Airflow
* Build a production-style ETL pipeline

---

## Architecture

Pipeline Architecture:

Raw Data Generation (Python)
↓
Raw CSV Files
↓
Data Cleaning (Python)
↓
Cleaned CSV Files
↓
Staging Table Load (MySQL)
↓
SCD Type 2 Logic Applied
↓
Dimension Table (dim_customer_scd2)
↓
Airflow Automation

---

## Data Flow

Step 1: Generate raw customer data using Python and Faker
Step 2: Save raw data as CSV files
Step 3: Clean and standardize data using Pandas
Step 4: Load cleaned data into MySQL staging table
Step 5: Apply SCD Type 2 logic
Step 6: Insert new records and expire old records
Step 7: Store final data in dimension table
Step 8: Airflow automates entire pipeline

---

## Tech Stack

Python – Data generation and cleaning
Pandas – Data processing
MySQL – Data warehouse
Apache Airflow – Workflow orchestration
Linux – Execution environment
Faker – Test data generation

---

## Project Structure

```
customer-scd-type2-data-pipeline/

│
├── dags/
│   └── customer_scd2_pipeline_dag.py
│
├── python_scripts/
│   ├── generate_customer_raw_data.py
│   ├── clean_customer_data.py
│   ├── load_customer_staging.py
│
├── mysql_scripts/
│   ├── create_staging_customer.sql
│   ├── create_dim_customer_scd2.sql
│   ├── scd2_customer_load.sql
│
├── scd_type2_rawdata/
├── scd_type2_cleaned/
├── mysql_load_files/
│
├── requirements.txt
└── README.md
```

---

## Database Schema

### Staging Table: stg_customer

* customer_id
* first_name
* last_name
* email
* phone_number
* city
* plan
* subscription_status
* billing_cycle
* event_date

---

### Dimension Table: dim_customer_scd2

* customer_sk (Surrogate Key)
* customer_id
* first_name
* last_name
* email
* phone_number
* city
* plan
* subscription_status
* billing_cycle
* effective_date
* end_date
* is_current

---

## SCD Type 2 Logic Explanation

When customer data changes:

1. Existing record is expired

   * end_date is updated
   * is_current is set to 0

2. New record is inserted

   * effective_date is current date
   * end_date is NULL
   * is_current is set to 1

This preserves full history of customer changes.

---

## Airflow Pipeline

The Airflow DAG performs the following tasks:

1. Generate raw data
2. Clean data
3. Copy cleaned data
4. Load staging table
5. Apply SCD Type 2 logic

Pipeline runs automatically on schedule.

---

## Key Features

* Fully automated ETL pipeline
* SCD Type 2 implementation
* Historical data tracking
* Modular ETL design
* Automated workflow using Airflow
* Production-style architecture
* Incremental data loading

---

## Performance Optimization

* Incremental data loading
* Staging table used for efficient processing
* Modular scripts improve maintainability
* Airflow automation eliminates manual work

---

## Challenges Faced

* Implementing correct SCD Type 2 logic
* Automating pipeline execution
* Handling incremental monthly data
* Preventing duplicate records
* Managing Airflow scheduling

---

## Future Improvements

* Integrate AWS S3 for storage
* Use Snowflake or BigQuery as warehouse
* Add real-time pipeline using Kafka
* Add monitoring and alert system
* Add data quality validation checks

---

## How to Run the Project

Step 1: Clone repository

```
git clone https://github.com/yourusername/customer-scd-type2-data-pipeline.git
```

Step 2: Create virtual environment

```
python -m venv venv
source venv/bin/activate
```

Step 3: Install dependencies

```
pip install -r requirements.txt
```

Step 4: Start Airflow

```
airflow standalone
```

Step 5: Enable DAG from Airflow UI

---

## Sample Output

Dimension table stores full customer history with active and expired records.

Example:

customer_id | plan | effective_date | end_date | is_current
101 | Basic | 2024-01-01 | 2024-03-01 | 0
101 | Pro | 2024-03-01 | NULL | 1

---

## Conclusion

This project demonstrates a real-world data engineering pipeline using Airflow, Python, and MySQL. It implements SCD Type 2 to maintain full historical data and follows industry-standard ETL architecture.

This project showcases skills in:

* ETL pipeline development
* Data warehousing
* Workflow orchestration
* Python data processing
* SQL and database design
* Airflow automation

---

## Author

Ponraj K
Data Engineering Project
