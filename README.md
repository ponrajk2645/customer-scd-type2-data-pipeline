# 🚀 Customer SCD Type 2 Data Pipeline using Airflow and MySQL

## 📌 Overview

This project implements an automated end-to-end ETL data pipeline to track customer data history using Slowly Changing Dimension Type 2 (SCD Type 2).

The pipeline generates customer data, cleans it, loads it into MySQL staging tables, and applies SCD Type 2 logic to maintain full historical records.

Apache Airflow is used to orchestrate and automate the entire workflow.

---

## ❓ Problem Statement

Customer data changes frequently in real-world systems, such as:

* Subscription plan changes
* Billing cycle updates
* Subscription status updates

If data is updated normally, old records are overwritten and history is lost.

Businesses need historical data for:

* Analytics and reporting
* Customer behavior tracking
* Audit requirements
* Business intelligence

This project solves this problem using SCD Type 2 methodology.

---

## 🎯 Project Objective

The objectives of this project are:

* Generate customer data automatically
* Clean and standardize raw data
* Load data into MySQL staging table
* Apply SCD Type 2 logic
* Maintain full historical records
* Automate pipeline using Airflow
* Build production-style ETL pipeline

---

## 🏗️ Architecture

```
Python Data Generator
        ↓
Raw CSV Files (scd_type2_rawdata)
        ↓
Data Cleaning Script
        ↓
Cleaned CSV Files (scd_type2_cleaned)
        ↓
MySQL Staging Table (stg_customer)
        ↓
SCD Type 2 Logic
        ↓
Dimension Table (dim_customer_scd2)
        ↓
Airflow DAG Automation
```

---

## 🔄 Data Flow

Step 1: Generate raw customer data using Python
Step 2: Store raw data in scd_type2_rawdata folder
Step 3: Clean and standardize data using Pandas
Step 4: Store cleaned data in scd_type2_cleaned folder
Step 5: Copy cleaned data into mysql_load_files folder
Step 6: Load data into MySQL staging table
Step 7: Apply SCD Type 2 logic using SQL
Step 8: Store historical records in dimension table
Step 9: Airflow automates entire pipeline

---

## 🛠️ Tech Stack

* Python
* Pandas
* MySQL
* Apache Airflow
* Linux
* Faker

---

## 📁 Project Structure (Actual)

```
airflow_project/

│
├── dags/
│   └── scd_type2_pipeline.py
│
├── airflow_home/
│   └── dags/
│       ├── create_stg_customer.sql
│       ├── create_dim_customer_scd2.sql
│       └── scd_type2_load.sql
│
├── python_scripts/
│   ├── generate_raw_customers.py
│   ├── clean_customers_data.py
│   └── load_to_mysql.py
│
├── scd_type2_rawdata/
│
├── scd_type2_cleaned/
│
├── venv/
│
└── README.md
```

---

## 🗄️ Database Schema

### Staging Table: stg_customer

Columns:

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

Columns:

* customer_sk
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

## 🧠 SCD Type 2 Logic

When customer data changes:

Expire old record:

```
end_date = current_date
is_current = 0
```

Insert new record:

```
effective_date = current_date
end_date = NULL
is_current = 1
```

This maintains full historical data.

---

## ⚙️ Airflow Pipeline Tasks

The Airflow DAG performs:

1. Generate raw data (Python)
2. Clean data (Python)
3. Copy cleaned data
4. Load staging table (MySQL)
5. Apply SCD Type 2 logic (SQL)

Pipeline runs automatically based on schedule.

---

## ✨ Key Features

* Automated ETL pipeline
* SCD Type 2 implementation
* Full history tracking
* Airflow automation
* Incremental loading
* Modular pipeline design

---

## ⚡ Performance Optimization

* Incremental processing
* Staging table usage
* Automated scheduling
* Efficient SQL logic

---

## 🧩 Challenges Faced

* Implementing SCD Type 2 logic correctly
* Automating Airflow pipeline
* Managing incremental loads
* Preventing duplicate records

---

## ▶️ How to Run

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
pip install pandas faker mysql-connector-python apache-airflow
```

Step 4: Start Airflow

```
airflow standalone
```

Step 5: Enable DAG in Airflow UI

---

## 📊 Sample Output

Example dimension table:

| customer_id | plan  | effective_date | end_date   | is_current |
| ----------- | ----- | -------------- | ---------- | ---------- |
| 101         | Basic | 2024-01-01     | 2024-03-01 | 0          |
| 101         | Pro   | 2024-03-01     | NULL       | 1          |

---

## 🎓 Skills Demonstrated

* ETL pipeline development
* Data warehousing
* SCD Type 2 implementation
* Airflow orchestration
* Python data processing
* SQL and MySQL
* Production-style pipeline design

---

## 👨‍💻 Author

Ponraj K
Data Engineering Project
