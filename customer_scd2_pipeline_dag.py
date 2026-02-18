"""
====================================================================================================
DAG: customer_scd2_pipeline_dag.py
====================================================================================================
DAG Purpose:
    This Airflow DAG orchestrates the complete SCD Type 2 customer data pipeline.
    It automates the end-to-end ETL workflow from raw data generation to loading
    historical records into the dimension table.

Pipeline Workflow:
    1. Generate raw customer data
        - Executes generate_customer_raw_data.py
        - Creates simulated customer source data

    2. Clean customer data
        - Executes clean_customer_data.py
        - Performs data cleansing and prepares structured data

    3. Load data into staging table
        - Executes load_to_staging.py
        - Loads cleaned data into staging_customer table

    4. Apply SCD Type 2 logic
        - Executes scd2_customer_load.sql
        - Inserts new records
        - Expires changed records
        - Maintains full customer history

Key Features:
    - Fully automated ETL pipeline using Apache Airflow
    - Implements Slowly Changing Dimension Type 2
    - Maintains historical customer data
    - Ensures data consistency and traceability

Dependencies:
    generate_customer_raw_data.py
    clean_customer_data.py
    load_to_staging.py
    scd2_customer_load.sql

Usage:
    - Place this DAG in the Airflow dags folder
    - Start Airflow scheduler and webserver
    - Trigger DAG manually or schedule as required

Owner:
    Data Engineering Project

====================================================================================================
"""

import os
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
import pendulum

PROJECT_PATH = "/home/spach/airflow_project"
SQL_PATH = os.path.join(PROJECT_PATH, "airflow_home/dags")
VENV_PYTHON = os.path.join(PROJECT_PATH, "venv/bin/python3")

default_args = {
    "owner": "spach",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

local_tz = pendulum.timezone("Asia/Kolkata")

with DAG(
    dag_id="scd_type2_learning_pipeline",
    default_args=default_args,
    start_date=pendulum.datetime(2024, 1, 1, 0, 0, tz=local_tz),
    schedule="0 * * * *",  # every hour
    catchup=False,
    max_active_runs=1,
    render_template_as_native_obj=True,
    tags=["scd2", "learning"],
    template_searchpath=[SQL_PATH],
) as dag:

    generate_raw = BashOperator(
        task_id="generate_raw_csv",
        bash_command=f"{VENV_PYTHON} {PROJECT_PATH}/python_scripts/generate_raw_customers.py"
    )

    clean_csv = BashOperator(
        task_id="clean_csv",
        bash_command=f"{VENV_PYTHON} {PROJECT_PATH}/python_scripts/clean_customers_data.py"
    )

    copy_csv = BashOperator(
        task_id="copy_csv_to_mysql_folder",
        bash_command=f"cp {PROJECT_PATH}/scd_type2_cleaned/customers_cleaned_*.csv {PROJECT_PATH}/mysql_load_files/"
    )

    truncate_stg = MySqlOperator(
        task_id="truncate_stg_customer",
        mysql_conn_id="mysql_scd",
        sql="truncate_stg.sql"
    )

    load_stg = BashOperator(
        task_id="load_stg_customer",
        bash_command=f"{VENV_PYTHON} {PROJECT_PATH}/python_scripts/load_to_mysql.py"
    )

    run_scd2 = MySqlOperator(
        task_id="apply_scd_type2",
        mysql_conn_id="mysql_scd",
        sql="scd_type2_load.sql"
    )

    generate_raw >> clean_csv >> copy_csv >> truncate_stg >> load_stg >> run_scd2
