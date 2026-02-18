"""
====================================================================================================
Script: load_to_staging.py
====================================================================================================
Script Purpose:
    This script loads cleaned customer data from the Silver layer into the Staging tables
    in the data warehouse.

    It reads the cleaned CSV file and inserts the records into the staging table without
    applying any business transformations.

    The staging table acts as a temporary storage area before loading data into the
    final Dimension tables (SCD Type 2).

Usage:
    - Run this script after clean_customer_data.py
    - Ensures staging table contains the latest cleaned customer data
====================================================================================================
"""

import pandas as pd
import pymysql
import os

CLEAN_DIR = "/home/spach/airflow_project/scd_type2_cleaned"

# pick latest cleaned CSV
existing_files = sorted([f for f in os.listdir(CLEAN_DIR) if f.startswith("customers_cleaned_")])
if not existing_files:
    print("No cleaned files found!")
    exit(1)

latest_file = existing_files[-1]  # latest month CSV
file_path = os.path.join(CLEAN_DIR, latest_file)

print("Loading:", file_path)
df = pd.read_csv(file_path)

conn = pymysql.connect(
    host="localhost",
    user="pyuser",
    password="Rohit@2645",
    database="scd_dw"
)
cursor = conn.cursor()

# truncate staging
cursor.execute("TRUNCATE TABLE stg_customer")

# insert rows
for _, row in df.iterrows():
    cursor.execute("""
        INSERT INTO stg_customer (
            customer_id,
            first_name,
            last_name,
            email,
            phone_number,
            email_verified,
            city,
            plan,
            subscription_status,
            billing_cycle,
            source_system,
            event_date
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, tuple(row))

conn.commit()
cursor.close()
conn.close()

print("LOAD SUCCESS for:", latest_file)
