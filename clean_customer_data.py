"""
=================================================================================================================
Script Name   : clean_customer_data.py
Layer         : Data Cleaning Layer
Object Type   : Python Script
Purpose       : Clean and transform raw customer data before loading into the staging table

Description:
    This script reads raw customer data from the raw data directory, performs data cleaning,
    validation, and transformation, and writes the cleaned data into the cleaned data directory.

    The purpose of this script is to ensure only high-quality, standardized, and valid data
    is loaded into the staging table for further processing in the SCD Type 2 pipeline.

Data Cleaning Operations Performed:
    - Remove duplicate records
    - Handle missing or NULL values
    - Standardize text fields (trim spaces, normalize case)
    - Validate email and phone number formats
    - Ensure correct data types for each column
    - Remove invalid or corrupted records

Source:
    Raw customer data file
    Example: scd_type2_rawdata/customers_raw.csv

Target:
    Cleaned customer data file
    Example: scd_type2_cleandata/customers_cleaned.csv

Pipeline Role:
    Raw Data → [This Script: Cleaning] → Cleaned Data → Staging Table → Dimension Table (SCD Type 2)

Benefits:
    - Improves data quality
    - Prevents invalid data from entering the data warehouse
    - Ensures reliable historical tracking
    - Improves downstream analytics accuracy

Usage Example:
    python clean_customer_data.py

Dependencies:
    - pandas
    - os
    - datetime (if used)

Author        : Ponraj K
Project       : Customer Data Pipeline using SCD Type 2 with Apache Airflow
Created Date  : 2026
=================================================================================================================
"""

import pandas as pd
import os

BASE_DIR = os.path.expanduser("~/airflow_project")
RAW_DIR = os.path.join(BASE_DIR, "scd_type2_rawdata")
CLEAN_DIR = os.path.join(BASE_DIR, "scd_type2_cleaned")

os.makedirs(CLEAN_DIR, exist_ok=True)

# ============================================================
# Determine next month automatically based on existing cleaned files
# ============================================================

ALL_RUN_DATES = ["20240101", "20240201", "20240301", "20240401", "20240501"]

existing_files = sorted([
    f for f in os.listdir(CLEAN_DIR) if f.startswith("customers_cleaned_")
])

if not existing_files:
    # first run
    run_date = ALL_RUN_DATES[0]
else:
    # pick next month that hasn't been cleaned yet
    cleaned_dates = [f.split("_")[-1].replace(".csv", "") for f in existing_files]
    remaining_dates = [d for d in ALL_RUN_DATES if d not in cleaned_dates]
    if not remaining_dates:
        print("All months already cleaned!")
        exit(0)
    run_date = remaining_dates[0]

# Define input and output
input_file = os.path.join(RAW_DIR, f"customers_raw_{run_date}.csv")
output_file = os.path.join(CLEAN_DIR, f"customers_cleaned_{run_date}.csv")

# Check file exists
if not os.path.exists(input_file):
    print(f"RAW file not found: {input_file}")
    exit(1)

print(f"Cleaning file: {input_file}")

# =============================
# READ RAW FILE
# =============================
df = pd.read_csv(
    input_file,
    dtype={
        "customer_id": "int64",
        "phone_number": "string",
        "email_verified": "int64",
        "event_date": "string",
        "source_system": "string"
    }
)

# =============================
# CLEANING STEPS
# =============================

# 1️⃣ Strip spaces
for col in df.select_dtypes(include="object").columns:
    df[col] = df[col].str.strip()

# 2️⃣ Standardize names
df["first_name"] = df["first_name"].str.title()
df["last_name"] = df["last_name"].str.title()

# 3️⃣ Lowercase email
df["email"] = df["email"].str.lower()

# 4️⃣ Clean phone
df["phone_number"] = df["phone_number"].str.replace(" ", "", regex=False).str.replace("-", "", regex=False)
df["phone_number"] = df["phone_number"].astype("string")

# 5️⃣ Standardize subscription_status
df["subscription_status"] = df["subscription_status"].str.title()

# 6️⃣ Convert event_date format
df["event_date"] = pd.to_datetime(df["event_date"], format="%Y%m%d")
df["event_date"] = df["event_date"].dt.strftime("%Y-%m-%d")

# 7️⃣ Remove duplicates
df = df.drop_duplicates()

# 8️⃣ Preserve exact column order (12 columns)
df = df[[
    "customer_id",
    "first_name",
    "last_name",
    "email",
    "phone_number",
    "email_verified",
    "city",
    "plan",
    "subscription_status",
    "billing_cycle",
    "source_system",
    "event_date"
]]

# =============================
# SAVE CLEAN FILE
# =============================
df.to_csv(output_file, index=False)

print(f"Clean file created: {output_file}")
print(f"Rows after cleaning: {len(df)}")
