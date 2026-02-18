"""
=================================================================================================================
Script Name   : generate_customer_raw_data.py
Layer         : Data Generation Layer (Raw Data Layer)
Object Type   : Python Script
Purpose       : Generate synthetic raw customer data for testing the SCD Type 2 pipeline

Description:
    This script generates sample customer data and saves it as a CSV file in the raw data directory.
    The generated data simulates real-world customer information such as subscriptions, plans,
    billing cycles, and status changes.

    This raw data acts as the source input for the ETL pipeline and is used to test historical
    tracking using the Slowly Changing Dimension (SCD Type 2) logic.

Data Generated Includes:
    - customer_id
    - first_name
    - last_name
    - email
    - phone_number
    - email_verified
    - city
    - plan
    - billing_cycle
    - subscription_status
    - event_date
    - source_system

Features:
    - Generates realistic customer records
    - Simulates customer changes over time
    - Helps test SCD Type 2 history tracking
    - Supports pipeline testing and development without real production data

Source:
    Synthetic data generated using Python libraries (random, faker, pandas)

Target:
    Raw data file saved to:
    Example: scd_type2_rawdata/customers_raw.csv

Pipeline Role:
    [This Script: Generate Data] → Raw Data → Clean Data → Staging Table → Dimension Table (SCD Type 2)

Benefits:
    - Enables full pipeline testing
    - Simulates real-world customer lifecycle changes
    - Helps validate SCD Type 2 logic implementation
    - Eliminates dependency on external production data sources

Usage Example:
    python generate_customer_raw_data.py

Dependencies:
    - pandas
    - random
    - faker
    - datetime
    - os

Output Example:
    customer_id | first_name | city    | plan  | billing_cycle | subscription_status | event_date
    --------------------------------------------------------------------------------------------
    101         | Ravi       | Chennai | Pro   | Monthly       | Active              | 2024-01-01
    101         | Ravi       | Chennai | Pro   | Yearly        | Active              | 2024-04-01

Author        : Ponraj K
Project       : Customer Data Pipeline using SCD Type 2 with Apache Airflow
Created Date  : 2026
=================================================================================================================
"""

import os
import pandas as pd
import random
import logging
from faker import Faker
from airflow.models import Variable

fake = Faker("en_IN")

from datetime import datetime
from dateutil.relativedelta import relativedelta

# =============================
# Configurable paths via environment or default
# =============================
BASE_DIR = os.getenv("BASE_DIR", os.path.expanduser("~/airflow_project"))
RAW_DIR = os.getenv("RAW_DIR", os.path.join(BASE_DIR, "scd_type2_rawdata"))
MASTER_FILE = os.path.join(RAW_DIR, "master_customers.csv")
NUM_CUSTOMERS = int(os.getenv("NUM_CUSTOMERS", 1000))

os.makedirs(RAW_DIR, exist_ok=True)

# =============================
# Logging
# =============================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# =============================
# Determine next month via Airflow Variable
# =============================
ALL_RUN_DATES = ["20240101", "20240201", "20240301", "20240401", "20240501"]
last_month = Variable.get("last_generated_month", default_var=None)

if last_month is None:
    run_date = ALL_RUN_DATES[0]
else:
    remaining_dates = [d for d in ALL_RUN_DATES if d > last_month]
    if not remaining_dates:
        logger.info("All months already generated!")
        exit(0)
    run_date = remaining_dates[0]

output_file = os.path.join(RAW_DIR, f"customers_raw_{run_date}.csv")

if os.path.exists(output_file):
    logger.info(f"{output_file} already exists, skipping.")
    exit(0)

logger.info(f"Generating data for: {run_date}")

# =============================
# Phone generator & random functions
# =============================
def generate_phone():
    first_digit = random.choice(["7", "8", "9"])
    remaining = ''.join(random.choices("0123456789", k=9))
    return first_digit + remaining

def dirty_phone(num):
    return f"{num[:3]} {num[3:8]}-{num[8:]}"

def random_case(text):
    return ''.join(c.upper() if random.random() > 0.5 else c.lower() for c in text)

def add_random_space(text):
    if random.random() < 0.3:
        return f" {text} "
    return text

# =============================
# Create MASTER if not exists
# =============================
if not os.path.exists(MASTER_FILE):
    logger.info("Creating master base file...")
    rows = []
    for cid in range(1, NUM_CUSTOMERS + 1):
        first = fake.first_name()
        last = fake.last_name()
        phone_clean = generate_phone()
        row = {
            "customer_id": cid,
            "first_name": first,
            "last_name": last,
            "email": f"{first.lower()}@example.com",
            "phone_number": phone_clean,
            "email_verified": random.choice([0, 1]),
            "city": random.choice(["Mumbai", "Delhi", "Chennai", "Hyderabad"]),
            "plan": random.choice(["Basic", "Pro"]),
            "subscription_status": random.choice(["Active", "Cancelled"]),
            "billing_cycle": random.choice(["Monthly", "Yearly"]),
            "source_system": random.choice(["CRM", "APP"])
        }
        rows.append(row)

    master_df = pd.DataFrame(rows)
    master_df.to_csv(MASTER_FILE, index=False)
    logger.info("Master file created.")
else:
    master_df = pd.read_csv(MASTER_FILE, dtype={"customer_id": "int64", "phone_number": "string", "email_verified": "int64"})

# =============================
# Apply monthly business changes (5–15%)
# =============================
df = master_df.copy()
num_changes = random.randint(NUM_CUSTOMERS // 20, NUM_CUSTOMERS // 10)  # 5-10%
change_ids = random.sample(list(df["customer_id"]), num_changes)

for cid in change_ids:
    idx = df[df["customer_id"] == cid].index[0]
    df.loc[idx, "plan"] = random.choice(["Basic", "Pro"])
    df.loc[idx, "subscription_status"] = random.choice(["Active", "Cancelled"])
    df.loc[idx, "billing_cycle"] = random.choice(["Monthly", "Yearly"])

# =============================
# Create RAW dirty version
# =============================
raw_df = df.copy()
raw_df["first_name"] = raw_df["first_name"].apply(random_case).apply(add_random_space)
raw_df["last_name"] = raw_df["last_name"].apply(random_case).apply(add_random_space)
raw_df["email"] = raw_df["email"].apply(random_case).apply(add_random_space)
raw_df["phone_number"] = raw_df["phone_number"].astype(str).apply(dirty_phone)
raw_df["subscription_status"] = raw_df["subscription_status"].apply(add_random_space)
raw_df["event_date"] = run_date

raw_df = raw_df[[
    "customer_id", "first_name", "last_name", "email", "phone_number",
    "email_verified", "city", "plan", "subscription_status",
    "billing_cycle", "source_system", "event_date"
]]

raw_df.to_csv(output_file, index=False)
df.to_csv(MASTER_FILE, index=False)

# Update Airflow Variable
Variable.set("last_generated_month", run_date)

logger.info(f"Generated RAW file: {output_file}")
logger.info(f"Rows: {len(raw_df)} | Business changes this month: {num_changes}")
