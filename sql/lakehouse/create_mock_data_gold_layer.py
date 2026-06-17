# Create Mock Gold Layer Data for Microsoft Fabric Lakehouse
# Purpose: Generate realistic dimensional data for CarPro Insurance Analytics
#          to test semantic models and Power BI dashboards.
# Span: 2025 - 2026 (for Time Intelligence & YoY growth validation)
# Design: Strict temporal integrity, zero unresolved keys (-1), 
#         and realistic business metric distributions.

import os
import random
import numpy as np
import pandas as pd
from datetime import datetime, date, timedelta
from decimal import Decimal
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

# Initialize Spark Session
spark = SparkSession.builder.getOrCreate()

print("[INFO] Starting mock Gold data generation...")

# Current simulation Date (Anchor for current/expired calculations)
SIM_DATE = datetime(2026, 6, 17)

# ---------------------------------------------------------
# 1. GENERATE DIM_DATE (2025 - 2026)
# ---------------------------------------------------------
print("[INFO] Generating dim_date...")
start_date = date(2025, 1, 1)
end_date = date(2026, 12, 31)
date_range = pd.date_range(start=start_date, end=end_date)

date_records = []
for dt in date_range:
    date_records.append({
        "date_key": int(dt.strftime("%Y%m%d")),
        "full_date": dt.date(),
        "day_number": int(dt.day),
        "day_name": dt.strftime("%A"),
        "week_number": int(dt.isocalendar()[1]),
        "month_number": int(dt.month),
        "month_name": dt.strftime("%B"),
        "quarter_number": int((dt.month - 1) // 3 + 1),
        "year_number": int(dt.year),
        "year_month": dt.strftime("%Y-%m"),
        "is_weekend": bool(dt.dayofweek >= 5)
    })

df_date = pd.DataFrame(date_records)

# ---------------------------------------------------------
# 2. GENERATE SCD1 & LOOKUP DIMENSIONS
# ---------------------------------------------------------
print("[INFO] Generating SCD1 dimensions...")

# Unknown member metadata columns helper
def add_metadata(df, update_time=datetime.now()):
    df["created_at"] = update_time
    df["updated_at"] = update_time
    return df

# Helper to create SCD1 lists
def create_scd1_df(records, key_name, code_name):
    rows = [{"package_key" if key_name == "package_key" else key_name: -1, code_name: "Unknown"}]
    for idx, code in enumerate(records, start=1):
        rows.append({key_name: idx, code_name: code})
    df = pd.DataFrame(rows)
    return add_metadata(df)

# Setup dim tables
df_package = create_scd1_df(["BASIC", "STANDARD", "PREMIUM", "VIP"], "package_key", "package_code")
df_coverage = create_scd1_df(["Physical Damage", "Theft", "Third Party Liability", "Personal Accident"], "coverage_key", "coverage_type")
df_qstatus = create_scd1_df(["QUOTED", "ACCEPTED", "REJECTED", "EXPIRED", "CONVERTED"], "quotation_status_key", "quotation_status_code")
df_pstatus = create_scd1_df(["ISSUED", "ACTIVE", "EXPIRED", "CANCELLED"], "policy_status_key", "policy_status_code")
df_paystatus = create_scd1_df(["PAID", "PENDING", "FAILED"], "payment_status_key", "payment_status_code")
df_paymethod = create_scd1_df(["BANK_TRANSFER", "CREDIT_CARD", "E_WALLET"], "payment_method_key", "payment_method_code")
df_cancellation_reason = create_scd1_df(["Customer Request", "Vehicle Sold", "Non-Payment", "Policy Re-written"], "cancellation_reason_key", "cancellation_reason")

# Dim Quotation & Dim Policy (Business Key mapping tables)
df_dim_quotation = pd.DataFrame([{"quotation_key": -1, "quotation_id": "Unknown", "quotation_expiry_date": None}])
df_dim_quotation = add_metadata(df_dim_quotation)

df_dim_policy = pd.DataFrame([{"policy_key": -1, "policy_id": "Unknown"}])
df_dim_policy = add_metadata(df_dim_policy)

# ---------------------------------------------------------
# 3. GENERATE SCD2 DIMENSIONS (WITH TEMPORAL HISTORY)
# ---------------------------------------------------------
print("[INFO] Generating SCD2 dimensions...")

# Providers (SCD2)
provider_records = [
    {"provider_key": -1, "provider_code": "Unknown", "provider_name": "Unknown", "provider_group": "Unknown", "active_flag": -1, "effective_from": datetime(1900, 1, 1), "effective_to": datetime(9999, 12, 31, 23, 59, 59), "is_current": True},
    {"provider_key": 1, "provider_code": "BV", "provider_name": "Bao Viet Insurance", "provider_group": "Domestic", "active_flag": 1, "effective_from": datetime(2025, 1, 1), "effective_to": datetime(9999, 12, 31, 23, 59, 59), "is_current": True},
    {"provider_key": 2, "provider_code": "LIB", "provider_name": "Liberty Insurance", "provider_group": "International", "active_flag": 1, "effective_from": datetime(2025, 1, 1), "effective_to": datetime(9999, 12, 31, 23, 59, 59), "is_current": True},
    {"provider_key": 3, "provider_code": "PVI", "provider_name": "PVI Insurance", "provider_group": "Domestic", "active_flag": 1, "effective_from": datetime(2025, 1, 1), "effective_to": datetime(9999, 12, 31, 23, 59, 59), "is_current": True},
    {"provider_key": 4, "provider_code": "MIC", "provider_name": "MIC Insurance", "provider_group": "Domestic", "active_flag": 1, "effective_from": datetime(2025, 1, 1), "effective_to": datetime(9999, 12, 31, 23, 59, 59), "is_current": True}
]
df_provider = add_metadata(pd.DataFrame(provider_records))

# Agents (SCD2)
agent_records = [
    {"agent_key": -1, "agent_id": "Unknown", "agent_name": "Unknown", "region": "Unknown", "branch": "Unknown", "manager_name": "Unknown", "effective_from": datetime(1900, 1, 1), "effective_to": datetime(9999, 12, 31, 23, 59, 59), "is_current": True},
    {"agent_key": 1, "agent_id": "AG001", "agent_name": "Nguyen Van An", "region": "North", "branch": "Ha Noi", "manager_name": "Tran Minh", "effective_from": datetime(2025, 1, 1), "effective_to": datetime(2025, 7, 31, 23, 59, 59), "is_current": False},
    {"agent_key": 5, "agent_id": "AG001", "agent_name": "Nguyen Van An", "region": "North", "branch": "Ha Noi", "manager_name": "Phan Thanh", "effective_from": datetime(2025, 8, 1), "effective_to": datetime(9999, 12, 31, 23, 59, 59), "is_current": True},
    {"agent_key": 2, "agent_id": "AG002", "agent_name": "Tran Thi Hoa", "region": "South", "branch": "Ho Chi Minh", "manager_name": "Le Anh", "effective_from": datetime(2025, 1, 1), "effective_to": datetime(9999, 12, 31, 23, 59, 59), "is_current": True},
    {"agent_key": 3, "agent_id": "AG003", "agent_name": "Pham Minh Duc", "region": "Central", "branch": "Da Nang", "manager_name": "Nguyen Long", "effective_from": datetime(2025, 1, 1), "effective_to": datetime(9999, 12, 31, 23, 59, 59), "is_current": True},
    {"agent_key": 4, "agent_id": "AG004", "agent_name": "Le Thi Mai", "region": "South", "branch": "Can Tho", "manager_name": "Le Anh", "effective_from": datetime(2025, 1, 1), "effective_to": datetime(9999, 12, 31, 23, 59, 59), "is_current": True}
]
df_agent = add_metadata(pd.DataFrame(agent_records))

# Customers (SCD2) & Vehicles (SCD2)
print("[INFO] Generating customers & vehicles...")
customer_records = [
    {"customer_key": -1, "customer_id": "Unknown", "full_name": "Unknown", "gender": "Unknown", "dob": None, "phone_number": "Unknown", "email": "Unknown", "city": "Unknown", "district": "Unknown", "effective_from": datetime(1900, 1, 1), "effective_to": datetime(9999, 12, 31, 23, 59, 59), "is_current": True}
]
vehicle_records = [
    {"vehicle_key": -1, "vehicle_id": "Unknown", "customer_id": "Unknown", "plate_number": "Unknown", "vehicle_brand": "Unknown", "vehicle_model": "Unknown", "manufacture_year": None, "vehicle_value": None, "effective_from": datetime(1900, 1, 1), "effective_to": datetime(9999, 12, 31, 23, 59, 59), "is_current": True}
]

cust_base_data = []
random.seed(42)
np.random.seed(42)

for i in range(1, 1501):
    cus_id = f"CUS{i:04d}"
    veh_id = f"VEH{i:04d}"
    
    reg_days = random.randint(0, 500)
    reg_date = datetime(2025, 1, 1) + timedelta(days=reg_days)
    
    gender = "Male" if random.random() > 0.5 else "Female"
    dob = date(1970, 1, 1) + timedelta(days=random.randint(0, 11000))
    city = random.choice(["Ha Noi", "Ho Chi Minh", "Da Nang"])
    district = f"District {random.randint(1, 12)}"
    
    cust_base_data.append({
        "customer_id": cus_id,
        "vehicle_id": veh_id,
        "reg_date": reg_date,
        "city": city,
        "gender": gender
    })
    
    # Customer Version 1
    customer_records.append({
        "customer_key": i * 2,
        "customer_id": cus_id,
        "full_name": f"Customer {i}",
        "gender": gender,
        "dob": dob,
        "phone_number": f"090{random.randint(1000000, 9999999)}",
        "email": f"cus{i}@carpro-insurance.com",
        "city": city,
        "district": district,
        "effective_from": reg_date,
        "effective_to": datetime(9999, 12, 31, 23, 59, 59),
        "is_current": True
    })
    
    # Vehicle Version 1
    brand = random.choice(["Toyota", "Hyundai", "Mazda", "VinFast", "Honda", "Kia"])
    val = float(random.randint(400, 1500) * 1000000)
    vehicle_records.append({
        "vehicle_key": i * 2,
        "vehicle_id": veh_id,
        "customer_id": cus_id,
        "plate_number": f"{random.choice(['29', '30', '51', '43', '92'])}{random.choice(['A', 'F', 'H'])}-{random.randint(10000, 99999)}",
        "vehicle_brand": brand,
        "vehicle_model": f"{brand} Model {random.randint(1, 4)}",
        "manufacture_year": random.randint(2018, 2025),
        "vehicle_value": Decimal(str(val)),
        "effective_from": reg_date,
        "effective_to": datetime(9999, 12, 31, 23, 59, 59),
        "is_current": True
    })

# Add some SCD2 history updates for 50 random customers who moved city in mid-2025/2026
change_indices = random.sample(range(1, 1501), 50)
for idx in change_indices:
    cus_id = f"CUS{idx:04d}"
    orig_idx = next(i for i, c in enumerate(customer_records) if c["customer_id"] == cus_id)
    orig_reg_date = customer_records[orig_idx]["effective_from"]
    
    move_date = orig_reg_date + timedelta(days=random.randint(30, 200))
    if move_date >= SIM_DATE:
        move_date = SIM_DATE - timedelta(days=10)
        
    customer_records[orig_idx]["effective_to"] = move_date - timedelta(seconds=1)
    customer_records[orig_idx]["is_current"] = False
    
    new_city = "Ho Chi Minh" if customer_records[orig_idx]["city"] == "Ha Noi" else "Ha Noi"
    customer_records.append({
        "customer_key": (idx * 2) + 100000,
        "customer_id": cus_id,
        "full_name": customer_records[orig_idx]["full_name"],
        "gender": customer_records[orig_idx]["gender"],
        "dob": customer_records[orig_idx]["dob"],
        "phone_number": customer_records[orig_idx]["phone_number"],
        "email": customer_records[orig_idx]["email"],
        "city": new_city,
        "district": customer_records[orig_idx]["district"],
        "effective_from": move_date,
        "effective_to": datetime(9999, 12, 31, 23, 59, 59),
        "is_current": True
    })
    cust_base_data[idx-1]["city"] = new_city

df_customer = add_metadata(pd.DataFrame(customer_records))
df_vehicle = add_metadata(pd.DataFrame(vehicle_records))

# ---------------------------------------------------------
# Helper functions for PIT lookups
# ---------------------------------------------------------
def get_customer_key_pit(customer_id, tx_time):
    matched = df_customer[(df_customer["customer_id"] == customer_id) & 
                          (tx_time >= df_customer["effective_from"]) & 
                          (tx_time <= df_customer["effective_to"])]
    return int(matched.iloc[0]["customer_key"]) if len(matched) > 0 else -1

def get_agent_key_pit(agent_id, tx_time):
    matched = df_agent[(df_agent["agent_id"] == agent_id) & 
                       (tx_time >= df_agent["effective_from"]) & 
                       (tx_time <= df_agent["effective_to"])]
    return int(matched.iloc[0]["agent_key"]) if len(matched) > 0 else -1

def get_provider_key_pit(provider_code, tx_time):
    matched = df_provider[(df_provider["provider_code"] == provider_code) & 
                          (tx_time >= df_provider["effective_from"]) & 
                          (tx_time <= df_provider["effective_to"])]
    return int(matched.iloc[0]["provider_key"]) if len(matched) > 0 else -1

def get_vehicle_key_pit(vehicle_id, tx_time):
    matched = df_vehicle[(df_vehicle["vehicle_id"] == vehicle_id) & 
                         (tx_time >= df_vehicle["effective_from"]) & 
                         (tx_time <= df_vehicle["effective_to"])]
    return int(matched.iloc[0]["vehicle_key"]) if len(matched) > 0 else -1

# ---------------------------------------------------------
# 4. GENERATE FACTS (QUOTATIONS, POLICIES, PAYMENTS, CANCELLATIONS)
# ---------------------------------------------------------
print("[INFO] Generating facts...")

fact_quotation_records = []
fact_quotation_item_records = []
fact_policy_records = []
fact_payment_records = []
fact_cancellation_records = []

# Map codes to keys
package_key_map = dict(zip(df_package["package_code"], df_package["package_key"]))
coverage_key_map = dict(zip(df_coverage["coverage_type"], df_coverage["coverage_key"]))
qstatus_key_map = dict(zip(df_qstatus["quotation_status_code"], df_qstatus["quotation_status_key"]))
pstatus_key_map = dict(zip(df_pstatus["policy_status_code"], df_pstatus["policy_status_key"]))
paystatus_key_map = dict(zip(df_paystatus["payment_status_code"], df_paystatus["payment_status_key"]))
paymethod_key_map = dict(zip(df_paymethod["payment_method_code"], df_paymethod["payment_method_key"]))
cancel_reason_key_map = dict(zip(df_cancellation_reason["cancellation_reason"], df_cancellation_reason["cancellation_reason_key"]))

num_quotations = 5000
dim_quo_id_rows = []
dim_pol_id_rows = []

policy_counter = 1
pay_counter = 1
cancel_counter = 1
item_counter = 1

for i in range(1, num_quotations + 1):
    q_id = f"QUO{i:05d}"
    cust = random.choice(cust_base_data)
    cust_id = cust["customer_id"]
    veh_id = cust["vehicle_id"]
    reg_date = cust["reg_date"]
    
    max_days = (datetime(2026, 5, 31) - reg_date).days
    if max_days <= 0:
        quotation_at = reg_date
    else:
        while True:
            offset = random.randint(0, max_days)
            candidate_dt = reg_date + timedelta(days=offset)
            candidate_dt = candidate_dt.replace(hour=random.randint(8, 18), minute=random.randint(0, 59), second=random.randint(0, 59))
            month = candidate_dt.month
            accept_prob = 1.0 if month in [11, 12] else (0.8 if month in [2, 3] else 0.5)
            if random.random() < accept_prob:
                quotation_at = candidate_dt
                break
                
    agent_id = random.choice(["AG001", "AG002", "AG003", "AG004"])
    conv_threshold = {"AG001": 0.50, "AG002": 0.30, "AG003": 0.35, "AG004": 0.18}[agent_id]
        
    provider_rand = random.random()
    if provider_rand < 0.42:
        provider_code, base_prem, provider_conv_mod = "BV", 3000000.0, +0.05
    elif provider_rand < 0.60:
        provider_code, base_prem, provider_conv_mod = "LIB", 8000000.0, -0.08
    elif provider_rand < 0.85:
        provider_code, base_prem, provider_conv_mod = "PVI", 5000000.0, 0.0
    else:
        provider_code, base_prem, provider_conv_mod = "MIC", 4500000.0, 0.0

    pkg_rand = random.random()
    package_code = "BASIC" if pkg_rand < 0.40 else ("STANDARD" if pkg_rand < 0.70 else ("PREMIUM" if pkg_rand < 0.90 else "VIP"))
    pkg_mod = {"BASIC": 0.8, "STANDARD": 1.0, "PREMIUM": 1.4, "VIP": 1.8}[package_code]
    premium = round(base_prem * pkg_mod * random.uniform(0.9, 1.1), -3)
    
    final_conv_prob = conv_threshold + provider_conv_mod
    if random.random() < final_conv_prob:
        quotation_status, converted_flag = "CONVERTED", True
    else:
        converted_flag = False
        sub_roll = random.random()
        quotation_status = "ACCEPTED" if sub_roll < 0.15 else ("REJECTED" if sub_roll < 0.45 else ("EXPIRED" if sub_roll < 0.75 else "QUOTED"))
            
    c_key = get_customer_key_pit(cust_id, quotation_at)
    a_key = get_agent_key_pit(agent_id, quotation_at)
    pr_key = get_provider_key_pit(provider_code, quotation_at)
    v_key = get_vehicle_key_pit(veh_id, quotation_at)
    q_key = i
    
    dim_quo_id_rows.append({"quotation_key": q_key, "quotation_id": q_id, "quotation_expiry_date": (quotation_at + timedelta(days=30)).date()})
    
    fact_quotation_records.append({
        "quotation_id": q_id, "customer_id": cust_id, "agent_id": agent_id, "provider_code": provider_code,
        "quotation_key": q_key, "customer_key": c_key, "agent_key": a_key, "provider_key": pr_key,
        "package_key": package_key_map[package_code], "quotation_status_key": qstatus_key_map[quotation_status],
        "quotation_date_key": int(quotation_at.strftime("%Y%m%d")), "quotation_expiry_date_key": int((quotation_at + timedelta(days=30)).strftime("%Y%m%d")),
        "vehicle_key": v_key, "premium_amount": Decimal(str(premium)), "converted_flag": converted_flag,
        "created_at": datetime.now(), "updated_at": datetime.now(), "_batch_id": "MOCK_BATCH_1",
        "_source_system": "CRM", "pipeline_run_id": "MOCK_SESSION_1", "is_deleted": False, "deleted_at": None, "delete_batch_id": None
    })
    
    num_items = random.randint(1, 3)
    coverages_selected = random.sample(["Physical Damage", "Theft", "Third Party Liability", "Personal Accident"], num_items)
    for cov in coverages_selected:
        cov_amt = round(premium * random.uniform(5, 15), -5)
        deduct_amt = 1000000.0 if cov == "Physical Damage" else 0.0
        
        fact_quotation_item_records.append({
            "quotation_item_id": f"QI{item_counter:06d}", "quotation_id": q_id, "quotation_key": q_key,
            "quotation_date_key": int(quotation_at.strftime("%Y%m%d")), "customer_key": c_key, "agent_key": a_key,
            "provider_key": pr_key, "package_key": package_key_map[package_code], "quotation_status_key": qstatus_key_map[quotation_status],
            "coverage_key": coverage_key_map[cov], "vehicle_key": v_key, "coverage_amount": Decimal(str(cov_amt)), "deductible_amount": Decimal(str(deduct_amt)),
            "created_at": datetime.now(), "updated_at": datetime.now(), "_batch_id": "MOCK_BATCH_1", "_source_system": "CRM",
            "pipeline_run_id": "MOCK_SESSION_1", "is_deleted": False, "deleted_at": None, "delete_batch_id": None
        })
        item_counter += 1
        
    if converted_flag:
        p_id = f"POL{policy_counter:05d}"
        p_num = f"CARPRO-{quotation_at.year}-{policy_counter:05d}"
        
        policy_start_dt = quotation_at + timedelta(days=random.randint(2, 7))
        policy_end_dt = policy_start_dt + timedelta(days=365)
        
        if policy_end_dt < SIM_DATE:
            policy_status = "EXPIRED"
        else:
            pol_roll = random.random()
            policy_status = "ISSUED" if pol_roll < 0.05 else ("CANCELLED" if pol_roll < 0.10 else "ACTIVE")
                
        c_key_p = get_customer_key_pit(cust_id, policy_start_dt)
        a_key_p = get_agent_key_pit(agent_id, policy_start_dt)
        pr_key_p = get_provider_key_pit(provider_code, policy_start_dt)
        v_key_p = get_vehicle_key_pit(veh_id, policy_start_dt)
        
        p_key = policy_counter
        dim_pol_id_rows.append({"policy_key": p_key, "policy_id": p_id})
        
        fact_policy_records.append({
            "policy_id": p_id, "policy_number": p_num, "quotation_id": q_id, "customer_id": cust_id, "provider_code": provider_code,
            "policy_key": p_key, "quotation_key": q_key, "customer_key": c_key_p, "provider_key": pr_key_p, "agent_key": a_key_p,
            "package_key": package_key_map[package_code], "policy_status_key": pstatus_key_map[policy_status],
            "issued_date_key": int(quotation_at.strftime("%Y%m%d")), "policy_start_date_key": int(policy_start_dt.strftime("%Y%m%d")), "policy_end_date_key": int(policy_end_dt.strftime("%Y%m%d")),
            "vehicle_key": v_key_p, "premium_amount": Decimal(str(premium)), "created_at": datetime.now(), "updated_at": datetime.now(),
            "_batch_id": "MOCK_BATCH_1", "_source_system": "CRM", "pipeline_run_id": "MOCK_SESSION_1", "is_deleted": False, "deleted_at": None, "delete_batch_id": None
        })
        
        if policy_status in ["ACTIVE", "EXPIRED", "CANCELLED"]:
            pay_roll = random.random()
            payment_status = "PAID" if pay_roll < 0.93 else ("FAILED" if pay_roll < 0.96 else "PENDING")
            payment_date = policy_start_dt + timedelta(days=random.randint(-2, 5)) if payment_status == "PAID" else (policy_start_dt + timedelta(days=2) if payment_status == "FAILED" else None)
        else:
            pay_roll = random.random()
            payment_status = "PENDING" if pay_roll < 0.50 else "FAILED"
            payment_date = None if payment_status == "PENDING" else policy_start_dt + timedelta(days=2)
                
        due_date_key = -1
        if payment_status == "PENDING":
            age_bucket_rand = random.random()
            if age_bucket_rand < 0.40:
                due_date = SIM_DATE - timedelta(days=random.randint(1, 7))
            elif age_bucket_rand < 0.70:
                due_date = SIM_DATE - timedelta(days=random.randint(8, 30))
            elif age_bucket_rand < 0.85:
                due_date = SIM_DATE - timedelta(days=random.randint(31, 60))
            elif age_bucket_rand < 0.95:
                due_date = SIM_DATE - timedelta(days=random.randint(61, 90))
            else:
                due_date = SIM_DATE - timedelta(days=random.randint(91, 150))
            due_date_key = int(due_date.strftime("%Y%m%d"))
        else:
            due_date_key = int(policy_start_dt.strftime("%Y%m%d"))

        pay_method = random.choice(["BANK_TRANSFER", "CREDIT_CARD", "E_WALLET"])
        
        fact_payment_records.append({
            "payment_id": f"PAY{pay_counter:05d}", "policy_id": p_id, "transaction_reference": f"TXN-{payment_status[:3]}-{random.randint(100000, 999999)}",
            "policy_key": p_key, "payment_status_key": paystatus_key_map[payment_status], "payment_method_key": paymethod_key_map[pay_method],
            "payment_date_key": int(payment_date.strftime("%Y%m%d")) if payment_date else -1, "issued_date_key": due_date_key,
            "customer_key": c_key_p, "provider_key": pr_key_p, "vehicle_key": v_key_p, "payment_amount": Decimal(str(premium)),
            "created_at": datetime.now(), "updated_at": datetime.now(), "_batch_id": "MOCK_BATCH_1", "_source_system": "CRM",
            "pipeline_run_id": "MOCK_SESSION_1", "is_deleted": False, "deleted_at": None, "delete_batch_id": None
        })
        pay_counter += 1
        
        if policy_status == "CANCELLED":
            cancellation_date = policy_start_dt + timedelta(days=random.randint(30, 180))
            refund = round((premium * (365.0 - (cancellation_date - policy_start_dt).days)) / 365.0, -3)
            reason = random.choice(["Customer Request", "Vehicle Sold", "Non-Payment", "Policy Re-written"])
            
            fact_cancellation_records.append({
                "cancellation_id": f"CAN{cancel_counter:05d}", "policy_id": p_id, "policy_key": p_key,
                "cancellation_reason_key": cancel_reason_key_map[reason], "cancellation_date_key": int(cancellation_date.strftime("%Y%m%d")),
                "customer_key": c_key_p, "provider_key": pr_key_p, "vehicle_key": v_key_p, "refund_amount": Decimal(str(refund)),
                "created_at": datetime.now(), "updated_at": datetime.now(), "_batch_id": "MOCK_BATCH_1", "_source_system": "CRM",
                "pipeline_run_id": "MOCK_SESSION_1", "is_deleted": False, "deleted_at": None, "delete_batch_id": None
            })
            cancel_counter += 1
            
        policy_counter += 1

print("[INFO] Finalizing header dictionaries...")
df_dim_quotation = add_metadata(pd.concat([df_dim_quotation, pd.DataFrame(dim_quo_id_rows)], ignore_index=True))
df_dim_policy = add_metadata(pd.concat([df_dim_policy, pd.DataFrame(dim_pol_id_rows)], ignore_index=True))

# ---------------------------------------------------------
# Helper to align schemas with actual Delta tables
# ---------------------------------------------------------
def cast_df_to_schema(df, target_table):
    target_schema = spark.table(target_table).schema
    select_exprs = []
    for field in target_schema.fields:
        col_name = field.name
        col_type = field.dataType
        if col_name in df.columns:
            select_exprs.append(F.col(col_name).cast(col_type).alias(col_name))
        else:
            select_exprs.append(F.lit(None).cast(col_type).alias(col_name))
    return df.select(*select_exprs)

# ---------------------------------------------------------
# Save to Delta Tables
# ---------------------------------------------------------
print("[INFO] Saving to Delta tables in gold schema...")

gold_tables = {
    "gold.dim_date": df_date,
    "gold.dim_package": df_package,
    "gold.dim_coverage": df_coverage,
    "gold.dim_quotation_status": df_qstatus,
    "gold.dim_policy_status": df_pstatus,
    "gold.dim_payment_status": df_paystatus,
    "gold.dim_payment_method": df_paymethod,
    "gold.dim_cancellation_reason": df_cancellation_reason,
    "gold.dim_quotation": df_dim_quotation,
    "gold.dim_policy": df_dim_policy,
    "gold.dim_provider": df_provider,
    "gold.dim_agent": df_agent,
    "gold.dim_customer": df_customer,
    "gold.dim_vehicle": df_vehicle,
    "gold.fact_quotation": pd.DataFrame(fact_quotation_records),
    "gold.fact_quotation_item": pd.DataFrame(fact_quotation_item_records),
    "gold.fact_policy": pd.DataFrame(fact_policy_records),
    "gold.fact_payment": pd.DataFrame(fact_payment_records),
    "gold.fact_cancellation": pd.DataFrame(fact_cancellation_records)
}

for table_name, pdf in gold_tables.items():
    print(f"[WRITE] Table {table_name}... ({len(pdf)} rows)")
    spark_df = spark.createDataFrame(pdf)
    
    # Try to align schema with the existing Delta table (to prevent delta field merge exceptions)
    try:
        spark_df = cast_df_to_schema(spark_df, table_name)
    except Exception as e:
        print(f"[WARNING] Could not align schema for {table_name}: {e}. Writing directly.")
        
    spark_df.write.format("delta").mode("overwrite").saveAsTable(table_name)

print("\n" + "="*50)
print("[SUCCESS] Mock Gold layer data generation completed successfully!")
print("="*50)

# Verify counts
spark.sql("""
SELECT 'dim_customer' AS table_name, COUNT(*) AS row_count FROM gold.dim_customer
UNION ALL SELECT 'dim_agent', COUNT(*) FROM gold.dim_agent
UNION ALL SELECT 'dim_provider', COUNT(*) FROM gold.dim_provider
UNION ALL SELECT 'dim_vehicle', COUNT(*) FROM gold.dim_vehicle
UNION ALL SELECT 'dim_date', COUNT(*) FROM gold.dim_date
UNION ALL SELECT 'fact_quotation', COUNT(*) FROM gold.fact_quotation
UNION ALL SELECT 'fact_quotation_item', COUNT(*) FROM gold.fact_quotation_item
UNION ALL SELECT 'fact_policy', COUNT(*) FROM gold.fact_policy
UNION ALL SELECT 'fact_payment', COUNT(*) FROM gold.fact_payment
UNION ALL SELECT 'fact_cancellation', COUNT(*) FROM gold.fact_cancellation
""").show()
