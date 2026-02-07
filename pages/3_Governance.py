import streamlit as st
import pandas as pd
import json
import random
from datetime import datetime, timedelta

# --- EXPANDED MOCK DATA GENERATOR ---
def generate_mock_logs(config):
    """
    Generates realistic audit entries mimicking a multi-step Airflow DAG:
    1. Folder Sensor Check
    2. Snowflake Extraction
    3. PII Masking
    4. Gemini Knowledge Sync
    """
    mock_data = []
    base_time = datetime.now() - timedelta(minutes=30)
    
    # Track which folders were "Newly Created" vs "Existing"
    # In a real run, the first time is CREATED, subsequent are EXISTS
    folder_states = {}

    for wb in config.get('workbooks', []):
        wb_name = wb['workbook_name']
        
        for db in wb.get('dashboards', []):
            db_name = db['dashboard_name']
            mask_cols = db.get('mask_columns', [])
            
            # Simulate Task 1: Folder Sensor
            state_key = f"{wb_name}/{db_name}"
            if state_key not in folder_states:
                folder_states[state_key] = random.choice(["NEWLY CREATED", "VERIFIED_EXISTING"])
            
            for i, sheet in enumerate(db.get('sheets', [])):
                # Stagger timestamps to look like a real sequence
                task_time = base_time + timedelta(minutes=i*2)
                
                # Vary row counts based on the dashboard type
                if "Revenue" in db_name:
                    rows = random.randint(10000, 50000)
                elif "Cost" in db_name:
                    rows = random.randint(500, 5000)
                else: # HR
                    rows = random.randint(100, 1000)

                mock_data.append({
                    "Timestamp": task_time.strftime("%Y-%m-%d %H:%M:%S"),
                    "Workbook": wb_name,
                    "Dashboard": db_name,
                    "Sheet_Tag": sheet,
                    "Action": "PIPELINE_SUCCESS",
                    "Folder_Status": folder_states[state_key],
                    "Masked_Cols": ", ".join(mask_cols) if mask_cols else "NONE",
                    "Rows": rows,
                    "Gemini_Sync": random.choice(["✅ SUCCESS", "✅ SUCCESS", "⏳ RETRY_SYNC"]), # Simulate intermittent lag
                    "Owner": wb.get('owner', 'N/A')
                })
                
    return pd.DataFrame(mock_data)

# --- UI LOGIC ---
st.set_page_config(layout="wide", page_title="Governance & Audit")
st.title("🛡️ Data Governance & Audit Log")

# Ensure config is loaded into session state for the preview
if 'config' not in st.session_state:
    # Use your provided JSON as the default for this example
    st.session_state.config = {
        "project_name": "Finance_Global",
        "governance_standard": "ISO-27001-Compliance",
        "workbooks": [
            {
                "workbook_name": "Q1_Review",
                "owner": "finance_ops_team",
                "dashboards": [
                    {
                        "dashboard_name": "Revenue_Tracking",
                        "sheets": ["FIN.Q1.REV_TRACK.NORTH_AMERICA", "FIN.Q1.REV_TRACK.EUROPE", "FIN.Q1.REV_TRACK.APAC"],
                        "mask_columns": ["CUSTOMER_NAME", "EMAIL", "PHONE_NUMBER"]
                    },
                    {
                        "dashboard_name": "Cost_Control",
                        "sheets": ["FIN.Q1.COST.OPEX_DETAIL", "FIN.Q1.COST.VENDORS"],
                        "mask_columns": ["VENDOR_BANK_DETAILS", "SSN"]
                    }
                ]
            },
            {
                "workbook_name": "Human_Capital_Management",
                "owner": "hr_analytics",
                "dashboards": [
                    {
                        "dashboard_name": "Headcount_Forecasting",
                        "sheets": ["HR.HCM.HEADCOUNT.GLOBAL", "HR.HCM.TURNOVER.ATTRITION"],
                        "mask_columns": ["EMPLOYEE_ID", "HOME_ADDRESS", "SALARY_EXACT"]
                    }
                ]
            }
        ]
    }

config = st.session_state.config

# 1. TOP LEVEL METRICS
df_gov = generate_mock_logs(config)
c1, c2, c3, c4 = st.columns(4)
c1.metric("Governance Standard", config.get('governance_standard', 'N/A'))
c2.metric("Total Files Extracted", len(df_gov))
c3.metric("Total Rows Processed", f"{df_gov['Rows'].sum():,}")
c4.metric("Retention Policy", f"{config.get('data_retention_days', 90)} Days")

st.divider()

# 2. THE AUDIT TABLE
st.subheader("📝 Activity Log & Masking Audit")
# Color coding the Gemini Sync column
def color_sync(val):
    color = 'green' if 'SUCCESS' in val else 'orange'
    return f'color: {color}'

st.dataframe(
    df_gov.style.applymap(color_sync, subset=['Gemini_Sync']),
    use_container_width=True,
    hide_index=True
)

st.divider()

# 3. FOLDER HIERARCHY TREE
st.subheader("📂 Directory & Data Lineage")
for wb in config.get('workbooks', []):
    with st.expander(f"📘 Workbook: {wb['workbook_name']} (Owner: {wb['owner']})"):
        for db in wb.get('dashboards', []):
            st.markdown(f"&nbsp;&nbsp;&nbsp;&nbsp;📂 Dashboard Sub-folder: `{db['dashboard_name']}`")
            # Show masking policy for this specific folder
            st.caption(f"&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;🛡️ Masking Policy: {', '.join(db.get('mask_columns', []))}")
            
            for sheet in db.get('sheets', []):
                st.write(f"&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;📄 `{sheet}.json`")

# 4. DOWNLOAD LOGS
st.download_button("📥 Download Audit CSV", df_gov.to_csv(index=False), "governance_audit.csv", "text/csv")