import streamlit as st
import pandas as pd
import random
from datetime import datetime, timedelta

def generate_quality_mock(config):
    """
    Generates mock results for the Gemini Intelligence Test (Task 3).
    Metrics follow real-world AI performance standards.
    """
    quality_results = []
    
    for wb in config.get('workbooks', []):
        for db in wb.get('dashboards', []):
            focus_areas = db.get('gem_config', {}).get('insight_focus', ["General Analysis"])
            
            for sheet in db.get('sheets', []):
                # Simulated metrics based on typical Gemini performance
                latency = round(random.uniform(3.5, 7.2), 2)  # Average ~4.5s
                accuracy = round(random.uniform(0.92, 0.99), 2) # Schema validation score
                
                # Determine outcome based on retries
                outcome = random.choices(
                    ["PASS", "RETRY_SUCCESS", "FAIL"], 
                    weights=[85, 12, 3]
                )[0]

                quality_results.append({
                    "Task ID": f"VAL_{sheet}",
                    "Sheet": sheet,
                    "AI Model": db.get('gem_config', {}).get('gem_name', 'Gemini 1.5 Pro'),
                    "Validation Test": f"Verify: {random.choice(focus_areas)}",
                    "Latency (s)": latency,
                    "Accuracy Score": f"{accuracy:.1%}",
                    "Result": outcome,
                    "Timestamp": datetime.now().strftime("%H:%M:%S")
                })
    return pd.DataFrame(quality_results)

# --- UI Rendering for 3_Data_Quality.py ---
if 'config' in st.session_state:
    st.subheader("🤖 Task 3: Automated AI Knowledge Validation")
    df_quality = generate_quality_mock(st.session_state.config)
    
    # Airflow-style status mapping
    def style_status(val):
        color = '#00ff00' if val == "PASS" else '#ffa500' if val == "RETRY_SUCCESS" else '#ff0000'
        return f'color: {color}; font-weight: bold;'

    st.dataframe(
        df_quality.style.applymap(style_status, subset=['Result']),
        use_container_width=True,
        hide_index=True
    )