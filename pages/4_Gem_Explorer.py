import streamlit as st
import pandas as pd
from datetime import datetime

# --- MOCK GEMINI TECHNICAL METADATA ---
def get_gemini_index_metadata(config):
    """
    Simulates fetching the current state of the Gemini Vector Index.
    """
    metadata = []
    for wb in config.get('workbooks', []):
        for db in wb.get('dashboards', []):
            gem_cfg = db.get('gem_config', {})
            metadata.append({
                "Gem Name": gem_cfg.get('gem_name', 'Unnamed Gem'),
                "Dashboard Context": db['dashboard_name'],
                "Index Path": f"gs://{config['project_name']}/{wb['workbook_name']}/{db['dashboard_name']}",
                "Model Configuration": "Gemini-1.5-Flash-Knowledge-Engine",
                "Last Successful Sync": datetime.now().strftime("%Y-%m-%d %H:%M"),
                "Persona Summary": gem_cfg.get('system_persona', 'General Analyst')[:50] + "...",
                "Update Frequency": gem_cfg.get('update_frequency', 'On-Demand')
            })
    return pd.DataFrame(metadata)

# --- UI LOGIC ---
st.set_page_config(layout="wide")
st.title("💎 Gemini Knowledge Health")
st.markdown("Monitor the live context and security boundaries of your Gemini Knowledge Base.")

# Ensure config is available
if 'config' in st.session_state and st.session_state.config:
    config = st.session_state.config
    
    # 1. Technical Health Overview
    st.subheader("🧠 Gemini Index Status")
    df_meta = get_gemini_index_metadata(config)
    st.dataframe(df_meta, use_container_width=True, hide_index=True)

    # 2. Knowledge Hierarchy (Dynamic Tree)
    st.divider()
    st.write("### 🏗️ Knowledge Hierarchy")
    
    # We use session logs if they exist, otherwise we mock the current tree
    if 'gov_logs' in st.session_state and st.session_state.gov_logs:
        df = pd.DataFrame(st.session_state.gov_logs)
        hierarchy = df.groupby(['Project', 'Workbook', 'Dashboard']).agg({
            'Sheet_Tag': 'count', 
            'Masked_Cols': 'first'
        }).reset_index()
        hierarchy.columns = ['Project', 'Workbook', 'Dashboard', 'Total_Files', 'Masked_PII']
        st.dataframe(hierarchy, use_container_width=True, hide_index=True)
    else:
        st.info("No live logs found. Displaying planned hierarchy from JSON.")

    # 3. Dynamic System Persona Generator
    st.divider()
    st.subheader("📝 Live Gem Instruction Preview")
    
    selected_db = st.selectbox("Choose a Dashboard to see its AI System Prompt:", df_meta['Dashboard Context'].unique())
    
    # Extract specific config for the selected dashboard
    db_details = next(db for wb in config['workbooks'] for db in wb['dashboards'] if db['dashboard_name'] == selected_db)
    gem_cfg = db_details.get('gem_config', {})
    
    prompt = f"""
    ### SYSTEM PROMPT FOR: {gem_cfg.get('gem_name')}
    
    **Identity:** {gem_cfg.get('system_persona')}
    
    **Knowledge Access:** You have read-access to files in the '{selected_db}' directory. 
    Focus your insights on: {", ".join(gem_cfg.get('insight_focus', []))}
    
    **Security & Privacy (ISO-27001):**
    The following fields are MASKED (***): {", ".join(db_details.get('mask_columns', []))}.
    - Do not attempt to reverse-engineer masked data.
    - If a user asks for PII, state that it is redacted per governance standards.
    """
    st.code(prompt, language="markdown")

else:
    st.info("👋 Please upload your **Hierarchy Config JSON** on the main page to view Gem Health.")