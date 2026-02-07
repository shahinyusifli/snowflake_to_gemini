import streamlit as st

# 1. Page Configuration
st.set_page_config(
    page_title="Snowflake to Gemini Orchestrator",
    page_icon="🚀",
    layout="wide"
)

# 2. Construction Notice & Contact Details
st.warning("🏗️ **Site Under Construction:** We are currently finalizing the core orchestration engine.")
st.info("📧 For technical inquiries or early access, please contact: **sahinyusifli98@gmail.com**")

st.divider()

# 3. Project Introduction & Logic
st.title("🚀 Snowflake to Gemini Pipeline")
st.markdown("""
### **Orchestration Logic**
This application provides a secure bridge between **Snowflake** and **Google Gemini**, designed specifically to analyze **Tableau Report Results**.

* **Snowflake Extraction:** The pipeline targets curated "Gold Layer" tables—the same ones powering your production **Tableau** dashboards.
* **Business Integrity:** By focusing only on report results, we ensure the AI analyzes verified business metrics rather than raw, noisy data.
* **Gemini Sync:** The extracted results are automatically indexed into a Gemini Knowledge Base for instant executive summaries.
""")

# 4. JSON Configuration Example
st.subheader("📝 Pipeline Configuration JSON")
st.markdown("Upload a JSON file in this format to define your **Project Hierarchy** and **Gemini Config**.")

dag_example = {
    "project_name": "Finance_Global",
    "governance_standard": "ISO-27001-Compliance",
    "workbooks": [
        {
            "workbook_name": "Executive_Q1_Review",
            "owner": "finance_ops_team",
            "dashboards": [
                {
                    "dashboard_name": "Tableau_Revenue_Tracking",
                    "description": "Finalized revenue as seen in Tableau Q1 reports.",
                    "sheets": [
                        "TABLEAU.FIN.REV_TRACK.NORTH_AMERICA", 
                        "TABLEAU.FIN.REV_TRACK.EUROPE"
                    ],
                    "mask_columns": ["CUSTOMER_NAME", "EMAIL"],
                    "gem_config": {
                        "gem_name": "Revenue Analyst Gem",
                        "system_persona": "You are a senior analyst specialized in Tableau report data.",
                        "insight_focus": ["YoY Growth", "Outlier Detection"]
                    }
                }
            ]
        }
    ]
}

# Use st.code for a copy-pasteable block or st.json for an interactive view
tab1, tab2 = st.tabs(["Copy-Paste Code", "Interactive Viewer"])
with tab1:
    st.code(dag_example, language="json")
with tab2:
    st.json(dag_example)

st.sidebar.success("Select a module above to explore.")