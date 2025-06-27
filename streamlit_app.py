import streamlit as st
import json
import os
import tempfile
from refined_extractor import extract_from_file, export_to_json

st.set_page_config(page_title="Data Lineage Extractor", page_icon="🔗", layout="wide")

st.title("🔗 Data Lineage Extractor")
st.write("Extract data lineage, mapping, and SQL queries from Python data movement code.")

# Sidebar
st.sidebar.header("Configuration")
model = st.sidebar.selectbox(
    "Model",
    ["deepseek-r1-distill-llama-70b", "llama-3.1-8b-instant", "mixtral-8x7b-32768"],
    index=0
)
input_method = st.sidebar.radio("Input method", ["Upload File", "Enter File Path"])
file_path = None

if input_method == "Upload File":
    uploaded_file = st.sidebar.file_uploader("Upload Python file", type=["py"])
    if uploaded_file:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".py") as tmp:
            tmp.write(uploaded_file.getvalue())
            file_path = tmp.name
else:
    file_path = st.sidebar.text_input("File path", value="Test_file/data_movement_processor.py")

if file_path and os.path.exists(file_path):
    st.success(f"File found: {file_path}")
    with st.expander("File Preview"):
        with open(file_path, "r", encoding="utf-8") as f:
            st.code(f.read(), language="python")
    if st.button("🚀 Extract Data Lineage"):
        with st.spinner("Extracting..."):
            result = extract_from_file(file_path, model)
            if result:
                st.success("Extraction complete!")
                tabs = st.tabs(["Lineage", "Mapping", "SQL Queries", "Raw JSON", "Export"])
                with tabs[0]:
                    st.subheader("Lineage Steps")
                    for step in result.get("lineage", []):
                        st.write(f"**Step {step.get('step')}**: {step.get('description')}")
                        st.write(f"Source: `{step.get('source')}` → Destination: `{step.get('destination')}`")
                        st.markdown("---")
                with tabs[1]:
                    st.subheader("Mapping")
                    st.json(result.get("mapping", {}))
                with tabs[2]:
                    st.subheader("SQL Queries")
                    for i, q in enumerate(result.get("sql_queries", [])):
                        with st.expander(f"Query {i+1}: {q.get('query_type')} in {q.get('method_name')}"):
                            st.write(f"**Purpose:** {q.get('purpose')}")
                            st.write(f"**Source Tables:** {', '.join(q.get('source_tables', []) or [])}")
                            st.write(f"**Target Tables:** {', '.join(q.get('target_tables', []) or [])}")
                            st.code(q.get('sql_text', ''), language="sql")
                with tabs[3]:
                    st.subheader("Raw JSON")
                    st.json(result)
                with tabs[4]:
                    st.download_button(
                        "Download JSON",
                        data=json.dumps(result, indent=2),
                        file_name="data_lineage_output.json",
                        mime="application/json"
                    )
                    if st.button("Save to extraction_output.json"):
                        export_to_json(result, "extraction_output.json")
                        st.success("Saved to extraction_output.json")
            else:
                st.error("Extraction failed.")
else:
    st.info("Please select a file to begin.")

st.markdown("---")
st.caption("Built with Streamlit and Groq API")