import streamlit as st
import json
import os
import tempfile
import graphviz
from refined_extractor import extract_from_file, export_to_json

st.set_page_config(page_title="Data Lineage Analyzer", page_icon="🔗", layout="wide")

# --- UI Components ---

def render_lineage_graph(lineage_data):
    """Renders a directed graph of the data lineage."""
    if not lineage_data:
        st.warning("No lineage data to display.")
        return

    dot = graphviz.Digraph('DataLineage', comment='Data Flow')
    dot.attr('graph', rankdir='LR', size='12,8', splines='ortho')
    dot.attr('node', shape='box', style='rounded,filled', fillcolor='lightblue')
    dot.attr('edge', color='gray40', arrowhead='open')

    nodes = set()
    for step in lineage_data:
        source = step.get('source', 'Unknown Source')
        target = step.get('target', 'Unknown Target')
        if source not in nodes:
            dot.node(source, source)
            nodes.add(source)
        if target not in nodes:
            dot.node(target, target)
            nodes.add(target)
        
        dot.edge(source, target, label=f"Step {step.get('step')}")

    st.graphviz_chart(dot)

def main_page(file_path, model):
    """Renders the main analysis page for a given file."""
    st.success(f"Ready to analyze: {os.path.basename(file_path)}")
    
    with st.expander("File Preview"):
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                st.code(f.read(), language="python")
        except Exception as e:
            st.error(f"Could not read file: {e}")
            return

    if st.button("🚀 Analyze Data Lineage", key="analyze_button"):
        with st.spinner("Analyzing code... This may take a moment."):
            result = extract_from_file(file_path, model)
            st.session_state.extraction_result = result

    if 'extraction_result' in st.session_state and st.session_state.extraction_result:
        result = st.session_state.extraction_result
        st.success("Analysis complete!")

        tabs = st.tabs(["Lineage Graph", "Lineage Steps", "SQL Queries", "Detailed Mapping", "Raw JSON"])
        
        with tabs[0]:
            st.header("Visual Data Flow")
            render_lineage_graph(result.get("lineage", []))

        with tabs[1]:
            st.header("Data Lineage Steps")
            for step in result.get("lineage", []):
                st.write(f"**Step {step.get('step')}**: {step.get('description')}")
                st.write(f"*Source:* `{step.get('source')}` ➔ *Target:* `{step.get('target')}`")
                st.markdown("---")

        with tabs[2]:
            st.header("Extracted SQL Queries")
            for i, q in enumerate(result.get("sql_queries", [])):
                with st.expander(f"Query {i+1}: {q.get('query_type', 'N/A')} in `{q.get('method_name', 'N/A')}`"):
                    st.write(f"**Purpose:** {q.get('purpose', 'Not specified')}")
                    st.write(f"**Source Tables:** {q.get('source_tables', [])}")
                    st.write(f"**Target Tables:** {q.get('target_tables', [])}")
                    st.code(q.get('sql_text', ''), language="sql")

        with tabs[3]:
            st.header("Detailed Data Mapping")
            st.json(result.get("mapping", {}))

        with tabs[4]:
            st.header("Raw JSON Output")
            st.json(result)
            st.download_button(
                "Download JSON",
                data=json.dumps(result, indent=2),
                file_name="data_lineage_output.json",
                mime="application/json"
            )

# --- App Layout ---

st.title("🔗 Data Lineage Analyzer")
st.write("Upload your Python data processing scripts or other text files to automatically extract and visualize data lineage, mappings, and SQL queries.")

# Sidebar for configuration
st.sidebar.header("Configuration")
model = st.sidebar.selectbox(
    "Choose a Model",
    ["deepseek-r1-distill-llama-70b", "llama-3.1-8b-instant", "mixtral-8x7b-32768"],
    index=0,
    help="Select the LLM for analysis. The default model is recommended for its balance of speed and accuracy."
)

input_method = st.sidebar.radio("Choose input method", ["Upload a File", "Provide a File Path"])

file_path = None
if input_method == "Upload a File":
    uploaded_file = st.sidebar.file_uploader("Upload your code", type=["py", "txt"])
    if uploaded_file:
        with tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(uploaded_file.name)[1]) as tmp:
            tmp.write(uploaded_file.getvalue())
            file_path = tmp.name
else:
    file_path = st.sidebar.text_input("Enter the file path", value="Test_file/data_movement_processor.py")

if file_path:
    if os.path.exists(file_path):
        main_page(file_path, model)
    else:
        st.error("File not found. Please check the path.")
else:
    st.info("Please provide a file to begin the analysis.")

st.markdown("---")
st.caption("Powered by Streamlit, Groq, and advanced code analysis techniques.")
