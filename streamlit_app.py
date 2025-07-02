import streamlit as st
import json
import os
import tempfile
import graphviz
from refined_extractor import extract_from_file, export_to_json
import glob
import pandas as pd
import zipfile
import shutil
import re

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

def folder_summary_page(file_paths, model):
    results = []
    all_lineage = []
    file_results = {}
    for path in file_paths:
        try:
            result = extract_from_file(path, model)
            results.append({
                "File": os.path.basename(path),
                "Path": path,
                "Lineage Steps": len(result.get("lineage", [])),
                "SQL Queries": len(result.get("sql_queries", [])),
                "Status": "Success"
            })
            all_lineage.extend(result.get("lineage", []))
            file_results[path] = result
        except Exception as e:
            results.append({
                "File": os.path.basename(path),
                "Path": path,
                "Lineage Steps": 0,
                "SQL Queries": 0,
                "Status": f"Error: {e}"
            })
    df = pd.DataFrame(results)
    st.header("\U0001F4CA Folder Scan Summary")
    st.dataframe(df)
    if all_lineage:
        st.header("\U0001F310 Aggregate Lineage Graph (All Files)")
        render_lineage_graph(all_lineage)
    st.header("\U0001F4C2 File Details")
    for idx, path in enumerate(file_paths):
        with st.expander(f"{os.path.basename(path)} ({path})"):
            if path in file_results:
                result = file_results[path]
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
                        st.subheader(f"Query {i+1}: {q.get('query_type', 'N/A')} in `{q.get('method_name', 'N/A')}`")
                        st.write(f"**Purpose:** {q.get('purpose', 'Not specified')}")
                        st.write(f"**Source Tables:** {q.get('source_tables', [])}")
                        st.write(f"**Target Tables:** {q.get('target_tables', [])}")
                        st.code(q.get('sql_text', ''), language="sql")
                        st.markdown('---')
                with tabs[3]:
                    st.header("Detailed Data Mapping")
                    st.json(result.get("mapping", {}))
                with tabs[4]:
                    st.header("Raw JSON Output")
                    
                    st.json(result)
                    st.download_button(
                        "Download JSON",
                        data=json.dumps(result, indent=2),
                        file_name=f"{os.path.basename(path)}_data_lineage_output.json",
                        mime="application/json"
                    )
            else:
                st.error("No results for this file.")

def find_referenced_files(file_paths):
    referenced = set()
    for path in file_paths:
        try:
            if path.endswith('.py'):
                with open(path, 'r', encoding='utf-8', errors='ignore') as f:
                    code = f.read()
                    # Python imports
                    for match in re.findall(r'import\s+([\w_]+)', code):
                        referenced.add(match + '.py')
                    for match in re.findall(r'from\s+([\w_]+)\s+import', code):
                        referenced.add(match + '.py')
                    # open('filename')
                    for match in re.findall(r'open\(["\']([^"\']+)["\']', code):
                        referenced.add(match)
                    # pandas read_csv/read_excel
                    for match in re.findall(r'read_csv\(["\']([^"\']+)["\']', code):
                        referenced.add(match)
                    for match in re.findall(r'read_excel\(["\']([^"\']+)["\']', code):
                        referenced.add(match)
            elif path.endswith('.sql') or path.endswith('.txt'):
                with open(path, 'r', encoding='utf-8', errors='ignore') as f:
                    code = f.read()
                    # SQL: look for INCLUDE, LOAD, or file path references
                    for match in re.findall(r'INCLUDE\s+["\']([^"\']+)["\']', code, re.IGNORECASE):
                        referenced.add(match)
                    for match in re.findall(r'LOAD\s+["\']([^"\']+)["\']', code, re.IGNORECASE):
                        referenced.add(match)
                    # Generic file path patterns
                    for match in re.findall(r'["\']([^"\']+\.(py|sql|txt|csv|json|xlsx))["\']', code):
                        referenced.add(match[0])
        except Exception:
            continue
    return referenced

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

uploaded_zip = st.sidebar.file_uploader("Upload a folder (as ZIP)", type=["zip"])
file_paths = []
skipped_files = []
if uploaded_zip:
    with tempfile.TemporaryDirectory() as tmpdir:
        zip_path = os.path.join(tmpdir, "uploaded.zip")
        with open(zip_path, "wb") as f:
            f.write(uploaded_zip.getvalue())
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(tmpdir)
        # Index files, include only supported types (ignore Excel and CSV files)
        supported_exts = ('.py', '.sql', '.txt', '.json', '.java', '.scala')
        for root, dirs, files in os.walk(tmpdir):
            for file in files:
                if file.endswith(supported_exts):
                    file_paths.append(os.path.join(root, file))
                else:
                    skipped_files.append(os.path.join(root, file))
        if not file_paths:
            st.warning(f"No supported files (.py, .sql, .txt, .json, .java, .scala) found in the uploaded ZIP.")
        else:
            st.success(f"{len(file_paths)} files ready for analysis.")
            folder_summary_page(file_paths, model)
        if skipped_files:
            st.info(f"The following files were skipped (unsupported types):\n" + '\n'.join([os.path.relpath(f, tmpdir) for f in skipped_files]))
else:
    # Fallback to previous logic for single file or file path
    input_method = st.sidebar.radio("Choose input method", ["Upload a File", "Provide a File Path"])
    if input_method == "Upload a File":
        uploaded_file = st.sidebar.file_uploader("Upload your code", type=["py", "txt", "sql"])
        
        if uploaded_file:
            with tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(uploaded_file.name)[1]) as tmp:
                tmp.write(uploaded_file.getvalue())
                file_paths = [tmp.name]
    elif input_method == "Provide a File Path":
        file_path = st.sidebar.text_input("Enter the file path", value="Test_file/data_movement_processor.py")
        if file_path:
            file_paths = [file_path]
    if file_paths:
        if len(file_paths) == 1:
            path = file_paths[0]
            if os.path.exists(path):
                main_page(path, model)
            else:
                st.error(f"File not found: {path}")
    else:
        st.info("Please provide a file or folder to begin the analysis.")

st.markdown("---")
st.caption("Powered by Streamlit, Groq, and advanced code analysis techniques.")
