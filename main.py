import streamlit as st
import json
import tempfile
import os
from tools.extractor import extract_with_sqlglot, extract_with_sqlparser, extract_with_llm, ensemble_results
from tools.sql_lineage_extractor import LLMSQLLineageExtractor
import pandas as pd

# Initialize session state
if 'queries' not in st.session_state:
    st.session_state['queries'] = None
if 'lineage' not in st.session_state:
    st.session_state['lineage'] = None
if 'file_content' not in st.session_state:
    st.session_state['file_content'] = None

def navbar():
    st.markdown(
        """
        <head><meta charset="UTF-8">
        <link href="https://cdnjs.cloudflare.com/ajax/libs/mdbootstrap/4.19.1/css/mdb.min.css" rel="stylesheet">
        <link href="https://cdnjs.cloudflare.com/ajax/libs/bootstrap/5.3.2/css/bootstrap.min.css" rel="stylesheet">
        <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/4.0.0/css/bootstrap.min.css" integrity="sha384-Gn5384xqQ1aoWXA+058RXPxPg6fy4IWvTNh0E263XmFcJlSAwiGgFAW/dAiS6JXm" crossorigin="anonymous">
        <style>
            header {visibility: hidden;}
            .main {
                margin-top: 80px;
                padding-top: 10px;
            }
            #MainMenu {visibility: hidden;}
            footer {visibility: hidden;}
            .navbar {
                padding: 1rem;
                margin-bottom: 2rem;
                background-color: #4267B2;
                color: white;
                z-index: 10;
                position: fixed;
                width: 100%;
            }
            .card {
                padding: 1rem;
                margin-bottom: 1rem;
                transition: transform 0.2s;
                border-radius: 5px;
                box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            }
            .query-card {
                background-color: #f8f9fa;
                border: 1px solid #dee2e6;
                border-radius: 5px;
                padding: 10px;
                margin-bottom: 10px;
            }
            .lineage-table {
                font-size: 14px;
                margin-top: 20px;
            }
            .status-message {
                padding: 10px;
                border-radius: 5px;
                margin: 10px 0;
            }
            .success {
                background-color: #d4edda;
                color: #155724;
            }
            .error {
                background-color: #f8d7da;
                color: #721c24;
            }
        </style>
       <nav class="navbar fixed-top navbar-expand-lg navbar-dark text-bold shadow-sm" >
            <a class="navbar-brand-logo">
                <img src="https://www.4th-ir.com/favicon.ico" style='width:50px'>
                Universal Scanner 
            </a>
        </nav>
        """,
        unsafe_allow_html=True
    )

def process_file_content(file_content):
    """Process the file content to extract queries and analyze lineage"""
    temp_file_path = None
    temp_queries_file = None
    temp_lineage_file = None
    
    try:
        # Create a temporary file to store the content
        with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as temp_file:
            temp_file.write(file_content)
            temp_file_path = temp_file.name

        # Extract queries using all methods
        sqlglot_queries = extract_with_sqlglot(file_content)
        sqlparser_queries = extract_with_sqlparser(file_content)
        llm_queries = extract_with_llm(temp_file_path, file_content) if temp_file_path else []
        
        # Combine results
        all_queries = ensemble_results(sqlglot_queries, sqlparser_queries, llm_queries)
        
        if not all_queries:
            st.warning("No SQL queries were found in the uploaded file.")
            return None, None
        
        # Save queries to session state
        st.session_state['queries'] = all_queries
        
        # Initialize lineage extractor
        lineage_extractor = LLMSQLLineageExtractor()
        
        # Save queries to temporary file for lineage analysis
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as temp_queries:
            json.dump({"queries": all_queries}, temp_queries)
            temp_queries_file = temp_queries.name
        
        # Process lineage
        with tempfile.NamedTemporaryFile(suffix='.json', delete=False) as temp_lineage:
            temp_lineage_file = temp_lineage.name
            lineage_extractor.process_json_file(temp_queries_file, temp_lineage_file)
        
        # Read lineage results
        try:
            with open(temp_lineage_file, 'r') as f:
                lineage_data = json.load(f)
                # Save lineage to session state
                st.session_state['lineage'] = lineage_data
        except json.JSONDecodeError:
            st.error("Failed to parse lineage data")
            lineage_data = None
        
        return all_queries, lineage_data
        
    except Exception as e:
        st.error(f"Error processing file: {str(e)}")
        return None, None
        
    finally:
        # Clean up temporary files
        for file_path in [temp_file_path, temp_queries_file, temp_lineage_file]:
            try:
                if file_path and os.path.exists(file_path):
                    os.unlink(file_path)
            except Exception:
                pass

def display_queries(queries):
    """Display extracted queries in the sidebar"""
    if not queries:
        st.sidebar.warning("No queries found")
        return
        
    st.sidebar.header("Extracted Queries")
    
    for idx, query in enumerate(queries, 1):
        with st.sidebar.expander(f"Query {idx} - {query.get('query_type', 'Unknown')}"):
            st.code(query['query'], language='sql')
            st.write(f"Confidence: {query.get('confidence', 'N/A')}")
            st.write(f"Source: {query.get('source', 'N/A')}")
            st.write(f"Line number: {query.get('line_number', 'N/A')}")

def display_lineage(lineage_data):
    """Display lineage information in both JSON and table format"""
    st.header("Data Lineage Analysis")
    
    # Display as JSON
    with st.expander("View Raw Lineage JSON"):
        st.json(lineage_data)
    
    # Display as table
    if lineage_data and isinstance(lineage_data, list):
        df = pd.DataFrame(lineage_data)
        st.subheader("Lineage Table View")
        st.dataframe(df, use_container_width=True)
    else:
        st.warning("No lineage data available or invalid format")

def file_uploader():
    """Handle file upload in the sidebar"""
    st.sidebar.title("File Upload")
    file_path = st.sidebar.file_uploader("Upload a Python file", type=["py"])
    
    if file_path:
        file_content = file_path.read().decode('utf-8')
        # Save file content to session state
        st.session_state['file_content'] = file_content
        st.sidebar.success(f"File uploaded: {file_path.name}")
        
        if st.sidebar.button("Extract Queries & Lineage"):
            with st.spinner("Processing file..."):
                queries, lineage = process_file_content(file_content)
                return True
    return False

def main():
    navbar()
    
    # Display queries from session state if available
    if st.session_state['queries']:
        display_queries(st.session_state['queries'])
    
    # Handle file upload and processing
    processed = file_uploader()
    
    # Display lineage if available
    if st.session_state['lineage']:
        display_lineage(st.session_state['lineage'])
    elif not st.session_state['queries']:
        st.info("Please upload a Python file and click 'Extract Queries & Lineage' to begin analysis")

if __name__ == "__main__":
    main()
