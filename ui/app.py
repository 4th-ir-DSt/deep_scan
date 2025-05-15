import logging
from datetime import datetime
import sys
import json
from pathlib import Path
import pandas as pd
import streamlit as st

# Get the absolute path to the project root
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Add the project root to Python path if it's not already there
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    from config.settings import settings
    from parser.parser import CodeParser
    from extractor.extractor import SQLExtractor
except ImportError as e:
    st.error(f"Error importing modules: {str(e)}")
    st.error(f"Python path: {sys.path}")
    st.error(f"Project root: {PROJECT_ROOT}")
    st.stop()

# Configure logging
logging.basicConfig(level=logging.INFO)

def main():
    st.set_page_config(
        page_title="Universal Deep Scanner POC",
        page_icon="🔍",
        layout="wide"
    )

    # Add a visually appealing, modern header
    st.markdown(
        """
        <div style='
            background: linear-gradient(90deg, rgba(37,99,235,0.7) 0%, rgba(59,130,246,0.7) 100%);
            padding: 1rem 1.5rem;
            border-radius: 16px;
            margin-bottom: 2.5rem;
            box-shadow: 0 4px 16px rgba(37,99,235,0.10), 0 1.5px 6px rgba(0,0,0,0.04);
            display: flex;
            align-items: center;
            gap: 1.2rem;
        '>
            <div style='font-size: 2.5rem; line-height: 1; color: #fff;'>🔎</div>
            <div>
                <h1 style='color: white; margin-bottom: 0.3rem; font-family: "Segoe UI", "Roboto", "Arial", sans-serif; font-size: 2.2rem; letter-spacing: -1px;'>Universal Deep Scanner</h1>
                <div style='color: #e0e7ef; font-size: 1.15rem; font-family: "Segoe UI", "Roboto", "Arial", sans-serif;'>Extract and analyze data transformations from code with ease</div>
            </div>
        </div>
        """,
        unsafe_allow_html=True
    )

    st.sidebar.header("Upload & Settings")
    uploaded_file = st.sidebar.file_uploader(
        "Choose a Python file",
        type=settings.supported_extensions
    )

    st.sidebar.markdown("---")
    st.sidebar.header("Detected SQL Queries")


    if uploaded_file is not None:
        temp_path = Path("temp") / uploaded_file.name
        temp_path.parent.mkdir(exist_ok=True)
        temp_path.write_bytes(uploaded_file.getvalue())

        try:
            parser = CodeParser()
            parsed_data = parser.parse_file(str(temp_path))
            sql_styles = parser.get_sql_style()
            extracted_sql_queries = parser.get_extracted_sql_queries()

            for style in sql_styles:
                st.sidebar.write(style)

            if extracted_sql_queries:
                st.sidebar.markdown("### Extracted SQL Queries for Analysis")
                for i, sql_q in enumerate(extracted_sql_queries):
                    with st.sidebar.expander(f"Query {i+1}"):
                        st.code(sql_q, language='sql')
            else:
                st.sidebar.warning("No SQL queries were automatically extracted from the file.")

            if st.sidebar.button("Run Lineage Extraction"):
                if not extracted_sql_queries:
                    st.warning("No SQL queries were extracted from the file. Cannot run lineage extraction.")
                    return

                with st.spinner("Analyzing SQL queries for lineage..."):
                    extractor = SQLExtractor()
                    all_lineage_operations = []
                    status_placeholder = st.empty()
                    error_count = 0
                    for i, sql_query in enumerate(extracted_sql_queries):
                        status_placeholder.write(f"Processing SQL Query {i+1} of {len(extracted_sql_queries)}...")
                        try:
                            operations_str_for_query = extractor.extract_transformations_from_sql_query(
                                sql_query,
                                sql_styles
                            )
                            import re
                            sanitized_output = operations_str_for_query.strip()
                            sanitized_output = re.sub(r'^```json|```$', '', sanitized_output, flags=re.MULTILINE).strip()
                            start = sanitized_output.find('[')
                            end = sanitized_output.rfind(']')
                            if start != -1 and end != -1 and end > start:
                                json_str = sanitized_output[start:end+1]
                            else:
                                json_str = sanitized_output  # fallback
                            try:
                                current_query_operations = json.loads(json_str)
                                flagged_ops = []
                                for op in current_query_operations:
                                    if (
                                        not op.get('TGT_TABLE_NAME') or
                                        op.get('TGT_TABLE_NAME') == 'unknown_target'
                                    ):
                                        flagged_ops.append(op)
                                if flagged_ops:
                                    st.warning(f"{len(flagged_ops)} lineage row(s) have an unknown or missing target table. Please review these entries.")
                                    st.code(flagged_ops)
                            except json.JSONDecodeError as e:
                                error_count += 1
                                st.error(f"Error parsing JSON from model output for query {i+1}. Please check the SQL or model output.")
                                st.code(sanitized_output)
                                current_query_operations = []
                            if isinstance(current_query_operations, list):
                                all_lineage_operations.extend(
                                    current_query_operations)
                            else:
                                error_count += 1
                                st.warning(
                                    f"Expected a list of operations from query {i+1}, but got {type(current_query_operations)}. Skipping this query's results.")
                                logging.warning(
                                    f"Non-list output for query {i+1}: {current_query_operations}")
                        except Exception as e:
                            error_count += 1
                            st.error(f"Error processing query {i+1}. Please check the SQL or model output.")
                    status_placeholder.write("All queries processed.")

                    if not all_lineage_operations:
                        st.warning("No lineage operations could be extracted from the SQL queries.")
                        return

                    result = {"column_level_lineage": all_lineage_operations}

                    tab1, tab2, tab3 = st.tabs(
                        ["JSON Output", "Table Preview", "Download"])

                    with tab1:
                        st.json(result, expanded=False)

                    with tab2:
                        df = pd.DataFrame(all_lineage_operations)
                        if not df.empty:
                            st.dataframe(df)
                        else:
                            st.info("No data to display in table format.")

                    with tab3:
                        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                        json_str = json.dumps(result, indent=2)
                        st.download_button(
                            "Download JSON",
                            json_str,
                            file_name=f"lineage_extract_{timestamp}.json",
                            mime="application/json"
                        )
                        if not df.empty:
                            csv = df.to_csv(index=False)
                            st.download_button(
                                "Download CSV",
                                csv,
                                file_name=f"lineage_extract_{timestamp}.csv",
                                mime="text/csv"
                            )
                            excel_file_name = f"lineage_extract_{timestamp}.xlsx"
                            excel_temp_path = temp_path.parent / excel_file_name
                            try:
                                with pd.ExcelWriter(excel_temp_path, engine='openpyxl') as writer:
                                    df.to_excel(writer, index=False)
                                with open(excel_temp_path, "rb") as f_excel:
                                    st.download_button(
                                        "Download Excel",
                                        f_excel,
                                        file_name=excel_file_name,
                                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                                    )
                            except Exception as e:
                                st.error(f"Error preparing Excel file for download.")
                            finally:
                                if excel_temp_path.exists():
                                    excel_temp_path.unlink()  # Clean up temp Excel file
                        else:
                            st.info("No data to download for CSV/Excel.")
        except Exception as e:
            st.error(f"Error processing file: {str(e)}")
            logging.exception("Error in file processing UI:")
        finally:
            if temp_path.exists():
                temp_path.unlink()
    else:
        pass

if __name__ == "__main__":
    main()