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

    st.title("Universal Deep Scanner POC")
    st.markdown("Analyze source code for data transformations")

    # File Upload
    uploaded_file = st.sidebar.file_uploader(
        "Choose a Python file",
        type=settings.supported_extensions
    )

    if uploaded_file is not None:
        # Save uploaded file temporarily
        temp_path = Path("temp") / uploaded_file.name
        temp_path.parent.mkdir(exist_ok=True)
        temp_path.write_bytes(uploaded_file.getvalue())

        try:
            # Parse code
            parser = CodeParser()
            parsed_data = parser.parse_file(str(temp_path))
            sql_styles = parser.get_sql_style()

            # Get extracted SQL queries from the parser
            extracted_sql_queries = parser.get_extracted_sql_queries()

            # Show detected SQL styles
            st.sidebar.markdown("### Detected SQL Styles")
            for style in sql_styles:
                st.sidebar.info(f"🔍 {style}")

            # Display extracted SQL queries in the sidebar for review (optional)
            if extracted_sql_queries:
                st.sidebar.markdown("### Extracted SQL Queries for Analysis")
                for i, sql_q in enumerate(extracted_sql_queries):
                    with st.sidebar.expander(f"Query {i+1}"):
                        st.code(sql_q, language='sql')
            else:
                st.sidebar.warning(
                    "No SQL queries were automatically extracted from the file.")

            if st.sidebar.button("Run Lineage Extraction"):
                if not extracted_sql_queries:
                    st.warning(
                        "No SQL queries were extracted from the file. Cannot run lineage extraction.")
                    return

                with st.spinner("Analyzing SQL queries for lineage..."):
                    extractor = SQLExtractor()
                    all_lineage_operations = []  # To store results from all queries

                    for i, sql_query in enumerate(extracted_sql_queries):
                        st.info(f"Processing SQL Query {i+1}...")
                        logging.info(
                            f"Sending to extractor - SQL Query: {sql_query}, Styles: {sql_styles}")
                        # Call the renamed method with a single SQL query
                        try:
                            operations_str_for_query = extractor.extract_transformations_from_sql_query(
                                sql_query,
                                sql_styles
                            )
                            logging.info(
                                f"Extractor output for query {i+1}: {operations_str_for_query}")

                            # --- Improved sanitizer: extract only the JSON array ---
                            import re
                            sanitized_output = operations_str_for_query.strip()
                            # Remove code block markers if present
                            sanitized_output = re.sub(r'^```json|```$', '', sanitized_output, flags=re.MULTILINE).strip()
                            # Extract everything from the first '[' to the last ']'
                            start = sanitized_output.find('[')
                            end = sanitized_output.rfind(']')
                            if start != -1 and end != -1 and end > start:
                                json_str = sanitized_output[start:end+1]
                            else:
                                json_str = sanitized_output  # fallback
                            try:
                                current_query_operations = json.loads(json_str)
                                # --- Post-processing: flag unknown or missing target tables ---
                                # flagged_ops = []
                                # for op in current_query_operations:
                                #     if (
                                #         not op.get('TGT_TABLE_NAME') or
                                #         op.get('TGT_TABLE_NAME') == 'unknown_target'
                                #     ):
                                #         flagged_ops.append(op)
                                # if flagged_ops:
                                #     st.warning(f"{len(flagged_ops)} lineage row(s) have an unknown or missing target table. Please review these entries.")
                                #     st.code(flagged_ops)
                            except json.JSONDecodeError as e:
                                st.error(f"Error parsing JSON from model output for query {i+1}: {str(e)}")
                                st.code(sanitized_output)
                                current_query_operations = []

                            if isinstance(current_query_operations, list):
                                all_lineage_operations.extend(
                                    current_query_operations)
                            else:
                                st.warning(
                                    f"Expected a list of operations from query {i+1}, but got {type(current_query_operations)}. Skipping this query's results.")
                                logging.warning(
                                    f"Non-list output for query {i+1}: {current_query_operations}")

                        except Exception as e:
                            st.error(
                                f"Error processing query {i+1} with the extractor: {str(e)}")
                            # Optionally, continue to the next query or stop

                    if not all_lineage_operations:
                        st.warning(
                            "No lineage operations could be extracted from the SQL queries.")
                        return

                    logging.info(
                        "All aggregated lineage operations: %s", all_lineage_operations)

                    # The final result is a flat list of all operations from all queries
                    result = {"column_level_lineage": all_lineage_operations}

                    # Display results in tabs
                    tab1, tab2, tab3 = st.tabs(
                        ["JSON Output", "Table Preview", "Download"])

                    with tab1:
                        st.json(result, expanded=False)

                    with tab2:
                        # Use the aggregated list
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
                                # Use pandas ExcelWriter context manager for safety
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
                                st.error(f"Error preparing Excel file: {e}")
                            finally:
                                if excel_temp_path.exists():
                                    excel_temp_path.unlink()  # Clean up temp Excel file
                        else:
                            st.info("No data to download for CSV/Excel.")

        except Exception as e:
            st.error(f"Error processing file: {str(e)}")
            # Log full traceback
            logging.exception("Error in file processing UI:")

        finally:
            # Cleanup
            if temp_path.exists():
                temp_path.unlink()


if __name__ == "__main__":
    main()