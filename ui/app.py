import logging
from datetime import datetime
import sys
import json
from pathlib import Path
import pandas as pd
import streamlit as st
import re
import tempfile # For secure temporary file handling

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
logging.basicConfig(level=logging.INFO, # Set to DEBUG for more verbose logs if needed
                    format='%(asctime)s - %(levelname)s - %(module)s - %(funcName)s - %(message)s')


def main():
    st.set_page_config(
        page_title="Universal Deep Scanner POC",
        page_icon="🔍",
        layout="wide"
    )

    # --- Header Markdown ---
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

    # Initialize parser instance that will be used for the main parsing pass
    parser = CodeParser()
    temp_main_process_file_path_str = None

    st.sidebar.header("Detected SQL Information")

    if uploaded_file is not None:
        uploaded_file.seek(0)
        try:
            with tempfile.NamedTemporaryFile(mode="wb", suffix=f"_main_{uploaded_file.name}", delete=False) as tmp_script_file_main:
                tmp_script_file_main.write(uploaded_file.getvalue())
                temp_main_process_file_path_str = tmp_script_file_main.name

            # Use the LLM-based extraction method
            extracted_sql_queries = parser.extract_sql_with_llm(temp_main_process_file_path_str)

            if extracted_sql_queries:
                st.sidebar.markdown(
                    f"### Extracted {len(extracted_sql_queries)} SQL Queries")
                for i, sql_query in enumerate(extracted_sql_queries):
                    with st.sidebar.expander(f"SQL Query {i+1}"):
                        st.code(sql_query, language='sql')
            else:
                st.sidebar.warning("No SQL queries were extracted from the file.")

            if st.sidebar.button("Run Lineage Extraction"):
                if not extracted_sql_queries:
                    st.warning("No SQL queries to process. Cannot run lineage extraction.")
                    st.stop()

                logging.info(f"Proceeding to analyze {len(extracted_sql_queries)} SQL statements.")
                if extracted_sql_queries:
                    with st.expander("Final SQL Statements for Lineage Analysis", expanded=False):
                        for idx, final_sql_output_str in enumerate(extracted_sql_queries):
                            st.markdown(f"**Statement {idx+1}:**")
                            st.code(final_sql_output_str, language='sql')
                else:
                    st.info("No SQL queries to analyze.")

                if extracted_sql_queries:
                    with st.spinner("Analyzing resolved SQL statements for lineage..."):
                        extractor = SQLExtractor()
                        all_lineage_operations_raw = [] # Collect raw LLM outputs here
                        error_count = 0

                        for i, sql_query in enumerate(extracted_sql_queries):
                            logging.info(f"Processing SQL Statement {i+1}/{len(extracted_sql_queries)} with LLM...")
                            try:
                                operations_str_for_query = extractor.extract_transformations_from_sql_query(
                                    sql_query,
                                    [] # No dialects needed, as we are not detecting them anymore
                                )
                                sanitized_output = operations_str_for_query.strip()
                                cleaned_json_str = re.sub(r'^```json|```$', '', sanitized_output, flags=re.MULTILINE).strip()

                                parsed_json_data = None
                                try:
                                    parsed_json_data = json.loads(cleaned_json_str)
                                except json.JSONDecodeError:
                                    try:
                                        start_index = cleaned_json_str.find('[')
                                        end_index = cleaned_json_str.rfind(']')
                                        if start_index != -1 and end_index != -1 and end_index > start_index:
                                            json_array_str = cleaned_json_str[start_index:end_index+1]
                                            parsed_json_data = json.loads(json_array_str)
                                        else:
                                            raise json.JSONDecodeError("No valid JSON array found with fallback", cleaned_json_str, 0)
                                    except json.JSONDecodeError as e_json_fallback:
                                        error_count += 1
                                        st.error(f"Error parsing JSON from LLM for statement {i+1}: {e_json_fallback}")
                                        st.text("Raw LLM Output:")
                                        st.code(operations_str_for_query, language='text')
                                        continue # Skip to next SQL query

                                if isinstance(parsed_json_data, list):
                                    all_lineage_operations_raw.extend(parsed_json_data)
                                elif parsed_json_data: # If LLM returned a single dict instead of a list
                                    all_lineage_operations_raw.append(parsed_json_data)
                                    logging.warning(f"LLM returned a single dict for statement {i+1}, expected list. Added as single item.")

                            except Exception as e_main_proc:
                                error_count += 1
                                st.error(f"Error processing statement {i+1} (LLM stage): {e_main_proc}")
                                logging.error(f"Problematic SQL for statement {i+1}:\n{sql_query}", exc_info=True)

                        logging.info("All SQL statements processed by LLM. Now processing and validating collected lineage data.")

                        # --- NEW: Centralized processing and validation of collected lineage data ---
                        all_lineage_operations_processed = []
                        if all_lineage_operations_raw:
                            for entry_raw in all_lineage_operations_raw:
                                if isinstance(entry_raw, dict):
                                    entry_processed = entry_raw.copy() # Work on a copy
                                    # Keys that might contain lists from LLM or need string conversion
                                    keys_to_stringify_if_list = ['SRC_COLUMN_NAME', 'SRC_TABLE_NAME', 'TGT_COLUMN_NAME', 'TGT_TABLE_NAME', 'BUSINESS_RULE']

                                    for key in keys_to_stringify_if_list:
                                        if key in entry_processed:
                                            if isinstance(entry_processed[key], list):
                                                logging.debug(f"Converting list in '{key}' to string for entry: {entry_processed[key]}")
                                                entry_processed[key] = ', '.join(map(str, entry_processed[key]))
                                            elif entry_processed[key] is None:
                                                entry_processed[key] = "" # Replace None with empty string
                                            elif not isinstance(entry_processed[key], str):
                                                # If it's not a string, list, or None, convert to string
                                                logging.debug(f"Converting non-string/list/None type in '{key}' ({type(entry_processed[key])}) to string.")
                                                entry_processed[key] = str(entry_processed[key])
                                    all_lineage_operations_processed.append(entry_processed)
                                else:
                                    logging.warning(f"Skipping non-dictionary entry in all_lineage_operations_raw: {type(entry_raw)}")

                            all_lineage_operations = all_lineage_operations_processed
                        else: # all_lineage_operations_raw is empty
                            all_lineage_operations = []
                        # --- END NEW: Centralized processing ---

                        if not all_lineage_operations and error_count == 0:
                            st.warning("Lineage extraction ran, but no valid operations were derived.")
                        elif not all_lineage_operations and error_count > 0:
                            st.error(f"Lineage extraction: no operations derived & {error_count} error(s) occurred.")
                        elif all_lineage_operations:
                            st.success(f"Successfully processed and validated {len(all_lineage_operations)} lineage operation(s).")

                        if all_lineage_operations:
                            result = {"column_level_lineage": all_lineage_operations}
                            tab1, tab2, tab3 = st.tabs(["JSON Output", "Table Preview", "Download"])
                            with tab1:
                                st.json(result, expanded=True)
                            with tab2:
                                try:
                                    df = pd.DataFrame(all_lineage_operations)
                                    if not df.empty:
                                        st.dataframe(df)
                                    else:
                                        st.info("No data for table preview.")
                                except Exception as e_df:
                                    st.error(f"Error creating DataFrame for preview: {e_df}")
                                    logging.error("Error creating DataFrame from all_lineage_operations", exc_info=True)
                                    st.markdown("Could not display table. Please check JSON output and logs.")

                            with tab3: # Download logic
                                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                                try:
                                    json_dl_str = json.dumps(result, indent=2)
                                    st.download_button(
                                        "Download JSON", json_dl_str, file_name=f"lineage_extract_{timestamp}.json", mime="application/json")
                                except Exception as e_json_dump:
                                    st.error(f"Error preparing JSON for download: {e_json_dump}")

                                if all_lineage_operations: 
                                    try:
                                        df_download = pd.DataFrame(all_lineage_operations)
                                        if not df_download.empty:
                                            csv = df_download.to_csv(index=False)
                                            st.download_button(
                                                "Download CSV", csv, file_name=f"lineage_extract_{timestamp}.csv", mime="text/csv")
                                            
                                            excel_temp_file_path_str = None
                                            try:
                                                with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp_excel_file:
                                                    excel_temp_file_path_str = tmp_excel_file.name
                                                
                                                with pd.ExcelWriter(excel_temp_file_path_str, engine='openpyxl') as writer:
                                                    df_download.to_excel(writer, index=False)
                                                
                                                with open(excel_temp_file_path_str, "rb") as f_excel:
                                                    st.download_button("Download Excel", f_excel, file_name=f"lineage_extract_{timestamp}.xlsx",
                                                                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                                            except Exception as e_excel:
                                                st.error(f"Error preparing Excel file: {e_excel}")
                                            finally:
                                                if excel_temp_file_path_str and Path(excel_temp_file_path_str).exists():
                                                    try: Path(excel_temp_file_path_str).unlink()
                                                    except OSError as e_unlink_excel: logging.error(f"Error deleting temp Excel: {e_unlink_excel}")
                                        else:
                                            st.info("No data for CSV/Excel download.")
                                    except Exception as e_df_dl:
                                        st.error(f"Error creating DataFrame for download: {e_df_dl}")
                                else:
                                    st.info("No data to download.")
                else: 
                    st.info("No final SQL statements were resolved for lineage analysis.")

        except ValueError as ve:
             st.error(f"Error processing Python file: {str(ve)}")
             logging.exception("ValueError during file processing UI:")
        except FileNotFoundError as fnfe:
             st.error(f"Error: {str(fnfe)}")
             logging.exception("FileNotFoundError during file processing UI:")
        except Exception as e:
            st.error(f"An unexpected error occurred: {str(e)}")
            logging.exception("Error in file processing UI:")
        finally:
            if temp_main_process_file_path_str and Path(temp_main_process_file_path_str).exists():
                try:
                    Path(temp_main_process_file_path_str).unlink()
                except OSError as e_unlink_main:
                    logging.error(f"Error deleting temp main process file: {e_unlink_main}")
    else:
        st.info("Upload a Python file to begin.")

if __name__ == "__main__":
    main()