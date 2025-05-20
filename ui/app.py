import logging
from datetime import datetime
import sys
import json
from pathlib import Path
import pandas as pd
import streamlit as st
import re
# import ast # Not directly used in app.py, can be removed if not needed elsewhere implicitly
import tempfile # --- ADDED ---

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
logging.basicConfig(level=logging.INFO, # Changed to INFO for production, DEBUG is verbose
                    format='%(asctime)s - %(levelname)s - %(module)s - %(funcName)s - %(message)s')


def main():
    st.set_page_config(
        page_title="Universal Deep Scanner POC",
        page_icon="🔍",
        layout="wide"
    )

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

    config_file_uploader = st.sidebar.file_uploader(
        "Upload Configuration File (e.g., .ini, .cfg)",
        type=["ini", "cfg"]
    )

    detected_config_obj_name = "config_op_obj"
    detected_config_obj_names_list = []
    parser = CodeParser()

    # --- MODIFIED: Temp file for config object detection ---
    temp_detect_file_path_str = None # Initialize
    # --- END MODIFIED ---

    if uploaded_file is not None:
        try:
            # --- MODIFIED: Use tempfile for detection phase ---
            with tempfile.NamedTemporaryFile(mode="wb", suffix=f"_{uploaded_file.name}", delete=False) as tmp_script_file_detect:
                tmp_script_file_detect.write(uploaded_file.getvalue())
                temp_detect_file_path_str = tmp_script_file_detect.name
            
            # Create a fresh parser instance or ensure state is reset if using the same one
            # For simplicity, we use the existing 'parser' instance and parse_file should reset its internal state (raw_code, tree)
            parser.parse_file(temp_detect_file_path_str)
            detected_config_obj_names_list = parser.detect_config_object_names()
            if detected_config_obj_names_list:
                detected_config_obj_name = detected_config_obj_names_list[0]
                if len(detected_config_obj_names_list) > 1:
                    st.sidebar.caption(
                        f"Detected multiple config objects: {', '.join(detected_config_obj_names_list)}. Using '{detected_config_obj_name}'.")
            else:
                st.sidebar.caption("Could not auto-detect config object name. Please specify if needed.")
            # --- END MODIFIED ---
        except Exception as e_detect:
            st.sidebar.warning(
                f"Could not auto-detect config object name due to error: {e_detect}")
        finally:
            # --- MODIFIED: Clean up temp detection file ---
            if temp_detect_file_path_str and Path(temp_detect_file_path_str).exists():
                try:
                    Path(temp_detect_file_path_str).unlink()
                except OSError as e_unlink:
                    logging.error(f"Error deleting temp detection file {temp_detect_file_path_str}: {e_unlink}")
            # --- END MODIFIED ---


    config_obj_name_in_script = st.sidebar.text_input(
        "Config Object Name in Python Script",
        value=detected_config_obj_name,
        help="The name of the variable in your Python script that holds the parsed configuration, e.g., `config_op_obj`."
    )

    st.sidebar.markdown("---")
    

    parsed_config_data = None
    if config_file_uploader is not None:
        try:
            config_content = config_file_uploader.read().decode()
            # Re-instantiate parser or ensure parse_config_file doesn't depend on prior parse_file state
            # For now, assuming parser.parse_config_file is independent.
            config_parser_instance = CodeParser() # Use a fresh instance for config parsing if unsure
            parsed_config_data = config_parser_instance.parse_config_file(config_content)
            st.sidebar.success("Configuration file parsed.")
        except Exception as e: # Catches configparser.Error re-raised from parse_config_file
            st.sidebar.error(f"Error parsing config file: {e}")
            parsed_config_data = None # Ensure it's None on error


    st.sidebar.header("Detected SQL Information") # Moved for better flow

    # --- MODIFIED: Temp file for main processing ---
    temp_main_process_file_path_str = None
    # --- END MODIFIED ---

    if uploaded_file is not None:
        uploaded_file.seek(0) # Reset pointer for re-reading
        try:
            # --- MODIFIED: Use tempfile for main processing pass ---
            with tempfile.NamedTemporaryFile(mode="wb", suffix=f"_{uploaded_file.name}", delete=False) as tmp_script_file_main:
                tmp_script_file_main.write(uploaded_file.getvalue())
                temp_main_process_file_path_str = tmp_script_file_main.name
            
            # Use the main 'parser' instance, parse_file will overwrite its state
            parser.parse_file(temp_main_process_file_path_str)
            # --- END MODIFIED ---

            sql_styles = parser.get_sql_style()
            extracted_sqls_with_args = parser.get_extracted_sql_queries_with_args()

            if sql_styles:
                st.sidebar.write("Detected SQL Styles:")
                for style in sql_styles:
                    st.sidebar.caption(style)
            
            if extracted_sqls_with_args:
                st.sidebar.markdown(
                    f"### Extracted {len(extracted_sqls_with_args)} SQL Entries (Templates + Args)")
                for i, (template, pos_args_tuple, kw_args_tuple_of_items, fmt_type) in enumerate(extracted_sqls_with_args):
                    with st.sidebar.expander(f"SQL Entry {i+1} (Type: {fmt_type})"):
                        st.code(template, language='sql')
                        if pos_args_tuple:
                            st.caption(f"{len(pos_args_tuple)} positional arg(s) found.")
                        if kw_args_tuple_of_items:
                            st.caption(f"{len(kw_args_tuple_of_items)} keyword arg(s) found.")
            else:
                st.sidebar.warning("No SQL query entries were extracted from the file.")

            if st.sidebar.button("Run Lineage Extraction"):
                if not extracted_sqls_with_args:
                    st.warning("No SQL query entries to process. Cannot run lineage extraction.")
                    st.stop() # Use st.stop() instead of return for Streamlit

                final_sql_queries_to_process = parser.resolve_and_format_sql_queries(
                    extracted_sqls_with_args,
                    parsed_config_data,
                    config_obj_name_in_script
                )

                logging.info(f"Proceeding to analyze {len(final_sql_queries_to_process)} SQL queries.")
                with st.expander("Final SQL Queries for Lineage Analysis", expanded=False):
                    for final_sql_output_str in final_sql_queries_to_process:
                        st.code(final_sql_output_str, language='sql')

                with st.spinner("Analyzing resolved SQL queries for lineage..."):
                    extractor = SQLExtractor()
                    all_lineage_operations = []
                    error_count = 0
                    for i, sql_query in enumerate(final_sql_queries_to_process):
                        logging.info(f"Processing SQL Query {i+1}/{len(final_sql_queries_to_process)} with model...")
                        try:
                            operations_str_for_query = extractor.extract_transformations_from_sql_query(
                                sql_query,
                                sql_styles
                            )
                            # --- MODIFIED: Robust JSON parsing ---
                            sanitized_output = operations_str_for_query.strip()
                            cleaned_json_str = re.sub(r'^```json|```$', '', sanitized_output, flags=re.MULTILINE).strip()
                            
                            parsed_json_data = None
                            try:
                                # Attempt to parse the cleaned string directly
                                parsed_json_data = json.loads(cleaned_json_str)
                            except json.JSONDecodeError:
                                # Fallback: if direct parsing fails, try to find the outermost array
                                try:
                                    start_index = cleaned_json_str.find('[')
                                    end_index = cleaned_json_str.rfind(']')
                                    if start_index != -1 and end_index != -1 and end_index > start_index:
                                        json_array_str = cleaned_json_str[start_index:end_index+1]
                                        parsed_json_data = json.loads(json_array_str)
                                    else:
                                        # If no array found, re-raise to be caught by the outer except
                                        raise json.JSONDecodeError("No valid JSON array found with fallback", cleaned_json_str, 0) 
                                except json.JSONDecodeError as e_json_fallback:
                                    error_count += 1
                                    st.error(f"Error parsing JSON from model output for query {i+1}: {e_json_fallback}")
                                    st.code(operations_str_for_query, language='text') # Show original LLM output
                                    continue # Skip to next query
                            # --- END MODIFIED ---

                            if isinstance(parsed_json_data, list):
                                # --- ADDED: Validate lineage data from LLM ---
                                try:
                                    # Assuming 'parser' is the CodeParser instance
                                    validated_operations = parser.validate_lineage_data(parsed_json_data)
                                    all_lineage_operations.extend(validated_operations)
                                except ValueError as e_validation:
                                    error_count += 1
                                    st.error(f"Validation error for lineage data from query {i+1}: {e_validation}")
                                    logging.warning(f"Lineage data from query {i+1} failed validation: {parsed_json_data}")
                                # --- END ADDED ---
                            elif parsed_json_data: # If not a list but not empty (e.g. a single dict)
                                error_count += 1
                                st.warning(f"Expected a list of operations from query {i+1}, but got {type(parsed_json_data)}. Skipping results.")
                                logging.warning(f"Non-list output for query {i+1}: {parsed_json_data}")

                        except Exception as e_main_proc:
                            error_count += 1
                            st.error(f"Error processing query {i+1} (model extraction stage): {e_main_proc}")
                            logging.error(f"Problematic SQL for query {i+1}:\n{sql_query}", exc_info=True)
                    
                    logging.info("All queries processed by model.")

                    if not all_lineage_operations and error_count == 0:
                        st.warning("Lineage extraction ran, but no lineage operations were derived.")
                    elif not all_lineage_operations and error_count > 0:
                        st.error(f"Lineage extraction attempted, but no operations derived and {error_count} error(s) occurred.")
                    elif all_lineage_operations:
                        st.success(f"Successfully extracted {len(all_lineage_operations)} lineage operation(s).")

                    if all_lineage_operations:
                        result = {"column_level_lineage": all_lineage_operations}
                        tab1, tab2, tab3 = st.tabs(["JSON Output", "Table Preview", "Download"])
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
                            json_dl_str = json.dumps(result, indent=2)
                            st.download_button(
                                "Download JSON", json_dl_str, file_name=f"lineage_extract_{timestamp}.json", mime="application/json")
                            
                            df_download = pd.DataFrame(all_lineage_operations)
                            if not df_download.empty:
                                csv = df_download.to_csv(index=False)
                                st.download_button(
                                    "Download CSV", csv, file_name=f"lineage_extract_{timestamp}.csv", mime="text/csv")
                                
                                # --- MODIFIED: Temp file for Excel export ---
                                excel_temp_file_path_str = None
                                try:
                                    with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp_excel_file:
                                        excel_temp_file_path_str = tmp_excel_file.name
                                    
                                    # Pandas ExcelWriter needs a path, it will create/overwrite the file.
                                    # So, we use the temp file path obtained above.
                                    with pd.ExcelWriter(excel_temp_file_path_str, engine='openpyxl') as writer:
                                        df_download.to_excel(writer, index=False)
                                    
                                    with open(excel_temp_file_path_str, "rb") as f_excel:
                                        st.download_button("Download Excel", f_excel, file_name=f"lineage_extract_{timestamp}.xlsx",
                                                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                                except Exception as e_excel:
                                    st.error(f"Error preparing Excel file: {e_excel}")
                                finally:
                                    if excel_temp_file_path_str and Path(excel_temp_file_path_str).exists():
                                        try:
                                            Path(excel_temp_file_path_str).unlink()
                                        except OSError as e_unlink_excel:
                                            logging.error(f"Error deleting temp Excel file {excel_temp_file_path_str}: {e_unlink_excel}")
                                # --- END MODIFIED ---
                            else:
                                st.info("No data to download for CSV/Excel.")
        except ValueError as ve: # Catch syntax errors from parser.parse_file
             st.error(f"Error processing Python file: {str(ve)}")
             logging.exception("ValueError during file processing UI:")
        except FileNotFoundError as fnfe:
             st.error(f"Error: {str(fnfe)}")
             logging.exception("FileNotFoundError during file processing UI:")
        except Exception as e:
            st.error(f"An unexpected error occurred: {str(e)}")
            logging.exception("Error in file processing UI:")
        finally:
            # --- MODIFIED: Clean up main processing temp file ---
            if temp_main_process_file_path_str and Path(temp_main_process_file_path_str).exists():
                try:
                    Path(temp_main_process_file_path_str).unlink()
                except OSError as e_unlink_main:
                    logging.error(f"Error deleting temp main process file {temp_main_process_file_path_str}: {e_unlink_main}")
            # --- END MODIFIED ---
    else:
        st.info("Upload a Python file and optionally a configuration file to begin.")

if __name__ == "__main__":
    main()