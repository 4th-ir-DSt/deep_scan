import logging
from datetime import datetime
import sys
import json
from pathlib import Path
import pandas as pd
import streamlit as st
import re
import tempfile

# Get the absolute path to the project root
PROJECT_ROOT = Path(__file__).resolve().parent.parent

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    from config.settings import settings
    from parser.parser import CodeParser
    from extractor.extractor import SQLExtractor # Assuming this is set for OpenAI
except ImportError as e:
    st.error(f"Error importing modules: {str(e)}")
    st.error(f"Python path: {sys.path}")
    st.error(f"Project root: {PROJECT_ROOT}")
    st.stop()

# Configure logging
logging.basicConfig(level=logging.INFO,
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

    config_file_uploader = st.sidebar.file_uploader(
        "Upload Configuration File (e.g., .ini, .cfg)",
        type=["ini", "cfg"]
    )

    parser = CodeParser()
    detected_config_obj_name = "config_op_obj"
    temp_detect_file_path_str = None
    script_prefix_for_temp_tables = "" # Initialize

    if uploaded_file is not None:
        # --- Determine script prefix early and make it uppercase ---
        try:
            script_file_name_for_prefix = uploaded_file.name
            script_base_name = Path(script_file_name_for_prefix).stem
            temp_prefix = re.sub(r'[^\w_.]', '_', script_base_name) 
            temp_prefix = re.sub(r'\.', '_', temp_prefix) 
            script_prefix_for_temp_tables = temp_prefix.upper() + "_" # Ensure prefix is uppercase
            logging.info(f"Script prefix for temporary tables: {script_prefix_for_temp_tables}")
        except Exception as e_prefix:
            logging.error(f"Could not determine script prefix: {e_prefix}")
            script_prefix_for_temp_tables = "UNKNOWN_SCRIPT_" # Uppercase default
        # --- END script prefix determination ---

        try: # Config object detection
            with tempfile.NamedTemporaryFile(mode="wb", suffix=f"_detect_{uploaded_file.name}", delete=False) as tmp_script_file_detect:
                tmp_script_file_detect.write(uploaded_file.getvalue())
                temp_detect_file_path_str = tmp_script_file_detect.name
            
            detection_parser = CodeParser()
            detection_parser.parse_file(temp_detect_file_path_str)
            detected_config_obj_names_list = detection_parser.detect_config_object_names()
            
            if detected_config_obj_names_list:
                detected_config_obj_name = detected_config_obj_names_list[0]
                if len(detected_config_obj_names_list) > 1:
                    st.sidebar.caption(
                        f"Detected multiple config objects: {', '.join(detected_config_obj_names_list)}. Using '{detected_config_obj_name}'.")
            else:
                st.sidebar.caption("Could not auto-detect config object name. Please specify if needed.")
        except Exception as e_detect:
            st.sidebar.warning(f"Could not auto-detect config object name: {e_detect}")
            logging.error(f"Error during config object detection: {e_detect}", exc_info=True)
        finally:
            if temp_detect_file_path_str and Path(temp_detect_file_path_str).exists():
                try: Path(temp_detect_file_path_str).unlink()
                except OSError as e_unlink: logging.error(f"Error deleting temp detection file {temp_detect_file_path_str}: {e_unlink}")

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
            config_parser_instance = CodeParser()
            parsed_config_data = config_parser_instance.parse_config_file(config_content)
            st.sidebar.success("Configuration file parsed.")
        except Exception as e:
            st.sidebar.error(f"Error parsing config file: {e}")
            parsed_config_data = None

    st.sidebar.header("Detected SQL Information")
    temp_main_process_file_path_str = None

    if uploaded_file is not None:
        uploaded_file.seek(0)
        try:
            with tempfile.NamedTemporaryFile(mode="wb", suffix=f"_main_{uploaded_file.name}", delete=False) as tmp_script_file_main:
                tmp_script_file_main.write(uploaded_file.getvalue())
                temp_main_process_file_path_str = tmp_script_file_main.name
            
            parse_result = parser.parse_file(temp_main_process_file_path_str)
            sql_styles = parser.detected_sql_dialects

            extracted_sqls_with_args = parser.get_extracted_sql_queries_with_args()

            if sql_styles:
                st.sidebar.write("Detected SQL Styles/Dialects:")
                for style in sql_styles: st.sidebar.caption(style)
            
            if extracted_sqls_with_args:
                logging.info(f"Found {len(extracted_sqls_with_args)} raw SQL entries (not displayed in sidebar).")
            else:
                st.sidebar.warning("No raw SQL query entries were extracted from the file.")

            if st.sidebar.button("Run Lineage Extraction"):
                if not extracted_sqls_with_args:
                    st.warning("No SQL entries to process. Cannot run lineage extraction.")
                    st.stop()

                final_sql_queries_to_process = parser.resolve_and_format_sql_queries(
                    extracted_sqls_with_args,
                    parsed_config_data,
                    config_obj_name_in_script
                )

                logging.info(f"Proceeding to analyze {len(final_sql_queries_to_process)} SQL statements.")
                if final_sql_queries_to_process:
                    with st.expander("Final SQL Statements for Lineage Analysis", expanded=True):
                        for idx, final_sql_output_str in enumerate(final_sql_queries_to_process):
                            st.markdown(f"**Statement {idx+1}:**")
                            st.code(final_sql_output_str, language='sql')
                elif extracted_sqls_with_args :
                    st.info("SQL entries were found, but none were resolved into final processable SQL statements.")
                else:
                     st.info("No SQL queries to analyze.")

                if final_sql_queries_to_process:
                    with st.spinner("Analyzing resolved SQL statements for lineage..."):
                        extractor = SQLExtractor()
                        all_lineage_operations_raw = []
                        error_count = 0
                        
                        for i, sql_query in enumerate(final_sql_queries_to_process):
                            logging.info(f"Processing SQL Statement {i+1}/{len(final_sql_queries_to_process)} with LLM...")
                            try:
                                operations_str_for_query = extractor.extract_transformations_from_sql_query(sql_query, sql_styles)
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
                                        continue 
                                
                                if isinstance(parsed_json_data, list):
                                    all_lineage_operations_raw.extend(parsed_json_data)
                                elif parsed_json_data: 
                                    all_lineage_operations_raw.append(parsed_json_data)
                                    logging.warning(f"LLM returned a single dict for statement {i+1}, expected list.")
                            
                            except Exception as e_main_proc:
                                error_count += 1
                                st.error(f"Error processing statement {i+1} (LLM stage): {e_main_proc}")
                                logging.error(f"Problematic SQL for statement {i+1}:\n{sql_query}", exc_info=True)
                        
                        logging.info("All SQL statements processed by LLM. Now post-processing collected lineage data.")

                        # --- POST-PROCESSING STAGE ---
                        # 1. Convert list-like and None values to strings for DataFrame compatibility
                        all_lineage_operations_temp_types_converted = []
                        if all_lineage_operations_raw:
                            for entry_raw in all_lineage_operations_raw:
                                if isinstance(entry_raw, dict):
                                    entry_processed = entry_raw.copy() 
                                    keys_to_stringify = ['SRC_COLUMN_NAME', 'SRC_TABLE_NAME', 'TGT_COLUMN_NAME', 'TGT_TABLE_NAME', 'BUSINESS_RULE']
                                    for key in keys_to_stringify:
                                        if key in entry_processed:
                                            if isinstance(entry_processed[key], list):
                                                entry_processed[key] = ', '.join(map(str, entry_processed[key]))
                                            elif entry_processed[key] is None:
                                                entry_processed[key] = "" 
                                            elif not isinstance(entry_processed[key], str):
                                                entry_processed[key] = str(entry_processed[key])
                                    all_lineage_operations_temp_types_converted.append(entry_processed)
                                else:
                                    logging.warning(f"Skipping non-dictionary entry in raw lineage data: {type(entry_raw)}")
                            
                        # 2. Validate the type-converted data
                        validated_lineage_data = []
                        if all_lineage_operations_temp_types_converted:
                            try:
                                validated_lineage_data = parser.validate_lineage_data(all_lineage_operations_temp_types_converted)
                            except ValueError as e_validation:
                                error_count += 1
                                st.error(f"Validation error for overall lineage data: {e_validation}")
                                logging.warning(f"Overall lineage data failed validation. Using pre-validation data. Sample: {all_lineage_operations_temp_types_converted[:2]}")
                                validated_lineage_data = all_lineage_operations_temp_types_converted 
                            except Exception as e_val_generic:
                                error_count += 1
                                st.error(f"Unexpected error during lineage data validation: {e_val_generic}")
                                validated_lineage_data = all_lineage_operations_temp_types_converted
                        
                        # 3. Rename temporary tables AND Convert to Uppercase
                        all_lineage_operations = [] # This will be the final list
                        if validated_lineage_data:
                            logging.info(f"Applying script prefix '{script_prefix_for_temp_tables}' and uppercasing to table/column names.")
                            for entry in validated_lineage_data:
                                new_entry = entry.copy() 
                                
                                # Handle SRC_TABLE_NAME
                                src_table_name_str = new_entry.get('SRC_TABLE_NAME', "")
                                if isinstance(src_table_name_str, str): # Ensure it's a string after previous processing
                                    if src_table_name_str.upper().startswith("RESULT_OF_"):
                                        new_entry['SRC_TABLE_NAME'] = script_prefix_for_temp_tables + src_table_name_str.upper()
                                    else:
                                        new_entry['SRC_TABLE_NAME'] = src_table_name_str.upper()
                                
                                # Handle TGT_TABLE_NAME
                                tgt_table_name_str = new_entry.get('TGT_TABLE_NAME', "")
                                if isinstance(tgt_table_name_str, str):
                                    if tgt_table_name_str.upper().startswith("RESULT_OF_"):
                                        new_entry['TGT_TABLE_NAME'] = script_prefix_for_temp_tables + tgt_table_name_str.upper()
                                    elif tgt_table_name_str.upper() == "UNKNOWN_TARGET": # Keep "UNKNOWN_TARGET" as is, but uppercase
                                        new_entry['TGT_TABLE_NAME'] = "UNKNOWN_TARGET"
                                    else:
                                        new_entry['TGT_TABLE_NAME'] = tgt_table_name_str.upper()

                                # Uppercase other relevant column name fields
                                for key_to_upper in ['SRC_COLUMN_NAME', 'TGT_COLUMN_NAME']:
                                    if key_to_upper in new_entry and isinstance(new_entry[key_to_upper], str):
                                        new_entry[key_to_upper] = new_entry[key_to_upper].upper()
                                
                                all_lineage_operations.append(new_entry)
                        # --- END POST-PROCESSING STAGE ---

                        if not all_lineage_operations and error_count == 0:
                            st.warning("Lineage extraction ran, but no valid operations were derived or validated.")
                        elif not all_lineage_operations and error_count > 0:
                            st.error(f"Lineage extraction: no operations derived/validated & {error_count} error(s).")
                        elif all_lineage_operations:
                            st.success(f"Successfully processed, validated, and formatted {len(all_lineage_operations)} lineage operation(s).")

                        if all_lineage_operations:
                            result = {"column_level_lineage": all_lineage_operations}
                            tab1, tab2, tab3 = st.tabs(["JSON Output", "Table Preview", "Download"])
                            with tab1: st.json(result, expanded=True)
                            with tab2:
                                try:
                                    df = pd.DataFrame(all_lineage_operations)
                                    if not df.empty: st.dataframe(df)
                                    else: st.info("No data for table preview.")
                                except Exception as e_df:
                                    st.error(f"Error creating DataFrame for preview: {e_df}")
                                    logging.error("Error creating DataFrame from all_lineage_operations", exc_info=True)
                            with tab3: 
                                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                                try:
                                    json_dl_str = json.dumps(result, indent=2)
                                    st.download_button("Download JSON", json_dl_str, file_name=f"lineage_extract_{timestamp}.json", mime="application/json")
                                except Exception as e_json_dump: st.error(f"Error preparing JSON for download: {e_json_dump}")
                                if all_lineage_operations: 
                                    try:
                                        df_download = pd.DataFrame(all_lineage_operations)
                                        if not df_download.empty:
                                            csv_data = df_download.to_csv(index=False).encode('utf-8')
                                            st.download_button("Download CSV", csv_data, file_name=f"lineage_extract_{timestamp}.csv", mime="text/csv")
                                            excel_temp_file_path_str = None
                                            try:
                                                with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp_excel_file:
                                                    excel_temp_file_path_str = tmp_excel_file.name
                                                with pd.ExcelWriter(excel_temp_file_path_str, engine='openpyxl') as writer:
                                                    df_download.to_excel(writer, index=False)
                                                with open(excel_temp_file_path_str, "rb") as f_excel:
                                                    st.download_button("Download Excel", f_excel, file_name=f"lineage_extract_{timestamp}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                                            except Exception as e_excel: st.error(f"Error preparing Excel file: {e_excel}")
                                            finally:
                                                if excel_temp_file_path_str and Path(excel_temp_file_path_str).exists():
                                                    try: Path(excel_temp_file_path_str).unlink()
                                                    except OSError as e_unlink_excel: logging.error(f"Error deleting temp Excel: {e_unlink_excel}")
                                        else: st.info("No data for CSV/Excel download.")
                                    except Exception as e_df_dl: st.error(f"Error creating DataFrame for download: {e_df_dl}")
                                else: st.info("No data to download.")
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
                try: Path(temp_main_process_file_path_str).unlink()
                except OSError as e_unlink_main: logging.error(f"Error deleting temp main process file: {e_unlink_main}")
    else:
        st.info("Upload a Python file and optionally a configuration file to begin.")

if __name__ == "__main__":
    main()