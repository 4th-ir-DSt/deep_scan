import logging
from datetime import datetime
import sys
import json
from pathlib import Path
import pandas as pd
import streamlit as st
import re
import ast

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
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(message)s')


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

    config_file_uploader = st.sidebar.file_uploader(
        "Upload Configuration File (e.g., .ini, .cfg)",
        type=["ini", "cfg"]
    )

    # Initialize detected_config_obj_name before it's potentially set
    detected_config_obj_name = "config_op_obj"  # Default value
    detected_config_obj_names_list = []

    parser = CodeParser()  # Instantiate parser early to use for detection

    # Temp path handling for config object detection needs to happen before text_input if script is uploaded
    # However, the full script parsing for SQL extraction will happen later as before.
    # This is a bit tricky as we need the tree for detection but full processing later.
    # For now, let's assume detection happens IF a file is uploaded, then the text input is created.

    if uploaded_file is not None:
        # Perform a preliminary parse just for config object detection
        # This is a bit redundant with the later full parse, but necessary to get the detected name *before* text_input
        # To avoid issues with temp_path being deleted too early, we might need to manage its lifecycle carefully
        # or pass the raw script content if the parser can take it directly for this limited detection task.
        # For simplicity now, let's re-read if necessary, or ensure parser.parse_file can be called multiple times safely.

        # Read content for detection without saving to temp_path yet, or use a temporary instance of parser
        # to avoid state conflicts if parse_file has side effects beyond setting self.tree and self.raw_code.
        try:
            # We need the AST for detection. We'll parse fully later.
            # To avoid issues, parse to a temp var or make parse_file re-entrant.
            # Let's make parse_file store the tree on the instance, which is fine.
            script_content_for_detection = uploaded_file.getvalue().decode('utf-8')
            # Create a temporary parser instance for detection to avoid state issues
            # or ensure parse_file is safe to call multiple times if it resets state.
            # For now, let parser.parse_file set self.tree and self.raw_code. This will be overwritten later if needed.

            # Create a temporary path for this initial parsing step if parser requires a file path
            temp_detect_path = Path("temp") / f"detect_{uploaded_file.name}"
            temp_detect_path.parent.mkdir(exist_ok=True, parents=True)
            temp_detect_path.write_bytes(uploaded_file.getvalue())

            parser.parse_file(str(temp_detect_path))  # This sets parser.tree
            detected_config_obj_names_list = parser.detect_config_object_names()
            if detected_config_obj_names_list:
                # Use the first detected name as default
                detected_config_obj_name = detected_config_obj_names_list[0]
                if len(detected_config_obj_names_list) > 1:
                    st.sidebar.caption(
                        f"Detected multiple config objects: {', '.join(detected_config_obj_names_list)}. Using '{detected_config_obj_name}'.")
            else:
                st.sidebar.caption(
                    "Could not auto-detect config object name. Please specify if needed.")

            if temp_detect_path.exists():  # Clean up the temporary detection file
                temp_detect_path.unlink()

        except Exception as e_detect:
            st.sidebar.warning(
                f"Could not auto-detect config object name due to error: {e_detect}")
            # Default value remains "config_op_obj"

    config_obj_name_in_script = st.sidebar.text_input(
        "Config Object Name in Python Script",
        value=detected_config_obj_name,  # Pre-fill with detected or default
        help="The name of the variable in your Python script that holds the parsed configuration, e.g., `config_op_obj`."
    )

    st.sidebar.markdown("---")
    st.sidebar.header("Detected SQL Information")

    parsed_config_data = None
    if config_file_uploader is not None:
        try:
            config_content = config_file_uploader.read().decode()
            parsed_config_data = parser.parse_config_file(config_content)
            st.sidebar.success("Configuration file parsed.")
        except Exception as e:
            st.sidebar.error(f"Error parsing config file: {e}")

    if uploaded_file is not None:
        # Reset uploaded_file internal pointer to the beginning for the main parsing pass
        uploaded_file.seek(0)
        # Main temp file for full processing
        temp_path = Path("temp") / uploaded_file.name
        temp_path.parent.mkdir(exist_ok=True, parents=True)
        temp_path.write_bytes(uploaded_file.getvalue())

        try:
            # Main parsing pass using the (now potentially different) parser instance state
            parser.parse_file(str(temp_path))
            sql_styles = parser.get_sql_style()
            # This now returns tuples where pos_args is a tuple and kw_args is a tuple of items
            extracted_sqls_with_args = parser.get_extracted_sql_queries_with_args()

            # Display SQL styles
            if sql_styles:
                st.sidebar.write("Detected SQL Styles:")
                for style in sql_styles:
                    st.sidebar.caption(style)

            # Display extracted query templates
            if extracted_sqls_with_args:
                st.sidebar.markdown(
                    f"### Extracted {len(extracted_sqls_with_args)} SQL Entries (Templates + Args)")
                for i, (template, pos_args_tuple, kw_args_tuple_of_items, fmt_type) in enumerate(extracted_sqls_with_args):
                    with st.sidebar.expander(f"SQL Entry {i+1} (Type: {fmt_type})"):
                        st.code(template, language='sql')
                        if pos_args_tuple:
                            st.caption(
                                f"{len(pos_args_tuple)} positional arg(s) found. Trigger Run for resolution.")
                        if kw_args_tuple_of_items:
                            st.caption(
                                f"{len(kw_args_tuple_of_items)} keyword arg(s) found. Trigger Run for resolution.")
            else:
                st.sidebar.warning(
                    "No SQL query entries were extracted from the file.")

            if st.sidebar.button("Run Lineage Extraction"):
                if not extracted_sqls_with_args:
                    st.warning(
                        "No SQL query entries to process. Cannot run lineage extraction.")
                    return

                final_sql_queries_to_process = parser.resolve_and_format_sql_queries(
                    extracted_sqls_with_args,
                    parsed_config_data,
                    config_obj_name_in_script
                )

                logging.info(
                    f"Proceeding to analyze {len(final_sql_queries_to_process)} SQL queries.")
                with st.expander("Final SQL Queries for Lineage Analysis", expanded=False):
                    for i, final_sql_output_str in enumerate(final_sql_queries_to_process):
                        # Determine language for syntax highlighting
                        # If it's a representation of a Python .format() call, use 'python'
                        # Otherwise, assume it's a direct SQL query and use 'sql'
                        display_lang = 'sql'  # Default for literal SQL
                        # Check if it looks like a Python .format() string representation
                        if (final_sql_output_str.startswith("'") or final_sql_output_str.startswith('"')) and \
                           ".format(" in final_sql_output_str:
                            # This heuristic identifies strings like '"SQL".format(args)'
                            display_lang = 'python'

                        st.code(final_sql_output_str, language=display_lang)

                with st.spinner("Analyzing resolved SQL queries for lineage..."):
                    extractor = SQLExtractor()
                    all_lineage_operations = []
                    error_count = 0
                    for i, sql_query in enumerate(final_sql_queries_to_process):
                        logging.info(
                            f"Processing SQL Query {i+1} of {len(final_sql_queries_to_process)} with model...")
                        try:
                            operations_str_for_query = extractor.extract_transformations_from_sql_query(
                                sql_query,
                                sql_styles
                            )
                            sanitized_output = operations_str_for_query.strip()
                            sanitized_output = re.sub(
                                r'^```json|```$', '', sanitized_output, flags=re.MULTILINE).strip()
                            start = sanitized_output.find('[')
                            end = sanitized_output.rfind(']')
                            if start != -1 and end != -1 and end > start:
                                json_str = sanitized_output[start:end+1]
                            else:
                                json_str = sanitized_output
                            try:
                                current_query_operations = json.loads(json_str)
                            except json.JSONDecodeError as e_json:
                                error_count += 1
                                st.error(
                                    f"Error parsing JSON from model output for query {i+1}: {e_json}")
                                st.code(sanitized_output, language='text')
                                current_query_operations = []

                            if isinstance(current_query_operations, list):
                                all_lineage_operations.extend(
                                    current_query_operations)
                            elif current_query_operations:
                                error_count += 1
                                st.warning(
                                    f"Expected a list of operations from query {i+1}, but got {type(current_query_operations)}. Skipping results.")
                                logging.warning(
                                    f"Non-list output for query {i+1}: {current_query_operations}")
                        except Exception as e_main_proc:
                            error_count += 1
                            st.error(
                                f"Error processing query {i+1} (model extraction stage): {e_main_proc}")
                            logging.error(
                                f"Problematic SQL for query {i+1}:\n{sql_query}")

                    logging.info("All queries processed by model.")

                    if not all_lineage_operations and error_count == 0:
                        st.warning(
                            "Lineage extraction ran, but no lineage operations were derived from the SQL queries.")
                    elif not all_lineage_operations and error_count > 0:
                        st.error(
                            f"Lineage extraction attempted, but no operations derived and {error_count} error(s) occurred during processing.")
                    elif all_lineage_operations:
                        st.success(
                            f"Successfully extracted {len(all_lineage_operations)} lineage operation(s).")

                    if all_lineage_operations:  # Only show tabs if there's data
                        result = {
                            "column_level_lineage": all_lineage_operations}
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
                            json_dl_str = json.dumps(result, indent=2)
                            st.download_button(
                                "Download JSON", json_dl_str, file_name=f"lineage_extract_{timestamp}.json", mime="application/json")
                            df_download = pd.DataFrame(all_lineage_operations)
                            if not df_download.empty:
                                csv = df_download.to_csv(index=False)
                                st.download_button(
                                    "Download CSV", csv, file_name=f"lineage_extract_{timestamp}.csv", mime="text/csv")
                                excel_file_name = f"lineage_extract_{timestamp}.xlsx"
                                excel_temp_path = temp_path.parent / excel_file_name
                                try:
                                    with pd.ExcelWriter(excel_temp_path, engine='openpyxl') as writer:
                                        df_download.to_excel(
                                            writer, index=False)
                                    with open(excel_temp_path, "rb") as f_excel:
                                        st.download_button("Download Excel", f_excel, file_name=excel_file_name,
                                                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                                except Exception as e_excel:
                                    st.error(
                                        f"Error preparing Excel file: {e_excel}")
                                finally:
                                    if excel_temp_path.exists():
                                        excel_temp_path.unlink()
                            else:
                                st.info("No data to download for CSV/Excel.")
        except Exception as e:
            st.error(f"Error processing file: {str(e)}")
            logging.exception("Error in file processing UI:")
        finally:
            if 'temp_path' in locals() and temp_path.exists():
                temp_path.unlink()
    else:
        st.info("Upload a Python file and optionally a configuration file to begin.")


if __name__ == "__main__":
    main()
