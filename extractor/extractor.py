from openai import OpenAI
from typing import List
from config.settings import settings
import gc
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)


class SQLExtractor:
    def __init__(self):
        self.client = OpenAI(api_key=settings.openai_api_key)

    def __del__(self):
        gc.collect()

    # Renamed and signature changed: accepts a single SQL query string
    def extract_transformations_from_sql_query(self, sql_query: str, sql_styles: List[str]) -> str:
        try:
            # Log input data
            logging.info("Input SQL query: %s", sql_query)
            # Retained if dialects are important
            logging.info("SQL styles: %s", sql_styles)

            system_content = """You are a SQL analysis expert. Your task is to meticulously analyze the provided SQL query and extract all column-level data transformations.
            Deconstruct the query, paying close attention to Common Table Expressions (CTEs), nested subqueries, and table/column aliases to trace the complete data lineage.
            The SQL query you receive may contain placeholders. These represent table names or other values that were variables in the original source code.
              - Placeholders might be anonymous (e.g., "{}", "{0}", "{1}").
              - OR, they might retain a semblance of the original variable name (e.g., "{table_name_var}", "{schema_placeholder}").

            Your output MUST be a JSON array, where each object represents a single column-level transformation with the following fields:

            "SRC_TABLE_NAME":
              - If the source is a base table whose name is a literal IN THE SQL QUERY YOU RECEIVE, use that literal name (e.g., "actual_sales_table").
              - If the source is an aliased subquery or CTE (e.g., "... FROM (SELECT ...) AS alias_name" or "WITH alias_name AS (...) ..."), the name should be "RESULT_OF_alias_name".
              - If the source is an unaliased intermediate result set, generate a sequential name like "intermediate_result_set_1", "intermediate_result_set_2".
              - If a table name in the SQL query is a NAMED placeholder (e.g., "{my_actual_table_name_var}", "{some_config_value}"), use that name directly in your output, stripping the curly braces (e.g., "my_actual_table_name_var", "some_config_value"). THIS IS PREFERRED.
              - If a table name in the SQL query is an ANONYMOUS placeholder (e.g., "{0}", "{1}", or a generic "{}"), then fall back to using "placeholder_table_0", "placeholder_table_1", "placeholder_table_unnamed" respectively.

            "SRC_COLUMN_NAME":
              - The name of the source column (e.g., "alt_key", "email").
              - DO NOT prefix this with table or subquery aliases (e.g., use "alt_key", not "t1.alt_key").

            "BUSINESS_RULE":
              - The exact expression or logic used for the transformation (e.g., "nvl(b.email,'')", "mkl.alt_key", "a.column_from_subquery").
              - THIS FIELD SHOULD REFLECT ALIASES if they are used in the expression (e.g., 'b.email' if 'b' is the alias in the expression).
              - For direct passthrough, this will be the qualified source column (e.g. "mkl.alt_key" if "mkl" is the alias of the source table/subquery in the expression).

            "TGT_TABLE_NAME":
              - If the query creates or inserts into a table whose name is a literal IN THE SQL QUERY YOU RECEIVE (e.g., "CREATE TABLE target_table AS ...", "INSERT INTO target_table ..."), use "target_table".
              - If the target table name in the SQL query is a NAMED placeholder (e.g., "{my_target_var}"), use that name directly, stripping the curly braces (e.g., "my_target_var"). THIS IS PREFERRED.
              - If the target table name in the SQL query is an ANONYMOUS placeholder (e.g., "{0}", "{}"), then fall back to using "placeholder_target_table_0", "placeholder_target_table_unnamed" respectively.
              - If the transformation defines columns for an aliased subquery or CTE (e.g. "WITH alias_name AS (SELECT col1 AS target_col ... )" or "(SELECT col1 AS target_col ...) AS alias_name"), then "TGT_TABLE_NAME" for that step is "RESULT_OF_alias_name".
              - If it's an unaliased intermediate result set being defined, use the same generated name as its corresponding "SRC_TABLE_NAME" when it's consumed (e.g., "intermediate_result_set_1").
              - For plain SELECT statements not explicitly storing results in a named structure recognizable from the query, use "query_output".

            "TGT_COLUMN_NAME":
              - The name of the target column, often an alias defined in the SELECT list (e.g., "householdmemberidentifier" from "mkl.alt_key AS householdmemberidentifier"). If no alias, it's the column name itself.

            IMPORTANT FOR COMPLETENESS:
            - Analyze EACH part of the SELECT list, EACH generated column in a CREATE TABLE AS, and EACH column in an INSERT INTO ... SELECT statement.
            - Unpack all expressions, including those within CASE statements, function calls, and arithmetic operations, into individual lineage rows if they map to a target column.
            - If a single source column directly maps to a target column without transformation, represent this as a row.
            - If multiple source columns are used in a business rule for one target column, create one row for that target column, listing all sources in the business rule.
            - Be meticulous: aim to extract ALL column-level transformations. The goal is to achieve a comprehensive lineage map, similar to what a programmatic parser would achieve.

            Ensure the JSON is valid. The example below illustrates the structure.

            Example output structure:
            [
                {
                    "SRC_TABLE_NAME": "wh_postx_shoprite_p1.master_key_lookup",
                    "SRC_COLUMN_NAME": "alt_key",
                    "BUSINESS_RULE": "mkl.alt_key",
                    "TGT_TABLE_NAME": "target_table_from_var",
                    "TGT_COLUMN_NAME": "householdmemberidentifier"
                },
                {
                    "SRC_TABLE_NAME": "wh_postx_shoprite_p1.shoprite_cn_mstr",
                    "SRC_COLUMN_NAME": "personalidentificationnumberprefix",
                    "BUSINESS_RULE": "nvl(b.personalidentificationnumberprefix,'')",
                    "TGT_TABLE_NAME": "target_table_from_var",
                    "TGT_COLUMN_NAME": "personalidentificationnumberprefix"
                },
                {
                    "SRC_TABLE_NAME": "RESULT_OF_sub_a",
                    "SRC_COLUMN_NAME": "derived_activity_col",
                    "BUSINESS_RULE": "CASE WHEN sub_a.derived_activity_col IS NULL THEN 'Unknown' ELSE sub_a.derived_activity_col END",
                    "TGT_TABLE_NAME": "target_table_from_var",
                    "TGT_COLUMN_NAME": "final_activity_status"
                },
                {
                    "SRC_TABLE_NAME": "some_base_table",
                    "SRC_COLUMN_NAME": "raw_value",
                    "BUSINESS_RULE": "cte_alias.raw_value * 1.1",
                    "TGT_TABLE_NAME": "RESULT_OF_my_cte",
                    "TGT_COLUMN_NAME": "adjusted_value"
                },
                {
                    "SRC_TABLE_NAME": "placeholder_table_0",
                    "SRC_COLUMN_NAME": "some_column",
                    "BUSINESS_RULE": "ph0.some_column",
                    "TGT_TABLE_NAME": "target_table_from_var",
                    "TGT_COLUMN_NAME": "target_column_for_placeholder_src"
                }
            ]
            """

            prompt = self._build_prompt_for_sql_query(sql_query, sql_styles)
            messages = [
                {"role": "system", "content": system_content},
                {"role": "user", "content": prompt}
            ]

            response = self.client.responses.create(
                model=settings.openai_model,
                input=messages
            )
            raw_output = response.output_text

            # Log output data
            logging.info("RAW MODEL OUTPUT:")
            logging.info(raw_output)

            return raw_output
        except Exception as e:
            gc.collect()
            raise e

    def _build_prompt_for_sql_query(self, sql_query: str, sql_styles: List[str]) -> str:
        detected_styles_info = ""
        if sql_styles:
            # Ensuring the model knows the context if specific SQL dialects are identified.
            detected_styles_info = f"Consider the following SQL dialect(s) if relevant to your analysis: {', '.join(sql_styles)}."

        # Streamlined user prompt, relying on the system prompt for detailed JSON structure and field definitions.
        prompt = f"""{detected_styles_info}

            Analyze the SQL query provided below and extract all column-level data transformations.
            Adhere strictly to the JSON output format, field descriptions, and examples detailed in the system message.

            SQL Query to Analyze:
            ```sql
            {sql_query}
            ```

            Your entire response must be ONLY the valid JSON array as specified. Do not include any explanations, apologies, or other text outside of the JSON structure.

        Response:
        """
        return prompt
