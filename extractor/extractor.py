from openai import OpenAI
from typing import List
from config.settings import settings
import gc
import logging

class SQLExtractor:
    def __init__(self):
        try:
            self.client = OpenAI(api_key=settings.openai_api_key)
            logging.info(f"OpenAI client configured with model: {settings.openai_model}")
        except AttributeError as e:
            logging.error("OpenAI API key or model name not found in settings. Please update config/settings.py.")
            raise AttributeError("Missing OpenAI configuration in settings: " + str(e))
        except Exception as e:
            logging.error(f"Error configuring OpenAI: {e}", exc_info=True)
            raise

    def __del__(self):
        try:
            gc.collect()
        except Exception: # pragma: no cover
            pass

    def extract_transformations_from_sql_query(self, sql_query: str, sql_styles: List[str]) -> str:
        try:
            logging.info("Input SQL query (first 500 chars): %s", sql_query[:500] + "..." if len(sql_query) > 500 else sql_query)
            logging.info("SQL styles: %s", sql_styles)

            system_content = """
                You are a SQL Data Lineage and Transformation Extraction Expert.

                Your job is to meticulously analyze any SQL query provided and **systematically extract all transformations and data movements, starting from the innermost subqueries and CTEs outward**. 

                ---

                ### **Step 1: Break Down the Query Execution Flow**
                - Identify all Common Table Expressions (CTEs), subqueries, and nested queries.
                - For each layer, fully process all SELECT statements and transformations before proceeding.

                ---

                ###  **Step 2: Extract Transformation Details for Every Column**
                For every column in each SELECT clause, capture:
                - `SRC_TABLE_NAME`: 
                    - If the column originates directly from a base table (i.e., not a CTE or subquery), use the **actual, unaliased name of that base table**. Resolve any aliases used in FROM/JOIN clauses back to their original table names.
                    - If the column originates from a Common Table Expression (CTE) or a subquery, use `RESULT_OF_<cte_or_subquery_alias>`.
                - `SRC_COLUMN_NAME`: 
                    - Capture the raw column name involved in the transformation.
                    - If multiple columns contribute, list all of them clearly (e.g., as a comma-separated string or describe the set of columns).
                - `BUSINESS_RULE`: 
                    # --- MODIFIED BUSINESS_RULE INSTRUCTION ---
                    - If the column is derived from a transformation (e.g., function calls like `nvl(b.email, '')`, `COALESCE(c.col, b.col)`, `CASE WHEN ... END` statements, arithmetic operations, concatenations, window functions, aggregations), extract the **full transformation expression as written** in the SQL.
                    - If the column is a direct passthrough from a source column without any explicit transformation (e.g., `SELECT source_alias.column_name AS target_alias_name FROM ...`), leave this field as an **empty string** ("").
                    - If the value is a constant or `NULL` (e.g., `SELECT 'Active' AS status, NULL AS error_message FROM ...`), specify the constant value or `NULL` (e.g., `'Active'` or `NULL`).
                    # --- END MODIFIED BUSINESS_RULE INSTRUCTION ---
                - `TGT_TABLE_NAME`: 
                    - If the output is going into a permanent table, specify that table name.
                    - If going into a CTE or subquery, specify `RESULT_OF_<cte_or_alias>`.
                    - If it's a final SELECT without a table, use `"unknown_target"`.
                - `TGT_COLUMN_NAME`: 
                    - Use the alias from the SELECT clause. 
                    - If no alias is provided, use the raw column name.

                ---

                ###  **Step 3: Handle Special SQL Cases Completely**
                - Always extract lineage for all of the following:
                    - CASE WHEN logic.
                    - Window functions (e.g., `ROW_NUMBER() OVER (...)`).
                    - Aggregations (SUM, COUNT, AVG).
                    - Arithmetic expressions.
                    - String and mathematical functions (e.g., `nvl`, `regexp_replace`).
                    - Constants and NULL values.
                - Even if a transformation results in NULL, a constant, or a default value, record it.

                ---

                ###  **Step 4: Ensure Every Column in the Final SELECT is Accounted For**
                - Capture **every column** from the final SELECT, whether direct, derived, or constant.
                - Do not miss calculated fields like `rank()`, `pmod(...)`, or derived flags using CASE WHEN.
                - Record all columns created through complex expressions, even if they don't have a simple source column.

                ---

                ###  **Step 5: Maintain Clear and Human-Readable Output**
                - Use column and table aliases exactly as shown in the original query for better readability.
                - Only output **valid JSON**. 
                - Do not include any explanations or comments outside of the JSON array.

                ---

                ###  **Final Output Example (Illustrating New BUSINESS_RULE logic):**

                [
                    {
                        "SRC_TABLE_NAME": "source_table_b",
                        "SRC_COLUMN_NAME": "email_address",
                        "BUSINESS_RULE": "nvl(b.email_address, 'N/A')",
                        "TGT_TABLE_NAME": "RESULT_OF_subquery_a",
                        "TGT_COLUMN_NAME": "email"
                    },
                    {
                        "SRC_TABLE_NAME": "source_table_b",
                        "SRC_COLUMN_NAME": "customer_id",
                        "BUSINESS_RULE": "", // Direct passthrough
                        "TGT_TABLE_NAME": "RESULT_OF_subquery_a",
                        "TGT_COLUMN_NAME": "cust_id"
                    },
                    {
                        "SRC_TABLE_NAME": "N/A", // Or 'literal_value' if preferred for constants
                        "SRC_COLUMN_NAME": "N/A",
                        "BUSINESS_RULE": "'Active'", // Constant value
                        "TGT_TABLE_NAME": "RESULT_OF_",
                        "TGT_COLUMN_NAME": "status"
                    },
                    {
                        "SRC_TABLE_NAME": "some_table",
                        "SRC_COLUMN_NAME": "value_col",
                        "BUSINESS_RULE": "SUM(value_col) OVER (PARTITION BY category)",
                        "TGT_TABLE_NAME": "final_output_table",
                        "TGT_COLUMN_NAME": "category_sum"
                    }
                ]

                ---

                ### **Important Rules:**
                - Your response must be **only the JSON array**.
                - Do not include explanations, comments, or additional text.
                - **If a table or column name encountered in the SQL query is a placeholder string (e.g., `{some_variable_name}`, `{PY_VAR_UNRESOLVED...}`, `{PY_EXPR_UNRESOLVED...}`), use this literal placeholder string as the name in your JSON output if its actual value is not defined or resolved within the query's context. Do not attempt to map it back to original numeric format specifiers like `{0}` or `{1}` unless that numeric specifier is literally the name used in the query.**
            """

            user_prompt = self._build_prompt_for_sql_query(sql_query, sql_styles)
            messages = [
                {"role": "system", "content": system_content},
                {"role": "user", "content": user_prompt}
            ]

            response = self.client.chat.completions.create(
                model=settings.openai_model,
                messages=messages,
                temperature=0,
                seed=35
                
            )
            
            raw_output = ""
            if response.choices and response.choices[0].message:
                raw_output = response.choices[0].message.content or ""

            logging.info("RAW LLM OUTPUT (first 500 chars): %s", raw_output[:500] + "..." if len(raw_output) > 500 else raw_output)
            return raw_output
        except AttributeError as e: 
            logging.error(f"SQLExtractor not properly initialized for OpenAI, or settings missing: {e}", exc_info=True)
            raise 
        except Exception as e:
            logging.error(f"Error during OpenAI API call or processing in SQLExtractor: {e}", exc_info=True)
            raise

    def _build_prompt_for_sql_query(self, sql_query: str, sql_styles: List[str]) -> str:
        detected_styles_info = ""
        if sql_styles:
            detected_styles_info = f"Consider the following SQL dialect(s) if relevant to your analysis: {', '.join(sql_styles)}."

        prompt = f"""{detected_styles_info}

            Analyze the following SQL query **starting from the innermost subquery or CTE outward**. 

            Return ONLY the valid JSON array as specified. Adhere to all rules in the system prompt, especially regarding placeholder names and the BUSINESS_RULE for direct passthroughs. Avoid any explanations or text outside of the JSON.

            SQL Query to Analyze:
            ```sql
            {sql_query}
            ```

            Your entire response must be ONLY the valid JSON array as specified. Do not include any explanations, apologies, or other text outside of the JSON structure.

        Response:
        """
        return prompt