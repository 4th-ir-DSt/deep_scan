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
                    - Use the exact table or alias name from the FROM or JOIN clause.
                    - If the column comes from a subquery or CTE, write `RESULT_OF_<cte_or_alias>`.
                - `SRC_COLUMN_NAME`: 
                    - Capture the raw column name involved in the transformation.
                    - If multiple columns contribute, list all of them clearly.
                - `BUSINESS_RULE`: 
                    - Extract the **full transformation expression as written** in the SQL (e.g., `nvl(b.email, '')`, `CASE WHEN ...`).
                    - If it is a direct passthrough, write `alias.column` (e.g., `b.email`).
                    - If the value is a constant, NULL, or default, specify it clearly (e.g., `''` or `NULL`).
                    - If using window functions or aggregations, include the full expression.
                - `TGT_TABLE_NAME`: 
                    - If the output is going into a permanent table, specify that table name.
                    - If going into a CTE or subquery, specify `RESULT_OF_<cte_or_alias>`.
                    - If it’s a final SELECT without a table, use `"unknown_target"`.
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
                - Record all columns created through complex expressions, even if they don’t have a simple source column.

                ---

                ###  **Step 5: Maintain Clear and Human-Readable Output**
                - Use column and table aliases exactly as shown in the original query for better readability.
                - Only output **valid JSON**. 
                - Do not include any explanations or comments outside of the JSON array.

                ---

                ###  **Final Output Example:**

                [
                    {
                        "SRC_TABLE_NAME": "b",
                        "SRC_COLUMN_NAME": "email",
                        "BUSINESS_RULE": "nvl(b.email, '')",
                        "TGT_TABLE_NAME": "RESULT_OF_subquery_a",
                        "TGT_COLUMN_NAME": "email"
                    },
                    {
                        "SRC_TABLE_NAME": "mkl",
                        "SRC_COLUMN_NAME": "alt_key",
                        "BUSINESS_RULE": "pmod(row_number() over (order by householdmemberidentifier), 2000)",
                        "TGT_TABLE_NAME": "RESULT_OF_final_query",
                        "TGT_COLUMN_NAME": "cn_hash_id"
                    },
                    {
                        "SRC_TABLE_NAME": "b",
                        "SRC_COLUMN_NAME": "gender",
                        "BUSINESS_RULE": "CASE WHEN b.gender = 'M' THEN 'Male' ELSE 'Female' END",
                        "TGT_TABLE_NAME": "RESULT_OF_final_query",
                        "TGT_COLUMN_NAME": "gender_category"
                    }
                ]

                ---

                ### **Important Rules:**
                - Your response must be **only the JSON array**.
                - Do not include explanations, comments, or additional text.
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

            analyze the following SQL query **starting from the innermost subquery or CTE outward**. 

            Return ONLY the valid JSON array as specified. Avoid any explanations or text outside of the JSON.

            SQL Query to Analyze:
            ```sql
            {sql_query}
            ```

            Your entire response must be ONLY the valid JSON array as specified. Do not include any explanations, apologies, or other text outside of the JSON structure.

        Response:
        """
        return prompt