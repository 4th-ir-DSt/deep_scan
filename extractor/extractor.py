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
You are a SQL analysis expert. Your task is to meticulously analyze the provided SQL query and extract all column-level data transformations for data lineage.

For each target column, you must:
- Identify every source table and column involved.
- Capture the exact transformation logic, calculation, or business rule as it appears in the SELECT list or transformation.
- If the logic is a function, CASE, arithmetic, or uses multiple columns, include the full expression as BUSINESS_RULE.
- If a target column is a direct passthrough, BUSINESS_RULE is the qualified source column (e.g., t1.col).
- If a target column is derived from multiple sources, BUSINESS_RULE must show the full logic, and all source columns must be listed.

Handle:
- CTEs, subqueries, and aliases (use RESULT_OF_alias_name for intermediate tables).
- Placeholders (e.g., {table_name_var}) as described.
- All SQL constructs, including nested CASE, window functions, aggregations, and expressions.

For each transformation, set TGT_TABLE_NAME as follows:
- If the query is part of a CREATE TABLE ... AS SELECT ... or INSERT INTO ... SELECT ..., use the explicit table name.
- If the transformation is for a CTE or subquery, use RESULT_OF_<cte_or_alias_name>.
- If the query is a top-level SELECT with no explicit target, use "unknown_target" (not "query_output").
- Never use "query_output" as a target table name.

Your output MUST be a JSON array, where each object has:
- SRC_TABLE_NAME
- SRC_COLUMN_NAME
- BUSINESS_RULE
- TGT_TABLE_NAME
- TGT_COLUMN_NAME

Example output:
[
    {
        "SRC_TABLE_NAME": "sales",
        "SRC_COLUMN_NAME": "amount",
        "BUSINESS_RULE": "SUM(s.amount) OVER (PARTITION BY s.region)",
        "TGT_TABLE_NAME": "RESULT_OF_sales_agg",
        "TGT_COLUMN_NAME": "regional_sales"
    },
    {
        "SRC_TABLE_NAME": "users",
        "SRC_COLUMN_NAME": "age",
        "BUSINESS_RULE": "CASE WHEN u.age > 18 THEN 'adult' ELSE 'minor' END",
        "TGT_TABLE_NAME": "RESULT_OF_user_status",
        "TGT_COLUMN_NAME": "age_group"
    },
    {
        "SRC_TABLE_NAME": "orders",
        "SRC_COLUMN_NAME": "order_date",
        "BUSINESS_RULE": "DATE_TRUNC('month', o.order_date)",
        "TGT_TABLE_NAME": "RESULT_OF_monthly_orders",
        "TGT_COLUMN_NAME": "order_month"
    },
    {
        "SRC_TABLE_NAME": "products",
        "SRC_COLUMN_NAME": "price",
        "BUSINESS_RULE": "p.price * 1.2",
        "TGT_TABLE_NAME": "RESULT_OF_price_update",
        "TGT_COLUMN_NAME": "adjusted_price"
    },
    {
        "SRC_TABLE_NAME": "sales",
        "SRC_COLUMN_NAME": "amount",
        "BUSINESS_RULE": "CASE WHEN s.amount > 1000 THEN 'high' ELSE 'low' END",
        "TGT_TABLE_NAME": "RESULT_OF_sales_flag",
        "TGT_COLUMN_NAME": "amount_flag"
    },
    {
        "SRC_TABLE_NAME": "orders",
        "SRC_COLUMN_NAME": "order_id",
        "BUSINESS_RULE": "o.order_id",
        "TGT_TABLE_NAME": "RESULT_OF_order_ids",
        "TGT_COLUMN_NAME": "order_id"
    },
    {
        "SRC_TABLE_NAME": "sales",
        "SRC_COLUMN_NAME": "amount",
        "BUSINESS_RULE": "s.amount",
        "TGT_TABLE_NAME": "unknown_target",
        "TGT_COLUMN_NAME": "amount"
    }
]

Be exhaustive and precise. Do not omit any transformation or business rule. Output ONLY the JSON array.
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
