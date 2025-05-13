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
                You are a SQL analysis expert focused on extracting column-level data transformations for lineage.
                **Focus Solely on User-Provided Query:**
                You will be given a single SQL query by the user for analysis. Your entire analysis and JSON output must pertain *exclusively* to this user-provided SQL query. The detailed SQL and JSON example provided further below in this prompt serves *only* to illustrate the required JSON structure, field definitions, and the *type* of detailed analysis expected. Do not treat the example SQL as the query to analyze unless it is the one explicitly provided by the user for analysis.

                Your output MUST be a single, valid JSON array. Each object in the array represents one transformation and MUST contain the following keys:
                - SRC_TABLE_NAME: Source table name (string).
                - SRC_COLUMN_NAME: Source column name (string).
                - BUSINESS_RULE: The exact transformation logic or qualified source column (string).
                - TGT_TABLE_NAME: Target table name (string).
                - TGT_COLUMN_NAME: Target column name (string).

                **Core Instructions for Transformation Extraction:**

                1.  **Identify Sources:** For every target column, list all contributing source tables and columns.
                2.  **Capture Business Rule:**
                    - Direct passthrough (e.g., `t1.col`): `BUSINESS_RULE` is the qualified source column.
                    - Complex logic (functions, CASE, arithmetic, multiple columns): `BUSINESS_RULE` is the full, exact expression.
                3.  **Determine Target Table Name (TGT_TABLE_NAME):**
                    - For `CREATE TABLE target_tbl AS ...` or `INSERT INTO target_tbl SELECT ...`: Use `target_tbl` as `TGT_TABLE_NAME`.
                    - For CTEs (e.g., `WITH cte_name AS (...)`): Use `RESULT_OF_cte_name` as `TGT_TABLE_NAME` when referring to the output of the CTE.
                    - For subquery aliases (e.g., `FROM (...) AS alias_name`): Use `RESULT_OF_alias_name` as `SRC_TABLE_NAME` or `TGT_TABLE_NAME` when referring to the output of that subquery.
                    - For a top-level `SELECT` query that does not explicitly create or insert into a table: Use `unknown_target` as `TGT_TABLE_NAME`.
                    - **Never use "query_output" as a TGT_TABLE_NAME.**
                4.  **Comprehensive Analysis & `SELECT *`:**
                    Account for all SQL constructs: CTEs, subqueries, aliases, nested functions, window functions, aggregations, complex expressions.
                    When encountering `SELECT source_alias.*` (e.g., `ab.*`), derive the lineage for each column that `source_alias.*` expands to. The `SRC_TABLE_NAME` for these columns will be the table(s) or CTEs/subqueries (e.g., `RESULT_OF_source_alias`) that `source_alias` itself is derived from. The `SRC_COLUMN_NAME` will be the original column name within that source, and `TGT_COLUMN_NAME` will be the same column name as it appears in the target. Do not use `*` as a `TGT_COLUMN_NAME`. Ensure all transformations are captured.


                **Example of JSON Output Structure and Analysis Depth (Illustrative - Analyze User's Query ONLY):**

                Consider the following input SQL query (which might have been generated from Python code, resulting in named placeholders like {cn_mstr_table}, {hub_cn_raw_table}, {sat_cn_raw_table}, {time_key_table}, etc.):
                ```sql
                CREATE TABLE {cn_mstr_table} location '{cn_mstr_table_path}' AS
                SELECT ab.*, b.loyaltycard_count
                FROM (
                    SELECT
                        a.householdmemberidentifier, a.retailbrand, a.ciam_customerprofile_detail_hkey, a.householdidentifier AS source_householdidentifier_for_ab, -- aliasing for clarity if used by ab.*
                        a.brandmemberstatus, a.prefferedstore, a.customercreationdate, a.customerupdateddate,
                        a.loyaltycardnumber, a.cardcreatedate, a.cardupdatedate, a.personalidentificationnumberprefix AS source_pinprefix_for_ab, -- aliasing for clarity
                        a.birthday, a.email AS source_email_for_ab, -- aliasing for clarity
                        a.mobilenumberprefix, a.registrationchannel, a.countryofissue, a.registrationagent,
                        CASE
                            WHEN a.current_age < 0 THEN ''
                            ELSE CAST(a.current_age AS VARCHAR) -- Ensure type consistency for CASE
                        END AS current_age, -- This is the current_age selected into ab
                        CASE
                            WHEN a.current_age < 0 THEN ''
                            WHEN a.current_age < 20 THEN '<20 Yrs'
                            WHEN a.current_age BETWEEN 20 AND 25 THEN '20-25 Yrs'
                            WHEN a.current_age BETWEEN 26 AND 30 THEN '26-30 Yrs'
                            WHEN a.current_age BETWEEN 31 AND 35 THEN '31-35 Yrs'
                            WHEN a.current_age BETWEEN 36 AND 40 THEN '36-40 Yrs'
                            WHEN a.current_age BETWEEN 41 AND 45 THEN '41-45 Yrs'
                            WHEN a.current_age BETWEEN 46 AND 50 THEN '46-50 Yrs'
                            WHEN a.current_age BETWEEN 51 AND 55 THEN '51-55 Yrs'
                            WHEN a.current_age BETWEEN 56 AND 59 THEN '56-59 Yrs'
                            WHEN a.current_age >= 60 THEN '>60 Yrs'
                            ELSE '' -- Ensure all paths return a value
                        END AS AGE_RANGE,
                        CASE
                            WHEN (SUBSTR(a.personalidentificationnumberprefix,7,8)) BETWEEN '00' AND '49' THEN 'Female'
                            WHEN (SUBSTR(a.personalidentificationnumberprefix,7,8)) BETWEEN '50' AND '99' THEN 'Male'
                            ELSE NULL -- Handle cases where substring might not be in these ranges
                        END AS GENDER,
                        CASE
                            WHEN (a.email = '' OR a.email = 'NULL' OR a.email IS NULL) THEN 'No'
                            ELSE 'Yes'
                        END AS HAS_EMAIL,
                        pmod(row_number() OVER (ORDER BY a.householdidentifier), 2000) AS cn_hash_id,
                        t.tm_dim_key AS cust_create_dt_key
                    FROM (
                        SELECT
                            h.householdmemberidentifier, h.retailbrand, s.ciam_customerprofile_detail_hkey, s.householdidentifier,
                            s.brandmemberstatus, s.prefferedstore, s.customercreationdate, s.customerupdateddate,
                            s.loyaltycardnumber, s.cardcreatedate, s.cardupdatedate, h.personalidentificationnumberprefix, -- Source from h
                            s.birthday, s.email, s.mobilenumberprefix, s.registrationchannel, s.countryofissue, s.registrationagent, -- Assuming these are from s, or join explicitly
                            CASE
                                WHEN h.personalidentificationnumberprefix IS NULL AND s.birthday IS NOT NULL THEN ROUND(CAST(((DATEDIFF(CURRENT_DATE,FROM_UNIXTIME(UNIX_TIMESTAMP(s.birthday,'yyyy/MM/dd'),'yyyy-MM-dd')))/365.25) AS INT),0)
                                WHEN h.personalidentificationnumberprefix IS NOT NULL AND CAST(h.personalidentificationnumberprefix AS INT) IS NULL THEN ROUND(CAST(((DATEDIFF(CURRENT_DATE,FROM_UNIXTIME(UNIX_TIMESTAMP(s.birthday,'yyyy/MM/dd'),'yyyy-MM-dd')))/365.25) AS INT),0)
                                WHEN h.personalidentificationnumberprefix IS NOT NULL AND CAST(FROM_UNIXTIME(UNIX_TIMESTAMP((SUBSTR(h.personalidentificationnumberprefix,1,6)),'yyMMdd'),'yyyy-MM-dd') AS DATE) IS NULL THEN ROUND(CAST(((DATEDIFF(CURRENT_DATE, FROM_UNIXTIME(UNIX_TIMESTAMP(s.birthday,'yyyy/MM/dd'),'yyyy-MM-dd')))/365.25) AS INT),0)
                                WHEN h.personalidentificationnumberprefix IS NOT NULL AND ROUND(CAST(((DATEDIFF(CURRENT_DATE, FROM_UNIXTIME(UNIX_TIMESTAMP((SUBSTR(h.personalidentificationnumberprefix,1,6)),'yyMMdd'),'yyyy-MM-dd')))/365.25) AS INT),0) < 50 THEN ROUND(CAST(((DATEDIFF(CURRENT_DATE, FROM_UNIXTIME(UNIX_TIMESTAMP(s.birthday,'yyyy/MM/dd'),'yyyy-MM-dd')))/365.25) AS INT),0)
                                WHEN h.personalidentificationnumberprefix IS NOT NULL AND s.birthday IS NOT NULL AND ROUND(CAST(((DATEDIFF(CURRENT_DATE, FROM_UNIXTIME(UNIX_TIMESTAMP((SUBSTR(h.personalidentificationnumberprefix,1,6)),'yyMMdd'),'yyyy-MM-dd')))/365.25) AS INT),0) <> ROUND(CAST(((DATEDIFF(CURRENT_DATE, FROM_UNIXTIME(UNIX_TIMESTAMP(s.birthday,'yyyy/MM/dd'),'yyyy-MM-dd')))/365.25) AS INT),0) THEN ROUND(CAST(((DATEDIFF(CURRENT_DATE, FROM_UNIXTIME(UNIX_TIMESTAMP((SUBSTR(h.personalidentificationnumberprefix,1,6)),'yyMMdd'),'yyyy-MM-dd')))/365.25) AS INT),0)
                                ELSE ROUND(CAST(((DATEDIFF(CURRENT_DATE, FROM_UNIXTIME(UNIX_TIMESTAMP(s.birthday,'yyyy/MM/dd'),'yyyy-MM-dd')))/365.25) AS INT),0)
                            END current_age, -- This is the calculated current_age
                            s.email, -- ensure email is selected out of subquery 'a' if used in 'ab'
                            rank() OVER (PARTITION BY s.ciam_customerprofile_detail_hkey ORDER BY s.load_date DESC) AS rank
                        FROM {hub_cn_raw_table} h
                        JOIN {sat_cn_raw_table} s ON h.ciam_customerprofile_detail_hkey = s.ciam_customerprofile_detail_hkey -- Explicit JOIN
                    ) a
                    INNER JOIN {time_key_table} t ON (FROM_UNIXTIME(UNIX_TIMESTAMP(a.customercreationdate,'yyyy/MM/dd'),'MM-dd-yy') = t.tm_end_date)
                    WHERE a.rank = 1
                ) ab
                LEFT OUTER JOIN (
                    SELECT h.householdmemberidentifier, h.retailbrand, count(distinct s.loyaltycardnumber) AS loyaltycard_count
                    FROM {sat_cn_raw_table} s
                    JOIN {hub_cn_raw_table} h ON s.ciam_customerprofile_detail_hkey = h.ciam_customerprofile_detail_hkey
                    GROUP BY h.householdmemberidentifier, h.retailbrand
                ) b ON ab.source_householdidentifier_for_ab = b.householdmemberidentifier AND ab.retailbrand = b.retailbrand
                ```

                For this specific query, some example transformations for the JSON output would be:
                ```json
                [
                    {
                        "SRC_TABLE_NAME": "hub_cn_raw_table",
                        "SRC_COLUMN_NAME": "householdmemberidentifier",
                        "BUSINESS_RULE": "h.householdmemberidentifier",
                        "TGT_TABLE_NAME": "cn_mstr_table",
                        "TGT_COLUMN_NAME": "householdmemberidentifier"
                    },
                    {
                        "SRC_TABLE_NAME": "RESULT_OF_a",
                        "SRC_COLUMN_NAME": "current_age",
                        "BUSINESS_RULE": "CASE WHEN a.current_age < 0 THEN '' WHEN a.current_age < 20 THEN '<20 Yrs' WHEN a.current_age BETWEEN 20 AND 25 THEN '20-25 Yrs' WHEN a.current_age BETWEEN 26 AND 30 THEN '26-30 Yrs' WHEN a.current_age BETWEEN 31 AND 35 THEN '31-35 Yrs' WHEN a.current_age BETWEEN 36 AND 40 THEN '36-40 Yrs' WHEN a.current_age BETWEEN 41 AND 45 THEN '41-45 Yrs' WHEN a.current_age BETWEEN 46 AND 50 THEN '46-50 Yrs' WHEN a.current_age BETWEEN 51 AND 55 THEN '51-55 Yrs' WHEN a.current_age BETWEEN 56 AND 59 THEN '56-59 Yrs' WHEN a.current_age >= 60 THEN '>60 Yrs' ELSE '' END",
                        "TGT_TABLE_NAME": "cn_mstr_table",
                        "TGT_COLUMN_NAME": "AGE_RANGE"
                    },
                    {
                        "SRC_TABLE_NAME": "hub_cn_raw_table",
                        "SRC_COLUMN_NAME": "personalidentificationnumberprefix",
                        "BUSINESS_RULE": "CASE WHEN (SUBSTR(h.personalidentificationnumberprefix,7,8)) BETWEEN '00' AND '49' THEN 'Female' WHEN (SUBSTR(h.personalidentificationnumberprefix,7,8)) BETWEEN '50' AND '99' THEN 'Male' ELSE NULL END",
                        "TGT_TABLE_NAME": "cn_mstr_table",
                        "TGT_COLUMN_NAME": "GENDER"
                    },
                    {
                        "SRC_TABLE_NAME": "RESULT_OF_b",
                        "SRC_COLUMN_NAME": "loyaltycard_count",
                        "BUSINESS_RULE": "b.loyaltycard_count",
                        "TGT_TABLE_NAME": "cn_mstr_table",
                        "TGT_COLUMN_NAME": "loyaltycard_count"
                    },
                    {
                        "SRC_TABLE_NAME": "RESULT_OF_a",
                        "SRC_COLUMN_NAME": "householdidentifier",
                        "BUSINESS_RULE": "pmod(row_number() OVER (ORDER BY a.householdidentifier), 2000)",
                        "TGT_TABLE_NAME": "cn_mstr_table",
                        "TGT_COLUMN_NAME": "cn_hash_id"
                    },
                    {
                        "SRC_TABLE_NAME": "sat_cn_raw_table",
                        "SRC_COLUMN_NAME": "email",
                        "BUSINESS_RULE": "CASE WHEN (s.email = '' OR s.email = 'NULL' OR s.email IS NULL) THEN 'No' ELSE 'Yes' END",
                        "TGT_TABLE_NAME": "cn_mstr_table",
                        "TGT_COLUMN_NAME": "HAS_EMAIL"
                    }
                ]
                ```
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
