import json
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
import os
from groq import Groq
from dotenv import load_dotenv
import logging

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class LineageEntry:
    src_table_name: str
    src_column_name: str
    business_rule: str
    tgt_table_name: str
    tgt_column_name: str

    def to_dict(self) -> Dict[str, str]:
        return {
            "SRC_TABLE_NAME": self.src_table_name,
            "SRC_COLUMN_NAME": self.src_column_name,
            "BUSINESS_RULE": self.business_rule,
            "TGT_TABLE_NAME": self.tgt_table_name,
            "TGT_COLUMN_NAME": self.tgt_column_name
        }

class LLMSQLLineageExtractor:
    def __init__(self, api_key: Optional[str] = None):
        """Initialize the LLM-based SQL lineage extractor."""
        self.api_key = api_key or os.getenv("GROQ_API_KEY")
        if not self.api_key:
            raise ValueError(
                "Groq API key not found. Please either:\n"
                "1. Create a .env file with GROQ_API_KEY=your_key_here\n"
                "2. Set GROQ_API_KEY environment variable\n"
                "3. Pass the API key directly to LLMSQLLineageExtractor()"
            )
        
        self.client = Groq(api_key=self.api_key)
        self.lineage_entries: List[LineageEntry] = []

    def _clean_llm_response(self, response: str) -> str:
        """Clean the LLM response to ensure valid JSON."""
        # Remove any markdown code block markers
        response = response.replace("```json", "").replace("```", "").strip()
        
        # Find the first '[' and last ']' to extract just the JSON array
        start = response.find('[')
        end = response.rfind(']')
        
        if start != -1 and end != -1:
            return response[start:end+1]
        return response

    def _split_query(self, query: str) -> List[str]:
        """Split a large query into smaller, analyzable chunks."""
        # Split on major SQL clauses
        chunks = []
        current_chunk = []
        depth = 0
        current_word = []
        
        for char in query:
            if char == '(':
                depth += 1
            elif char == ')':
                depth -= 1
            
            current_word.append(char)
            
            if char.isspace() and depth == 0:
                word = ''.join(current_word).strip()
                current_word = []
                
                current_chunk.append(word)
                
                # Check if this word is a major SQL clause
                if word.upper() in {'SELECT', 'FROM', 'WHERE', 'GROUP BY', 'ORDER BY', 'HAVING'}:
                    if len(' '.join(current_chunk)) > 100:  # If chunk is getting large
                        chunks.append(' '.join(current_chunk))
                        current_chunk = [word]
            
        # Add the last chunk
        if current_word:
            current_chunk.append(''.join(current_word).strip())
        if current_chunk:
            chunks.append(' '.join(current_chunk))
        
        return chunks

    def _extract_lineage_with_llm(self, query: str, query_type: str) -> List[Dict[str, str]]:
        """Use Groq LLM to extract lineage information from a SQL query."""
        
        system_content = """You are a SQL analysis expert. Your task is to meticulously analyze the provided SQL query and extract all column-level data transformations.
            Deconstruct the query, paying close attention to Common Table Expressions (CTEs), nested subqueries, table/column aliases, and especially temporal transformations to trace the complete data lineage.
            
            Your output MUST be a JSON array, where each object represents a single column-level transformation with the following fields:

            "SRC_TABLE_NAME":
              - If the source is a base table whose name is a literal IN THE SQL QUERY YOU RECEIVE, use that literal name (e.g., "actual_sales_table").
              - If the source is an aliased subquery or CTE (e.g., "... FROM (SELECT ...) AS alias_name" or "WITH alias_name AS (...) ..."), the name should be "RESULT_OF_alias_name".
              - If the source is an unaliased intermediate result set, generate a sequential name like "intermediate_result_set_1", "intermediate_result_set_2".
              - For date/temporal fields, always trace back to the original source table, not intermediate transformations.

            "SRC_COLUMN_NAME":
              - The name of the source column (e.g., "alt_key", "email", "effective_date").
              - DO NOT prefix this with table or subquery aliases (e.g., use "alt_key", not "t1.alt_key").
              - For date fields, identify the actual source column name, even if it's being transformed (e.g., "effective_start_date" not just "date").

            "BUSINESS_RULE":
              - The exact expression or logic used for the transformation (e.g., "nvl(b.email,'')", "mkl.alt_key", "date_trunc('month', effective_date)").
              - THIS FIELD SHOULD REFLECT ALIASES if they are used in the expression (e.g., 'b.email' if 'b' is the alias in the expression).
              - For date transformations, include the FULL transformation logic (e.g., "CAST(date_column AS DATE)", "DATE_ADD(start_date, INTERVAL 1 DAY)").
              - For direct passthrough, this will be the qualified source column (e.g. "mkl.alt_key" if "mkl" is the alias).
              - For date range columns, capture any BETWEEN, comparison operators, or date arithmetic being used.

            "TGT_TABLE_NAME":
              - If the query creates or inserts into a table whose name is a literal IN THE SQL QUERY YOU RECEIVE (e.g., "CREATE TABLE target_table AS ...", "INSERT INTO target_table ..."), use "target_table".
              - If the transformation defines columns for an aliased subquery or CTE (e.g. "WITH alias_name AS (SELECT col1 AS target_col ... )" or "(SELECT col1 AS target_col ...) AS alias_name"), then "TGT_TABLE_NAME" for that step is "RESULT_OF_alias_name".
              - If it's an unaliased intermediate result set being defined, use the same generated name as its corresponding "SRC_TABLE_NAME" when it's consumed (e.g., "intermediate_result_set_1").
              - For plain SELECT statements not explicitly storing results in a named structure recognizable from the query, use "query_output".

            "TGT_COLUMN_NAME":
              - The name of the target column, often an alias defined in the SELECT list (e.g., "householdmemberidentifier" from "mkl.alt_key AS householdmemberidentifier").
              - For date fields, use the exact alias or target column name as specified in the query.

            IMPORTANT FOR COMPLETENESS:
            - Pay special attention to date/temporal fields:
              * Track all date manipulations (CAST, CONVERT, date arithmetic)
              * For date ranges (date_from, date_to), identify the actual source columns and any range calculations
              * Look for date functions like DATE_TRUNC, DATE_ADD, DATEADD, etc.
            - Analyze EACH part of the SELECT list, EACH generated column in a CREATE TABLE AS, and EACH column in an INSERT INTO ... SELECT statement.
            - Unpack all expressions, including those within CASE statements, function calls, and arithmetic operations, into individual lineage rows if they map to a target column.
            - If a single source column directly maps to a target column without transformation, represent this as a row.
            - If multiple source columns are used in a business rule for one target column, create one row for that target column, listing all sources in the business rule.
            - Be meticulous: aim to extract ALL column-level transformations. The goal is to achieve a comprehensive lineage map, similar to what a programmatic parser would achieve.
            - i dont want to see anything as column_name 0r column1 if it is not a column name. do well to understand the query and extract the correct column names.
            """

        # Split large queries into chunks
        if len(query) > 2000:  # Threshold for splitting
            query_chunks = self._split_query(query)
            print(f"Split query into {len(query_chunks)} chunks")
        else:
            query_chunks = [query]

        all_lineage_entries = []
        
        for i, chunk in enumerate(query_chunks):
            chunk_context = f"This is part {i+1} of {len(query_chunks)} of a larger query. " if len(query_chunks) > 1 else ""
            
            user_content = f"""SQL Query Type: {query_type}

{chunk_context}SQL Query to analyze:
{chunk}

Remember: Your response must be ONLY the valid JSON array without any explanations or comments outside of it."""

            try:
                response = self.client.chat.completions.create(
                    model="deepseek-r1-distill-llama-70b",
                    messages=[
                        {"role": "system", "content": system_content},
                        {"role": "user", "content": user_content}
                    ],
                    temperature=0,
                    max_tokens=4000
                )

                # Get the response content
                content = response.choices[0].message.content
                print(f"Raw LLM Response for chunk {i+1}:\n{content}\n")

                # Clean and parse the response
                cleaned_content = self._clean_llm_response(content)
                print(f"Cleaned Response for chunk {i+1}:\n{cleaned_content}\n")

                try:
                    lineage_data = json.loads(cleaned_content)
                    if isinstance(lineage_data, list):
                        all_lineage_entries.extend(lineage_data)
                    else:
                        print(f"Error: LLM response for chunk {i+1} is not a JSON array")
                except json.JSONDecodeError as e:
                    print(f"Error parsing cleaned response for chunk {i+1}: {e}")

            except Exception as e:
                print(f"Error calling LLM API for chunk {i+1}: {e}")

        return all_lineage_entries

    def process_single_query(self, query: str, query_type: str) -> List[LineageEntry]:
        """Process a single SQL query and extract its lineage information.
        
        Args:
            query (str): The SQL query to process
            query_type (str): The type of SQL query (e.g., 'SELECT', 'INSERT', etc.)
            
        Returns:
            List[LineageEntry]: List of extracted lineage entries for this query
        """
        logger.info(f"Processing {query_type} query of length {len(query)}")
        
        query_lineage_entries = []
        try:
            # Extract lineage using LLM
            lineage_entries = self._extract_lineage_with_llm(query, query_type)
            
            # Convert to LineageEntry objects
            for entry in lineage_entries:
                try:
                    lineage_entry = LineageEntry(
                        src_table_name=entry['SRC_TABLE_NAME'],
                        src_column_name=entry['SRC_COLUMN_NAME'],
                        business_rule=entry['BUSINESS_RULE'],
                        tgt_table_name=entry['TGT_TABLE_NAME'],
                        tgt_column_name=entry['TGT_COLUMN_NAME']
                    )
                    query_lineage_entries.append(lineage_entry)
                except KeyError as e:
                    logger.error(f"Missing required field in entry: {e}")
                    logger.debug(f"Problematic entry: {entry}")
                    continue
                except Exception as e:
                    logger.error(f"Error processing entry: {e}")
                    logger.debug(f"Problematic entry: {entry}")
                    continue
                    
            logger.info(f"Successfully extracted {len(query_lineage_entries)} lineage entries from query")
            
        except Exception as e:
            logger.error(f"Error processing query: {e}")
            logger.debug(f"Problematic query: {query}")
            
        return query_lineage_entries

    def process_json_file(self, input_file: str, output_file: str):
        """Process a JSON file containing SQL queries and generate lineage information."""
        try:
            # Read input JSON file
            logger.info(f"Reading input file: {input_file}")
            with open(input_file, 'r') as f:
                data = json.load(f)

            total_queries = len(data.get('queries', []))
            logger.info(f"Found {total_queries} queries to process")

            # Process each query individually
            for i, query_info in enumerate(data.get('queries', []), 1):
                query = query_info.get('query', '').strip()
                query_type = query_info.get('type', '')
                
                # Skip empty queries
                if not query:
                    logger.warning(f"Skipping empty query at index {i}")
                    continue

                logger.info(f"Processing query {i}/{total_queries}")
                
                # Process single query and collect its lineage entries
                query_lineage_entries = self.process_single_query(query, query_type)
                self.lineage_entries.extend(query_lineage_entries)

            # Write lineage information to output file
            lineage_output = [entry.to_dict() for entry in self.lineage_entries]
            
            logger.info(f"Writing {len(self.lineage_entries)} lineage entries to {output_file}")
            with open(output_file, 'w') as f:
                json.dump(lineage_output, f, indent=2)

            logger.info(f"Successfully processed all queries. Output written to {output_file}")

        except Exception as e:
            logger.error(f"Error processing file: {str(e)}")
            raise

def main():
    # Example usage
    extractor = LLMSQLLineageExtractor()
    input_file = "result.json"  # The input JSON file containing SQL queries
    output_file = "lineage.json"  # The output file for lineage information
    extractor.process_json_file(input_file, output_file)

if __name__ == "__main__":
    main() 