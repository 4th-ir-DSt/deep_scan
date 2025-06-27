#!/usr/bin/env python3
import json
import sys
import re
import os
from json import JSONDecodeError
from groq import Groq
from typing import Dict, List, Optional, Tuple
import tiktoken


def count_tokens(text: str, model: str = "deepseek-r1-distill-llama-70b") -> int:
    """
    Count tokens in text using tiktoken.
    """
    try:
        encoding = tiktoken.encoding_for_model(model)
        return len(encoding.encode(text))
    except:
        # Fallback: rough estimation (1 token ≈ 4 characters)
        return len(text) // 4


def chunk_code_by_methods(code_text: str, max_tokens: int = 4000) -> List[str]:
    """
    Split code into chunks by methods to stay within token limits.
    """
    chunks = []
    current_chunk = ""
    
    # Split by class and method definitions
    lines = code_text.split('\n')
    i = 0
    
    while i < len(lines):
        line = lines[i]
        
        # Check if this line starts a class or method
        if (line.strip().startswith('class ') or 
            line.strip().startswith('def ') or
            line.strip().startswith('    def ')):
            
            # If current chunk is getting large, save it and start new
            if count_tokens(current_chunk) > max_tokens:
                if current_chunk.strip():
                    chunks.append(current_chunk.strip())
                current_chunk = ""
            
            # Add the method/class definition
            current_chunk += line + '\n'
            i += 1
            
            # Add subsequent lines until we hit another method/class or end
            while i < len(lines):
                next_line = lines[i]
                if (next_line.strip().startswith('class ') or 
                    next_line.strip().startswith('def ') or
                    next_line.strip().startswith('    def ')):
                    break
                
                current_chunk += next_line + '\n'
                i += 1
        else:
            current_chunk += line + '\n'
            i += 1
    
    # Add the last chunk
    if current_chunk.strip():
        chunks.append(current_chunk.strip())
    
    return chunks


def extract_key_sections(code_text: str) -> str:
    """
    Extract only the most relevant sections for lineage analysis.
    """
    relevant_sections = []
    
    # Extract class definition and imports
    lines = code_text.split('\n')
    class_start = -1
    process_method_start = -1
    process_method_end = -1
    
    for i, line in enumerate(lines):
        if 'class DataMovementProcessor:' in line:
            class_start = i
        elif 'def process(self):' in line:
            process_method_start = i
        elif process_method_start != -1 and line.strip().startswith('def ') and i > process_method_start:
            process_method_end = i
            break
    
    # Add imports and class definition
    if class_start != -1:
        relevant_sections.append('\n'.join(lines[:class_start + 10]))
    
    # Add process method
    if process_method_start != -1:
        if process_method_end == -1:
            process_method_end = len(lines)
        relevant_sections.append('\n'.join(lines[process_method_start:process_method_end]))
    
    # Add key configuration methods
    key_methods = ['getConfigurationInfo', 'getTableConfigDict', 'getSQLQuery']
    for method in key_methods:
        for i, line in enumerate(lines):
            if f'def {method}(self):' in line:
                # Find method end
                j = i + 1
                while j < len(lines) and (lines[j].startswith(' ') or lines[j].strip() == ''):
                    j += 1
                relevant_sections.append('\n'.join(lines[i:j]))
                break
    
    return '\n\n'.join(relevant_sections)


def extract_from_file(file_path: str, model: str = "deepseek-r1-distill-llama-70b") -> Optional[Dict]:
    """
    Extract data lineage and mapping from a given Python file.
    Returns structured JSON output with improved token management.
    """
    client = Groq()

    # Read the wrapper code
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            code_text = f.read()
    except IOError as e:
        print(f"Error reading file {file_path}: {e}", file=sys.stderr)
        return None

    # Extract only relevant sections to reduce token count
    relevant_code = extract_key_sections(code_text)
    
    # Check token count
    token_count = count_tokens(relevant_code)
    print(f"Token count for relevant code: {token_count}")
    
    if token_count > 5000:  # Conservative limit
        print("Code is still too large. Using chunking approach...")
        chunks = chunk_code_by_methods(relevant_code, max_tokens=3000)
        return extract_from_chunks(chunks, client, model)
    
    return extract_from_single_chunk(relevant_code, client, model)


def extract_from_chunks(chunks: List[str], client: Groq, model: str) -> Optional[Dict]:
    """
    Extract information from multiple code chunks and merge results.
    """
    all_results = []
    
    for i, chunk in enumerate(chunks):
        print(f"Processing chunk {i+1}/{len(chunks)}...")
        result = extract_from_single_chunk(chunk, client, model)
        if result:
            all_results.append(result)
    
    # Merge results
    if not all_results:
        return None
    
    merged_result = {
        'lineage': [],
        'mapping': {'metadata_tables': [], 'table_config_dict': {}},
        'sql_queries': []
    }
    
    for result in all_results:
        if 'lineage' in result:
            merged_result['lineage'].extend(result['lineage'])
        if 'mapping' in result:
            if 'metadata_tables' in result['mapping']:
                merged_result['mapping']['metadata_tables'].extend(result['mapping']['metadata_tables'])
            if 'table_config_dict' in result['mapping']:
                merged_result['mapping']['table_config_dict'].update(result['mapping']['table_config_dict'])
        if 'sql_queries' in result:
            merged_result['sql_queries'].extend(result['sql_queries'])
    
    return merged_result


def extract_from_single_chunk(code_text: str, client: Groq, model: str) -> Optional[Dict]:
    """
    Extract information from a single code chunk.
    """
    # System prompt to dynamically extract definitions from code
    system_prompt = (

    "You are a meticulous Data Engineering Expert. Your task is to perform a comprehensive and precise analysis "

    "of the provided Python code to extract data lineage, detailed data mapping, and ALL embedded SQL queries. "

    "Your analysis MUST be based SOLELY on the content of the provided code. "

    "The final output MUST be a valid JSON object with exactly three top-level keys: 'lineage', 'mapping', and 'sql_queries'.\n\n"
 
    "1.  **lineage**: Identify the sequence of data movement steps by thoroughly parsing the 'process' method within the code and any associated SQL queries or file operations. "

    "    For each distinct data movement step, create a JSON object with the following fields:\n"

    "    -   `step` (integer): A sequential number for the lineage step.\n"

    "    -   `source` (string): The origin of the data for this step (e.g., 'FTP', 'comm_ops.cntl_file_ftp_log', 'S3 staging', 'RDS database', 'Redshift data warehouse').\n"

    "    -   `target` (string): The destination of the data for this step (e.g., 'comm_ops.cntl_file_ftp_log', 'comm_ops.cntl_file_log', 'S3 path', 'RDS table', 'Redshift table', 'SFTP', 'FTP').\n"

    "    -   `description` (string): A concise, technical description of the data movement or transformation occurring in this step.\n"

    "    Consider the following common flow patterns:\n"

    "    -   'FTP' to 'comm_ops.cntl_file_ftp_log' (Initial file arrival logging).\n"

    "    -   From 'comm_ops.cntl_file_ftp_log' (or 'comm_ops.cntl_file_log') to a staging 'S3 path' (via processing logic that might involve temporary EC2 storage).\n"

    "    -   From 'S3 staging' to a target database via COPY command to either 'RDS' or 'Redshift'.\n"

    "    -   From 'RDS' or 'Redshift' to 'S3' (for outbound data).\n"

    "    -   From 'EC2' (intermediate processing) to 'SFTP' or 'FTP' (for external delivery).\n\n"
 
    "2.  **mapping**: Parse the Python code, specifically focusing on SQL queries and methods that construct data configurations (e.g., `getTableConfigDict` or similar functions) to list detailed metadata for tables involved. \n"

    "    -   **`metadata_tables`**: A list of JSON objects, each representing a table from which metadata (columns) are explicitly selected or referenced in the code. Each object should have:\n"

    "        -   `name` (string): The fully qualified table name (e.g., 'comm_ops.cntl_src_mast').\n"

    "        -   `key_column` (string): The primary or joining column used for data identification (e.g., 'dtst_id' for all control tables mentioned).\n"

    "        -   `provides` (list of strings): A list of column names that are explicitly selected, used, or provided by this table in the code's context.\n"

    "        -   **Example Tables to look for:**\n"

    "            -   `comm_ops.cntl_src_mast`: Look for `bckt_nm`, `stg_path`, `delm`.\n"

    "            -   `comm_ops.cntl_file_ftp_log`: Look for `src_file_actl_nm`, `src_file_ptrn`.\n"

    "            -   `comm_ops.cntl_file_log`: Look for `file_log_id`, `dtst_id`, `cycl_time_id`, `scen_id`, `file_sta`.\n"

    "            -   `comm_ops.cntl_obj_metadata_mast`: Look for `obj_nm`.\n"

    "    -   **`table_config_dict`**: A dictionary (JSON object) representing configurations typically built for each `dtst_id` (dataset ID). List the keys and their purpose that are explicitly defined or set in the code within such configuration structures. Expected keys include, but are not limited to: `stg_path`, `target_table_nm`, `target_schema_nm`, `ec2_path`, `tab_nm`, `tbl_list`, `delimiter`, `src_file_pattern`.\n\n"
 
    "3.  **sql_queries**: Extract and meticulously analyze ALL SQL queries found within the provided Python code. This includes SQL strings assigned to variables, passed as arguments to functions, embedded in f-strings, or even appearing in comments if clearly distinguishable as SQL. "

    "    For each identified SQL query, create a JSON object with the following fields:\n"

    "    -   `query_type` (string): The type of SQL operation (e.g., 'SELECT', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP', 'ALTER', 'COPY', 'UNLOAD', etc.).\n"

    "    -   `source_tables` (list of strings): A list of fully qualified table names from which data is being read. Include tables from subqueries, joins, and any `FROM` or `JOIN` clauses.\n"

    "    -   `target_tables` (list of strings): A list of fully qualified table names to which data is being written or modified (e.g., in `INSERT INTO`, `UPDATE`, `CREATE TABLE`, `COPY INTO`, `UNLOAD TO` statements).\n"

    "    -   `purpose` (string): A concise technical explanation of what the query achieves in the context of data movement or processing within the application.\n"

    "    -   `sql_text` (string): The complete, actual SQL query as it appears in the code.\n"

    "    -   `method_name` (string): The name of the Python method or function where this SQL query is found.\n"

    "    **IMPORTANT**: Be extremely thorough and extract EVERY detectable SQL query. Parse complex SQL structures including subqueries, CTEs (Common Table Expressions), and intricate join conditions to identify all source and target tables accurately."

)
 

    user_prompt = f"""
Here is the code to analyze:
```python
{code_text}
```

extract the data lineage, mapping, and all SQL queries. Be extremely thorough and ensure that every SQL query, no matter how it is embedded or passed, is identified.
IMPORTANT: Look for ALL SQL queries in the code, including:
- String variables containing SQL
- Method calls that execute SQL
- Configuration queries
- Logging queries
- Data movement queries
- Any SQL-like operations
- Subqueries and complex joins
- COPY commands for data loading
- INSERT/UPDATE/DELETE operations
- CREATE/DROP/ALTER statements
"""

    # Invoke the model with retry logic
    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                max_tokens=2000,
                seed=35,
                temperature=0.0
            )
            raw_content = response.choices[0].message.content
            break
        except Exception as e:
            if "rate_limit" in str(e).lower() or "413" in str(e):
                print(f"Rate limit hit on attempt {attempt + 1}. Waiting...")
                import time
                time.sleep(2 ** attempt)  # Exponential backoff
                continue
            else:
                print(f"Error calling Groq API: {e}", file=sys.stderr)
                return None
    else:
        print("Max retries exceeded for API calls", file=sys.stderr)
        return None

    # Parse JSON output robustly
    return parse_json_response(raw_content)


def parse_json_response(raw_content: str) -> Optional[Dict]:
    """
    Robustly parse JSON from model response.
    """
    try:
        return json.loads(raw_content)
    except JSONDecodeError:
        # Try to extract JSON from code fences
        fence_match = re.search(r'```json\s*(\{.*?\})\s*```', raw_content, re.S)
        if fence_match:
            json_str = fence_match.group(1)
        else:
            # Try to find JSON between braces
            start = raw_content.find('{')
            end = raw_content.rfind('}')
            if start != -1 and end != -1 and end > start:
                json_str = raw_content[start:end+1]
            else:
                print("\nFailed to parse JSON from model response. Raw response:\n", raw_content)
                return None
        
        try:
            return json.loads(json_str)
        except JSONDecodeError:
            print("\nExtracted segment not valid JSON. Segment:\n", json_str)
            return None


def export_to_json(data: Dict, output_path: str = "extraction_output.json") -> None:
    """
    Exports the extracted data lineage and mapping to a JSON file.
    """
    try:
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        print(f"Exported JSON file: {output_path}")
    except Exception as e:
        print(f"Error exporting JSON file: {e}", file=sys.stderr)


def main():
    file_path = r"Test_file/rule_engine_executor_rs.py"
    model = "deepseek-r1-distill-llama-70b"

    print(f"Processing file: {file_path}")
    
    # Check if file exists
    if not os.path.exists(file_path):
        print(f"Error: File {file_path} does not exist.")
        return

    # Extract data from the given file
    result = extract_from_file(file_path, model)

    if result:
        # Export the result to JSON
        export_to_json(result, "extraction_output.json")
        print("Data extraction completed successfully!")
    else:
        print("Error extracting data from the file.")


if __name__ == "__main__":
    main() 