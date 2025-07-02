#!/usr/bin/env python3
import json
import sys
import re
import os
import ast
from json import JSONDecodeError
from groq import Groq
from typing import Dict, List, Optional, Any
import tiktoken

# --- Token Management ---

def count_tokens(text: str, model: str = "deepseek-r1-distill-llama-70b") -> int:
    """Counts tokens in a string using tiktoken, with a fallback for unsupported models."""
    try:
        encoding = tiktoken.encoding_for_model(model)
        return len(encoding.encode(text))
    except KeyError:
        return len(text) // 4

# --- Structure-Aware Code Chunking ---

def chunk_code_by_structure(code_text: str, max_tokens: int = 3800, model: str = "deepseek-r1-distill-llama-70b") -> List[str]:
    """
    Splits Python code into logically coherent chunks (imports, classes, functions)
    using AST, ensuring each chunk is within the token limit.
    """
    try:
        tree = ast.parse(code_text)
        chunks = []
        current_chunk_nodes = []
        
        # Separate top-level nodes
        top_level_nodes = list(ast.iter_child_nodes(tree))
        
        for node in top_level_nodes:
            node_source = ast.get_source_segment(code_text, node)
            if not node_source:
                continue

            # If adding the next node exceeds the token limit, finalize the current chunk
            current_chunk_text = "\n".join(ast.get_source_segment(code_text, n) for n in current_chunk_nodes)
            if current_chunk_nodes and count_tokens(current_chunk_text + "\n" + node_source, model) > max_tokens:
                chunks.append(current_chunk_text)
                current_chunk_nodes = []

            current_chunk_nodes.append(node)

        # Add the last remaining chunk
        if current_chunk_nodes:
            chunks.append("\n".join(ast.get_source_segment(code_text, n) for n in current_chunk_nodes))
            
        # If any chunk is still too large, fall back to simple chunking for that piece
        final_chunks = []
        for chunk in chunks:
            if count_tokens(chunk, model) > max_tokens:
                final_chunks.extend(chunk_code_by_lines(chunk, max_tokens, model))
            else:
                final_chunks.append(chunk)

        return final_chunks

    except SyntaxError:
        # If code is not valid Python, fall back to line-based chunking
        return chunk_code_by_lines(code_text, max_tokens, model)

def chunk_code_by_lines(code_text: str, max_tokens: int, model: str) -> List[str]:
    """A fallback chunking method that splits code by lines."""
    lines = code_text.split('\n')
    chunks = []
    current_chunk = ""
    for line in lines:
        if count_tokens(current_chunk + line + '\n', model) > max_tokens:
            chunks.append(current_chunk)
            current_chunk = ""
        current_chunk += line + '\n'
    if current_chunk:
        chunks.append(current_chunk)
    return chunks

# --- LLM-based Extraction ---

def get_llm_analysis(code_chunk: str, client: Groq, model: str, is_chunk: bool) -> Optional[Dict]:
    """Uses an LLM to perform deep analysis of a code chunk."""
    chunk_context_prompt = ""
    if is_chunk:
        chunk_context_prompt = "Note: You are analyzing a chunk of a larger script. The data lineage steps should be relative to this chunk, and will be re-sequenced later. Focus on accurately identifying sources, targets, and transformations within this specific piece of code."

    system_prompt = f"""
You are a world-class Data Engineering Expert. Your task is to analyze the provided code 
to extract data lineage, data mapping, and all embedded SQL queries. Your analysis must be 
based solely on the provided code. {chunk_context_prompt}

The final output must be a valid JSON object with three keys: 'lineage', 'mapping', and 'sql_queries'.

1.  **lineage**: A list of objects, each describing a data movement step. Each object should have:
    -   `step` (integer): Sequential step number, starting from 1 for this chunk.
    -   `source` (string): The data's origin (e.g., 'FTP', 'S3 Bucket', 'Database Table'). Be specific.
    -   `target` (string): The data's destination. Be specific.
    -   `description` (string): A clear, concise description of the step, including any business logic or transformations applied.

2.  **mapping**: An object describing data transformations. It should contain:
    -   `metadata_tables`: A list of tables with their key columns and provided fields.
    -   `table_config_dict`: A dictionary of configurations used for data processing.

3.  **sql_queries**: A list of all SQL queries found. Each object should have:
    -   `query_type` (string): The SQL operation type (e.g., 'SELECT', 'INSERT', 'COPY').
    -   `source_tables` (list of strings): Tables data is read from.
    -   `target_tables` (list of strings): Tables data is written to.
    -   `purpose` (string): A technical explanation of the query's goal.
    -   `sql_text` (string): The full SQL query.
    -   `method_name` (string): The Python method where the query is located.
"""

    user_prompt = f"""
Analyze the following code and extract the data lineage, mapping, and all SQL queries.
Be extremely thorough.

```python
{code_chunk}
```
"""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                max_tokens=4096,
                seed=42,
                temperature=0.0
            )
            return parse_json_response(response.choices[0].message.content)
        except Exception as e:
            if "rate_limit" in str(e).lower() or "413" in str(e):
                print(f"Rate limit hit, attempt {attempt + 1}. Retrying...")
                import time
                time.sleep(2 ** attempt)
            else:
                print(f"Error calling Groq API: {e}", file=sys.stderr)
                return None
    print("Max retries exceeded for API calls.", file=sys.stderr)
    return None

def parse_json_response(raw_content: str) -> Optional[Dict]:
    """Robustly parses JSON from a model's response."""
    try:
        match = re.search(r'```json\s*({.*?})\s*```', raw_content, re.S)
        if match:
            return json.loads(match.group(1))
        
        start = raw_content.find('{')
        end = raw_content.rfind('}')
        if start != -1 and end != -1:
            return json.loads(raw_content[start:end+1])
            
    except JSONDecodeError as e:
        print(f"Failed to parse JSON: {e}\nRaw response:\n{raw_content}", file=sys.stderr)
    
    return None

# --- Main Orchestration ---

def extract_from_file(file_path: str, model: str = "deepseek-r1-distill-llama-70b") -> Optional[Dict]:
    """
    Extracts data lineage and mapping from a file using a hybrid, structure-aware approach.
    """
    client = Groq()

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read()
    except IOError as e:
        print(f"Error reading file {file_path}: {e}", file=sys.stderr)
        return None

    # Use structure-aware chunking for Python, line-based for others
    file_extension = os.path.splitext(file_path)[1].lower()
    is_python_file = file_extension == '.py' or 'python' in content.lower()

    token_count = count_tokens(content, model)
    print(f"Token count for {os.path.basename(file_path)}: {token_count}")

    max_tokens_per_chunk = 3800
    if token_count > max_tokens_per_chunk:
        print("Content is large. Using structure-aware chunking...")
        if is_python_file:
            chunks = chunk_code_by_structure(content, max_tokens=max_tokens_per_chunk, model=model)
        else:
            chunks = chunk_code_by_lines(content, max_tokens=max_tokens_per_chunk, model=model)
        
        print(f"Split content into {len(chunks)} chunks.")
        
        all_results = []
        for i, chunk in enumerate(chunks):
            print(f"Analyzing chunk {i+1}/{len(chunks)}...")
            res = get_llm_analysis(chunk, client, model, is_chunk=True)
            if res:
                all_results.append(res)
        
        return merge_results(all_results)
    else:
        return get_llm_analysis(content, client, model, is_chunk=False)

def merge_results(results: List[Dict]) -> Optional[Dict]:
    """Merges analysis results from multiple chunks into a single coherent report."""
    if not results:
        return None
    
    merged = {
        'lineage': [],
        'mapping': {'metadata_tables': [], 'table_config_dict': {}},
        'sql_queries': []
    }
    
    # Use sets to keep track of unique entries
    seen_sqls = set()
    seen_metadata_tables = set()

    lineage_step_counter = 1
    for res in results:
        # Merge and re-sequence lineage
        for step in res.get('lineage', []):
            step['step'] = lineage_step_counter
            merged['lineage'].append(step)
            lineage_step_counter += 1
            
        # Merge unique metadata tables
        for table in res.get('mapping', {}).get('metadata_tables', []):
            table_name = table.get('name')
            if table_name not in seen_metadata_tables:
                merged['mapping']['metadata_tables'].append(table)
                seen_metadata_tables.add(table_name)

        # Merge table config dictionaries
        merged['mapping']['table_config_dict'].update(res.get('mapping', {}).get('table_config_dict', {}))
        
        # Merge unique SQL queries
        for query in res.get('sql_queries', []):
            sql_text = query.get('sql_text', '').strip()
            if sql_text not in seen_sqls:
                merged['sql_queries'].append(query)
                seen_sqls.add(sql_text)
                
    return merged

def export_to_json(data: Dict, output_path: str = "extraction_output.json") -> None:
    """Exports extracted data to a JSON file."""
    try:
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        print(f"Exported JSON to {output_path}")
    except IOError as e:
        print(f"Error exporting to JSON: {e}", file=sys.stderr)

# --- Main Execution ---

def main():
    if len(sys.argv) < 2:
        print("Usage: python refined_extractor.py <file_path>")
        sys.exit(1)
        
    file_path = sys.argv[1]
    model = "deepseek-r1-distill-llama-70b"

    if not os.path.exists(file_path):
        print(f"Error: File not found at {file_path}", file=sys.stderr)
        sys.exit(1)

    print(f"Processing file: {file_path}")
    result = extract_from_file(file_path, model)

    if result:
        export_to_json(result)
        print("Data extraction completed successfully!")
    else:
        print("Failed to extract data from the file.")

if __name__ == "__main__":
    main()
