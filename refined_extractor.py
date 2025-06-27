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

# Import Streamlit for secrets management
try:
    import streamlit as st
    STREAMLIT_AVAILABLE = True
except ImportError:
    STREAMLIT_AVAILABLE = False

# --- Token Management ---

def count_tokens(text: str, model: str = "deepseek-r1-distill-llama-70b") -> int:
    """Counts tokens in a string using tiktoken, with a fallback for unsupported models."""
    try:
        encoding = tiktoken.encoding_for_model(model)
        return len(encoding.encode(text))
    except KeyError:
        # Fallback for models not in tiktoken: 1 token ≈ 4 characters
        return len(text) // 4

def chunk_code(code_text: str, max_tokens: int = 4000, model: str = "deepseek-r1-distill-llama-70b") -> List[str]:
    """Splits code into chunks, respecting logical boundaries if possible."""
    
    lines = code_text.split('\n')
    chunks = []
    current_chunk = ""

    for line in lines:
        if count_tokens(current_chunk + line, model) > max_tokens:
            if current_chunk.strip():
                chunks.append(current_chunk.strip())
            current_chunk = ""
        current_chunk += line + '\n'
    
    if current_chunk.strip():
        chunks.append(current_chunk.strip())
        
    return chunks

# --- Static Code Analysis with AST ---

class CodeAnalyzer(ast.NodeVisitor):
    """
    Analyzes Python code using AST to extract SQL queries and other relevant details.
    """
    def __init__(self):
        self.sql_queries: List[Dict[str, Any]] = []
        self.current_method: Optional[str] = None

    def visit_FunctionDef(self, node: ast.FunctionDef):
        self.current_method = node.name
        self.generic_visit(node)
        self.current_method = None

    def visit_Constant(self, node: ast.Constant):
        if isinstance(node.value, str):
            # Basic SQL query detection
            sql_keywords = ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP', 'ALTER', 'COPY', 'UNLOAD']
            if any(keyword in node.value.upper() for keyword in sql_keywords):
                self.sql_queries.append({
                    "query_type": "UNKNOWN",
                    "source_tables": [],
                    "target_tables": [],
                    "purpose": "Extracted via AST",
                    "sql_text": node.value,
                    "method_name": self.current_method or "global"
                })
        self.generic_visit(node)

def analyze_code_statically(code_text: str) -> Dict[str, Any]:
    """
    Performs static analysis on Python code to extract basic information.
    """
    try:
        tree = ast.parse(code_text)
        analyzer = CodeAnalyzer()
        analyzer.visit(tree)
        return {"sql_queries": analyzer.sql_queries}
    except SyntaxError as e:
        print(f"Error parsing Python code: {e}", file=sys.stderr)
        return {"sql_queries": []}

# --- LLM-based Extraction ---

def get_llm_analysis(code_text: str, client: Groq, model: str) -> Optional[Dict]:
    """
    Uses an LLM to perform deep analysis of the code.
    """
    system_prompt = """You are a Data Engineering Expert. Analyze the code to extract data lineage, mapping, and SQL queries. Return valid JSON with keys: 'lineage', 'mapping', 'sql_queries'.

lineage: List of objects with step, source, target, description
mapping: Object with metadata_tables and table_config_dict  
sql_queries: List of objects with query_type, source_tables, target_tables, purpose, sql_text, method_name"""

    user_prompt = f"Analyze this code:\n```\n{code_text}\n```"
    
    max_retries = 2  # Reduced retries for speed
    for attempt in range(max_retries):
        try:
            response = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                max_tokens=1500,  # Reduced for faster response
                seed=42,
                temperature=0.0
            )
            return parse_json_response(response.choices[0].message.content)
        except Exception as e:
            if "rate_limit" in str(e).lower() or "413" in str(e):
                print(f"Rate limit hit, attempt {attempt + 1}. Retrying...")
                import time
                time.sleep(1)  # Reduced sleep time
            else:
                print(f"Error calling Groq API: {e}", file=sys.stderr)
                return None
    print("Max retries exceeded for API calls.", file=sys.stderr)
    return None

def parse_json_response(raw_content: str) -> Optional[Dict]:
    """Robustly parses JSON from a model's response."""
    try:
        # Look for JSON within triple backticks
        match = re.search(r'```json\s*({.*?})\s*```', raw_content, re.S)
        if match:
            return json.loads(match.group(1))
        
        # Fallback to finding the first '{' and last '}'
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
    Extracts data lineage and mapping from a file using a hybrid approach.
    """
    # Get API key from Streamlit secrets or environment variable
    api_key = None
    if STREAMLIT_AVAILABLE:
        try:
            api_key = st.secrets.get("GROQ_API_KEY")
        except:
            pass
    
    if not api_key:
        api_key = os.getenv("GROQ_API_KEY")
    
    if not api_key:
        print("Error: GROQ_API_KEY not found in Streamlit secrets or environment variables", file=sys.stderr)
        return None
    
    client = Groq(api_key=api_key)

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read()
    except IOError as e:
        print(f"Error reading file {file_path}: {e}", file=sys.stderr)
        return None

    file_extension = os.path.splitext(file_path)[1].lower()
    
    # Static analysis for Python files
    static_results = None
    if file_extension == '.py':
        static_results = analyze_code_statically(content)

    # LLM analysis for all file types
    token_count = count_tokens(content, model)
    print(f"Token count for {file_path}: {token_count}")

    # Optimized processing based on file size
    if token_count > 15000:  # Very large files
        print("Very large file detected. Using fast processing mode...")
        # For very large files, use a more aggressive chunking strategy
        chunks = chunk_code(content, max_tokens=8000, model=model)
        print(f"Split into {len(chunks)} chunks for faster processing")
        
        # Process only the first few chunks for speed
        max_chunks = min(3, len(chunks))
        llm_results = []
        for i, chunk in enumerate(chunks[:max_chunks]):
            print(f"Processing chunk {i+1}/{max_chunks}...")
            result = get_llm_analysis(chunk, client, model)
            if result:
                llm_results.append(result)
        
        llm_result = merge_results(llm_results)
        
    elif token_count > 8000:  # Large files
        print("Large file detected. Using chunking...")
        chunks = chunk_code(content, max_tokens=6000, model=model)
        print(f"Split into {len(chunks)} chunks")
        
        llm_results = []
        for i, chunk in enumerate(chunks):
            print(f"Processing chunk {i+1}/{len(chunks)}...")
            result = get_llm_analysis(chunk, client, model)
            if result:
                llm_results.append(result)
        
        llm_result = merge_results(llm_results)
    else:
        # Small to medium files - process directly
        llm_result = get_llm_analysis(content, client, model)

    # Combine static and LLM results
    if llm_result and static_results:
        # Simple merge: LLM results are primary, static results can supplement
        # A more sophisticated merge could be implemented here
        if 'sql_queries' in llm_result and 'sql_queries' in static_results:
            # Avoid duplicates
            llm_sql = {q['sql_text'] for q in llm_result['sql_queries']}
            for q in static_results['sql_queries']:
                if q['sql_text'] not in llm_sql:
                    llm_result['sql_queries'].append(q)
        return llm_result
        
    return llm_result

def merge_results(results: List[Dict]) -> Optional[Dict]:
    """Merges results from multiple chunks."""
    if not results:
        return None
    
    merged = {
        'lineage': [],
        'mapping': {'metadata_tables': [], 'table_config_dict': {}},
        'sql_queries': []
    }
    
    for res in results:
        merged['lineage'].extend(res.get('lineage', []))
        merged['mapping']['metadata_tables'].extend(res.get('mapping', {}).get('metadata_tables', []))
        merged['mapping']['table_config_dict'].update(res.get('mapping', {}).get('table_config_dict', {}))
        merged['sql_queries'].extend(res.get('sql_queries', []))
        
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