import os
import argparse
import json
import re
import logging
from collections import defaultdict
import sqlglot
from sqlglot import parse_one, exp
from sqlglot.errors import ParseError, TokenError
import sqlparse
from dotenv import load_dotenv
from groq import Groq
from typing import List

logging.basicConfig(level=logging.INFO)
load_dotenv()

file_path = ""
output_file = "result.json"

# --- Method 1: SQLGlot Parser ---

def extract_with_sqlglot(file_content):
    """
    Extracts and validates SQL queries using SQLGlot parser.
    """
    potential_queries = []
    lines = file_content.split('\n')
    
    print("\n🔍 SQLGlot Debug Info:")
    print("="*50)
    
    # Find all string literals that might contain SQL
    string_pattern = r'(?:[rRuUfFbB]{0,2})?(?:\'\'\'(?:\\.|[^\'\\])?\'\'\'|\"\"\"(?:\\.|[^\"\\])?\"\"\"|\'(?:\\.|[^\'\\])?\'|\"(?:\\.|[^\"\\])?\")'
    
    for match in re.finditer(string_pattern, file_content):
        query_text = match.group(0)
        start_pos = match.start()
        line_number = file_content[:start_pos].count('\n') + 1
        
        # Strip quotes
        if query_text.startswith(('"""', "'''")):
            query_text = query_text[3:-3]
        else:
            query_text = query_text[1:-1]
        
        print(f"\nFound potential query at line {line_number}:")
        print(f"Raw text: {query_text[:100]}...")
        
        # Skip comments and non-SQL
        if query_text.strip().startswith(('/*','--')):
            print("Skipped: Looks like a comment")
            continue
        
        sql_keywords = {'select', 'from', 'where', 'insert', 'into', 'update', 'delete',
                        'create', 'table', 'drop', 'alter', 'truncate', 'msck', 'repair', 'union'}
        if not any(k in query_text.lower() for k in sql_keywords):
            print("Skipped: No SQL keywords found")
            continue
        
        try:
            # Replace format placeholders for parsing
            dummy = re.sub(r'\{(\d+)\}', 'dummy_table', query_text)
            dummy = re.sub(r'\{\}', 'dummy_table', dummy)
            
            print("Attempting to parse with SQLGlot...")
            parsed = parse_one(dummy, dialect='hive')
            
            if isinstance(parsed, exp.Query):
                print("✅ Successfully parsed query")
                
                qt = "UNKNOWN"
                low = query_text.lower()
                if 'create' in low: qt = "CREATE"
                elif 'drop' in low: qt = "DROP"
                elif 'select' in low: qt = "SELECT"
                elif 'insert' in low: qt = "INSERT"
                elif 'update' in low: qt = "UPDATE"
                elif 'delete' in low: qt = "DELETE"
                
                potential_queries.append({
                    "query": query_text,
                    "source": "sqlglot",
                    "parsed_structure": str(parsed),
                    "line_number": line_number,
                    "type": qt
                })
            else:
                print("Skipped: Not a valid SQL query")
        except (ParseError, TokenError) as e:
            print(f"Skipped: Parse error - {e}")
        except Exception as e:
            print(f"Warning: Unexpected error - {e}")
    
    print("\nSQLGlot Results Summary:")
    print(f"Total queries found: {len(potential_queries)}")
    print("="*50)
    return sorted(potential_queries, key=lambda x: x["line_number"])


# --- Method 2: SQLParser ---

def extract_with_sqlparser(file_content):
    """
    Extracts and validates SQL queries using SQLParser.
    """
    potential_queries = []
    string_pattern = r'(?:[rRuUfFbB]{0,2})?(?:\'\'\'(?:\\.|[^\'\\])?\'\'\'|\"\"\"(?:\\.|[^\"\\])?\"\"\"|\'(?:\\.|[^\'\\])?\'|\"(?:\\.|[^\"\\])?\")'
    
    for match in re.finditer(string_pattern, file_content):
        raw = match.group(0)
        start_pos = match.start()
        line_number = file_content[:start_pos].count('\n') + 1
        
        if raw.startswith(('"""',"'''")):
            query_text = raw[3:-3]
        else:
            query_text = raw[1:-1]
        
        try:
            parsed = sqlparse.parse(query_text)
            if parsed:
                stmt = parsed[0]
                t = stmt.get_type()
                if t in ('SELECT','INSERT','UPDATE','DELETE','CREATE','DROP'):
                    conf = 0.7
                    txt = str(stmt).upper()
                    if t == 'SELECT': conf += 0.1
                    if 'JOIN' in txt:        conf += 0.1
                    if 'WHERE' in txt:       conf += 0.1
                    if any(x in txt for x in ('GROUP BY','ORDER BY')):
                        conf += 0.1
                    conf = min(conf, 1.0)
                    potential_queries.append({
                        "query": query_text,
                        "confidence": conf,
                        "source": "sqlparser",
                        "query_type": t,
                        "line_number": line_number
                    })
        except Exception:
            continue
    
    return sorted(potential_queries, key=lambda x: x["confidence"], reverse=True)


# --- Method 3: LLM Extractor with refined prompt ---

SYSTEM_PROMPT = '''You are a SQL query extractor. Your task is to analyze code and extract SQL queries, paying special attention to .format() method calls.

For each query found, create a JSON object with these fields:
- "query": The SQL query with all placeholders replaced by their actual values
- "type": The query type (SELECT, INSERT, CREATE, etc.)
- "line_number": Approximate line number where the query appears (if discernible)

For queries using .format():
1. Replace all placeholders ({0}, {1}, etc.) with their corresponding .format() arguments
2. Use the actual variable names or values in place of the placeholders
3. Return only the fully resolved query with substituted values

Example response format:
{
    "extracted_queries": [
        {
            "query": "CREATE TABLE IF NOT EXISTS cn_bkp_table_name AS SELECT * FROM cn_mstr_table",
            "type": "CREATE",
            "line_number": 42
        },
        {
            "query": "INSERT INTO target_table SELECT * FROM source_table",
            "type": "INSERT",
            "line_number": 43
        }
    ]
}

Important:
1. Always substitute the actual values in place of placeholders
2. Never include the original placeholders in the output
3. For multi-line queries, look for the .format() call in subsequent lines
4. Return ONLY valid JSON, no markdown, no explanations'''

def chunk_file_content(content: str, max_chunk_size: int = 1500) -> List[str]:
    """Split file content into manageable chunks."""
    lines = content.split('\n')
    chunks, current_chunk, size = [], [], 0
    for line in lines:
        b = len(line.encode('utf-8'))
        if current_chunk and size + b > max_chunk_size:
            chunks.append('\n'.join(current_chunk))
            current_chunk, size = [], 0
        current_chunk.append(line)
        size += b
    if current_chunk:
        chunks.append('\n'.join(current_chunk))
    return chunks

def extract_with_llm(file_path, file_content):
    """
    Uses Groq LLM to extract SQL queries with confidence scores.
    """
    api_key = os.environ.get("GROQ_API_KEY")
    if not api_key:
        print("❌ Error: GROQ_API_KEY not set.")
        return []
    client = Groq(api_key=api_key)

    chunks = chunk_file_content(file_content)
    print(f"Split file into {len(chunks)} chunks")
    all_queries = []

    for i, chunk in enumerate(chunks, 1):
        print(f"\nProcessing chunk {i}/{len(chunks)}...")
        user_prompt = f"""Analyze this code and extract any SQL queries, paying special attention to .format() calls:

```python
{chunk}
```

For each query:
1. Replace all placeholders with their .format() arguments
2. Return only the fully resolved query with actual values

Example:
For: "SELECT * FROM {0}".format(table_name)
Return:
{{
    "query": "SELECT * FROM table_name",
    "type": "SELECT"
}}

Return ONLY a JSON object with the 'extracted_queries' array."""

        try:
            resp = client.chat.completions.create(
                seed=35,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": user_prompt}
                ],
                model="deepseek-r1-distill-llama-70b",
                response_format={"type": "json_object"},
                temperature=0.0
            )
            
            content = resp.choices[0].message.content
            try:
                # Clean up any potential markdown formatting
                if "```json" in content:
                    content = content.split("```json")[1].split("```")[0].strip()
                elif "```" in content:
                    content = content.split("```")[1].strip()
                
                extracted = json.loads(content)
                
                # Ensure we have the expected structure
                if not isinstance(extracted, dict):
                    extracted = {"extracted_queries": []}
                if "extracted_queries" not in extracted:
                    if isinstance(extracted, list):
                        extracted = {"extracted_queries": extracted}
                    else:
                        extracted = {"extracted_queries": []}
                
                for q in extracted["extracted_queries"]:
                    if isinstance(q, str):
                        # Convert simple string queries to proper objects
                        q = {
                            "query": q,
                            "type": "UNKNOWN"
                        }
                    q["source"] = "llm"
                    # Adjust line numbers if provided
                    if "line_number" in q:
                        q["line_number"] += sum(len(c.split('\n')) for c in chunks[:i-1])
                
                all_queries.extend(extracted["extracted_queries"])
                print(f"Found {len(extracted['extracted_queries'])} queries in chunk {i}")
            
            except json.JSONDecodeError as e:
                print(f"❌ Failed to parse JSON from chunk {i}: {e}")
                print(f"Raw content: {content[:200]}...")
                continue
                
        except Exception as e:
            print(f"❌ Error processing chunk {i}: {e}")
            continue
            
    return all_queries


# --- Ensemble and I/O ---

def ensemble_results(sqlglot_qs, sqlparser_qs, llm_qs):
    query_map = defaultdict(lambda: {"sources": set(), "details": {}, "lines": set()})
    for method, qs in [("sqlglot", sqlglot_qs), ("sqlparser", sqlparser_qs), ("llm", llm_qs)]:
        for q in qs:
            txt = q["query"].strip()
            ln = q.get("line_number", 0)
            key = (txt, ln)
            query_map[key]["sources"].add(method)
            query_map[key]["lines"].add(ln)
            query_map[key]["details"][method] = {
                "type": q.get("type", q.get("query_type", "UNKNOWN"))
            }

    final = []
    for (txt, ln), data in query_map.items():
        types = [data["details"][m]["type"] for m in data["sources"]]
        qt = max(set(types), key=types.count) if types else "UNKNOWN"
        is_union = "union" in txt.lower()
        final.append({
            "query": ' '.join(txt.split()),
            "type": "UNION" if is_union else qt,
            "validated_by": sorted(data["sources"]),
            "line_numbers": sorted(data["lines"]),
            "extraction_details": {
                "methods": {m: {"type": data["details"][m]["type"]} for m in data["sources"]},
                "is_union": is_union
            }
        })
    return sorted(final, key=lambda x: x["line_numbers"][0] if x["line_numbers"] else 0)


def save_results_to_file(path, report):
    try:
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2)
        print(f"💾 Saved results to {path}")
    except Exception as e:
        print(f"❌ Error saving file: {e}")


def main():
    parser = argparse.ArgumentParser(description='Extract SQL queries from Python files')
    parser.add_argument('file', help='Path to the Python file to analyze')
    args = parser.parse_args()
    
    try:
        with open(args.file, 'r', encoding='utf-8') as f:
            content = f.read()
    except FileNotFoundError:
        print(f"❌ File not found: {args.file}")
        return

    print("\n--- SQLGlot Extraction ---")
    sqlglot_qs = extract_with_sqlglot(content)
    print("\n--- SQLParser Extraction ---")
    sqlparser_qs = extract_with_sqlparser(content)
    print("\n--- LLM Extraction ---")
    llm_qs = extract_with_llm(file_path, content)

    print("\n--- Combining Results ---")
    combined = ensemble_results(sqlglot_qs, sqlparser_qs, llm_qs)

    report = {
        "source_file": os.path.basename(file_path),
        "summary": {
            "total_queries": len(combined),
            "query_types": {q["type"]: sum(1 for x in combined if x["type"] == q["type"]) for q in combined}
        },
        "queries": combined
    }

    print(json.dumps(report, indent=2))
    save_results_to_file(output_file, report)


if __name__ == "__main__":
    main()
