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
logging.basicConfig(level=logging.INFO)
from dotenv import load_dotenv
from groq import Groq

load_dotenv()

file_path = "scripts.py"

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
    string_pattern = r'(?:[rRuUfFbB]{0,2})?(?:\'\'\'(?:\\.|[^\'\\])*?\'\'\'|\"\"\"(?:\\.|[^\"\\])*?\"\"\"|\'(?:\\.|[^\'\\])*?\'|\"(?:\\.|[^\"\\])*?\")'
    
    for match in re.finditer(string_pattern, file_content):
        query_text = match.group(0)
        # Get the line number where this query was found
        start_pos = match.start()
        line_number = file_content[:start_pos].count('\n') + 1
        
        # Remove string quotes
        query_text = query_text[3:-3] if query_text.startswith(('"""', "'''")) else query_text[1:-1]
        
        print(f"\nFound potential query at line {line_number}:")
        print(f"Raw text: {query_text[:100]}...")  # Print first 100 chars
        
        # Skip if the text looks like a comment or non-SQL content
        if query_text.strip().startswith('/*') or query_text.strip().startswith('--'):
            print("Skipped: Looks like a comment")
            continue
            
        # Skip if the text is too short or doesn't contain SQL keywords
        sql_keywords = {'select', 'from', 'where', 'insert', 'into', 'update', 'delete',
                       'create', 'table', 'drop', 'alter', 'truncate', 'msck', 'repair', 'union'}
        if not any(keyword in query_text.lower() for keyword in sql_keywords):
            print("Skipped: No SQL keywords found")
            continue
        
        try:
            # Replace Python string formatting placeholders with dummy values for parsing
            dummy_query = query_text
            # Replace numbered placeholders {0}, {1}, etc.
            dummy_query = re.sub(r'\{(\d+)\}', 'dummy_table', dummy_query)
            # Replace unnumbered placeholders {}
            dummy_query = re.sub(r'\{\}', 'dummy_table', dummy_query)
            
            print("Attempting to parse with SQLGlot...")
            parsed = parse_one(dummy_query, dialect='hive')  # Using Hive dialect for better compatibility
            
            # Check if it's a valid SQL statement
            if isinstance(parsed, exp.Query):
                print("✅ Successfully parsed query")
                
                # Determine query type
                query_type = "UNKNOWN"
                if 'create' in query_text.lower():
                    query_type = "CREATE"
                elif 'drop' in query_text.lower():
                    query_type = "DROP"
                elif 'select' in query_text.lower():
                    query_type = "SELECT"
                elif 'insert' in query_text.lower():
                    query_type = "INSERT"
                elif 'update' in query_text.lower():
                    query_type = "UPDATE"
                elif 'delete' in query_text.lower():
                    query_type = "DELETE"
                
                potential_queries.append({
                    "query": query_text,
                    "source": "sqlglot",
                    "parsed_structure": str(parsed),
                    "line_number": line_number,
                    "type": query_type
                })
            else:
                print("Skipped: Not a valid SQL query")
        except (ParseError, TokenError) as e:
            print(f"Skipped: Parse error - {str(e)}")
            continue
        except Exception as e:
            print(f"Warning: Unexpected error parsing query with SQLGlot: {str(e)}")
            continue
    
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
    
    # Find all string literals that might contain SQL
    string_pattern = r'(?:[rRuUfFbB]{0,2})?(?:\'\'\'(?:\\.|[^\'\\])*?\'\'\'|\"\"\"(?:\\.|[^\"\\])*?\"\"\"|\'(?:\\.|[^\'\\])*?\'|\"(?:\\.|[^\"\\])*?\")'
    
    for match in re.finditer(string_pattern, file_content):
        query_text = match.group(0)
        # Get the line number where this query was found
        start_pos = match.start()
        line_number = file_content[:start_pos].count('\n') + 1
        
        # Remove string quotes
        query_text = query_text[3:-3] if query_text.startswith(('"""', "'''")) else query_text[1:-1]
        
        try:
            # Parse and format the query
            parsed = sqlparse.parse(query_text)
            
            if parsed and len(parsed) > 0:
                stmt = parsed[0]
                
                # Check if it's a valid SQL statement
                if stmt.get_type() in ('SELECT', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP'):
                    # Calculate confidence based on query complexity
                    confidence = 0.7  # Base confidence
                    
                    # Bonus for complex queries
                    if stmt.get_type() == 'SELECT':
                        confidence += 0.1
                    if 'JOIN' in str(stmt).upper():
                        confidence += 0.1
                    if 'WHERE' in str(stmt).upper():
                        confidence += 0.1
                    if 'GROUP BY' in str(stmt).upper() or 'ORDER BY' in str(stmt).upper():
                        confidence += 0.1
                    
                    # Cap at 1.0
                    confidence = min(confidence, 1.0)
                    
                    potential_queries.append({
                        "query": query_text,
                        "confidence": confidence,
                        "source": "sqlparser",
                        "query_type": stmt.get_type(),
                        "line_number": line_number
                    })
        except Exception:
            continue
    
    return sorted(potential_queries, key=lambda x: x["confidence"], reverse=True)

# --- Method 3: LLM Extractor ---

SYSTEM_PROMPT = """
You are an expert SQL code analyst. Your task is to meticulously analyze the provided Python script and extract all embedded SQL query templates.
**Instructions:**
1. **Identify SQL Queries:** Find all meaningful SQL commands, including `SELECT`, `CREATE TABLE`, `DROP TABLE`, `MSCK REPAIR TABLE`, and `WITH` clauses.
2. **Extract Templates:** Extract the raw string template, including placeholders like `{}` or `\"\"\" + variable + \"\"\"`.
3. **Ignore Non-Queries:** You must ignore shell commands, comments, and simple strings that are not valid SQL queries.
4. **Output Format:** Your response MUST be a single, valid JSON object with a key `extracted_queries`, which is a list of objects. Each object should have:
   - `query`: The extracted SQL query
   - `confidence`: A number between 0 and 1 indicating your confidence in this being a valid SQL query
   - `reasoning`: A brief explanation of why you think this is a SQL query
   - `query_type`: The type of SQL query (SELECT, INSERT, UPDATE, etc.)
"""

def extract_with_llm(file_path, file_content):
    """
    Uses the DeepSeek model through Groq API to extract SQL queries with confidence scores.
    """
    try:
        api_key = os.environ.get("GROQ_API_KEY")
        if not api_key:
            print("❌ Error: GROQ_API_KEY environment variable not set.")
            return None
        client = Groq(api_key=api_key)
        user_prompt = f"""Please analyze the following Python script and extract the SQL queries according to the rules.
For each query, also provide the line number where it was found in the original file.

**Filename:** `{os.path.basename(file_path)}`

**Script Content:**
```python
{file_content}
```

Please provide the line numbers in your response."""
        
        print("🤖 Sending request to Groq API (DeepSeek model) for context-aware extraction... Please wait.")
        chat_completion = client.chat.completions.create(
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt}
            ],
            model="deepseek-r1-distill-llama-70b",
            response_format={"type": "json_object"},
            temperature=0.0
        )
        response_content = chat_completion.choices[0].message.content
        queries = json.loads(response_content).get("extracted_queries", [])
        
        # Add source information
        for query in queries:
            query["source"] = "llm"
        
        return queries
    except Exception as e:
        print(f"❌ An unexpected error occurred during LLM extraction: {e}")
        return None

def ensemble_results(sqlglot_queries, sqlparser_queries, llm_queries):
    """
    Combines results from all three methods using an ensemble approach.
    """
    # Create a dictionary to store queries
    query_scores = defaultdict(lambda: {
        "sources": set(), 
        "details": {}, 
        "line_numbers": set(),
        "original_queries": set()
    })
    
    # Process all queries from each method
    for method, queries in [
        ("sqlglot", sqlglot_queries),
        ("sqlparser", sqlparser_queries),
        ("llm", llm_queries)
    ]:
        for query in queries:
            line_number = query.get("line_number", 0)
            query_text = query["query"]
            
            # Create a key based on the query text and line number
            key = (query_text, line_number)
            
            # Add the query details
            query_scores[key]["sources"].add(method)
            query_scores[key]["line_numbers"].add(line_number)
            query_scores[key]["original_queries"].add(query_text)
            query_scores[key]["details"][method] = {
                "type": query.get("type", query.get("query_type", "UNKNOWN"))
            }
    
    # Prepare final results
    final_queries = []
    for (query, line_number), data in query_scores.items():
        # Check for UNION queries
        is_union = any('union' in q.lower() for q in data["original_queries"])
        
        # Get the most common query type
        query_types = [data["details"][method]["type"] for method in data["sources"]]
        query_type = max(set(query_types), key=query_types.count) if query_types else "UNKNOWN"
        
        # Clean up the query text
        cleaned_query = ' '.join(query.split())
        
        # Create the final query entry
        final_queries.append({
            "query": cleaned_query,
            "type": "UNION" if is_union else query_type,
            "validated_by": list(data["sources"]),
            "line_numbers": sorted(list(data["line_numbers"])),
            "extraction_details": {
                "methods": {
                    method: {
                        "type": data["details"][method]["type"]
                    } for method in data["sources"]
                },
                "is_union": is_union,
                "original_queries": list(data["original_queries"])
            }
        })
    
    # Sort by line number
    return sorted(final_queries, key=lambda x: min(x["line_numbers"]))

def save_results_to_file(filepath, data):
    """Saves the provided data as a JSON file with pretty printing."""
    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            # Create a more readable format
            output = {
                "source_file": data["source_file"],
                "summary": data["summary"],
                "queries": []
            }
            
            # Process each query
            for query in data["queries"]:
                query_info = {
                    "query": query["query"],
                    "type": query["type"],
                    "line_numbers": query["line_numbers"],
                    "validated_by": query["validated_by"],
                    "extraction_details": query["extraction_details"]
                }
                output["queries"].append(query_info)
            
            json.dump(output, f, indent=2)
        print(f"💾 Results successfully saved to: {filepath}")
    except Exception as e:
        print(f"❌ Error saving file to {filepath}: {e}")

def get_unique_queries(queries):
    """
    Returns a list of unique SQL queries, removing duplicates.
    """
    unique_queries = set()
    
    for query in queries:
        # Clean and normalize the query
        cleaned_query = ' '.join(query["query"].split())
        unique_queries.add(cleaned_query)
    
    return sorted(list(unique_queries))

def main():
    parser = argparse.ArgumentParser(
        description="Extract SQL queries from Python scripts for lineage analysis."
    )
    parser.add_argument('file', metavar='FILE', type=str, help='Python script file to analyze.')
    parser.add_argument('--output', '-o', metavar='OUTPUT_FILE', type=str, required=True, help='Output file path for extracted queries.')
    args = parser.parse_args()

    try:
        print(f"📄 Reading file: {args.file}...")
        with open(args.file, 'r', encoding='utf-8') as f:
            file_content = f.read()
    except FileNotFoundError:
        print(f"❌ Error: Input file not found at {args.file}")
        return

    # Run all extraction methods
    sqlglot_queries = extract_with_sqlglot(file_content)
    sqlparser_queries = extract_with_sqlparser(file_content)
    llm_queries = extract_with_llm(args.file, file_content) or []

    # Combine results and get unique queries
    all_queries = ensemble_results(sqlglot_queries, sqlparser_queries, llm_queries)
    unique_queries = get_unique_queries(all_queries)

    # Save queries to file
    with open(args.output, 'w', encoding='utf-8') as f:
        for query in unique_queries:
            f.write(query + '\n\n')

    print(f"\n✅ Extracted {len(unique_queries)} unique SQL queries to {args.output}")
    print("\nExtracted Queries:")
    print("="*50)
    for i, query in enumerate(unique_queries, 1):
        print(f"\n{i}. {query}")
    print("\n" + "="*50)

if __name__ == "__main__":
    main()