import ast
from typing import List, Dict, Any, Set
from pathlib import Path
import sqlparse
import string
import re
import json


class SQLStringExtractor(ast.NodeVisitor):
    """
    An AST node visitor that extracts potential SQL query strings from Python code.
    It looks for string literals, f-strings, and .format() calls on strings.
    """

    def __init__(self, raw_python_code: str):
        super().__init__()
        self.sql_strings: Set[str] = set()
        self._sql_keywords = {'SELECT', 'INSERT', 'UPDATE', 'DELETE',
                              'CREATE', 'DROP', 'WITH', 'ALTER', 'TRUNCATE', 'MERGE', 'REPLACE'}
        self.raw_python_code = raw_python_code  # Store raw code for get_source_segment

    def _process_potential_sql(self, potential_sql_template: str) -> None:
        """
        Processes a string to see if it contains SQL statements.
        """
        template_upper = potential_sql_template.upper()
        if not any(keyword in template_upper for keyword in self._sql_keywords):
            return

        statements = sqlparse.split(potential_sql_template)
        for stmt in statements:
            stmt_stripped = stmt.strip()
            if not stmt_stripped:
                continue
            try:
                # Try to parse with sqlparse and check its type to ensure it's valid SQL
                parsed_statements = sqlparse.parse(stmt_stripped)
                if parsed_statements and parsed_statements[0].get_type() != 'UNKNOWN':
                    self.sql_strings.add(stmt_stripped)
            except Exception: 
                pass

    def visit_Str(self, node: ast.Str) -> None:  
        if isinstance(node.s, str):
            self._process_potential_sql(node.s)
        self.generic_visit(node)

    # Handles Python 3.8+ string literals
    def visit_Constant(self, node: ast.Constant) -> None:
        if isinstance(node.value, str):
            self._process_potential_sql(node.value)
        self.generic_visit(node)

    def visit_JoinedStr(self, node: ast.JoinedStr) -> None:  # f-strings
        template_parts = []
        for value_node in node.values:
            if isinstance(value_node, ast.Constant) and isinstance(value_node.value, str):
                template_parts.append(value_node.value)
            elif isinstance(value_node, ast.Str):
                template_parts.append(value_node.s)
            elif isinstance(value_node, ast.FormattedValue):
                try:
                    expr_source_raw = ast.get_source_segment(
                        self.raw_python_code, value_node.value)
                    if expr_source_raw is not None:
                        cleaned_expr_source = expr_source_raw.replace(
                            '\n', ' ').replace('\r', '').strip()
                        if cleaned_expr_source:
                            template_parts.append(f"{{{cleaned_expr_source}}}")
                        else:
                            # Fallback if cleaned source is empty
                            template_parts.append("{}")
                    else:
                        # Fallback if get_source_segment failed
                        template_parts.append("{}")
                except Exception:  # Fallback if get_source_segment fails for any reason
                    template_parts.append("{}")

        reconstructed_fstring = "".join(template_parts)
        self._process_potential_sql(reconstructed_fstring)
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:  # .format() calls
        is_format_call = False
        format_template_str = None

        if isinstance(node.func, ast.Attribute) and node.func.attr == 'format':
            # Check if the .format() is called on a string literal
            if isinstance(node.func.value, ast.Str):  # Python < 3.8 string literals
                format_template_str = node.func.value.s
                is_format_call = True
            # Python 3.8+ constants
            elif isinstance(node.func.value, ast.Constant) and isinstance(node.func.value.value, str):
                format_template_str = node.func.value.value
                is_format_call = True

        if is_format_call and format_template_str is not None:
            fmt = string.Formatter()
            new_template_parts = []

            # Pre-fetch and clean source segments for all arguments
            arg_sources = []
            for arg_node in node.args:
                try:
                    src = ast.get_source_segment(
                        self.raw_python_code, arg_node)
                    arg_sources.append(src.replace('\n', ' ').replace(
                        '\r', '').strip() if src else None)
                except Exception:
                    arg_sources.append(None)  # Failed to get source

            kwarg_map = {}
            for kwarg_node in node.keywords:
                if kwarg_node.arg:  # Ensure there's a keyword name
                    try:
                        src = ast.get_source_segment(
                            self.raw_python_code, kwarg_node.value)
                        kwarg_map[kwarg_node.arg] = src.replace(
                            '\n', ' ').replace('\r', '').strip() if src else None
                    except Exception:
                        # Failed to get source
                        kwarg_map[kwarg_node.arg] = None

            auto_arg_idx = 0
            for literal_text, field_name, format_spec, conversion in fmt.parse(format_template_str):
                new_template_parts.append(literal_text)

                if field_name is not None:
                    content_to_insert = None

                    if field_name == "":  # Automatic numbering: {}
                        if auto_arg_idx < len(arg_sources):
                            content_to_insert = arg_sources[auto_arg_idx]
                        auto_arg_idx += 1
                    # Positional numbering: {0}, {1}
                    elif field_name.isdigit():
                        idx = int(field_name)
                        if idx < len(arg_sources):
                            content_to_insert = arg_sources[idx]
                    else: 
                        content_to_insert = kwarg_map.get(field_name)

                    if content_to_insert:  
                        new_template_parts.append(f"{{{content_to_insert}}}")
                    else:
                        # Fallback: reconstruct the original placeholder specifier
                        original_placeholder_spec = field_name
                        if format_spec:
                            original_placeholder_spec += f":{format_spec}"
                        if conversion:
                            original_placeholder_spec += f"!{conversion}"
                        new_template_parts.append(
                            f"{{{original_placeholder_spec}}}")

            modified_template = "".join(new_template_parts)
            self._process_potential_sql(modified_template)
        self.generic_visit(node)


class CodeParser:
    def __init__(self):
        self.imports: List[Dict[str, str]] = []
        self.raw_code: str = ""
        self.tree: ast.AST | None = None

    def clean_json_data(self, json_str: str) -> str:
        """
        Clean JSON string by removing comments and ensuring proper formatting.
        
        Args:
            json_str: The JSON string to clean
            
        Returns:
            Cleaned JSON string
        """
        # Remove single line comments
        json_str = re.sub(r'//.*$', '', json_str, flags=re.MULTILINE)
        # Remove multi-line comments
        json_str = re.sub(r'/\*.*?\*/', '', json_str, flags=re.DOTALL)
        # Remove trailing commas
        json_str = re.sub(r',(\s*[}\]])', r'\1', json_str)
        return json_str

    def validate_lineage_data(self, lineage_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Validate lineage data and ensure all required fields are present.
        
        Args:
            lineage_data: List of lineage entries
            
        Returns:
            Validated lineage data with missing target tables handled
            
        Raises:
            ValueError: If validation fails
        """
        validated_data = []
        missing_target_tables = []
        
        for entry in lineage_data:
            # Check if target table is missing or undefined
            if not entry.get('TGT_TABLE_NAME') or entry.get('TGT_TABLE_NAME') == '{undefined}':
                # If source table is RESULT_OF_inner_query, use the previous target table
                if entry.get('SRC_TABLE_NAME') == 'RESULT_OF_inner_query':
                    # Find the previous entry with a valid target table
                    for prev_entry in reversed(validated_data):
                        if prev_entry.get('TGT_TABLE_NAME') and prev_entry.get('TGT_TABLE_NAME') != '{undefined}':
                            entry['TGT_TABLE_NAME'] = prev_entry['TGT_TABLE_NAME']
                            break
                    else:
                        missing_target_tables.append(entry)
                        continue
                else:
                    missing_target_tables.append(entry)
                    continue
            
            validated_data.append(entry)
        
        if missing_target_tables:
            error_msg = f"Found {len(missing_target_tables)} lineage row(s) with missing target table:\n"
            for entry in missing_target_tables:
                error_msg += f"- Source: {entry.get('SRC_TABLE_NAME')}, Column: {entry.get('SRC_COLUMN_NAME')}\n"
            raise ValueError(error_msg)
            
        return validated_data

    def parse_json(self, json_str: str) -> List[Dict[str, Any]]:
        """
        Parse JSON string with proper error handling and cleaning.
        
        Args:
            json_str: The JSON string to parse
            
        Returns:
            Parsed JSON data as a list of dictionaries
            
        Raises:
            ValueError: If JSON parsing fails
        """
        try:
            cleaned_json = self.clean_json_data(json_str)
            parsed_data = json.loads(cleaned_json)
            # Validate the lineage data
            return self.validate_lineage_data(parsed_data)
        except json.JSONDecodeError as e:
            raise ValueError(f"Error parsing JSON: {str(e)}")

    def parse_file(self, file_path: str) -> Dict[str, Any]:
        """Parse a Python file and extract imports and raw code."""
        file_path_obj = Path(file_path)
        if not file_path_obj.exists():
            raise FileNotFoundError(f"File not found: {file_path_obj}")

        with open(file_path_obj, 'r', encoding='utf-8') as f:
            self.raw_code = f.read()

        try:
            self.tree = ast.parse(self.raw_code)
            self._extract_imports(self.tree)
        except SyntaxError as e:
            self.tree = None
            raise ValueError(f"Invalid Python syntax: {str(e)}")

        return {
            "imports": self.imports,
            "raw_code": self.raw_code
        }

    def _extract_imports(self, tree: ast.AST) -> None:
        """Extract all imports from the AST."""
        self.imports = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for name in node.names:
                    self.imports.append({
                        "type": "import",
                        "module": name.name,
                        "alias": name.asname
                    })
            elif isinstance(node, ast.ImportFrom):
                module_name = node.module if node.module is not None else "."
                for name in node.names:
                    self.imports.append({
                        "type": "from_import",
                        "module": module_name,
                        "name": name.name,
                        "alias": name.asname
                    })

    def get_sql_style(self) -> List[str]:
        """Detect SQL styles used in the code based on imports and keywords."""
        styles: Set[str] = set()

        if any(imp.get("module", "").startswith("pyspark") for imp in self.imports):
            styles.add("pyspark")

        if any(imp.get("module", "").startswith("sqlalchemy") for imp in self.imports):
            styles.add("sqlalchemy")

        sql_keywords = ["SELECT ", "INSERT ", "UPDATE ", "DELETE ", "CREATE "]
        if self.raw_code and any(keyword in self.raw_code.upper() for keyword in sql_keywords):
            styles.add("raw_sql")

        return list(styles)

    def get_extracted_sql_queries(self) -> List[str]:
        """
        Extracts SQL query templates from the parsed Python code's AST.
        `parse_file` must be called successfully before this method.

        Returns:
            A list of unique SQL query strings (templates) found in the code.
        """
        if self.tree is None or not self.raw_code:
            return []

        extractor = SQLStringExtractor(raw_python_code=self.raw_code)
        extractor.visit(self.tree)
        queries = set(extractor.sql_strings)

        # --- Regex-based fallback extraction for SQL-like strings ---
        sql_regex = re.compile(r"\b(SELECT|INSERT|UPDATE|DELETE|CREATE|WITH)\b[\s\S]+?;", re.IGNORECASE)
        for match in sql_regex.finditer(self.raw_code):
            queries.add(match.group().strip())

        return list(queries)
