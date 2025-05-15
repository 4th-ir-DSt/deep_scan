import ast
from typing import List, Dict, Any, Set, Optional, Tuple
from pathlib import Path
import sqlparse
import string
import re
import json
import configparser
import logging

# Configure logging for this module if not configured at root
# logging.basicConfig(level=logging.INFO) # Can be configured at app level


class SQLStringExtractor(ast.NodeVisitor):
    """
    An AST node visitor that extracts potential SQL query strings from Python code.
    It looks for string literals, f-strings, and .format() calls on strings.
    """

    def __init__(self, raw_python_code: str):
        super().__init__()
        # Stores tuples of (sql_template, hashable_pos_args, hashable_kw_args, format_type)
        self.extracted_sqls: Set[Tuple[str, Optional[Tuple[ast.AST, ...]],
                                       Optional[Tuple[Tuple[str, ast.AST], ...]], str]] = set()
        self._sql_keywords = {'SELECT', 'INSERT', 'UPDATE', 'DELETE',
                              'CREATE', 'DROP', 'WITH', 'ALTER', 'TRUNCATE', 'MERGE', 'REPLACE'}
        self.raw_python_code = raw_python_code

    def _is_sql_like(self, potential_sql: str) -> bool:
        """Checks if a string likely contains SQL keywords."""
        # More robust check: presence of SELECT, FROM, WHERE, JOIN, CREATE TABLE, INSERT INTO, etc.
        # and ensure it's not just a random comment or string with a keyword.
        # For now, keeping it simple based on _sql_keywords but also checking for common DML/DDL structure.

        # Performance: convert to upper once
        potential_sql_upper = potential_sql.upper()
        if not any(keyword in potential_sql_upper for keyword in self._sql_keywords):
            return False

        # Attempt to parse and see if it's a known type
        try:
            parsed = sqlparse.parse(potential_sql)
            if parsed and parsed[0].get_type() != 'UNKNOWN':
                # Further check: if it's a simple select 1, "select 'foo'", it might not be what we want
                # This part can be refined based on how noisy the results are.
                # For instance, ignore if it's just "SELECT 'some_literal_string'" and nothing else.
                return True
        except Exception:
            return False
        return False

    def _add_sql(self, template: str, pos_args: Optional[List[ast.AST]] = None, kw_args: Optional[Dict[str, ast.AST]] = None, format_type: str = 'literal') -> None:
        if self._is_sql_like(template):
            # Sanitize template by removing typical Python newlines that aren't part of the SQL
            # but keep those that might be intentionally in the SQL string itself.
            # A simple strip and replace of \\n might be too aggressive if SQL has intentional newlines.
            # sqlparse.format might help here if we want to standardize, but could alter original.
            # For now, just basic cleaning.
            # Python's \\n -> SQL's \n (if needed by formatter)
            cleaned_template = template.strip().replace('\\\\n', '\\n')

            statements = sqlparse.split(cleaned_template)
            for stmt in statements:
                stmt_stripped = stmt.strip()
                if not stmt_stripped:
                    continue
                # Re-check with _is_sql_like after splitting, as split might yield non-SQL fragments
                if self._is_sql_like(stmt_stripped):
                    # Convert list and dict to hashable types (tuple and tuple of items)
                    hashable_pos_args = tuple(
                        pos_args) if pos_args is not None else None
                    hashable_kw_args = tuple(
                        sorted(kw_args.items())) if kw_args is not None else None

                    self.extracted_sqls.add((
                        stmt_stripped,
                        hashable_pos_args,
                        hashable_kw_args,
                        format_type
                    ))

    def visit_Str(self, node: ast.Str) -> None:
        if isinstance(node.s, str):
            self._add_sql(node.s, format_type='literal')
        self.generic_visit(node)

    def visit_Constant(self, node: ast.Constant) -> None:
        if isinstance(node.value, str):
            self._add_sql(node.value, format_type='literal')
        self.generic_visit(node)

    def visit_JoinedStr(self, node: ast.JoinedStr) -> None:
        template_parts = []
        fstring_arg_nodes: List[ast.AST] = []

        placeholder_idx = 0
        for value_node in node.values:
            if isinstance(value_node, (ast.Constant, ast.Str)):
                template_parts.append(value_node.s if isinstance(
                    value_node, ast.Str) else value_node.value)
            elif isinstance(value_node, ast.FormattedValue):
                template_parts.append(f"{{{placeholder_idx}}}")
                fstring_arg_nodes.append(value_node.value)
                placeholder_idx += 1

        reconstructed_template = "".join(template_parts)
        self._add_sql(reconstructed_template, pos_args=fstring_arg_nodes,
                      kw_args=None, format_type='fstring')
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        if isinstance(node.func, ast.Attribute) and node.func.attr == 'format':
            string_node = node.func.value
            format_template_str = None

            if isinstance(string_node, ast.Str):
                format_template_str = string_node.s
            elif isinstance(string_node, ast.Constant) and isinstance(string_node.value, str):
                format_template_str = string_node.value

            if format_template_str is not None:
                pos_arg_nodes = list(node.args)
                kw_arg_nodes_map = {
                    kw.arg: kw.value for kw in node.keywords if kw.arg}

                self._add_sql(format_template_str, pos_args=pos_arg_nodes,
                              kw_args=kw_arg_nodes_map, format_type='dot_format')

        self.generic_visit(node)


class CodeParser:
    def __init__(self):
        self.imports: List[Dict[str, str]] = []
        self.raw_code: str = ""
        self.tree: ast.AST | None = None

    def parse_config_file(self, config_content_str: str) -> configparser.ConfigParser:
        """
        Parses an INI-style configuration file content.
        Args:
            config_content_str: The string content of the configuration file.
        Returns:
            A ConfigParser object.
        """
        config = configparser.ConfigParser()
        try:
            config.read_string(config_content_str)
        except configparser.Error as e:
            # Handle or log parsing errors if necessary
            logging.error(f"Error parsing config file: {e}")
            # Return an empty or default config object depending on desired error handling
        return config

    def _find_config_assignment(
        self,
        target_var_name: str,
        config_obj_instance_name: str
    ) -> Optional[Tuple[str, str]]:
        """
        Finds if a target variable is assigned from a specific config object pattern.
        Looks for assignments like: target_var_name = config_obj_instance_name["section"]["key"]
        Args:
            target_var_name: The name of the Python variable to look for (e.g., "cn_mstr_table").
            config_obj_instance_name: The name of the config object instance in the script (e.g., "config_op_obj").
        Returns:
            A tuple (section_name, key_name) if found, else None.
        """
        if not self.tree:
            return None

        for node in ast.walk(self.tree):
            if isinstance(node, ast.Assign):
                # Ensure there's at least one target and it's a simple name
                if len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
                    if node.targets[0].id == target_var_name:
                        # Check the value: config_obj["section"]["key"]
                        # This corresponds to Subscript(value=Subscript(value=Name(id=config_obj_instance_name), slice=Constant(section)), slice=Constant(key))
                        value_node = node.value
                        if (isinstance(value_node, ast.Subscript) and  # outer subscript for ["key"]
                            # Py <3.9 uses Index, >=3.9 slice directly
                            isinstance(value_node.slice, (ast.Index, ast.Constant)) and
                            isinstance(value_node.slice.value if isinstance(value_node.slice, ast.Index) else value_node.slice, ast.Constant) and
                                # key is a string literal
                                isinstance((value_node.slice.value if isinstance(value_node.slice, ast.Index) else value_node.slice).value, str)):

                            key_name = (value_node.slice.value if isinstance(
                                value_node.slice, ast.Index) else value_node.slice).value

                            # Inner subscript for ["section"]
                            inner_subscript_node = value_node.value
                            if (isinstance(inner_subscript_node, ast.Subscript) and
                                isinstance(inner_subscript_node.value, ast.Name) and
                                inner_subscript_node.value.id == config_obj_instance_name and
                                isinstance(inner_subscript_node.slice, (ast.Index, ast.Constant)) and
                                isinstance(inner_subscript_node.slice.value if isinstance(inner_subscript_node.slice, ast.Index) else inner_subscript_node.slice, ast.Constant) and
                                    # section is a string literal
                                    isinstance((inner_subscript_node.slice.value if isinstance(inner_subscript_node.slice, ast.Index) else inner_subscript_node.slice).value, str)):

                                section_name = (inner_subscript_node.slice.value if isinstance(
                                    inner_subscript_node.slice, ast.Index) else inner_subscript_node.slice).value
                                return section_name, key_name
        return None

    def _get_constant_value(self, node: ast.AST) -> Any:
        """Helper to get value from ast.Constant or ast.Index(ast.Constant)."""
        if isinstance(node, ast.Constant):
            return node.value
        if isinstance(node, ast.Index) and isinstance(node.value, ast.Constant):  # Python < 3.9
            return node.value.value
        return None

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
        except ValueError as e:  # Catch validation errors
            raise e

    def detect_config_object_names(self) -> List[str]:
        """
        Detects potential names of configuration objects by looking for patterns
        like `var = config_obj_candidate["section"]["key"]`.

        Returns:
            A list of unique candidate names for the configuration object.
        """
        candidate_names: Set[str] = set()
        if not self.tree:
            return []

        for node in ast.walk(self.tree):
            if isinstance(node, ast.Assign):
                # Check the value: config_obj["section"]["key"]
                value_node = node.value
                if (isinstance(value_node, ast.Subscript) and  # Outer subscript for ["key"]
                        # Key is a string literal
                        isinstance(self._get_constant_value(value_node.slice), str)):

                    # This should be config_obj["section"]
                    inner_subscript_node = value_node.value
                    if (isinstance(inner_subscript_node, ast.Subscript) and
                        # Section is a string literal
                        isinstance(self._get_constant_value(inner_subscript_node.slice), str) and
                            # The object itself is a Name node
                            isinstance(inner_subscript_node.value, ast.Name)):

                        candidate_names.add(inner_subscript_node.value.id)
        return list(candidate_names)

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

    def get_extracted_sql_queries_with_args(self) -> List[Tuple[str, Optional[Tuple[ast.AST, ...]], Optional[Tuple[Tuple[str, ast.AST], ...]], str]]:
        """
        Extracts SQL query templates from the parsed Python code's AST along with their formatting arguments.
        Returns:
            A list of tuples: (sql_template_str, hashable_pos_args_tuple, hashable_kw_args_tuple_of_items, format_type).
        """
        if self.tree is None or not self.raw_code:
            return []

        extractor = SQLStringExtractor(raw_python_code=self.raw_code)
        extractor.visit(self.tree)
        return list(extractor.extracted_sqls)

    def _resolve_ast_node_value(self, node: ast.AST, parsed_config_data: Optional[configparser.ConfigParser], config_obj_name_in_script: str) -> str:
        """
        Resolves an AST node to a string representation suitable for inclusion as an argument
        in a reconstructed .format() call.
        - Literals (strings, numbers, bools, None) are repr'd.
        - Variables resolved from config are repr'd (as strings).
        - Unresolved variables become their name (unquoted).
        - Other complex nodes become a repr'd placeholder string.
        """
        if isinstance(node, ast.Constant):
            # Handles str, int, float, bool, None
            logging.debug(f"Resolved ast.Constant to: {repr(node.value)}")
            return repr(node.value)
        elif isinstance(node, ast.Str):  # Python < 3.8 string literals
            logging.debug(f"Resolved ast.Str to: {repr(node.s)}")
            return repr(node.s)
        elif isinstance(node, ast.Name):
            var_name = node.id
            if parsed_config_data and config_obj_name_in_script and self.tree:
                config_details = self._find_config_assignment(
                    var_name, config_obj_name_in_script)
                if config_details:
                    section, key = config_details
                    actual_value_str = None
                    if parsed_config_data.has_section(section) and parsed_config_data.has_option(section, key):
                        actual_value_str = parsed_config_data.get(section, key)

                    if actual_value_str is not None:
                        logging.debug(
                            f"Resolved variable '{var_name}' from config (section: {section}, key: {key}) to value: '{actual_value_str}'. Representing as: {repr(actual_value_str)}.")
                        # Value from config, repr'd
                        return repr(actual_value_str)
                    else:
                        logging.warning(
                            f"Config lookup: Variable '{var_name}' (section: {section}, key: {key}) not found in config. Using variable name '{var_name}' directly.")
                        return var_name  # Fallback to variable name itself, unquoted

            # Not assigned from config pattern or prerequisites missing for config lookup
            logging.info(
                f"Variable '{var_name}' not resolved from config or not a config pattern. Using variable name '{var_name}' directly.")
            return var_name  # Fallback to variable name itself, unquoted
        else:
            # For other node types (e.g., complex expressions, function calls)
            placeholder_text = f"UNRESOLVED_ARG<{type(node).__name__}>"
            try:
                if self.raw_code:
                    segment = ast.get_source_segment(self.raw_code, node)
                    if segment and len(segment) < 30:  # Keep it reasonably short
                        segment_cleaned = re.sub(r'\\s+', ' ', segment).strip()
                        placeholder_text = f"UNRESOLVED_ARG<{type(node).__name__}:{segment_cleaned}>"
            except Exception:
                pass  # Stick with simpler placeholder

            logging.warning(
                f"Argument AST node type {type(node).__name__} is not a simple variable or literal. Using placeholder: '{placeholder_text}'. Representing as: {repr(placeholder_text)}.")
            # Return as a string literal placeholder
            return repr(placeholder_text)

    def resolve_and_format_sql_queries(
        self,
        extracted_sqls_with_args: List[Tuple[str, Optional[Tuple[ast.AST, ...]], Optional[Tuple[Tuple[str, ast.AST], ...]], str]],
        parsed_config_data: Optional[configparser.ConfigParser],
        config_obj_name_in_script: str
    ) -> List[str]:
        final_sql_queries_to_process = []
        if not self.tree or not self.raw_code:
            logging.error(
                "CodeParser not fully initialized (missing AST tree or raw code). Cannot resolve SQL arguments effectively.")
            for template_str, _, _, _ in extracted_sqls_with_args:
                final_sql_queries_to_process.append(template_str)  # Fallback
            return final_sql_queries_to_process

        logging.info(
            f"Constructing string representations for {len(extracted_sqls_with_args)} SQL templates...")

        for i, (template_str, pos_args_tuple, kw_args_tuple_of_items, format_type) in enumerate(extracted_sqls_with_args):
            if format_type == 'literal':
                # For pure SQL literals, no .format() call to reconstruct
                final_sql_queries_to_process.append(template_str)
                logging.debug(
                    f"Template {i+1} is a literal: {template_str[:100]}...")
                continue

            # For .format() calls and f-strings, reconstruct the call as a string

            resolved_pos_args_str_list = []
            if pos_args_tuple:
                for arg_node in pos_args_tuple:
                    val_str_repr = self._resolve_ast_node_value(
                        arg_node, parsed_config_data, config_obj_name_in_script)
                    resolved_pos_args_str_list.append(val_str_repr)

            resolved_kw_args_str_list = []
            if kw_args_tuple_of_items:
                for name, arg_node in kw_args_tuple_of_items:
                    val_str_repr = self._resolve_ast_node_value(
                        arg_node, parsed_config_data, config_obj_name_in_script)
                    resolved_kw_args_str_list.append(f"{name}={val_str_repr}")

            all_args_parts = resolved_pos_args_str_list + resolved_kw_args_str_list
            args_str_for_format_call = ", ".join(all_args_parts)

            # The template_str itself is a Python string, so it needs repr() in the reconstructed expression
            reconstructed_call_str = f"{repr(template_str)}.format({args_str_for_format_call})"

            logging.debug(
                f"Constructed representation for template entry {i+1} (Type: {format_type}): {reconstructed_call_str[:200]}...")
            final_sql_queries_to_process.append(reconstructed_call_str)

        logging.info(
            f"Finished constructing representations for {len(extracted_sqls_with_args)} SQL templates.")
        return final_sql_queries_to_process
