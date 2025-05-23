import ast
from typing import List, Dict, Any, Set, Optional, Tuple
from pathlib import Path
import re
import json
import configparser
import logging
import sqlglot
import sqlglot.errors

# Configure logging - consider setting to DEBUG in your app.py for detailed logs
# logging.basicConfig(level=logging.DEBUG)

class SQLStringExtractor(ast.NodeVisitor):
    def __init__(self, raw_python_code: str, detected_dialects: List[str] = None):
        super().__init__()
        self.extracted_sqls: Set[Tuple[str, Optional[Tuple[ast.AST, ...]],
                                       Optional[Tuple[Tuple[str, ast.AST], ...]], str]] = set()
        self._sql_keywords = {'SELECT', 'INSERT', 'UPDATE', 'DELETE', 'VALUES',
                              'CREATE', 'DROP', 'WITH', 'ALTER', 'TRUNCATE', 'MERGE', 'REPLACE',
                              'MSCK', 'REPAIR', 'TABLE', 'AS', 'PURGE', 'LOCATION'}
        self.raw_python_code = raw_python_code
        self.detected_dialects = detected_dialects if detected_dialects is not None else []
        self.primary_dialect_for_check = None
        if self.detected_dialects:
            if 'spark' in self.detected_dialects:
                self.primary_dialect_for_check = 'spark'
            elif 'hive' in self.detected_dialects:
                self.primary_dialect_for_check = 'hive'

        # --- ADDED: Acceptable root keys for SQL statements ---
        self._acceptable_sql_statement_roots = {
            'select', 'insert', 'update', 'delete', 'create', 'drop', 'alter',
            'merge', 'with', 'truncate', 'values', 'command', # 'command' can be tricky, used by sqlglot for some DDL
            'msck', # For MSCK REPAIR TABLE
            'repair', # For REPAIR TABLE
            # Add more valid SQL statement starting keywords/types as needed if sqlglot uses them as .key
        }
        # --- END ADDED ---

    def _escape_literal_braces(self, text: str) -> str:
        return text.replace('{', '{{').replace('}', '}}')

    def _is_sql_like(self, potential_sql: str, is_template_containing_python_placeholders: bool = False) -> bool:
        potential_sql_strip = potential_sql.strip()
        if not potential_sql_strip:
            return False
            
        potential_sql_upper = potential_sql_strip.upper()
        has_keywords = any(keyword in potential_sql_upper for keyword in self._sql_keywords)

        if not has_keywords:
            if is_template_containing_python_placeholders and ('{' in potential_sql_strip and '}' in potential_sql_strip):
                logging.debug(f"Template has placeholders but no initial keywords, considering candidate: '{potential_sql_strip[:100]}...'")
                return True 
            logging.debug(f"No SQL keywords in: '{potential_sql_strip[:100]}...'")
            return False

        if is_template_containing_python_placeholders:
            logging.debug(f"Identified as template with placeholders and keywords/placeholders: '{potential_sql_strip[:100]}...'")
            return True # Optimistic for templates; strict parsing happens after Python formatting

      
        try:
            dialect_to_try = self.primary_dialect_for_check
            logging.debug(f"Attempting direct sqlglot.parse with dialect: {dialect_to_try or 'default'} for (literal/formatted): '{potential_sql_strip[:100]}'")
            parsed_expressions = sqlglot.parse(potential_sql_strip, read=dialect_to_try)
            
            if parsed_expressions:
                # --- MODIFIED: Check the type of the parsed expression ---
               
                first_expression_key = parsed_expressions[0].key.lower() if parsed_expressions[0].key else ""
                
                
                if first_expression_key in self._acceptable_sql_statement_roots:
                    # Additional check for 'command': some actual DDL/DML might parse as command
                   
                    if first_expression_key == 'command':
                        # For 'command', check if it starts with a known DDL/DML keyword to be safer
                        command_text_upper = parsed_expressions[0].sql(dialect=dialect_to_try).upper()
                        if any(cmd_keyword in command_text_upper for cmd_keyword in ['TRUNCATE', 'MSCK', 'REPAIR', 'SET', 'USE', 'DESCRIBE', 'SHOW']): # Add more if needed
                           logging.debug(f"sqlglot parsed (literal/formatted) as valid command type '{first_expression_key}': '{potential_sql_strip[:100]}...'")
                           return True
                        else:
                           logging.debug(f"sqlglot parsed (literal/formatted) as 'command' but not recognized DDL/DML: '{potential_sql_strip[:100]}...'")
                           return False
                    
                    logging.debug(f"sqlglot parsed (literal/formatted) as valid statement type '{first_expression_key}': '{potential_sql_strip[:100]}...'")
                    return True
                else:
                    logging.debug(f"sqlglot parsed (literal/formatted) but root key '{first_expression_key}' not in acceptable types: '{potential_sql_strip[:100]}...'")
                    return False
                # --- END MODIFIED ---
            else:
                logging.debug(f"sqlglot.parse returned empty for (literal/formatted): '{potential_sql_strip[:100]}...'")
                return False
        except sqlglot.errors.ParseError:
            logging.debug(f"sqlglot ParseError for (literal/formatted): '{potential_sql_strip[:100]}...'")
            return False
        except Exception as e:
            logging.warning(f"Unexpected error during sqlglot.parse in _is_sql_like (literal/formatted): {e} for: '{potential_sql_strip[:100]}...'")
            return False

    # ... _add_sql and visitor methods (visit_Str, visit_Constant, visit_JoinedStr, visit_Call) remain the same as the previous full version ...
    def _add_sql(self, template: str, pos_args: Optional[List[ast.AST]] = None, kw_args: Optional[Dict[str, ast.AST]] = None, format_type: str = 'literal') -> None:
        cleaned_template = template.strip().replace('\\n', '\n')
        if not cleaned_template:
            return

        is_template_with_python_placeholders = (format_type == 'dot_format' or format_type == 'fstring')

        if not self._is_sql_like(cleaned_template, is_template_containing_python_placeholders=is_template_with_python_placeholders):
            return

        hashable_pos_args = tuple(pos_args) if pos_args is not None else None
        hashable_kw_args = tuple(
            sorted(kw_args.items())) if kw_args is not None else None

        entry = (
            cleaned_template,
            hashable_pos_args,
            hashable_kw_args,
            format_type
        )
        if entry not in self.extracted_sqls:
            self.extracted_sqls.add(entry)
            logging.debug(
                f"Added SQL entry candidate (type: {format_type}): {cleaned_template[:100]}... "
                f"with {len(pos_args or [])} pos args, {len(kw_args or [])} kw args"
            )

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
                literal_part = value_node.s if isinstance(
                    value_node, ast.Str) else value_node.value
                template_parts.append(
                    self._escape_literal_braces(str(literal_part)))
            elif isinstance(value_node, ast.FormattedValue):
                template_parts.append(f"{{{placeholder_idx}}}")
                fstring_arg_nodes.append(value_node.value)
                placeholder_idx += 1
        reconstructed_template_for_later_formatting = "".join(template_parts)
        self._add_sql(reconstructed_template_for_later_formatting, pos_args=fstring_arg_nodes,
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
            elif isinstance(string_node, ast.Name):
                 logging.debug(f"'.format()' call on a variable '{string_node.id}', not a direct string literal. Template not extracted if variable not resolved elsewhere.")
            if format_template_str is not None:
                pos_arg_nodes = list(node.args)
                kw_arg_nodes_map = {
                    kw.arg: kw.value for kw in node.keywords if kw.arg}
                self._add_sql(format_template_str, pos_args=pos_arg_nodes,
                                kw_args=kw_arg_nodes_map, format_type='dot_format')
        self.generic_visit(node)


class CodeParser:
    # ... (constructor and other methods like parse_config_file, _find_config_assignment, _get_constant_value etc.
    #      remain the same as the previous full version provided in response to "I want the full updated code") ...
    # ... (Make sure to include all methods from CodeParser here if providing the full file) ...
    def __init__(self):
        self.imports: List[Dict[str, str]] = []
        self.raw_code: str = ""
        self.tree: ast.AST | None = None
        self.detected_sql_dialects: List[str] = []

    def parse_config_file(self, config_content_str: str) -> configparser.ConfigParser:
        config = configparser.ConfigParser()
        try:
            config.read_string(config_content_str)
        except configparser.Error as e:
            logging.error(f"Error parsing config file: {e}")
            raise
        return config

    def _find_config_assignment(
        self, target_var_name: str, config_obj_instance_name: str
    ) -> Optional[Tuple[str, str]]:
        if not self.tree: return None
        for node in ast.walk(self.tree):
            if isinstance(node, ast.Assign) and len(node.targets) == 1 and \
               isinstance(node.targets[0], ast.Name) and node.targets[0].id == target_var_name:
                value_node = node.value
                if isinstance(value_node, ast.Subscript) and \
                   isinstance(self._get_constant_value(value_node.slice), str):
                    key_name = self._get_constant_value(value_node.slice)
                    inner_subscript_node = value_node.value
                    if isinstance(inner_subscript_node, ast.Subscript) and \
                       isinstance(inner_subscript_node.value, ast.Name) and \
                       inner_subscript_node.value.id == config_obj_instance_name and \
                       isinstance(self._get_constant_value(inner_subscript_node.slice), str):
                        section_name = self._get_constant_value(inner_subscript_node.slice)
                        return section_name, key_name
        return None

    def _get_constant_value(self, node: ast.AST) -> Any:
        if isinstance(node, ast.Constant): return node.value
        if isinstance(node, ast.Index) and isinstance(node.value, ast.Constant): return node.value.value
        return None

    def clean_json_data(self, json_str: str) -> str:
        json_str = re.sub(r'//.*$', '', json_str, flags=re.MULTILINE)
        json_str = re.sub(r'/\*.*?\*/', '', json_str, flags=re.DOTALL)
        json_str = re.sub(r',(\s*[}\]])', r'\1', json_str)
        return json_str

    def validate_lineage_data(self, lineage_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        validated_data = []
        missing_target_tables = []
        for entry in lineage_data:
            if not entry.get('TGT_TABLE_NAME') or entry.get('TGT_TABLE_NAME') == '{undefined}':
                if entry.get('SRC_TABLE_NAME') == 'RESULT_OF_inner_query':
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
            for entry_detail in missing_target_tables:
                error_msg += f"- Source: {entry_detail.get('SRC_TABLE_NAME')}, Column: {entry_detail.get('SRC_COLUMN_NAME')}\n"
            raise ValueError(error_msg)
        return validated_data

    def parse_json(self, json_str: str) -> List[Dict[str, Any]]:
        try:
            cleaned_json = self.clean_json_data(json_str)
            parsed_data = json.loads(cleaned_json)
            return self.validate_lineage_data(parsed_data)
        except json.JSONDecodeError as e: raise ValueError(f"Error parsing JSON: {str(e)}")
        except ValueError as e: raise e

    def detect_config_object_names(self) -> List[str]:
        candidate_names: Set[str] = set()
        if not self.tree: return []
        for node in ast.walk(self.tree):
            if isinstance(node, ast.Assign):
                value_node = node.value
                if isinstance(value_node, ast.Subscript) and \
                   isinstance(self._get_constant_value(value_node.slice), str):
                    inner_subscript_node = value_node.value
                    if isinstance(inner_subscript_node, ast.Subscript) and \
                       isinstance(self._get_constant_value(inner_subscript_node.slice), str) and \
                       isinstance(inner_subscript_node.value, ast.Name):
                        candidate_names.add(inner_subscript_node.value.id)
        return list(candidate_names)

    def parse_file(self, file_path: str) -> Dict[str, Any]:
        file_path_obj = Path(file_path)
        if not file_path_obj.exists():
            raise FileNotFoundError(f"File not found: {file_path_obj}")
        with open(file_path_obj, 'r', encoding='utf-8') as f:
            self.raw_code = f.read()
        try:
            self.tree = ast.parse(self.raw_code)
            self._extract_imports(self.tree)
            self.detected_sql_dialects = self.get_sql_style()
        except SyntaxError as e:
            self.tree = None
            raise ValueError(f"Invalid Python syntax in '{file_path_obj.name}': {str(e)}")
        return {"imports": self.imports, "raw_code": self.raw_code, "sql_styles": self.detected_sql_dialects}

    def _extract_imports(self, tree: ast.AST) -> None:
        self.imports = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for name in node.names: self.imports.append({"type": "import", "module": name.name, "alias": name.asname})
            elif isinstance(node, ast.ImportFrom):
                module_name = node.module if node.module is not None else "."
                for name in node.names: self.imports.append({"type": "from_import", "module": module_name, "name": name.name, "alias": name.asname})

    def get_sql_style(self) -> List[str]:
        styles: Set[str] = set()
        if any(imp.get("module", "").startswith("pyspark") for imp in self.imports): styles.add("spark")
        if any(imp.get("module", "").startswith("sqlalchemy") for imp in self.imports): styles.add("sqlalchemy_generic")
        return list(styles)

    def get_extracted_sql_queries_with_args(self) -> List[Tuple[str, Optional[Tuple[ast.AST, ...]], Optional[Tuple[Tuple[str, ast.AST], ...]], str]]:
        if self.tree is None or not self.raw_code: return []
        extractor = SQLStringExtractor(raw_python_code=self.raw_code, detected_dialects=self.detected_sql_dialects)
        extractor.visit(self.tree)
        return list(extractor.extracted_sqls)

    def _resolve_ast_node_value(self, node: ast.AST, parsed_config_data: Optional[configparser.ConfigParser], config_obj_name_in_script: str) -> Any:
        if isinstance(node, ast.Constant): return node.value
        elif isinstance(node, ast.Str): return node.s
        elif isinstance(node, ast.Name):
            var_name = node.id
            if parsed_config_data and config_obj_name_in_script and self.tree:
                config_details = self._find_config_assignment(var_name, config_obj_name_in_script)
                if config_details:
                    section, key = config_details
                    actual_value_str = parsed_config_data.get(section, key, fallback=None)
                    if actual_value_str is not None: return actual_value_str
                    else:
                        logging.warning(f"Config lookup: Var '{var_name}' (section: {section}, key: {key}) not found. Placeholder: '{{PY_VAR_UNRESOLVED:{var_name}}}'.")
                        return f"{{PY_VAR_UNRESOLVED:{var_name}}}"
            logging.info(f"Var '{var_name}' not from config. Placeholder: '{{PY_VAR_UNRESOLVED:{var_name}}}'.")
            return f"{{PY_VAR_UNRESOLVED:{var_name}}}"
        else:
            source_repr_cleaned = type(node).__name__
            node_source_segment = "N/A"
            try:
                if self.raw_code:
                    segment = ast.get_source_segment(self.raw_code, node)
                    if segment:
                        node_source_segment = segment
                        if len(segment) < 40: source_repr_cleaned = f"{type(node).__name__}:{segment.replace('{', '{{').replace('}', '}}')}"
            except Exception: pass
            placeholder_name_core = re.sub(r'[^a-zA-Z0-9_]', '_', source_repr_cleaned)[:50]
            final_placeholder = f"{{PY_EXPR_UNRESOLVED:{placeholder_name_core}}}"
            logging.warning(f"Arg node type {type(node).__name__} (source: '{node_source_segment}') is complex. Placeholder: '{final_placeholder}'.")
            return final_placeholder

    def resolve_and_format_sql_queries(
        self,
        extracted_sqls_with_args: List[Tuple[str, Optional[Tuple[ast.AST, ...]], Optional[Tuple[Tuple[str, ast.AST], ...]], str]],
        parsed_config_data: Optional[configparser.ConfigParser],
        config_obj_name_in_script: str
    ) -> List[str]:
        final_sql_queries_to_process = []
        sql_checker_instance = SQLStringExtractor(self.raw_code or "", self.detected_sql_dialects)

        if not self.tree or not self.raw_code:
            logging.error("CodeParser not initialized. Cannot resolve SQL.")
            for template_str, _, _, format_type in extracted_sqls_with_args:
                 is_template = (format_type == 'dot_format' or format_type == 'fstring')
                 if sql_checker_instance._is_sql_like(template_str.strip(), is_template_containing_python_placeholders=is_template):
                     final_sql_queries_to_process.append(template_str.strip())
            return final_sql_queries_to_process

        logging.info(f"Resolving/formatting {len(extracted_sqls_with_args)} SQL templates...")
        
        primary_dialect_for_splitting = None
        if self.detected_sql_dialects:
            if 'spark' in self.detected_sql_dialects: primary_dialect_for_splitting = 'spark'
            elif 'hive' in self.detected_sql_dialects: primary_dialect_for_splitting = 'hive'

        for i, (template_str, pos_args_tuple, kw_args_tuple_of_items, format_type) in enumerate(extracted_sqls_with_args):
            fully_resolved_sql: str
            if format_type == 'literal':
                fully_resolved_sql = template_str
            else:
                resolved_pos_args = [self._resolve_ast_node_value(arg_node, parsed_config_data, config_obj_name_in_script) for arg_node in pos_args_tuple or []]
                resolved_kw_args = {name: self._resolve_ast_node_value(arg_node, parsed_config_data, config_obj_name_in_script) for name, arg_node in kw_args_tuple_of_items or []}
                try:
                    fully_resolved_sql = template_str.format(*resolved_pos_args, **resolved_kw_args)
                except Exception as e:
                    logging.error(f"Formatting error for template {i+1} (type {format_type}): {e}. Template: '{template_str[:100]}...'")
                    fully_resolved_sql = template_str 

            if fully_resolved_sql:
                statements_to_add = []
                try:
                    parsed_expressions = sqlglot.parse(fully_resolved_sql, read=primary_dialect_for_splitting)
                    if parsed_expressions:
                        for expr_tree in parsed_expressions:
                            if expr_tree: statements_to_add.append(expr_tree.sql(dialect=primary_dialect_for_splitting).strip())
                    elif sql_checker_instance._is_sql_like(fully_resolved_sql, is_template_containing_python_placeholders=False):
                        statements_to_add.append(fully_resolved_sql.strip())
                except sqlglot.errors.ParseError as e_sqlglot:
                    logging.warning(f"sqlglot could not parse/split fully resolved SQL (entry {i+1}): '{fully_resolved_sql[:100]}...'. Error: {e_sqlglot}. Adding as single if SQL-like.")
                    if sql_checker_instance._is_sql_like(fully_resolved_sql, is_template_containing_python_placeholders=False):
                        statements_to_add.append(fully_resolved_sql.strip())
                except Exception as e_gen_split:
                    logging.error(f"Unexpected error during sqlglot splitting for entry {i+1}: {e_gen_split}. Adding raw if SQL-like.")
                    if sql_checker_instance._is_sql_like(fully_resolved_sql, is_template_containing_python_placeholders=False):
                        statements_to_add.append(fully_resolved_sql.strip())
                
                for stmt_idx, stmt_stripped in enumerate(statements_to_add):
                    if stmt_stripped:
                        if sql_checker_instance._is_sql_like(stmt_stripped, is_template_containing_python_placeholders=False): # This is the crucial check
                            final_sql_queries_to_process.append(stmt_stripped)
                            logging.debug(f"Adding statement {stmt_idx+1} from entry {i+1}: '{stmt_stripped[:100]}...'")
                        else:
                            logging.debug(f"Split statement {stmt_idx+1} (entry {i+1}) not SQL-like after final check: '{stmt_stripped[:100]}...'")
            else:
                logging.warning(f"Template entry {i+1} resulted in empty string.")
        
        logging.info(f"Finished. Total individual SQL statements: {len(final_sql_queries_to_process)}.")
        return final_sql_queries_to_process