import configparser
import io
import re
import sqlparse

class BaseExtractor:
    """
    Base class for all extractors. Defines the interface for extracting
    metadata and lineage from a file's content.
    """
    def extract(self, file_content: str):
        """
        Extracts information from the given file content.
        This method should be implemented by all subclasses.
        """
        raise NotImplementedError("Subclasses must implement the 'extract' method.")

class IniExtractor(BaseExtractor):
    """
    Extractor for .ini configuration files.
    """
    def extract(self, file_content: str):
        """
        Parses an .ini file and returns a dictionary of its sections and keys.
        """
        config = configparser.ConfigParser()
        try:
            config.read_string(file_content)
            extracted_data = {section: dict(config.items(section)) for section in config.sections()}
            return extracted_data
        except configparser.Error as e:
            print(f"Error parsing INI file: {e}")
            return None

class PythonExtractor(BaseExtractor):
    """
    Extractor for Python files.
    """
    def extract(self, file_content: str):
        """
        Extracts references to configuration keys and embedded SQL from Python code.
        """
        # Regex to find config usages like: config_op_obj["section"]["key"]
        config_pattern = re.compile(r'config_op_obj\["([^"]+)"\]\["([^"]+)"\]')
        
        # Regex to find SQL queries embedded in Python strings (basic version)
        sql_query_pattern = re.compile(r'spark\\.sql\((["\\]{3}|["\\])(.*?)\1\)', re.DOTALL)

        config_references = []
        for match in config_pattern.finditer(file_content):
            config_references.append({
                "type": "config_reference",
                "section": match.group(1),
                "key": match.group(2)
            })
            
        sql_queries = []
        for match in sql_query_pattern.finditer(file_content):
            sql_queries.append({
                "type": "embedded_sql",
                "query": match.group(2).strip()
            })

        return {
            "config_references": config_references,
            "sql_queries": sql_queries
        }

class SqlExtractor(BaseExtractor):
    """
    Extractor for .sql files.
    """
    def extract(self, file_content: str):
        """
        Extracts SQL queries from a .sql file.
        """
        queries = []
        # Use sqlparse to split the content into individual statements
        statements = sqlparse.split(file_content)
        for statement in statements:
            # Clean up the statement
            clean_statement = sqlparse.format(statement, strip_comments=True).strip()
            if clean_statement:
                # Get the statement type
                statement_type = sqlparse.parse(clean_statement)[0].get_type()
                queries.append({
                    "query": clean_statement,
                    "type": statement_type
                })
        return {"queries": queries}