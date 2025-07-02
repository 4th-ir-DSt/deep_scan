
import os
import json
from typing import Dict, List, Type
from tools.extractors import BaseExtractor, IniExtractor, PythonExtractor, SqlExtractor

class Scanner:
    """
    A universal scanner to analyze codebases, identify file types,
    and extract lineage and metadata.
    """

    def __init__(self):
        self.file_index: Dict[str, Dict] = {}
        self.problems: List[str] = []
        self.extractors: Dict[str, Type[BaseExtractor]] = {
            "ini": IniExtractor,
            "python": PythonExtractor,
            "sql": SqlExtractor,
        }

    def _get_file_type(self, file_path: str) -> str:
        """
        Determines the type of a file based on its extension.
        """
        _, extension = os.path.splitext(file_path)
        ext_map = {
            ".py": "python",
            ".sql": "sql",
            ".java": "java",
            ".scala": "scala",
            ".ini": "ini",
            ".json": "json",
            ".xml": "xml",
            ".csv": "csv",
            ".yml": "yaml",
            ".yaml": "yaml",
        }
        return ext_map.get(extension.lower(), "unknown")

    def scan(self, scan_path: str):
        """
        Recursively scans a directory, identifies file types, and indexes them.
        """
        print(f"Starting scan of: {scan_path}")
        for root, _, files in os.walk(scan_path):
            for file in files:
                file_path = os.path.join(root, file)
                file_type = self._get_file_type(file_path)

                if file_type != "unknown":
                    try:
                        with open(file_path, 'r', encoding='utf-8') as f:
                            content = f.read()
                    except (UnicodeDecodeError, IOError) as e:
                        self.problems.append(f"Could not read file: {file_path} - {e}")
                        continue

                    self.file_index[file_path] = {
                        "file_type": file_type,
                        "path": file_path,
                        "extracted_data": None
                    }

                    if file_type in self.extractors:
                        extractor_class = self.extractors[file_type]
                        extractor = extractor_class()
                        extracted_data = extractor.extract(content)
                        self.file_index[file_path]["extracted_data"] = extracted_data
                        print(f"  Processed: {file_path} (type: {file_type})")
                    else:
                        print(f"  Found: {file_path} (type: {file_type}, no extractor available)")


        print("Scan complete.")
        return self.file_index

if __name__ == "__main__":
    # This allows the script to be run directly for testing
    scanner = Scanner()
    # The path to scan will be passed as a command-line argument
    import sys
    if len(sys.argv) > 1:
        scan_directory = sys.argv[1]
        results = scanner.scan(scan_directory)
        
        # Output the results to a JSON file
        output_filename = "scan_results.json"
        with open(output_filename, 'w') as f:
            json.dump(results, f, indent=4)
        
        print(f"\nResults saved to {output_filename}")
        
        if scanner.problems:
            print("\nProblems encountered during scan:")
            for problem in scanner.problems:
                print(f"- {problem}")

    else:
        print("Please provide a directory to scan.")

