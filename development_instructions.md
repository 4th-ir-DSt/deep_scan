# Development Instructions
## 1. Project Setup

Define a concise, modular directory layout with minimal nesting. Each folder represents a clear responsibility:

code_extractor/                   # Top-level package
|-- config/                       # configuration directory for the project
    |__ settings.py               # uses pydantic settings for configs
├── parser/                       # Import and script parsing
│   └── parser.py                 # AST-based module for extracting imports and raw code text
├── extractor/                    # SQL operation extraction
│   └── extractor.py              # LLM prompt definitions for parsing multiple SQL styles
├── assembler/                    # JSON assembly and validation
│   └── assembler.py              # Embeds full JSON schema and merges parsed data into the final payload
├── ui/                           # Streamlit proof-of-concept
│   ├── app.py                    # Main Streamlit application
│   └── components.py             # Reusable UI widgets (file selector, badges, tabs)
└── project.toml                  # UV-managed dependencies and project metadata

* **parser/**: Contains logic to read a Python file, parse imports using AST, and expose code text.
* **extractor/**: Defines LLM prompts that detect data operations across PySpark, raw SQL, and ORM calls.
* **assembler/**: Houses the JSON schema and prompt template to merge parsed imports and operations into the final JSON.
* **ui/**: Implements the Streamlit interface with a clean separation between application logic (`app.py`) and shared widgets (`components.py`).
* **tests/**: Organizes unit and integration tests aligned to each core module.
* **project.toml**: Manages all dependencies via UV and holds project configuration.

## 2. Import and Script Parsing. Import and Script Parsing

* Use Python’s AST module to enumerate imports (e.g., `pyspark.sql`, `sqlite3`, `psycopg2`).
* Extract raw script text for downstream LLM processing.

## 3. SQL Extraction Logic

Design the LLM prompt(s) to recognize and parse multiple SQL invocation styles:

1. **PySpark DataFrame API**

   * Detect calls like `spark.read.table()`, `DataFrame.selectExpr()`
   * Capture source table names and column expressions.

2. **SQL String Execution**

   * Identify `.sql("SELECT ...")` or `cursor.execute("...")` patterns.
   * Extract the SQL string, then parse the `SELECT` clause for columns and the `FROM` clause for sources.

3. **ORM or Library Calls**

   * Support common libraries (e.g., SQLAlchemy `session.execute()`).
   * Map ORM queries to equivalent SQL operations.

For each style, instruct the LLM to output a unified list of operations with fields: `operation`, `source`, `destination`, `parameters`.

## 4. JSON Assembly with Embedded Schema

* Hard‑code the `consumer_extract` JSON schema (excluding `User Defined-#` fields) into the assembler prompt. Use the following Draft-07 schema:

  ```json
  {
    "$schema":"http://json-schema.org/draft-07/schema#",
    "title":"Consumer Extract Mapping Specification",
    "type":"object",
    "required":["consumer_extract"],
    "properties":{
      "consumer_extract":{
        "type":"array",
        "items":{
          "type":"object",
          "required":[
            "MAP_NAME","SRC_SYSTEM_NAME","SRC_TABLE_NAME","SRC_COLUMN_NAME",
            "BUSINESS_RULE","TGT_SYSTEM_NAME","TGT_TABLE_NAME","TGT_COLUMN_NAME",
            "CREATED_BY","CREATED_DATETIME","LAST_MODIFIED_BY","LAST_MODIFIED_DATETIME",
            "SRC_TABLE_TYPE","TGT_TABLE_TYPE","MAP_SEQ_ID"
          ],
          "properties":{
            "MAP_NAME":{"type":"string"},
            "SRC_SYSTEM_NAME":{"type":"string"},
            "SRC_TABLE_NAME":{"type":"string"},
            "SRC_COLUMN_NAME":{"type":"string"},
            "BUSINESS_RULE":{"type":"string"},
            "TGT_SYSTEM_NAME":{"type":"string"},
            "TGT_TABLE_NAME":{"type":"string"},
            "TGT_COLUMN_NAME":{"type":"string"},
            "CREATED_BY":{"type":"string"},
            "CREATED_DATETIME":{"type":"string","format":"date-time"},
            "LAST_MODIFIED_BY":{"type":"string"},
            "LAST_MODIFIED_DATETIME":{"type":"string","format":"date-time"},
            "SRC_TABLE_TYPE":{"type":"string"},
            "TGT_TABLE_TYPE":{"type":"string"},
            "MAP_SEQ_ID":{"type":"integer"}
          },
          "additionalProperties":false
        }
      }
    },
    "additionalProperties":false
  }
  ```

* Provide a concise example in the prompt illustrating how imports and operation entries map to this schema.

## 5. Streamlit POC Design Pattern

Detail a modern, intuitive UI layout without writing code:

1. **Sidebar Navigation**

   * Include a file selector or drag-and-drop zone for choosing the Python script.
   * Show detected SQL styles as toggleable badges (Spark-API, raw SQL, ORM).
   * Display a “Run Extraction” button and a progress indicator (e.g., progress bar or spinner).

2. **Main Content Area**

   * **Tabbed View**: Organize output into tabs—"JSON Output", "Table Preview", and "Download".
   * **JSON Output Tab**: Render the full JSON via `st.json()` with collapsible nodes.
   * **Table Preview Tab**: Display a pandas DataFrame preview of the JSON converted into tabular form (`pandas.DataFrame.from_records()`).
   * **Download Tab**: Offer buttons for JSON, CSV, and Excel exports, clearly labeled.

3. **Responsive Layout**

   * Use containers and columns to ensure the UI adapts to different screen sizes.
   * Apply Tailwind-inspired styling via custom CSS or a Streamlit theming plugin.

4. **User Feedback**

   * Show toast notifications or status messages on successful extraction, errors, or validation failures.

\--- Streamlit POC Design

Outline the Streamlit UI without code:

1. **File Selector**: A widget to upload or browse for a `.py` file.
2. **Run Extraction**: Button to kick off parsing, extraction, and assembly.
3. **Results View**: Display the resulting JSON via `st.json()`.
4. **Export Options**: Buttons to download as JSON, CSV, or Excel.
5. **SQL Style Badge**: In the UI, indicate which SQL styles were detected (e.g., Spark‑API, raw SQL, ORM).

## 6. Testing Strategy

* **Parser Tests**: Feed sample scripts with each SQL style, assert correct import and code-text extraction.
* **Extraction Tests**: Mock LLM responses to verify that the prompt logic captures operations uniformly across styles.
* **Assembly Tests**: Validate final JSON against the embedded schema using `jsonschema`.

---

*End of development\_instructions.md*
