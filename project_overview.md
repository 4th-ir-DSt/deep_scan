# Project Overview

The **Local Code Extractor POC** is designed to provide organizations with a **privacy-first**, **on-premise** solution for automatically analyzing source code—including Python, Java, JavaScript, and SQL scripts—for embedded data transformations. Without sending any code to external services, the tool leverages a lightweight local LLM (Qwen Coder 0.6B) alongside language-appropriate static analysis (e.g., AST for Python/JavaScript, parsing libraries for others) to generate a **machine-readable JSON report** that adheres precisely to the `consumer_extract` schema—enabling seamless integration with downstream CSV/Excel exports or governance systems.

## Purpose and Scope

* **Automated Data Mapping**: Transform manual extraction processes into an automated pipeline that catalogs every data-read, transformation, and write operation at the column level.
* **Security and Compliance**: Maintain all analysis within the client environment, eliminating the risk of code leakage or external data exposure.
* **Scalable Foundation**: While this POC focuses on a single file, the architecture supports future expansion to multi-file and directory-level processing.

## Key Capabilities

* **Multi-Language Parsing**: Supports Python, Java, JavaScript/TypeScript, and SQL script analysis using appropriate parsers (AST for Python/JS, Java parsing tools, SQL regex-based or parser libraries).
* **Multi-Style SQL Recognition**: Detects and extracts operations across all languages from:

  * PySpark DataFrame APIs (`read.table`, `selectExpr`) and Java Spark APIs.
  * Embedded SQL strings in any host language (`.sql()`, `cursor.execute()`, `statement.executeQuery()`).
  * ORM or database client calls (e.g., SQLAlchemy, JDBC, knex.js).
* **Precise Schema Alignment**: Produces JSON output validated against a hard-coded Draft-07 schema for the `consumer_extract` sheet, ensuring each record contains required fields like `MAP_NAME`, `SRC_TABLE_NAME`, and `MAP_SEQ_ID`.
* **Extensible Architecture**: Modular design allows adding new language parsers or SQL styles by implementing additional parser modules.
* **User-Centric Streamlit UI**: Provides intuitive file or directory selection, detected language and SQL style badges, and easy exports to JSON, CSV, or Excel—all in a responsive layout.

## Embedded JSON Schema

The output JSON strictly follows this structure (simplified view):

```json
{
  "consumer_extract": [
    {
      "MAP_NAME": "string",
      "SRC_SYSTEM_NAME": "string",
      "SRC_TABLE_NAME": "string",
      "SRC_COLUMN_NAME": "string",
      "BUSINESS_RULE": "string",
      "TGT_SYSTEM_NAME": "string",
      "TGT_TABLE_NAME": "string",
      "TGT_COLUMN_NAME": "string",
      "CREATED_BY": "string",
      "CREATED_DATETIME": "string (date-time)",
      "LAST_MODIFIED_BY": "string",
      "LAST_MODIFIED_DATETIME": "string (date-time)",
      "SRC_TABLE_TYPE": "string",
      "TGT_TABLE_TYPE": "string",
      "MAP_SEQ_ID": "integer"
    }
  ]
}
```

All fields are mandatory, and no additional properties are allowed, ensuring compatibility with audit dashboards and data catalogs.

## Conclusion

This POC establishes a **secure**, **automated**, and **transparent** method for extracting detailed data-flow metadata from Python-based ETL scripts. It lays a robust foundation for scaling to complex codebases and integrating with enterprise data-governance workflows.
