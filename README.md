<<<<<<< HEAD
# Code Extractor POC

A privacy-first, on-premise solution for automatically analyzing source code and extracting data transformations. This tool uses a local LLM (Qwen Coder 0.6B) to analyze code without sending it to external services.

## Features

- Multi-language support (Python, Java, JavaScript/TypeScript, SQL)
- Detects multiple SQL styles:
  - PySpark DataFrame APIs
  - Raw SQL strings
  - ORM/database client calls
- Generates standardized JSON output
- Modern Streamlit UI with export options
- Privacy-first: all processing done locally

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd code-extractor
```

2. Create a virtual environment and install dependencies:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

## Usage

1. Start the Streamlit app:
```bash
streamlit run ui/app.py
```

2. Open your browser and navigate to the URL shown in the terminal (typically http://localhost:8501)

3. Upload a Python file containing data transformations

4. Click "Run Extraction" to analyze the code

5. View results in JSON format or download as CSV/Excel

## Project Structure

```
code_extractor/
├── config/                 # Configuration settings
├── parser/                # Code parsing logic
├── extractor/             # SQL operation extraction
├── assembler/             # JSON assembly and validation
├── ui/                    # Streamlit interface
└── project.toml          # Project dependencies
```

## Development

- The project uses Python 3.9+
- Dependencies are managed via project.toml
- Code follows PEP 8 style guide
- Tests are written using pytest

## License

[Your License Here]

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request 
=======
# SQL Lineage Extractor (LLM-Powered)

This Python script uses Groq's LLM (Large Language Model) to extract detailed data lineage information from SQL queries stored in a JSON file. It leverages advanced language models to understand complex SQL semantics and transformations.

## Features

- LLM-powered SQL analysis for accurate lineage extraction
- Supports complex SQL transformations and business rules
- Handles various SQL statement types (CREATE, SELECT, etc.)
- Generates detailed column-level lineage mapping
- Outputs lineage information in a structured JSON format

## How It Works

1. **LLM Analysis**: Uses Groq's Mixtral-8x7b model to analyze SQL queries
2. **Deep Understanding**: Comprehends complex SQL semantics and transformations
3. **Accurate Extraction**: Identifies source-target relationships and business rules
4. **Structured Output**: Generates standardized lineage information

## Installation

1. Clone this repository
2. Install the required dependencies:

```bash
pip install -r requirements.txt
```

3. Set up your Groq API key:
   - Get your API key from https://console.groq.com
   - Set it as an environment variable:
     ```bash
     export GROQ_API_KEY=your_api_key_here
     ```
   - Or create a .env file with:
     ```
     GROQ_API_KEY=your_api_key_here
     ```

## Usage

1. Prepare your input JSON file with SQL queries in the following format:

```json
{
  "queries": [
    {
      "query": "YOUR SQL QUERY",
      "type": "QUERY_TYPE",
      ...
    }
  ]
}
```

2. Run the script:

```bash
python sql_lineage_extractor.py
```

By default, the script will:
- Read from `result.json`
- Write the lineage output to `lineage.json`

## Output Format

The script generates lineage information in the following JSON format:

```json
[
  {
    "SRC_TABLE_NAME": "source_table",
    "SRC_COLUMN_NAME": "source_column",
    "BUSINESS_RULE": "exact_transformation_or_DIRECT",
    "TGT_TABLE_NAME": "target_table",
    "TGT_COLUMN_NAME": "target_column"
  }
]
```

## Advantages of LLM Approach

1. **Complex SQL Understanding**
   - Handles nested queries
   - Understands complex joins
   - Processes window functions
   - Analyzes CTEs and subqueries

2. **Accurate Transformation Detection**
   - Captures complex business rules
   - Understands conditional logic
   - Identifies data type conversions
   - Maps multi-source transformations

3. **Flexible and Adaptable**
   - Works with various SQL dialects
   - Handles custom functions
   - Adapts to different SQL styles
   - No hard-coded parsing rules

## Limitations

- Requires Groq API key and internet connection
- May have higher latency compared to regex-based approaches
- API costs associated with LLM usage
- Rate limits based on Groq API tier

## Future Enhancements

- Support for more SQL dialects
- Batch processing optimization
- Caching for repeated queries
- Enhanced error handling and validation
- Integration with other LLM providers
>>>>>>> 9c7f630 (codes for all folders)
