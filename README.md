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