import os
import json
import logging
import re
from groq import Groq
from langchain_text_splitters import Language, RecursiveCharacterTextSplitter
from config.settings import settings

class CodeParser:
    """
    CodeParser provides LLM-based SQL extraction from Python scripts.
    It uses Groq's DeepSeek model to extract SQL queries, including those built with f-strings and .format().
    Uses a Python-aware code splitter for robust chunking.
    """
    def extract_sql_with_llm(self, file_path: str, chunk_size: int = 10000, chunk_overlap: int = 1000) -> list[str]:
        """
        Extract SQL queries from a Python file using the DeepSeek LLM via Groq API.
        This method chunks the code using a Python-aware splitter, sends each chunk to the model, and collects/deduplicates the SQL queries.
        """

        GROQ_API_KEY = settings.groq_api_key
        DEEPSEEK_MODEL = settings.groq_model

        if not os.path.exists(file_path):
            logging.error(f"Error: File not found at {file_path}")
            return []

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                file_content = f.read()
        except Exception as e:
            logging.error(f"Error reading file {file_path}: {e}")
            return []

        # Use the Python-aware code splitter for robust chunking
        splitter = RecursiveCharacterTextSplitter.from_language(
            language=Language.PYTHON,
            chunk_size=chunk_size,
            chunk_overlap=chunk_overlap
        )
        docs = splitter.create_documents([file_content])
        code_chunks = [doc.page_content for doc in docs]
        all_queries = []

        client = Groq(api_key=GROQ_API_KEY)
        for i, chunk in enumerate(code_chunks):
            chunk_lines = chunk.splitlines()
            preview = '\n'.join(chunk_lines[:5]) + ('\n...\n' if len(chunk_lines) > 10 else '\n') + '\n'.join(chunk_lines[-5:])
            logging.info(f"Sending chunk {i+1}/{len(code_chunks)} to model. Total lines: {len(chunk_lines)}. Preview:\n{preview}")
            prompt = (
                "Extract all SQL queries from the following code/text. "
                "Include queries that are defined as string literals, f-strings, or created using the `.format()` method. "
                "For queries built with `.format()`, reconstruct the SQL by replacing placeholders (such as `{}`, `{0}`) with the variable names or values used in the `.format()` call, so the output is a complete, human-readable SQL string. "
                "Return ONLY a JSON array of the extracted SQL queries. Do NOT include any other text, explanation, reasoning, or formatting outside the JSON array. Do NOT include any <think> or reasoning blocks. "
                "Code/Text:\n" +
                chunk + "\n"
            )
            try:
                response = client.chat.completions.create(
                    model=DEEPSEEK_MODEL,
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.0,
                )
                # Log status code if available
                if hasattr(response, 'status_code'):
                    logging.info(f"Groq API status code for chunk {i+1}: {response.status_code}")
                    if response.status_code != 200:
                        logging.error(f"Non-200 status code for chunk {i+1}: {response.status_code}. Response: {response}")
                else:
                    logging.info(f"Groq API response object for chunk {i+1} does not have a status_code attribute.")
                raw_output = response.choices[0].message.content.strip()
                queries = self.extract_all_json_arrays(raw_output)
                if queries:
                    logging.info(f"Successfully extracted {len(queries)} queries from chunk {i+1}.")
                else:
                    logging.warning(f"No queries extracted from chunk {i+1}. Raw output:\n{raw_output}")
                all_queries.extend(queries)
            except json.JSONDecodeError as e:
                logging.error(f"Failed to parse JSON response from model for chunk {i+1}: {e}")
                print("Raw model output (JSON parsing failed):")
                print(raw_output)
            except Exception as e:
                logging.error(f"Error during API call to Groq/DeepSeek for chunk {i+1}: {e}")
                print(f"API Error: {e}")

        # Deduplicate and clean up queries
        unique_queries = []
        seen = set()
        for q in all_queries:
            q_clean = q.strip()
            if q_clean and q_clean not in seen:
                unique_queries.append(q_clean)
                seen.add(q_clean)
        return unique_queries

    def extract_all_json_arrays(self, text):
        # Remove code block markers and <think> blocks
        text = re.sub(r"```(?:json)?", "", text, flags=re.IGNORECASE)
        text = re.sub(r"<think>.*?</think>", "", text, flags=re.DOTALL | re.IGNORECASE)
        text = text.strip()

        # Find all JSON arrays in the text, even if they span multiple lines
        array_matches = re.findall(r"\[\s*[\s\S]*?\s*\]", text)
        all_queries = []
        for match in array_matches:
            try:
                queries = json.loads(match)
                if isinstance(queries, list):
                    all_queries.extend(queries)
            except Exception:
                continue

        # Fallback: try to parse the whole text as JSON if nothing found
        if not all_queries:
            try:
                queries = json.loads(text)
                if isinstance(queries, list):
                    all_queries.extend(queries)
            except Exception:
                pass

        return all_queries