from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import List
import json


class Settings(BaseSettings):
    # LLM Settings
    model_name: str = "gpt-4o"
    max_length: int = 4096
    temperature: float = 0.5

    # File Processing
    supported_extensions: List[str] = ["py", "sql"]
    max_file_size_mb: int = 10

    # Output Settings
    default_output_format: str = "json"
    supported_output_formats: List[str] = ["json", "csv", "excel"]

    # UI Settings
    streamlit_theme: str = "light"
    enable_dark_mode: bool = False

    # OpenAI Settings
    openai_api_key: str = ""  # This should be set in .env
    openai_model: str = "gpt-4.1"

    def __init__(self, **kwargs):
        # Convert string lists to actual lists
        if 'supported_extensions' in kwargs and isinstance(kwargs['supported_extensions'], str):
            kwargs['supported_extensions'] = json.loads(kwargs['supported_extensions'])
        if 'supported_output_formats' in kwargs and isinstance(kwargs['supported_output_formats'], str):
            kwargs['supported_output_formats'] = json.loads(kwargs['supported_output_formats'])
        super().__init__(**kwargs)

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding='utf-8')


settings = Settings()