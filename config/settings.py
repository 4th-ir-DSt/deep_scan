from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import List
import json
import os
import logging


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

    # Gemini Settings
    gemini_api_key: str = ""  # This should be set in .env
    gemini_model: str = "gemini-2.5-pro-preview-05-06"

    def __init__(self, **kwargs):
        # Convert string lists to actual lists
        if 'supported_extensions' in kwargs and isinstance(kwargs['supported_extensions'], str):
            kwargs['supported_extensions'] = json.loads(kwargs['supported_extensions'])
        if 'supported_output_formats' in kwargs and isinstance(kwargs['supported_output_formats'], str):
            kwargs['supported_output_formats'] = json.loads(kwargs['supported_output_formats'])
        
        # Try to get API keys from environment variables if not in kwargs
        if 'openai_api_key' not in kwargs or not kwargs['openai_api_key']:
            env_key = os.getenv('OPENAI_API_KEY')
            if env_key:
                logging.info("Found OPENAI_API_KEY in environment variables")
                kwargs['openai_api_key'] = env_key
            else:
                logging.warning("OPENAI_API_KEY not found in environment variables")
        
        super().__init__(**kwargs)
        
        # Log the final configuration
        logging.info("Settings initialized with:")
        logging.info(f"OpenAI API Key present: {'Yes' if self.openai_api_key else 'No'}")
        logging.info(f"OpenAI Model: {self.openai_model}")

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding='utf-8',
        extra='ignore',
        case_sensitive=True
    )


settings = Settings()