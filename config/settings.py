from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import List
import json
import os
import logging


class Settings(BaseSettings):
   
    # File Processing
    supported_extensions: List[str] = ["py", "sql"]
    max_file_size_mb: int = 10

    # Output Settings
    default_output_format: str = "json"
    supported_output_formats: List[str] = ["json", "csv", "excel"]

    # UI Settings
    streamlit_theme: str = "light"
    enable_dark_mode: bool = False

    # Groq/DeepSeek Settings
    groq_api_key: str
    groq_model: str

    def __init__(self, **kwargs):
        # Convert string lists to actual lists
        if 'supported_extensions' in kwargs and isinstance(kwargs['supported_extensions'], str):
            kwargs['supported_extensions'] = json.loads(kwargs['supported_extensions'])
        if 'supported_output_formats' in kwargs and isinstance(kwargs['supported_output_formats'], str):
            kwargs['supported_output_formats'] = json.loads(kwargs['supported_output_formats'])
        
        # Try to get Groq API keys from environment variables if not in kwargs
        if 'groq_api_key' not in kwargs or not kwargs['groq_api_key']:
            env_key = os.getenv('GROQ_API_KEY')
            if env_key:
                logging.info("Found GROQ_API_KEY in environment variables")
                kwargs['groq_api_key'] = env_key
            else:
                logging.warning("GROQ_API_KEY not found in environment variables")
        if 'groq_model' not in kwargs or not kwargs['groq_model']:
            env_model = os.getenv('GROQ_MODEL')
            if env_model:
                logging.info("Found GROQ_MODEL in environment variables")
                kwargs['groq_model'] = env_model
            else:
                logging.warning("GROQ_MODEL not found in environment variables")
        
        super().__init__(**kwargs)
        
        # Log the final configuration
        logging.info("Settings initialized with:")
        logging.info(f"Groq API Key present: {'Yes' if self.groq_api_key else 'No'}")
        logging.info(f"Groq Model: {self.groq_model}")

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding='utf-8',
        extra='ignore',
        case_sensitive=True
    )


settings = Settings()