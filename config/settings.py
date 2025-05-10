from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # LLM Settings
    model_name: str
    max_length: int
    temperature: float

    # File Processing
    supported_extensions: list[str]
    max_file_size_mb: int

    # Output Settings
    default_output_format: str
    supported_output_formats: list[str]

    # UI Settings
    streamlit_theme: str
    enable_dark_mode: bool

    # OpenAI Settings
    openai_api_key: str
    openai_model: str = 'gpt-4.1'

    model_config = SettingsConfigDict(env_file=".env")


settings = Settings()