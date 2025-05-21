import os
import sys
import logging
from pathlib import Path

# Add project root to Python path
project_root = str(Path(__file__).parent.parent)
sys.path.append(project_root)

from dotenv import load_dotenv
from config.settings import settings
import google.generativeai as genai

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def verify_api_key():
    """Verify the Google API key configuration."""
    try:
        # Load environment variables
        load_dotenv()
        
        # Check environment variable
        env_key = os.getenv('GOOGLE_API_KEY')
        logger.info(f"GOOGLE_API_KEY in environment: {'Present' if env_key else 'Not found'}")
        
        # Check settings
        settings_key = settings.gemini_api_key
        logger.info(f"API key in settings: {'Present' if settings_key else 'Not found'}")
        
        if not settings_key:
            logger.error("No API key found in settings. Please check your .env file and settings configuration.")
            return False
            
        # Try to initialize the Gemini client
        genai.configure(api_key=settings_key)
        
        # Try a simple model list request
        models = genai.list_models()
        logger.info("Successfully connected to Gemini API")
        logger.info(f"Available models: {[model.name for model in models]}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error verifying API key: {str(e)}")
        return False

if __name__ == "__main__":
    success = verify_api_key()
    if not success:
        logger.error("API key verification failed. Please check your configuration.")
        exit(1)
    logger.info("API key verification successful!") 