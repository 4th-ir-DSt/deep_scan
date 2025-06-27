# Streamlit Cloud Deployment Guide

## 🚀 Deploying to Streamlit Cloud

### Prerequisites
- A GitHub repository with your code
- A Streamlit Cloud account
- A Groq API key

### Step-by-Step Deployment

#### 1. **Prepare Your Repository**
Ensure your repository contains:
- `streamlit_app.py` (main app file)
- `refined_extractor.py` (analysis logic)
- `requirements.txt` (dependencies)
- `.streamlit/config.toml` (Streamlit configuration)
- `.streamlit/secrets.toml` (template for secrets)

#### 2. **Set Up Streamlit Cloud**
1. Go to [share.streamlit.io](https://share.streamlit.io)
2. Sign in with your GitHub account
3. Click "New app"
4. Select your repository and branch
5. Set the main file path to: `streamlit_app.py`

#### 3. **Configure Secrets**
In the Streamlit Cloud dashboard:
1. Go to your app settings
2. Navigate to "Secrets"
3. Add your Groq API key:
```toml
GROQ_API_KEY = "your_actual_groq_api_key_here"
```

#### 4. **Deploy**
1. Click "Deploy!"
2. Wait for the build to complete
3. Your app will be available at the provided URL

### Configuration Files

#### `.streamlit/config.toml`
```toml
[global]
developmentMode = false

[server]
headless = true
enableCORS = false
enableXsrfProtection = false
port = 8501

[browser]
gatherUsageStats = false
```

#### `requirements.txt`
```
streamlit>=1.45.0
groq>=0.4.0
tiktoken>=0.9.0
graphviz>=0.20.1
pandas>=2.2.3
openpyxl>=3.1.5
plotly>=6.1.2
pydantic-settings>=2.9.1
sqlparse>=0.5.3
jsonschema>=4.23.0
```

### Troubleshooting

#### Common Issues:
1. **Build fails**: Check that all dependencies are in `requirements.txt`
2. **API key not found**: Ensure `GROQ_API_KEY` is set in Streamlit secrets
3. **Import errors**: Verify all required packages are listed in requirements.txt
4. **Memory issues**: Large files may cause timeouts - the app is optimized for this

#### Performance Tips:
- The app automatically chunks large files for faster processing
- Very large files (>15k tokens) are processed in fast mode
- API calls are optimized for speed with reduced token limits

### Security Notes
- Never commit your actual API key to the repository
- Use Streamlit secrets for sensitive configuration
- The `.streamlit/secrets.toml` file in the repo is just a template

### Monitoring
- Check the Streamlit Cloud dashboard for app status
- Monitor API usage through your Groq dashboard
- Use the built-in logging for debugging

### Support
If you encounter issues:
1. Check the Streamlit Cloud logs
2. Verify your API key is correct
3. Ensure all dependencies are properly specified
4. Test locally first with `streamlit run streamlit_app.py` 