#!/usr/bin/env python3
"""
Deployment preparation script for the Data Lineage Analyzer Streamlit app.
"""
import os
import sys
import subprocess
import importlib.util

def check_dependencies():
    """Check if all required dependencies are available."""
    required_packages = [
        'streamlit',
        'groq',
        'tiktoken', 
        'graphviz',
        'pandas',
        'openpyxl',
        'plotly',
        'pydantic_settings',
        'sqlparse',
        'jsonschema'
    ]
    
    missing_packages = []
    for package in required_packages:
        try:
            importlib.import_module(package.replace('-', '_'))
            print(f"✅ {package}")
        except ImportError:
            missing_packages.append(package)
            print(f"❌ {package} - MISSING")
    
    return missing_packages

def check_environment_variables():
    """Check if required environment variables are set."""
    required_vars = ['GROQ_API_KEY']
    missing_vars = []
    
    for var in required_vars:
        if os.getenv(var):
            print(f"✅ {var} - SET")
        else:
            missing_vars.append(var)
            print(f"❌ {var} - NOT SET")
    
    return missing_vars

def check_files():
    """Check if all required files exist."""
    required_files = [
        'streamlit_app.py',
        'refined_extractor.py',
        'requirements.txt',
        '.streamlit/config.toml',
        'Procfile',
        'runtime.txt'
    ]
    
    missing_files = []
    for file in required_files:
        if os.path.exists(file):
            print(f"✅ {file}")
        else:
            missing_files.append(file)
            print(f"❌ {file} - MISSING")
    
    return missing_files

def main():
    print("🔍 Data Lineage Analyzer - Deployment Check")
    print("=" * 50)
    
    # Check dependencies
    print("\n📦 Checking Dependencies:")
    missing_deps = check_dependencies()
    
    # Check environment variables
    print("\n🔑 Checking Environment Variables:")
    missing_vars = check_environment_variables()
    
    # Check files
    print("\n📁 Checking Required Files:")
    missing_files = check_files()
    
    # Summary
    print("\n" + "=" * 50)
    print("📋 DEPLOYMENT SUMMARY:")
    
    if not missing_deps and not missing_vars and not missing_files:
        print("✅ All checks passed! Ready for deployment.")
        print("\n🚀 Deployment Commands:")
        print("For local testing: streamlit run streamlit_app.py")
        print("For production: Follow your deployment platform's instructions")
    else:
        print("❌ Issues found that need to be resolved:")
        
        if missing_deps:
            print(f"  - Install missing packages: pip install {' '.join(missing_deps)}")
        
        if missing_vars:
            print(f"  - Set environment variables: {', '.join(missing_vars)}")
        
        if missing_files:
            print(f"  - Create missing files: {', '.join(missing_files)}")
        
        sys.exit(1)

if __name__ == "__main__":
    main() 