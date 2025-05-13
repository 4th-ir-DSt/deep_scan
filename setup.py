from setuptools import setup, find_packages

setup(
    name="deep_scan",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "streamlit",
        "pandas",
        "pydantic-settings",
        "openpyxl",
    ],
    python_requires=">=3.8",
) 