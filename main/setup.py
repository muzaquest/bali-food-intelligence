#!/usr/bin/env python3
"""
Setup script for MuzaQuest Mini App
"""

from setuptools import setup, find_packages
import os

# Read the contents of README file
this_directory = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(this_directory, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

# Read requirements from requirements.txt
with open(os.path.join(this_directory, 'requirements.txt'), encoding='utf-8') as f:
    requirements = [line.strip() for line in f if line.strip() and not line.startswith('#')]

setup(
    name="muzaquest-mini-app",
    version="1.0.0",
    author="MuzaQuest Team",
    author_email="info@muzaquest.com",
    description="AI-powered restaurant sales analysis and prediction system",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/muzaquest/muzaquest-mini-app",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Intended Audience :: End Users/Desktop",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Scientific/Engineering :: Artificial Intelligence",
        "Topic :: Office/Business :: Financial",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    python_requires=">=3.8",
    install_requires=requirements,
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-cov>=4.0.0",
            "black>=22.0.0",
            "flake8>=5.0.0",
            "mypy>=0.991",
            "pre-commit>=2.20.0",
            "jupyter>=1.0.0",
            "notebook>=6.4.0",
        ],
        "docs": [
            "sphinx>=5.0.0",
            "sphinx-rtd-theme>=1.0.0",
            "sphinx-autodoc-typehints>=1.19.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "muzaquest=main:main",
        ],
    },
    include_package_data=True,
    package_data={
        "": ["*.md", "*.txt", "*.yml", "*.yaml", "*.json"],
        "models": ["*.joblib", "*.json"],
        "data": ["*.sqlite", "*.db"],
        "api_integrations": ["*.py"],
    },
    zip_safe=False,
    keywords="machine-learning ai restaurant sales prediction analytics",
    project_urls={
        "Bug Reports": "https://github.com/muzaquest/muzaquest-mini-app/issues",
        "Source": "https://github.com/muzaquest/muzaquest-mini-app",
        "Documentation": "https://muzaquest-mini-app.readthedocs.io/",
    },
)