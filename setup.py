"""
Setup script for the Subjective Abstract Data Source Package
"""

from setuptools import setup, find_packages
import os

# Read the README file
def read_readme():
    readme_path = os.path.join(os.path.dirname(__file__), 'README.md')
    if os.path.exists(readme_path):
        with open(readme_path, 'r', encoding='utf-8') as f:
            return f.read()
    return "Subjective Abstract Data Source Package for BrainBoost data pipelines"

# Read version from __init__.py
def get_version():
    init_path = os.path.join(os.path.dirname(__file__), 'subjective_abstract_data_source_package', '__init__.py')
    if os.path.exists(init_path):
        with open(init_path, 'r', encoding='utf-8') as f:
            for line in f:
                if line.startswith('__version__'):
                    return line.split('=')[1].strip().strip('"\'')
    return "1.0.0"

setup(
    name="subjective-abstract-data-source-package",
    version=get_version(),
    author="Pablo Tomas Borda",
    author_email="pablo.borda@subjectivetechnologies.com",
    description="Abstract base classes for building data source addons for BrainBoost data pipelines",
    long_description=read_readme(),
    long_description_content_type="text/markdown",
    url="https://github.com/brainboost/subjective-abstract-data-source-package",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Database",
        "Topic :: Scientific/Engineering :: Information Analysis",
    ],
    python_requires=">=3.8",
    install_requires=[
        "brainboost-data-source-logger-package @ git+ssh://git@github.com/PabloBorda/brainboost_data_source_logger_package.git",
        "brainboost-configuration-package @ git+ssh://git@github-personal/PabloBorda/brainboost_configuration_package.git",
        "alive-progress>=3.1.5",
        "pandas>=1.5.0",
    ],
    extras_require={
        "dev": [
            "pytest>=6.0",
            "pytest-cov>=2.0",
            "black>=21.0",
            "flake8>=3.8",
            "mypy>=0.800",
        ],
        "docs": [
            "sphinx>=4.0",
            "sphinx-rtd-theme>=1.0",
        ],
    },
    keywords="data-source, abstract, brainboost, pipeline, addon",
    project_urls={
        "Bug Reports": "https://github.com/brainboost/subjective-abstract-data-source-package/issues",
        "Source": "https://github.com/brainboost/subjective-abstract-data-source-package",
        "Documentation": "https://subjective-abstract-data-source-package.readthedocs.io/",
    },
    include_package_data=True,
    zip_safe=False,
) 
