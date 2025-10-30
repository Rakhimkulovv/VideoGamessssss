# [file name]: etl/__init__.py
"""
Пакет ETL для обработки данных видеоигр
"""

__version__ = "1.0.0"
__author__ = "Almas Rakhimkulov"

from .extract import extract_data
from .transform import transform_data
from .load import load_data
from .validate import comprehensive_validation
from .main import run_etl_pipeline

__all__ = [
    'extract_data',
    'transform_data', 
    'load_data',
    'comprehensive_validation',
    'run_etl_pipeline'
]