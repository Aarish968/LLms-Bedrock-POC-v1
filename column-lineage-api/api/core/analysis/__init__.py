"""
Column Lineage Analysis Module

This module contains the core analysis functionality for processing
database views and generating column lineage information.
"""

from .main import process_all_views, save_results_to_csv, get_analysis_summary
from .integrated_parser import CompleteIntegratedParser
from .config import (
    SQL_KEYWORDS,
    DERIVED_EXPRESSION_TYPES,
    AGGREGATION_PATTERNS,
    DEFAULT_DIALECT,
    CSV_HEADERS
)

__all__ = [
    'process_all_views',
    'save_results_to_csv',
    'get_analysis_summary',
    'CompleteIntegratedParser',
    'SQL_KEYWORDS',
    'DERIVED_EXPRESSION_TYPES',
    'AGGREGATION_PATTERNS',
    'DEFAULT_DIALECT',
    'CSV_HEADERS'
]