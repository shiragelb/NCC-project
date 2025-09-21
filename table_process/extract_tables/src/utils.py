"""
Utility functions for configuration and common operations.
"""

import os
import yaml
import logging
import json
from pathlib import Path


def load_config(config_path="extract_tables/config.yaml"):
    """Load configuration from YAML file."""
    if not os.path.exists(config_path):
        return get_default_config()
    
    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    return config


def get_default_config():
    """Get default configuration."""
    return {
        'reports_dir': 'extract_tables/temp/reports',
        'tables_dir': 'chain/table-chain-matching/tables',
        'encoding': 'utf-8-sig',
        'table_marker': 'לוח',
        'exclude_marker': 'תרשים',
    }


def setup_logging(verbose=False):
    """Setup logging configuration."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )


def validate_year_range(years):
    """Categorize years by extraction method."""
    categorized = {
        '2001_2016': [],
        '2019_2024': [],
        'special': [],
        'invalid': []
    }
    
    for year in years:
        if 2001 <= year <= 2016:
            categorized['2001_2016'].append(year)
        elif year in [2019, 2021, 2022, 2023, 2024]:
            categorized['2019_2024'].append(year)
        elif year in [2017, 2018, 2020]:
            categorized['special'].append(year)
        else:
            categorized['invalid'].append(year)
    
    return categorized
