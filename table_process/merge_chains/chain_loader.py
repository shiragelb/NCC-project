"""
Chain Loader module
Handles loading chain configurations, tables, and masks
"""

import os
import json
import pandas as pd
import re
from typing import Dict, List, Optional, Any
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ChainLoader:
    def __init__(self, base_path: str = "."):
        self.base_path = base_path

    def load_chain_config(self, chapter: int) -> Dict:
        """Load chain configuration for a specific chapter"""
        config_path = f"chains_chapter_{chapter}.json"

        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Chain config not found: {config_path}")

        with open(config_path, 'r', encoding='utf-8') as f:
            chains = json.load(f)

        logger.info(f"Loaded {len(chains)} chains for chapter {chapter}")
        return chains

    def load_table(self, table_id: str, year: int, chapter: int) -> Optional[pd.DataFrame]:
        """Load a specific table CSV file"""
        paths_to_try = [
            f"tables/{year}/{chapter:02d}/{table_id}.csv",
            f"tables/{year}/{chapter}/{table_id}.csv",
        ]

        for path in paths_to_try:
            if os.path.exists(path):
                try:
                    # Skip first 2 rows for years 2001-2016
                    if 2001 <= year <= 2016:
                        df = pd.read_csv(path, encoding='utf-8-sig', skiprows=2, header=None)
                        logger.info(f"Loaded table from {path} (skipped 2 rows for year {year})")
                    else:
                        df = pd.read_csv(path, encoding='utf-8-sig')
                        logger.info(f"Loaded table from {path}")
                    return df
                except Exception as e:
                    logger.error(f"Error loading {path}: {e}")

        logger.warning(f"Table not found: {table_id} for year {year}")
        return None

    def create_empty_placeholder(self) -> pd.DataFrame:
        """Create an empty DataFrame placeholder"""
        return pd.DataFrame()

    def load_mask(self, mask_path: str) -> pd.DataFrame:
        """Load mask CSV file"""
        # Remove '../' prefix if present (from JSON files)
        if mask_path.startswith('../'):
            mask_path = mask_path[3:]

        if not os.path.exists(mask_path):
            logger.warning(f"Mask file not found: {mask_path}")
            return pd.DataFrame()

        # Extract year from path to determine if we need to skip rows
        year_match = re.search(r'/(\d{4})/', mask_path)
        if year_match:
            year = int(year_match.group(1))
            if 2001 <= year <= 2016:
                mask_data = pd.read_csv(mask_path, encoding='utf-8-sig', skiprows=2, header=None)
                logger.info(f"Loaded mask (skipped 2 rows for year {year})")
            else:
                mask_data = pd.read_csv(mask_path, encoding='utf-8-sig')
        else:
            mask_data = pd.read_csv(mask_path, encoding='utf-8-sig')

        # Validate mask contains only allowed values
        valid_values = {'feature', 'data-point', 'undecided'}
        mask_values = set(mask_data.values.flatten())

        if not mask_values.issubset(valid_values):
            invalid = mask_values - valid_values
            logger.warning(f"Invalid mask values found: {invalid}")

        return mask_data
