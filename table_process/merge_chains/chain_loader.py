"""
Chain Loader module
Handles loading chain configurations from JSON files and tables/masks from BigQuery
No fallback to local CSV files - BigQuery only for data
"""

import os
import json
import pandas as pd
import numpy as np
import re
from typing import Dict, List, Optional, Any
import logging
from google.cloud import bigquery
from google.auth import default

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ChainLoader:
    def __init__(self, base_path: str = ".", project_id: str = None, dataset_id: str = "chains_dataset"):
        """
        Initialize loader - always uses BigQuery for table/mask data
        Chain configs are read from local JSON files in chain_configs/ folder
        """
        self.base_path = base_path
        
        try:
            # Get credentials and project
            creds, default_project = default()
            self.project_id = project_id or default_project or os.getenv('GCP_PROJECT_ID', 'ncc-data-bigquery')
            self.dataset_id = dataset_id
            
            # Initialize BigQuery client
            self.client = bigquery.Client(project=self.project_id, credentials=creds)
            self.dataset_ref = f"{self.project_id}.{self.dataset_id}"
            
            logger.info(f"Initialized BigQuery loader for {self.dataset_ref}")
        except Exception as e:
            logger.error(f"Failed to initialize BigQuery: {e}")
            raise RuntimeError(f"BigQuery initialization failed. Ensure GCP credentials are configured: {e}")

    def load_chain_config(self, chapter: int) -> Dict:
        """Load chain configuration from JSON files in chain_configs folder"""
        config_path = f"chain_configs/chains_chapter_{chapter}.json"
        
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Chain config not found: {config_path}")
        
        with open(config_path, 'r', encoding='utf-8') as f:
            chains = json.load(f)
        
        logger.info(f"Loaded {len(chains)} chains from {config_path}")
        return chains

    def load_table(self, table_id: str, year: int, chapter: int) -> Optional[pd.DataFrame]:
        """Load table from BigQuery - no fallback to local files"""
        try:
            query = f"""
            SELECT 
                row_index,
                col_index,
                cell_value
            FROM `{self.dataset_ref}.tables_data`
            WHERE table_id = '{table_id}'
            ORDER BY row_index, col_index
            """
            
            df = self.client.query(query).to_dataframe()
            
            if df.empty:
                logger.warning(f"No data found in BigQuery for table {table_id}")
                return None
            
            # Pivot the long format back to wide format
            pivoted = df.pivot(index='row_index', columns='col_index', values='cell_value')
            pivoted = pivoted.reset_index(drop=True)
            pivoted = pivoted.reindex(sorted(pivoted.columns, key=lambda x: int(x)), axis=1)
            pivoted.columns = range(len(pivoted.columns))
            
            logger.info(f"Loaded table {table_id} from BigQuery: {pivoted.shape}")
            
            # Apply same skiprows logic for years 2001-2016
            if 2001 <= year <= 2016 and len(pivoted) > 2:
                pivoted = pivoted.iloc[2:].reset_index(drop=True)
                logger.info(f"Skipped first 2 rows for year {year}")
            
            return pivoted
            
        except Exception as e:
            logger.error(f"Error loading table from BigQuery: {e}")
            return None

    def create_empty_placeholder(self) -> pd.DataFrame:
        """Create an empty DataFrame placeholder"""
        return pd.DataFrame()

    def load_mask(self, mask_path: str) -> pd.DataFrame:
        """Load mask from BigQuery - no fallback to local files"""
        # Extract table_id from mask_path
        match = re.search(r'(\d+_\d+_\d+)\.csv', mask_path)
        if not match:
            logger.warning(f"Could not extract table_id from mask path: {mask_path}")
            return pd.DataFrame()
        
        table_id = match.group(1)
        
        try:
            query = f"""
            SELECT 
                row_index,
                col_index,
                CASE 
                    WHEN is_feature = true THEN 'feature'
                    ELSE 'data-point'
                END as mask_value
            FROM `{self.dataset_ref}.masks_data`
            WHERE table_id = '{table_id}'
            ORDER BY row_index, col_index
            """
            
            df = self.client.query(query).to_dataframe()
            
            if df.empty:
                logger.warning(f"No mask found in BigQuery for table {table_id}")
                return pd.DataFrame()
            
            # Pivot back to original format
            pivoted = df.pivot(index='row_index', columns='col_index', values='mask_value')
            pivoted = pivoted.reset_index(drop=True)
            pivoted = pivoted.reindex(sorted(pivoted.columns, key=lambda x: int(x)), axis=1)
            pivoted.columns = range(len(pivoted.columns))
            
            logger.info(f"Loaded mask from BigQuery for {table_id}")
            
            # Apply skiprows logic for years 2001-2016 if needed
            year_match = re.search(r'_(\d{4})$', table_id)
            if year_match:
                year = int(year_match.group(1))
                if 2001 <= year <= 2016 and len(pivoted) > 2:
                    pivoted = pivoted.iloc[2:].reset_index(drop=True)
                    logger.info(f"Skipped first 2 rows in mask for year {year}")
            
            return pivoted
            
        except Exception as e:
            logger.error(f"Error loading mask from BigQuery: {e}")
            return pd.DataFrame()
