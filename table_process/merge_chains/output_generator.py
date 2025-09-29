"""
Output Generator module
Handles writing merged results to both local CSV and BigQuery
"""

import os
import json
import pandas as pd
import logging
from typing import Dict, Any
from datetime import datetime
from google.cloud import bigquery
from google.auth import default

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class OutputGenerator:
    def __init__(self, config: Dict):
        self.config = config
        self.output_dir = config.get('output', {}).get('directory', 'output')
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Initialize BigQuery client
        try:
            creds, default_project = default()
            self.project_id = os.getenv('GCP_PROJECT_ID', 'ncc-data-bigquery')
            self.dataset_id = 'chains_dataset'
            self.client = bigquery.Client(project=self.project_id, credentials=creds)
            
            # Ensure merged_chains table exists
            self._ensure_merged_chains_table()
            logger.info(f"Initialized BigQuery output to {self.project_id}.{self.dataset_id}")
        except Exception as e:
            logger.error(f"Failed to initialize BigQuery for output: {e}")
            self.client = None

    def _ensure_merged_chains_table(self):
        """Create merged_chains table if it doesn't exist"""
        table_id = f"{self.project_id}.{self.dataset_id}.merged_chains"
        
        schema = [
            bigquery.SchemaField("chain_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("chapter_id", "INTEGER"),
            bigquery.SchemaField("meta_year", "INTEGER"),
            bigquery.SchemaField("row_index", "INTEGER"),
            bigquery.SchemaField("column_name", "STRING"),
            bigquery.SchemaField("cell_value", "STRING"),
            bigquery.SchemaField("merge_timestamp", "TIMESTAMP"),
            bigquery.SchemaField("merge_status", "STRING"),
        ]
        
        table = bigquery.Table(table_id, schema=schema)
        
        try:
            # Check if table exists
            self.client.get_table(table_id)
            logger.info(f"Table {table_id} already exists")
        except:
            # Create table if it doesn't exist
            table = self.client.create_table(table)
            logger.info(f"Created table {table_id}")
            
            # Pre-populate with pending status for all chains
            self._populate_pending_chains()
    
    def _populate_pending_chains(self):
        """Pre-populate table with pending status for all chains"""
        query = f"""
        INSERT INTO `{self.project_id}.{self.dataset_id}.merged_chains` 
        (chain_id, chapter_id, merge_status, merge_timestamp)
        SELECT DISTINCT 
            chain_id, 
            chapter_id,
            'pending' as merge_status,
            CURRENT_TIMESTAMP() as merge_timestamp
        FROM `{self.project_id}.{self.dataset_id}.chains_metadata`
        WHERE chain_id NOT IN (
            SELECT DISTINCT chain_id 
            FROM `{self.project_id}.{self.dataset_id}.merged_chains`
        )
        """
        try:
            self.client.query(query).result()
            logger.info("Pre-populated pending chains")
        except Exception as e:
            logger.warning(f"Could not pre-populate pending chains: {e}")

    def write_outputs(self, merged_df: pd.DataFrame, chain_id: str, metadata: Dict) -> Dict:
        """Write outputs to both CSV and BigQuery"""
        result = {
            'success': False,
            'csv_path': None,
            'bigquery_rows': 0
        }
        
        try:
            # Write CSV (existing functionality)
            csv_path = os.path.join(self.output_dir, f'merged_{chain_id}.csv')
            merged_df.to_csv(csv_path, index=False, encoding='utf-8-sig')
            result['csv_path'] = csv_path
            logger.info(f"Written CSV to {csv_path}")
            
            # Write metadata
            metadata_path = os.path.join(self.output_dir, f'metadata_{chain_id}.json')
            with open(metadata_path, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, indent=2, ensure_ascii=False)
            
            # Write to BigQuery if client is available
            if self.client:
                rows_written = self._write_to_bigquery(merged_df, chain_id, metadata)
                result['bigquery_rows'] = rows_written
                logger.info(f"Written {rows_written} rows to BigQuery for chain {chain_id}")
            
            result['success'] = True
            
        except Exception as e:
            logger.error(f"Failed to write outputs for {chain_id}: {e}")
            result['error'] = str(e)
        
        return result
    
    def _write_to_bigquery(self, df: pd.DataFrame, chain_id: str, metadata: Dict) -> int:
        """Write merged data to BigQuery in long format"""
        try:
            # Extract chapter_id from chain_id (e.g., chain_1_01_2001 -> 01)
            parts = chain_id.split('_')
            chapter_id = int(parts[2]) if len(parts) >= 3 else None
            
            # First, delete any existing data for this chain
            delete_query = f"""
            DELETE FROM `{self.project_id}.{self.dataset_id}.merged_chains`
            WHERE chain_id = '{chain_id}' AND merge_status != 'pending'
            """
            self.client.query(delete_query).result()
            
            # Convert to long format for BigQuery
            rows_to_insert = []
            
            for row_idx, row in df.iterrows():
                meta_year = row.get('meta_year', None)
                
                for col_name in df.columns:
                    if col_name != 'meta_year':  # Skip the meta_year column itself
                        rows_to_insert.append({
                            'chain_id': chain_id,
                            'chapter_id': chapter_id,
                            'meta_year': int(meta_year) if meta_year else None,
                            'row_index': int(row_idx),
                            'column_name': str(col_name),
                            'cell_value': str(row[col_name]) if pd.notna(row[col_name]) else None,
                            'merge_timestamp': datetime.now().isoformat(),
                            'merge_status': 'completed'
                        })
            
            # Insert in batches
            table_id = f"{self.project_id}.{self.dataset_id}.merged_chains"
            
            if rows_to_insert:
                for i in range(0, len(rows_to_insert), 500):
                    batch = rows_to_insert[i:i+500]
                    errors = self.client.insert_rows_json(table_id, batch)
                    if errors:
                        logger.error(f"BigQuery insert errors: {errors}")
                
                # Update the pending status to completed
                update_query = f"""
                UPDATE `{self.project_id}.{self.dataset_id}.merged_chains`
                SET merge_status = 'completed', merge_timestamp = CURRENT_TIMESTAMP()
                WHERE chain_id = '{chain_id}' AND merge_status = 'pending'
                """
                self.client.query(update_query).result()
            
            return len(rows_to_insert)
            
        except Exception as e:
            logger.error(f"Failed to write to BigQuery: {e}")
            # Mark as failed in BigQuery
            try:
                fail_query = f"""
                UPDATE `{self.project_id}.{self.dataset_id}.merged_chains`
                SET merge_status = 'failed', merge_timestamp = CURRENT_TIMESTAMP()
                WHERE chain_id = '{chain_id}' AND merge_status = 'pending'
                """
                self.client.query(fail_query).result()
            except:
                pass
            return 0

    def write_report(self, report_content: str, chapter: int):
        """Write validation report"""
        report_path = os.path.join(self.output_dir, f'report_chapter_{chapter}.txt')
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(report_content)
        logger.info(f"Written report to {report_path}")
