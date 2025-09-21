"""
Main Pipeline orchestrator
Coordinates the entire table chain merging process
"""

import os
import json
import pandas as pd
import logging
from typing import Dict, List, Optional
from datetime import datetime

from config import load_config
from chain_loader import ChainLoader
from table_normalizer import TableNormalizer
from merger_engine import MergerEngine
from output_generator import OutputGenerator

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class TableChainMerger:
    def __init__(self, config_path: Optional[str] = None):
        self.config = load_config(config_path)
        self.loader = ChainLoader()
        self.normalizer = TableNormalizer(self.config)  # Pass config to normalizer
        self.merger = MergerEngine(self.config)
        self.output_generator = OutputGenerator(self.config)

    def process_single_chain(self, chain_id: str, chain_config: Dict) -> Dict:
        """Process a single chain"""
        logger.info(f"Processing chain: {chain_id}")

        normalized_tables = []

        # Extract chapter from first table ID
        first_table = chain_config['tables'][0]
        chapter = int(first_table.split('_')[1])

        # Process each year
        for i, year in enumerate(chain_config['years']):
            table_id = chain_config['tables'][i]

            logger.info(f"Processing table {table_id} for year {year}")

            # Load table
            raw_table = self.loader.load_table(table_id, year, chapter)
            if raw_table is None:
                raw_table = self.loader.create_empty_placeholder()

            # Load mask
            if i < len(chain_config.get('mask_references', [])):
                mask_path = chain_config['mask_references'][i]
                mask = self.loader.load_mask(mask_path)
            else:
                mask = pd.DataFrame()

            # Normalize table structure with year context
            try:
                # Pass year to normalizer for first row override logic
                normalized = self.normalizer.normalize_table_structure(
                    raw_table,
                    mask,
                    year=year  # Pass year for first row override
                )
            except Exception as e:
                logger.error(f"Failed to normalize table {table_id}: {e}")
                normalized = raw_table

            # Add to list
            normalized_tables.append({
                'table': normalized,
                'year': year,
                'table_id': table_id
            })

        # Merge all tables
        merged_result = self.merger.merge_chain(normalized_tables)

        # Generate metadata
        metadata = {
            'chain_id': chain_id,
            'tables_processed': chain_config['tables'],
            'years': chain_config['years'],
            'config_used': {
                'semantic_matching': self.config.get('matching', {}).get('use_semantic_matching', False),
                'semantic_alpha': self.config.get('matching', {}).get('semantic_similarity_alpha', 0.7),
                'auto_accept_threshold': self.config.get('matching', {}).get('auto_accept_threshold', 0.85)
            }
        }

        # Write outputs
        output_result = self.output_generator.write_outputs(merged_result, chain_id, metadata)

        return {
            'chain_id': chain_id,
            'success': output_result['success'],
            'output_path': output_result['csv_path'],
            'table_count': len(chain_config['tables']),
            'column_count': len(merged_result.columns),
            'rows_count': len(merged_result)
        }

    def process_chapter(self, chapter: int, chain_ids: Optional[List[str]] = None) -> List[Dict]:
        """Process all chains for a chapter"""
        # Load chain configurations
        chains = self.loader.load_chain_config(chapter)

        results = []

        # Filter chains if specific IDs provided
        if chain_ids:
            chains = {k: v for k, v in chains.items() if k in chain_ids}

        # Log configuration being used
        logger.info(f"Processing chapter {chapter} with config:")
        logger.info(f"- Semantic matching: {self.config.get('matching', {}).get('use_semantic_matching', False)}")
        logger.info(f"- Semantic alpha: {self.config.get('matching', {}).get('semantic_similarity_alpha', 0.7)}")

        # Process each chain
        total_chains = len(chains)
        for idx, (chain_id, chain_config) in enumerate(chains.items(), 1):
            logger.info(f"Processing chain {idx}/{total_chains}: {chain_id}")
            try:
                result = self.process_single_chain(chain_id, chain_config)
                results.append(result)
            except Exception as e:
                logger.error(f"Failed to process chain {chain_id}: {e}")
                results.append({
                    'chain_id': chain_id,
                    'success': False,
                    'error': str(e)
                })

        # Generate summary
        self.generate_summary(results, chapter)

        return results

    def generate_summary(self, results: List[Dict], chapter: int):
        """Generate summary report for all processed chains"""
        summary = {
            'chapter': chapter,
            'timestamp': datetime.now().isoformat(),
            'total_chains': len(results),
            'successful': len([r for r in results if r.get('success')]),
            'failed': len([r for r in results if not r.get('success')]),
            'configuration': {
                'semantic_matching_enabled': self.config.get('matching', {}).get('use_semantic_matching', False),
                'semantic_similarity_alpha': self.config.get('matching', {}).get('semantic_similarity_alpha', 0.7),
                'auto_accept_threshold': self.config.get('matching', {}).get('auto_accept_threshold', 0.85),
                'manual_review_threshold': self.config.get('matching', {}).get('manual_review_threshold', 0.5)
            },
            'results': results
        }

        # Calculate statistics
        successful_results = [r for r in results if r.get('success')]
        if successful_results:
            summary['statistics'] = {
                'avg_columns': sum(r.get('column_count', 0) for r in successful_results) / len(successful_results),
                'avg_rows': sum(r.get('rows_count', 0) for r in successful_results) / len(successful_results),
                'total_tables_processed': sum(r.get('table_count', 0) for r in successful_results)
            }

        # Write summary
        os.makedirs('output', exist_ok=True)
        summary_path = f'output/summary_chapter_{chapter}.json'
        with open(summary_path, 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)

        logger.info(f"Summary written to {summary_path}")
        logger.info(f"Processed {summary['successful']}/{summary['total_chains']} chains successfully")

        # Print detailed failure info if any
        if summary['failed'] > 0:
            logger.warning("Failed chains:")
            for result in results:
                if not result.get('success'):
                    logger.warning(f"  - {result['chain_id']}: {result.get('error', 'Unknown error')}")

# Utility function to test specific chains
def test_single_chain(chapter: int, chain_id: str, config_path: Optional[str] = None):
    """Test processing of a single chain"""
    merger = TableChainMerger(config_path)
    results = merger.process_chapter(chapter, [chain_id])
    return results[0] if results else None

# Utility function to validate outputs
def validate_outputs(output_dir: str = 'output'):
    """Validate all generated outputs"""
    validation_results = []

    for filename in os.listdir(output_dir):
        if filename.startswith('merged_') and filename.endswith('.csv'):
            filepath = os.path.join(output_dir, filename)
            try:
                df = pd.read_csv(filepath, encoding='utf-8-sig')
                chain_id = filename.replace('merged_', '').replace('.csv', '')

                validation = {
                    'chain_id': chain_id,
                    'file': filename,
                    'rows': len(df),
                    'columns': len(df.columns),
                    'has_meta_year': 'meta_year' in df.columns,
                    'years': sorted(df['meta_year'].unique().tolist()) if 'meta_year' in df.columns else [],
                    'completeness': calculate_completeness(df),
                    'valid': True
                }

                # Check for issues
                issues = []
                if not validation['has_meta_year']:
                    issues.append("Missing meta_year column")
                if validation['rows'] == 0:
                    issues.append("No data rows")
                if validation['columns'] <= 1:
                    issues.append("No data columns (only meta_year)")

                validation['issues'] = issues
                validation['valid'] = len(issues) == 0

                validation_results.append(validation)

            except Exception as e:
                validation_results.append({
                    'chain_id': chain_id,
                    'file': filename,
                    'valid': False,
                    'error': str(e)
                })

    # Write validation report
    validation_path = os.path.join(output_dir, 'validation_report.json')
    with open(validation_path, 'w', encoding='utf-8') as f:
        json.dump(validation_results, f, indent=2, ensure_ascii=False)

    logger.info(f"Validation report written to {validation_path}")

    # Summary
    valid_count = len([v for v in validation_results if v.get('valid')])
    logger.info(f"Validation complete: {valid_count}/{len(validation_results)} files valid")

    return validation_results

def calculate_completeness(df: pd.DataFrame) -> float:
    """Calculate data completeness percentage"""
    exclude_cols = ['meta_year']
    data_cols = [col for col in df.columns if col not in exclude_cols]

    if not data_cols:
        return 0.0

    total_cells = len(df) * len(data_cols)
    if total_cells == 0:
        return 0.0

    non_null_cells = sum(df[col].notna().sum() for col in data_cols)
    return (non_null_cells / total_cells) * 100

# Main execution
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Process table chains')
    parser.add_argument('--chapter', type=int, required=True, help='Chapter number to process')
    parser.add_argument('--chains', nargs='*', help='Specific chain IDs to process (optional)')
    parser.add_argument('--config', type=str, help='Path to config file')
    parser.add_argument('--validate', action='store_true', help='Validate outputs after processing')

    args = parser.parse_args()

    # Process chains
    merger = TableChainMerger(args.config)
    results = merger.process_chapter(args.chapter, args.chains)

    # Validate if requested
    if args.validate:
        validate_outputs()