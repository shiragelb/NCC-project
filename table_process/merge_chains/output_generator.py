"""
Output Generator module
Handles writing CSV outputs, metadata, and validation reports
"""

import os
import json
import pandas as pd
from datetime import datetime
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)

class OutputGenerator:
    def __init__(self, config: Dict):
        self.config = config

    def write_outputs(self, merged_result: pd.DataFrame, chain_id: str, metadata: Dict = None):
        """Write all output files"""
        # Create output directory
        output_dir = 'output'
        os.makedirs(output_dir, exist_ok=True)

        # Write main CSV
        csv_path = os.path.join(output_dir, f"merged_{chain_id}.csv")
        merged_result.to_csv(csv_path, index=False, encoding=self.config['output']['encoding'])
        logger.info(f"Wrote merged CSV to {csv_path}")

        # Generate and write metadata if configured
        if self.config['output']['include_metadata']:
            if metadata is None:
                metadata = self.generate_metadata(merged_result, chain_id)
            json_path = os.path.join(output_dir, f"metadata_{chain_id}.json")
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, indent=2, ensure_ascii=False)
            logger.info(f"Wrote metadata to {json_path}")

        # Generate validation report if configured
        if self.config['output']['include_validation_report']:
            report = self.generate_validation_report(merged_result, metadata)
            report_path = os.path.join(output_dir, f"report_{chain_id}.txt")
            with open(report_path, 'w', encoding='utf-8') as f:
                f.write(report)
            logger.info(f"Wrote validation report to {report_path}")

        return {
            'csv_path': csv_path,
            'success': True
        }

    def generate_metadata(self, merged_result: pd.DataFrame, chain_id: str) -> Dict:
        """Generate metadata for the merged table"""
        metadata = {
            'chain_id': chain_id,
            'processing_timestamp': datetime.now().isoformat(),
            'years_processed': sorted(merged_result['meta_year'].unique().tolist()) if 'meta_year' in merged_result.columns else [],
            'table_count': len(merged_result['meta_year'].unique()) if 'meta_year' in merged_result.columns else 0,
            'columns': {},
            'processing_stats': {
                'total_columns': len(merged_result.columns) - 1 if 'meta_year' in merged_result.columns else len(merged_result.columns),
                'total_rows': len(merged_result)
            }
        }

        # Add column statistics
        for col in merged_result.columns:
            if col != 'meta_year':
                metadata['columns'][col] = {
                    'data_type': str(merged_result[col].dtype),
                    'null_count': int(merged_result[col].isna().sum()),
                    'unique_values': int(merged_result[col].nunique()),
                    'sample_values': merged_result[col].dropna().head(3).tolist()
                }

        return metadata

    def generate_validation_report(self, merged_result: pd.DataFrame, metadata: Dict = None) -> str:
        """Generate validation report"""
        report = []
        report.append("TABLE CHAIN MERGER VALIDATION REPORT")
        report.append("=" * 50)
        report.append(f"Processing Date: {datetime.now().isoformat()}")
        report.append("")

        # Summary statistics
        report.append("SUMMARY STATISTICS")
        report.append("-" * 30)

        if 'meta_year' in merged_result.columns:
            report.append(f"Years processed: {merged_result['meta_year'].nunique()}")
            report.append(f"Total columns: {len(merged_result.columns) - 1}")
        else:
            report.append(f"Total columns: {len(merged_result.columns)}")

        report.append(f"Total rows: {len(merged_result)}")
        report.append(f"Data completeness: {self.calculate_completeness(merged_result):.1f}%")
        report.append("")

        # Year distribution
        if 'meta_year' in merged_result.columns:
            report.append("ROWS PER YEAR")
            report.append("-" * 30)
            year_counts = merged_result['meta_year'].value_counts().sort_index()
            for year, count in year_counts.items():
                report.append(f"Year {year}: {count} rows")
            report.append("")

        # Column completeness
        report.append("COLUMN COMPLETENESS")
        report.append("-" * 30)
        for col in merged_result.columns:
            if col != 'meta_year':
                completeness = (1 - merged_result[col].isna().sum() / len(merged_result)) * 100
                report.append(f"{col}: {completeness:.1f}%")

        return "\n".join(report)

    def calculate_completeness(self, table: pd.DataFrame) -> float:
        """Calculate overall data completeness percentage"""
        exclude_cols = ['meta_year']
        data_cols = [col for col in table.columns if col not in exclude_cols]

        if not data_cols:
            return 0.0

        total_cells = len(table) * len(data_cols)
        if total_cells == 0:
            return 0.0

        non_null_cells = sum(table[col].notna().sum() for col in data_cols)
        return (non_null_cells / total_cells) * 100