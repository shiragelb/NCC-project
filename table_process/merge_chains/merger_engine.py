"""
Merger Engine module
Core merging logic for stacking tables with meta_year tracking
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional
import uuid
import logging
from config import ColumnSchema

logger = logging.getLogger(__name__)

class MergerEngine:
    def __init__(self, config: Dict):
        self.config = config

    def merge_chain(self, normalized_tables: List[Dict]) -> pd.DataFrame:
        """Main merge function - stacks tables with meta_year tracking"""
        if not normalized_tables:
            return pd.DataFrame()

        # Initialize schema from first non-empty table
        schema = None
        for table_info in normalized_tables:
            if not table_info['table'].empty:
                schema = self.initialize_schema(table_info['table'], table_info['year'])
                break

        if schema is None:
            return pd.DataFrame()

        # Process remaining tables
        for table_info in normalized_tables[1:]:
            current_table = table_info['table']
            current_year = table_info['year']

            if current_table.empty:
                self.add_empty_year_data(schema, current_year)
            else:
                from column_matcher import ColumnMatcher
                matcher = ColumnMatcher(self.config)
                matches = matcher.match_columns_to_schema(schema, current_table, current_year)
                schema = self.update_schema_for_stacking(schema, current_table, matches, current_year)

            # Detect and fix unit changes if configured
            if self.config['processing']['handle_unit_changes']:
                self.detect_and_fix_unit_changes(schema, current_table, current_year)

        # Build final stacked table
        final_table = self.build_stacked_table(schema)
        return final_table

    def initialize_schema(self, first_table: pd.DataFrame, first_year: int) -> Dict:
        """Initialize schema from first table"""
        schema = {
            'columns': [],
            'data_by_year': {first_year: []},
            'years': [first_year],
            'metadata': {}
        }

        for col_name in first_table.columns:
            if col_name != 'meta_year':
                column = {
                    'id': str(uuid.uuid4()),
                    'current_name': col_name,
                    'name_history': [{'year': first_year, 'name': col_name}],
                    'years_present': [first_year],
                    'data_type': self.infer_data_type(first_table[col_name])
                }
                schema['columns'].append(column)

        # Store data for first year
        for idx, row in first_table.iterrows():
            row_data = {'meta_year': first_year}
            for col in schema['columns']:
                col_name = col['current_name']
                if col_name in row:
                    row_data[col['id']] = row[col_name]
            schema['data_by_year'][first_year].append(row_data)

        return schema

    def add_empty_year_data(self, schema: Dict, year: int):
        """Add year with empty data"""
        schema['years'].append(year)
        schema['data_by_year'][year] = []

        # Create single row with all NaN except meta_year
        empty_row = {'meta_year': year}
        for col in schema['columns']:
            empty_row[col['id']] = None
        schema['data_by_year'][year].append(empty_row)

    def update_schema_for_stacking(self, schema: Dict, new_table: pd.DataFrame,
                                   matches: List[Dict], year: int) -> Dict:
        """Update schema with new table data"""
        matched_new_cols = set()

        # Update existing columns with matches
        for match in matches:
            existing_col = schema['columns'][match['existing_idx']]
            new_col_name = new_table.columns[match['new_idx']]

            # Update column history
            existing_col['name_history'].append({'year': year, 'name': new_col_name})
            existing_col['years_present'].append(year)

            matched_new_cols.add(match['new_idx'])

        # Add unmatched columns as new
        for col_idx, col_name in enumerate(new_table.columns):
            if col_idx not in matched_new_cols and col_name != 'meta_year':
                new_column = {
                    'id': str(uuid.uuid4()),
                    'current_name': col_name,
                    'name_history': [{'year': year, 'name': col_name}],
                    'years_present': [year],
                    'data_type': self.infer_data_type(new_table[col_name])
                }
                schema['columns'].append(new_column)

        # Store data for this year
        schema['data_by_year'][year] = []
        for idx, row in new_table.iterrows():
            row_data = {'meta_year': year}

            # Add matched columns
            for match in matches:
                col_id = schema['columns'][match['existing_idx']]['id']
                new_col_name = new_table.columns[match['new_idx']]
                if new_col_name in row:
                    row_data[col_id] = row[new_col_name]

            # Add new columns
            for col in schema['columns']:
                if col['id'] not in row_data:
                    if year in col['years_present']:
                        # Find this column in the current table
                        col_name = self.get_column_name_for_year(col, year)
                        if col_name and col_name in row:
                            row_data[col['id']] = row[col_name]
                        else:
                            row_data[col['id']] = None
                    else:
                        row_data[col['id']] = None

            schema['data_by_year'][year].append(row_data)

        if year not in schema['years']:
            schema['years'].append(year)

        return schema

    def build_stacked_table(self, schema: Dict) -> pd.DataFrame:
        """Build final stacked table from schema"""
        # Create column mapping from IDs to final names
        column_mapping = {}
        for col in schema['columns']:
            # Use most recent name as final column name
            column_mapping[col['id']] = col['name_history'][-1]['name']

        # Stack all years' data
        all_rows = []
        for year in sorted(schema['years']):
            for row_data in schema['data_by_year'].get(year, []):
                final_row = {'meta_year': year}
                for col_id, value in row_data.items():
                    if col_id != 'meta_year' and col_id in column_mapping:
                        col_name = column_mapping[col_id]
                        final_row[col_name] = value
                all_rows.append(final_row)

        # Create final DataFrame
        if all_rows:
            final_table = pd.DataFrame(all_rows)

            # Ensure meta_year is first column
            cols = ['meta_year'] + [col for col in final_table.columns if col != 'meta_year']
            final_table = final_table[cols]
        else:
            final_table = pd.DataFrame()

        return final_table

    def get_column_name_for_year(self, column: Dict, year: int) -> Optional[str]:
        """Get column name for specific year from history"""
        for entry in column['name_history']:
            if entry['year'] == year:
                return entry['name']
        return column.get('current_name')

    def detect_and_fix_unit_changes(self, schema: Dict, current_table: pd.DataFrame, current_year: int):
        """Detect and fix unit changes in numeric columns"""
        # Simplified implementation - would need historical data tracking
        logger.info(f"Checking for unit changes in year {current_year}")

    def infer_data_type(self, series: pd.Series) -> str:
        """Infer data type of a pandas series"""
        try:
            pd.to_numeric(series.dropna())
            return 'numeric'
        except:
            try:
                pd.to_datetime(series.dropna())
                return 'date'
            except:
                return 'text'