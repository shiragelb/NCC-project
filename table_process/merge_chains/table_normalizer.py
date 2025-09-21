"""
Table Normalizer module
Handles different table structures (STANDARD, TABLE_GOES_DOWN, HAMSHECH, DISTORTED)
"""

import pandas as pd
import numpy as np
from typing import List, Dict, Optional, Tuple
import logging

logger = logging.getLogger(__name__)

class TableNormalizer:
    """Table Normalizer class"""
    def __init__(self, config: Dict = None):
        self.config = config or {}
        self.structure_handlers = {
            "STANDARD": self.normalize_standard,
            "TABLE_GOES_DOWN": self.normalize_table_goes_down,
            "HAMSHECH": self.normalize_hamshech,
            "DISTORTED": self.handle_distortion
        }

    def normalize_table_structure(self, table: pd.DataFrame, mask: pd.DataFrame, year: int = None) -> pd.DataFrame:
        """Main normalization function with year context"""
        self.current_year = year  # Store year for use in other methods

        if table.empty:
            return table

        structure_type = self.detect_table_structure(table, mask)
        logger.info(f"Detected structure type: {structure_type}")

        handler = self.structure_handlers.get(structure_type)
        if handler:
            return handler(table, mask)
        else:
            raise ValueError(f"Unknown structure type: {structure_type}")



    def detect_table_structure(self, table: pd.DataFrame, mask: pd.DataFrame) -> str:
        """Detect the structure type of the table"""
        if self.has_column_change_midtable(table):
            return "DISTORTED"

        if self.has_hamshech_markers(table):
            return "HAMSHECH"

        feature_rows = self.get_feature_rows(mask)
        if self.has_multiple_feature_batches(feature_rows):
            return "TABLE_GOES_DOWN"

        return "STANDARD"

    def has_column_change_midtable(self, table: pd.DataFrame) -> bool:
        """Check if column count changes mid-table"""
        col_counts = table.apply(lambda row: row.notna().sum(), axis=1)
        return col_counts.nunique() > 1

    def has_hamshech_markers(self, table: pd.DataFrame) -> bool:
        """Check for המשך markers in the table"""
        return table.astype(str).apply(lambda x: x.str.contains('המשך', na=False)).any().any()

    def get_feature_rows(self, mask: pd.DataFrame) -> List[int]:
        """Get indices of feature rows from mask"""
        feature_indices = []
        for idx in range(len(mask)):
            if 'feature' in mask.iloc[idx].values:
                feature_indices.append(idx)
        return feature_indices

    def has_multiple_feature_batches(self, feature_rows: List[int]) -> bool:
        """Check if there are multiple feature batches"""
        if len(feature_rows) < 2:
            return False

        # Check for gaps in feature rows indicating multiple batches
        gaps = []
        for i in range(1, len(feature_rows)):
            if feature_rows[i] - feature_rows[i-1] > 1:
                gaps.append(i)

        return len(gaps) > 0


    def identify_feature_batches(self, table: pd.DataFrame, mask: pd.DataFrame) -> List:
        """Group consecutive rows labeled 'feature' into batches, with first row override"""
        batches = []
        current = []
        limit = min(len(table), len(mask)) if not mask.empty else len(table)

        # Get year from self.current_year (set in normalize_table_structure)
        year = getattr(self, 'current_year', None)

        # Determine first unignored row based on year
        if year and year < 2017:
            first_unignored_row = 2  # Skip first 2 rows for years 2001-2016
        else:
            first_unignored_row = 0  # Row 0 for year >= 2017 or unknown year

        for idx in range(limit):
            is_feature = False

            # First row override logic
            if idx == first_unignored_row:
                is_feature = True
                logger.info(f"Row {idx} treated as feature (first unignored row for year {year})")
            elif not mask.empty and idx < len(mask):
                # Normal mask-based detection
                is_feature = 'feature' in mask.iloc[idx].values

            if is_feature:
                if idx < len(table):
                    current.append(table.iloc[idx])
            else:
                if current:
                    batches.append(current)
                    current = []

        if current:
            batches.append(current)

        return batches


    def extract_rows_by_mask_type(self, table: pd.DataFrame, mask: pd.DataFrame, mask_type: str) -> pd.DataFrame:
        """Extract rows where mask matches specific type"""
        rows = []
        for idx in range(min(len(table), len(mask))):
            vals = set(str(v) for v in mask.iloc[idx].values)
            if mask_type == 'data':
                cond = ('data-point' in vals) or ('undecided' in vals)
            else:
                cond = (mask_type in vals)
            if cond:
                rows.append(table.iloc[idx])
        return pd.DataFrame(rows) if rows else pd.DataFrame()


    def join_with_delimiter(self, items: List, delimiter: str = " | ") -> str:
        """Join list of items with delimiter, removing empty strings"""
        cleaned = [str(item).strip() for item in items if pd.notna(item) and str(item).strip()]
        return delimiter.join(cleaned)

    def _dedupe_headers(self, headers: List[str]) -> List[str]:
       seen = {}
       out = []
       for h in headers:
           key = str(h) if h is not None else "Col"
           if key not in seen:
               seen[key] = 1
               out.append(key)
           else:
               seen[key] += 1
               out.append(f"{key}__{seen[key]}")
       return out

    def normalize_standard(self, table: pd.DataFrame, mask: pd.DataFrame) -> pd.DataFrame:
        """Normalize standard table structure"""
        feature_rows = self.extract_rows_by_mask_type(table, mask, 'feature')
        data_rows = self.extract_rows_by_mask_type(table, mask, 'data')  # includes 'data-point' + 'undecided'


        if feature_rows.empty or data_rows.empty:
            logger.warning("No feature or data rows found")
            return table

        # Create headers from feature rows
        headers = []
        for col_idx in range(len(table.columns)):
            column_features = []
            for _, row in feature_rows.iterrows():
                if col_idx < len(row):
                    column_features.append(row.iloc[col_idx])

            merged_header = self.join_with_delimiter(column_features)
            headers.append(merged_header if merged_header else f"Column_{col_idx}")

        headers = self._dedupe_headers(headers)

        # Create new dataframe with normalized headers
        normalized_df = data_rows.copy()
        normalized_df.columns = headers
        normalized_df.reset_index(drop=True, inplace=True)

        return normalized_df

    def normalize_table_goes_down(self, table: pd.DataFrame, mask: pd.DataFrame) -> pd.DataFrame:
        """Handle table-goes-down structure WITHOUT semantic batch comparison"""
        P = len(table.columns)

        # Identify feature batches
        feature_batches = self.identify_feature_batches(table, mask)
        r = len(feature_batches)

        if r == 0:
            return self.normalize_standard(table, mask)

        # Get batch start indices
        batch_starts = []
        in_feat = False
        limit = min(len(table), len(mask)) if not mask.empty else len(table)

        for idx in range(limit):
            is_feat = ('feature' in mask.iloc[idx].values) if not mask.empty and idx < len(mask) else False
            year = getattr(self, 'current_year', None)
            first_unignored_row = 2 if year and year < 2017 else 0
            if idx == first_unignored_row:
                is_feat = True

            if is_feat and not in_feat:
                batch_starts.append(idx)
                in_feat = True
            elif not is_feat and in_feat:
                in_feat = False

        # ALWAYS treat each batch as replacement (no chaining)
        stacks = feature_batches

        # Build headers for each stack
        all_headers = []
        for stack in stacks:
            batch_headers = []
            for col in range(P):
                parts = []
                for row in stack:
                    if col < len(row):
                        parts.append(row.iloc[col])
                header = self.join_with_delimiter(parts)
                batch_headers.append(header if header else f"Col{col}")
            all_headers.extend(batch_headers)

        # Build data segments (same as before)
        segments = []
        for i in range(r):
            start = batch_starts[i] + len(feature_batches[i])
            end = batch_starts[i + 1] if i + 1 < r else limit
            seg_idx = []
            for idx in range(start, end):
                if not mask.empty and idx < len(mask):
                    if ('data-point' in mask.iloc[idx].values) or ('undecided' in mask.iloc[idx].values):
                        seg_idx.append(idx)
                elif mask.empty:
                    seg_idx.append(idx)
            seg_df = table.iloc[seg_idx].reset_index(drop=True) if seg_idx else pd.DataFrame(columns=table.columns)
            segments.append(seg_df)

        # Align and concatenate
        max_rows = max((len(s) for s in segments), default=0)
        segments = [s.reindex(range(max_rows)).reset_index(drop=True) for s in segments]

        if segments:
            expanded_data = pd.concat(segments, axis=1)
            all_headers = self._dedupe_headers(all_headers)
            expanded_data.columns = all_headers
            return expanded_data
        else:
            return pd.DataFrame()



    def normalize_hamshech(self, table: pd.DataFrame, mask: pd.DataFrame) -> pd.DataFrame:
        """Handle המשך (continuation) markers"""
        # Find rows containing המשך
        hamshech_mask = table.astype(str).apply(lambda x: x.str.contains('המשך', na=False)).any(axis=1)

        # Remove המשך rows from table
        cleaned_table = table[~hamshech_mask].reset_index(drop=True)

        # Remove corresponding rows from mask to keep alignment
        if not mask.empty and len(mask) == len(table):
            cleaned_mask = mask[~hamshech_mask].reset_index(drop=True)
        else:
            cleaned_mask = mask

        # Recursively call normalize_table_structure with cleaned data
        # Pass along the year context if it exists
        year = getattr(self, 'current_year', None)
        return self.normalize_table_structure(cleaned_table, cleaned_mask, year)

    def handle_distortion(self, table: pd.DataFrame, mask: pd.DataFrame) -> pd.DataFrame:
        """Handle distorted tables"""
        logger.warning("Distorted table detected - applying best effort normalization")
        # Try to normalize what we can
        try:
            return self.normalize_standard(table, mask)
        except Exception as e:
            logger.error(f"Failed to normalize distorted table: {e}")
            return table