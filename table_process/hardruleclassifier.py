# -*- coding: utf-8 -*-

class HardRuleClassifier:
    """Applies hard-coded classification rules to table cells."""

    def __init__(self, threshold: float = 0.8, consistency_threshold: float = 0.3):
        """
        Initialize the classifier with configurable threshold.

        Args:
            threshold: Minimum ratio for row identity rule (default 0.9)
        """
        import pandas as pd
        import re

        self.pd = pd
        self.re = re
        self.threshold = threshold
        self.consistency_threshold = consistency_threshold

    def _is_missing_value(self, cell_value) -> bool:
        """
        Check if a cell value represents a missing value.

        Args:
            cell_value: Cell value to check (any type)

        Returns:
            True if cell represents a missing value
        """
        # Handle None values directly
        if cell_value is None or self.pd.isna(cell_value):
            return True

        # Convert to string and strip whitespace for pattern matching
        cell_str = str(cell_value).strip()

        # Empty string or whitespace-only
        if not cell_str:
            return True

        # Convert to lowercase for case-insensitive matching
        cell_lower = cell_str.lower()

        # Common missing value indicators (case-insensitive)
        missing_indicators = {
            "n/a", "na", "null", "nan", "none", "missing", "unknown", ""
        }

        if cell_lower in missing_indicators:
            return True

        return False

    def classify(self, table_df):
        """
        Apply all hard-coded rules to classify cells.

        Args:
            table_df: Input table DataFrame

        Returns:
            Mask DataFrame with "feature", "data-point", "None", or "undecided"
        """
        # Initialize mask with "undecided"
        mask = self.pd.DataFrame("undecided", index=table_df.index, columns=table_df.columns)

        # Rule 0: Missing value check (highest priority)
        for i in range(len(table_df)):
            for j in range(len(table_df.columns)):
                cell_value = table_df.iloc[i, j]
                if self._is_missing_value(cell_value):
                    mask.iloc[i, j] = "None"

        # Rule 1: Row identity check
        for idx, row in table_df.iterrows():
            if self._check_row_identity(row):
                # Only set cells to "feature" if they're not already classified as "None"
                for col_idx in range(len(table_df.columns)):
                    if mask.iloc[idx, col_idx] != "None":
                        mask.iloc[idx, col_idx] = "feature"

        # Rules 2-4: Numeric patterns and missing values
        for i in range(len(table_df)):
            for j in range(len(table_df.columns)):
                # Only check if cell is still undecided (protects "None" and "feature" classifications)
                if mask.iloc[i, j] == "undecided":
                    cell_value = str(table_df.iloc[i, j]).strip()
                    if self._is_numeric_pattern(cell_value):
                        mask.iloc[i, j] = "data-point"

        # Rule 5: Row consistency enforcement
        mask = self._enforce_row_consistency(mask)

        return mask

    def _enforce_row_consistency(self, mask):
        """
        If a row is more features or data points, change all cells to that classification,
        given that more than 30% of the row is that classification.
        "None" values are treated as neutral and never changed.

        Args:
            mask: Current mask DataFrame

        Returns:
            Updated mask with row consistency enforced
        """
        for idx, row in mask.iterrows():
            # Count occurrences of each classification (excluding None)
            feature_count = (row == "feature").sum()
            datapoint_count = (row == "data-point").sum()
            none_count = (row == "None").sum()
            undecided_count = (row == "undecided").sum()

            # Total cells excluding None values for percentage calculation
            total_non_none_cells = len(row) - none_count

            # Skip if no non-None cells to evaluate
            if total_non_none_cells == 0:
                continue

            # Determine which classification should be applied
            target_classification = None
            if feature_count > datapoint_count:
                if feature_count / total_non_none_cells >= self.consistency_threshold:
                    target_classification = "feature"
            elif datapoint_count > feature_count:
                if datapoint_count / total_non_none_cells >= self.consistency_threshold:
                    target_classification = "data-point"

            # Apply the target classification only to non-None cells
            if target_classification:
                for col_idx, cell_value in enumerate(row):
                    if cell_value != "None":  # Protect None values from being overwritten
                        mask.iloc[idx, col_idx] = target_classification

        return mask

    def _check_row_identity(self, row) -> bool:
        """
        Check if a row meets the identity threshold.
        Missing values are excluded from identity calculations.

        Args:
            row: A row from the DataFrame

        Returns:
            True if >= threshold of non-missing cells are identical
        """
        if len(row) == 0:
            return False

        # Filter out missing values before checking identity
        non_missing_values = []
        for cell_value in row:
            if not self._is_missing_value(cell_value):
                non_missing_values.append(str(cell_value).strip())

        # If no non-missing values or a singular value, cannot determine identity
        if len(non_missing_values) < 2:
            return False

        # Convert to Series for value_counts
        cleaned_row = self.pd.Series(non_missing_values)
        value_counts = cleaned_row.value_counts()

        if len(value_counts) == 0:
            return False

        most_common_count = value_counts.iloc[0]
        ratio = most_common_count / len(non_missing_values)

        return ratio >= self.threshold

    def _is_numeric_pattern(self, cell: str) -> bool:
        """
        Check if cell matches any numeric pattern.

        Args:
            cell: Cell value as string

        Returns:
            True if cell matches numeric patterns
        """
        # Pattern 1: Decimal numbers (e.g., 123.45)
        if self.re.match(r'^\d+\.\d+', cell):
            return True

        # Pattern 2: Numbers with thousand separators (e.g., 1,234)
        if self.re.match(r'^\d{1,3}(,\d{3})+', cell):
            return True

        # Pattern 3: Combined - thousand separators with decimal (e.g., 1,234.56)
        if self.re.match(r'^\d{1,3}(,\d{3})+\.\d+', cell):
            return True

        # Pattern 4: Whole numbers (excluding single digits and years 1900-2030)
        if self.re.match(r'^\d+$', cell):
            # Convert to int for range checking
            try:
                num = int(cell)
                # Exclude single digits (0-9) and years (1900-2030)
                if not (0 <= num <= 9 or 1900 <= num <= 2030):
                    return True
            except ValueError:
                # If conversion fails, it's not a valid number
                pass

        # Dash patterns
        if cell in ["-", "--", "---"]:
            return True

        return False
