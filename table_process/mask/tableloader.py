# -*- coding: utf-8 -*-


class TableLoader:
    """Handles table loading and cleaning operations."""

    def __init__(self):
        import pandas as pd
        from pathlib import Path
        self.pd = pd
        self.Path = Path

    def load_and_clean(self, csv_path: str):
        """
        Load CSV file and clean the data.

        Args:
            csv_path: Path to the CSV file

        Returns:
            Cleaned DataFrame with asterisks removed
        """
        # Before 2017 there were indices and title in the first two rows, after it there's just the table
        identifier = self.Path(csv_path).stem
        _, _, year = identifier.split('_')
        if int(year) < 2017:
            header=0
        else:
            header=None
        # Read CSV treating all rows equally (no header assumption)
        df = self.pd.read_csv(csv_path, header=header, dtype=str, keep_default_na=False)

        # Remove asterisks from all cells
        df = df.map(lambda x: str(x).replace('*', '') if self.pd.notna(x) else '')

        return df
