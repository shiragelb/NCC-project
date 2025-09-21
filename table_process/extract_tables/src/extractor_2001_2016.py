"""
Table extractor for years 2001-2016.
"""

# ========================================================================
# INSTRUCTIONS: PASTE YOUR CODE FROM NOTEBOOK
# ========================================================================
# FROM LINES: ~350-650
# CONTENT: The first TableExtractor class
# 
# MODIFICATIONS NEEDED:
# 1. Rename class from TableExtractor to TableExtractor2001_2016
# 2. Change YEAR_RANGE = (2001, 2025) to YEAR_RANGE = (2001, 2017)
# ========================================================================

import os
import json
import pandas as pd
from docx import Document
import logging

logger = logging.getLogger(__name__)

# First TableExtractor class

class TableExtractor2001_2016:
    """Simple class for extracting tables from Word documents with statistics tracking."""

    def __init__(self, base_dir="/content/reports", out_dir="/content/tables"):
        """Initialize the extractor with directories and statistics."""
        self.base_dir = base_dir
        self.out_dir = out_dir

        # Configuration constants
        self.YEAR_RANGE = (2001, 2017)
        self.VALID_EXTENSION = ".docx"
        self.TABLE_MARKER = "לוח"  # Hebrew for "table"
        self.EXCLUDE_MARKER = "תרשים"  # Hebrew for "diagram" - exclude these
        self.ENCODING = "utf-8-sig"
        self.SUMMARY_FILE = "tables_summary.json"
        self.COLUMNS_FILE = "tables_columns.json"

        # Metadata collectors
        self.all_summaries = {}
        self.all_colnames = {}

        # Create output directory
        os.makedirs(self.out_dir, exist_ok=True)

    def _is_valid_table(self, table):
        """
        Check if a table is valid (contains Hebrew table marker in first row).

        Args:
            table: A docx table object

        Returns:
            tuple: (is_valid: bool, table_name: str)
        """
        if len(table.rows) <= 1:
            return False, ""

        # Check first row cells for table marker
        for cell in table.rows[0].cells:
            cell_text = cell.text
            if self.TABLE_MARKER in cell_text and self.EXCLUDE_MARKER not in cell_text:
                return True, cell_text.strip()

        return False, ""

    def _extract_table_data(self, table):
        """
        Extract data from a docx table and convert to DataFrame.

        Args:
            table: A docx table object

        Returns:
            pd.DataFrame: Table data as a DataFrame
        """
        data = [[cell.text.strip() for cell in row.cells] for row in table.rows]
        return pd.DataFrame(data)

    def _save_table_data(self, df, identifier, year, chapter):
        """
        Save DataFrame as CSV file in the appropriate directory structure.

        Args:
            df: pandas DataFrame to save
            identifier: Unique identifier for the table
            year: Year of the document
            chapter: Chapter identifier

        Returns:
            str: Path where the file was saved
        """
        save_dir = os.path.join(self.out_dir, str(year), chapter)
        os.makedirs(save_dir, exist_ok=True)

        save_path = os.path.join(save_dir, f"{identifier}.csv")
        df.to_csv(save_path, index=False, encoding=self.ENCODING)

        return save_path

    def _process_document(self, fpath, year, chapter):
        """
        Process a single Word document and extract all valid tables.

        Args:
            fpath: Full path to the document
            year: Year of the document
            chapter: Chapter identifier from filename

        Returns:
            int: Number of tables extracted from this document
        """
        summary = {}
        colnames_map = {}
        tables_extracted = 0

        try:
            doc = Document(fpath)
        except Exception as e:
            print(f"skip {fpath}: {e}")
            return 0

        serial = 1

        for table in doc.tables:
            # Validate table
            is_valid, table_name = self._is_valid_table(table)
            if not is_valid:
                continue

            # Extract data
            df = self._extract_table_data(table)

            # Skip empty tables
            if len(df) == 0:
                continue

            # Create identifier
            chapter = chapter.replace(".docx", "")
            identifier = f"{serial}_{chapter}_{year}"

            # Record mapping for JSON
            if len(df) > 0:
                # Deduplicate consecutive repeated text in header
                header_cells = df.iloc[0].astype(str).tolist()
                unique_header = []
                for cell in header_cells:
                    if not unique_header or cell != unique_header[-1]:
                        unique_header.append(cell)
                summary[identifier] = " ".join(unique_header)
            else:
                continue

            # Combine rows [1] and [2] for column names
            if len(df) > 2:
                row1 = df.iloc[1].astype(str).tolist()
                row2 = df.iloc[2].astype(str).tolist()
                colnames_map[identifier] = [f"{r1} {r2}".strip() for r1, r2 in zip(row1, row2)]
            elif len(df) > 1:
                colnames_map[identifier] = df.iloc[1].astype(str).tolist()
            else:
                colnames_map[identifier] = []

            # Save to CSV
            self._save_table_data(df, identifier, year, chapter)

            serial += 1

        # Update metadata collectors
        self.all_summaries.update(summary)
        self.all_colnames.update(colnames_map)

    def _save_metadata(self):
        """Save summary and column metadata to JSON files."""
        with open(os.path.join(self.out_dir, self.SUMMARY_FILE), "w", encoding="utf-8") as f:
            json.dump(self.all_summaries, f, ensure_ascii=False, indent=2)

        with open(os.path.join(self.out_dir, self.COLUMNS_FILE), "w", encoding="utf-8") as f:
            json.dump(self.all_colnames, f, ensure_ascii=False, indent=2)

    def process_files(self, years=None, chapters=None):
        """
        Process Word documents filtered by years and chapters.

        Args:
            years: List/range of years to process (None = all years in YEAR_RANGE)
            chapters: List of chapter identifiers to process (None = all chapters)

        Example:
            extractor.process_files()  # Process all files
            extractor.process_files(years=[2023, 2024])  # Specific years
            extractor.process_files(chapters=['1', '2', '3'])  # Specific chapters
            extractor.process_files(years=range(2020, 2025), chapters=['1', '2'])  # Both
        """
        # Reset statistics for new extraction session
        self.all_summaries = {}
        self.all_colnames = {}

        # Determine which years to process
        if years is None:
            years_to_process = range(*self.YEAR_RANGE)
        else:
            years_to_process = years

        # Convert chapters to set for faster lookup (if provided)
        chapters_to_process = set(map(str, chapters)) if chapters else None

        # Process each year
        for year in years_to_process:
            print(year)
            year_path = os.path.join(self.base_dir, str(year))

            if not os.path.isdir(year_path):
                continue

            # Process each document in the year directory
            for fname in os.listdir(year_path):
                if not fname.endswith(self.VALID_EXTENSION):
                    continue

                # Extract chapter from filename
                chapter = fname.split("_")[0]

                # Skip if not in chapters to process
                if chapters_to_process and chapter not in chapters_to_process:
                    continue

                fpath = os.path.join(year_path, fname)

                # Process the document
                self._process_document(fpath, year, chapter)

        # Save consolidated metadata
        self._save_metadata()

    def _identify_continuation_groups(self, summaries):
        """
        Identify groups of tables that should be combined (original + continuations).
        Groups are formed by sequential position - any table with "(המשך)" belongs
        to the most recent table without "(המשך)".

        Args:
            summaries: Dictionary of table summaries

        Returns:
            dict: Groups of related tables {original_id: [original_id, continuation_ids...]}
        """
        groups = {}
        continuation_marker = "(המשך)"

        # Sort identifiers to process them in order (important for maintaining sequence)
        sorted_ids = sorted(summaries.keys(), key=lambda x: (
            int(x.split('_')[2]),  # year
            x.split('_')[1],        # chapter
            int(x.split('_')[0])    # serial number
        ))

        current_group_original = None

        for identifier in sorted_ids:
            header = summaries[identifier]

            # Check if this is a continuation
            if continuation_marker in header:
                # This is a continuation - add to current group
                if current_group_original:
                    groups[current_group_original].append(identifier)
                else:
                    # This shouldn't happen - continuation without an original
                    print(f"Warning: Continuation table found without a preceding original: {identifier}")
            else:
                # This is an original table (not a continuation)
                # Start a new group
                current_group_original = identifier
                groups[identifier] = [identifier]  # Group starts with the original

        # Filter out groups with only one table (no continuations)
        groups_with_continuations = {k: v for k, v in groups.items() if len(v) > 1}

        return groups_with_continuations

    def _combine_csv_files(self, identifiers, summaries):
        """
        Load and combine multiple CSV files into one, removing duplicate headers.

        Args:
            identifiers: List of table identifiers [original, continuation1, ...]
            summaries: Dictionary of table summaries (not used in simplified version)

        Returns:
            pd.DataFrame: Combined dataframe
        """
        if not identifiers:
            return None

        combined_df = None
        original_id = identifiers[0]

        # Parse identifier to get year and chapter
        parts = original_id.split('_')
        year = parts[2]
        chapter = parts[1]

        for i, identifier in enumerate(identifiers):
            # Build path to CSV file
            csv_path = os.path.join(self.out_dir, year, chapter, f"{identifier}.csv")

            if not os.path.exists(csv_path):
                print(f"Warning: CSV file not found: {csv_path}")
                continue

            # Load the CSV
            df = pd.read_csv(csv_path, encoding=self.ENCODING)

            if i == 0:
                # First table (original) - keep everything
                combined_df = df
            else:
                # Continuation table - skip first row (the title row)
                if len(df) > 1:
                    combined_df = pd.concat([combined_df, df.iloc[1:]],
                                           ignore_index=True)
                else:
                    # If continuation only has header, skip it entirely
                    print(f"  Note: Continuation {identifier} has no data rows")

        return combined_df

    def combine_continuation_tables(self):
      """
      Combine continuation tables with their originals after extraction.
      This should be called after process_files() to merge any continuation tables.

      Returns:
          dict: Information about combined tables
      """
      # Load current summaries
      summary_path = os.path.join(self.out_dir, self.SUMMARY_FILE)
      columns_path = os.path.join(self.out_dir, self.COLUMNS_FILE)

      if not os.path.exists(summary_path):
          print("No summaries file found. Run process_files() first.")
          return {}

      # Load metadata
      with open(summary_path, 'r', encoding='utf-8') as f:
          summaries = json.load(f)

      with open(columns_path, 'r', encoding='utf-8') as f:
          colnames = json.load(f)

      # Identify continuation groups
      groups = self._identify_continuation_groups(summaries)

      if not groups:
          print("No continuation tables found.")
          return {}

      print(f"\nFound {len(groups)} table(s) with continuations to combine...")

      # Track what we combined
      combined_info = {}

      # Process each group
      for original_id, identifier_list in groups.items():
          print(f"\nCombining {original_id} with {len(identifier_list)-1} continuation(s)...")

          # Combine the CSV files
          combined_df = self._combine_csv_files(identifier_list, summaries)

          if combined_df is not None:
              # Parse identifier to get year and chapter
              parts = original_id.split('_')
              year = parts[2]
              chapter = parts[1]

              # Save the combined CSV (overwriting the original)
              save_path = os.path.join(self.out_dir, year, chapter, f"{original_id}.csv")
              combined_df.to_csv(save_path, index=False, encoding=self.ENCODING)

              # Delete continuation CSV files
              for continuation_id in identifier_list[1:]:  # Skip the original
                  continuation_path = os.path.join(self.out_dir, year, chapter, f"{continuation_id}.csv")
                  if os.path.exists(continuation_path):
                      os.remove(continuation_path)
                      print(f"  Removed: {continuation_id}.csv")

              # Track combination info
              combined_info[original_id] = {
                  'parts_combined': len(identifier_list),
                  'continuation_ids': identifier_list[1:],
                  'rows_in_combined': len(combined_df)
              }

              print(f"  Combined table saved as: {original_id}.csv ({len(combined_df)} rows)")

      # Remove continuation entries from metadata
      summaries_without_continuations = {k: v for k, v in summaries.items()
                                        if "(המשך)" not in v}
      colnames_without_continuations = {k: v for k, v in colnames.items()
                                      if "(המשך)" not in summaries.get(k, "")}

      # Renumber tables sequentially per chapter-year
      print("\nRenumbering tables sequentially...")

      # Group by chapter and year
      grouped = {}
      for identifier in summaries_without_continuations.keys():
          parts = identifier.split('_')
          if len(parts) >= 3:
              chapter = parts[1]
              year = parts[2]
              key = f"{chapter}_{year}"
              if key not in grouped:
                  grouped[key] = []
              grouped[key].append(identifier)

      # Sort each group by original serial number
      for key in grouped:
          grouped[key].sort(key=lambda x: int(x.split('_')[0]))

      # Create new dictionaries with sequential numbering
      new_summaries = {}
      new_colnames = {}
      rename_map = {}  # Track old -> new identifier mapping

      for chapter_year, identifiers in grouped.items():
          chapter, year = chapter_year.split('_')

          for new_serial, old_identifier in enumerate(identifiers, start=1):
              new_identifier = f"{new_serial}_{chapter}_{year}"
              rename_map[old_identifier] = new_identifier

              # Copy to new dictionaries with new key
              new_summaries[new_identifier] = summaries_without_continuations[old_identifier]
              if old_identifier in colnames_without_continuations:
                  new_colnames[new_identifier] = colnames_without_continuations[old_identifier]

      # Rename CSV files
      for old_id, new_id in rename_map.items():
          if old_id != new_id:  # Only rename if different
              parts_old = old_id.split('_')
              parts_new = new_id.split('_')
              year = parts_old[2]
              chapter = parts_old[1]

              old_path = os.path.join(self.out_dir, year, chapter, f"{old_id}.csv")
              new_path = os.path.join(self.out_dir, year, chapter, f"{new_id}.csv")

              if os.path.exists(old_path):
                  os.rename(old_path, new_path)
                  print(f"  Renamed: {old_id}.csv -> {new_id}.csv")

      # Update combined_info with new identifiers
      updated_combined_info = {}
      for old_id, info in combined_info.items():
          new_id = rename_map.get(old_id, old_id)
          updated_combined_info[new_id] = info

      # Save updated metadata with sequential numbering
      with open(summary_path, 'w', encoding='utf-8') as f:
          json.dump(new_summaries, f, ensure_ascii=False, indent=2)

      with open(columns_path, 'w', encoding='utf-8') as f:
          json.dump(new_colnames, f, ensure_ascii=False, indent=2)

      # Save combination tracking info
      tracking_path = os.path.join(self.out_dir, "combined_tables_info.json")
      with open(tracking_path, 'w', encoding='utf-8') as f:
          json.dump(updated_combined_info, f, ensure_ascii=False, indent=2)

      print(f"\n✓ Combination complete! Combined {len(groups)} table(s)")
      print(f"  Tables renumbered sequentially per chapter-year")
      print(f"  Combination details saved to: combined_tables_info.json")

      return updated_combined_info

    def calculate_statistics(self):
      """
      Calculate statistics from the table_summary.json file.

      Returns:
          dict: Statistics with 'total' and 'per_chapter_year' breakdown
      """
      summary_path = os.path.join(self.out_dir, self.SUMMARY_FILE)

      if not os.path.exists(summary_path):
          return {'total': 0, 'per_chapter_year': {}}

      # Load summaries
      with open(summary_path, 'r', encoding='utf-8') as f:
          summaries = json.load(f)

      # Calculate statistics
      total = len(summaries)
      per_chapter_year = {}

      for identifier in summaries.keys():
          # Parse identifier: "serial_chapter_year"
          parts = identifier.split('_')
          if len(parts) >= 3:
              chapter = parts[1]
              year = int(parts[2])

              if chapter not in per_chapter_year:
                  per_chapter_year[chapter] = {}
              if year not in per_chapter_year[chapter]:
                  per_chapter_year[chapter][year] = 0
              per_chapter_year[chapter][year] += 1

      return {
          'total': total,
          'per_chapter_year': per_chapter_year
      }

    def print_summary(self):
        """Print a formatted summary of extraction statistics."""
        stats = self.calculate_statistics()

        print("\n" + "="*50)
        print("EXTRACTION SUMMARY")
        print("="*50)
        print(f"Total tables extracted: {stats['total']}")

        if stats['per_chapter_year']:
            print("\nTables per chapter per year:")
            for chapter in sorted(stats['per_chapter_year'].keys()):
                print(f"\nChapter {chapter}:")
                for year in sorted(stats['per_chapter_year'][chapter].keys()):
                    count = stats['per_chapter_year'][chapter][year]
                    if count > 0:  # Only show years with tables
                        print(f"  {year}: {count}")
        else:
            print("\nNo tables found.")
        print("="*50)
        
