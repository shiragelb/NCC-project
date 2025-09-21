import os
import json
import pandas as pd
import re

class TableLoader:
    def __init__(self, tables_dir="/content/tables",
                 reference_json="tables_summary.json",
                 mask_dir="/content/mask"):
        self.tables_dir = tables_dir
        self.reference_json = reference_json
        self.mask_dir = mask_dir
        self.tables_metadata = {}
        self.tables_by_year = {}

    def load_metadata(self):
        # Load the main tables summary (identifier → header)
        with open(self.reference_json, 'r', encoding='utf-8') as f:
            identifier_to_header = json.load(f)

        # Process each identifier
        for identifier, header in identifier_to_header.items():
            # Parse new format: serial_chapter_year (e.g., "1_03_2021")
            match = re.match(r'(\d+)_(\d+)_(\d{4})', identifier)
            if match:
                serial, chapter, year = match.groups()
                serial = int(serial)
                chapter_str = chapter  # Keep as string with leading zeros
                chapter_int = int(chapter)
                year = int(year)

                # Build filepath
                filepath = os.path.join(self.tables_dir, str(year),
                                    chapter_str, f"{identifier}.csv")

                # Build mask reference path (relative to output dir)
                mask_reference = os.path.join("..", "mask", str(year),
                                             chapter_str, f"{identifier}.csv")

                # Store metadata with identifier as key
                self.tables_metadata[identifier] = {
                    'id': identifier,
                    'file': filepath,
                    'header': header,
                    'year': year,
                    'chapter': chapter_int,
                    'serial': serial,
                    'mask_reference': mask_reference  # Add mask reference
                }

                # Group by year
                if year not in self.tables_by_year:
                    self.tables_by_year[year] = []
                self.tables_by_year[year].append(identifier)

        return len(self.tables_metadata)

    def load_table_data(self, table_id):
        """Load actual CSV data for a table"""
        metadata = self.tables_metadata.get(table_id)
        if metadata and os.path.exists(metadata['file']):
            return pd.read_csv(metadata['file'], header=None)
        return None

    def get_header_for_identifier(self, identifier):
        """Get header text for an identifier"""
        metadata = self.tables_metadata.get(identifier)
        return metadata['header'] if metadata else None

    def get_mask_reference_for_identifier(self, identifier):
        """Get mask reference for an identifier"""
        metadata = self.tables_metadata.get(identifier)
        return metadata['mask_reference'] if metadata else None