"""
Table extractor for years 2019, 2021-2024.
"""

# ========================================================================
# INSTRUCTIONS: PASTE YOUR CODE FROM NOTEBOOK - THREE SECTIONS
# ========================================================================
# SECTION 1 - FROM LINES: ~865-870
#   CONTENT: The iter_block_items() function
#   MODIFICATIONS: None, paste as-is
#
# SECTION 2 - FROM LINES: ~870-1050  
#   CONTENT: The extract_tables_with_headers() function
#   MODIFICATIONS: 
#   1. Make it a class method by adding 'self' as first parameter
#   2. Replace 'global unnamed' with 'self.unnamed_count'
#   3. Replace 'unnamed += 1' with 'self.unnamed_count += 1'
#
# SECTION 3 - FROM LINES: ~1060-1120
#   CONTENT: The loop that processes years [2019, 2021, 2022, 2023, 2024]
#   MODIFICATIONS: Put inside the process_years() method below
# ========================================================================

import os
import csv
import json
import logging
from docx import Document
from docx.oxml.table import CT_Tbl
from docx.oxml.text.paragraph import CT_P
from docx.table import Table
from docx.text.paragraph import Paragraph

logger = logging.getLogger(__name__)

def iter_block_items(parent):
    """Yield paragraphs and tables in document order."""
    parent_elm = parent._element.body
    for child in parent_elm.iterchildren():
        if isinstance(child, CT_P):
            yield Paragraph(child, parent)
        elif isinstance(child, CT_Tbl):
            yield Table(child, parent)


class TableExtractor2019_2024:
    """Extracts tables from Word documents for years 2019, 2021-2024."""
    
    def __init__(self, reports_dir="/content/reports", tables_dir="/content/tables"):
        self.reports_dir = reports_dir
        self.tables_dir = tables_dir
        self.unnamed_count = 0
        self.encoding = "utf-8-sig"
    
    # extract_tables_with_headers
    def extract_tables_with_headers(self, docx_path, output_dir, year, chapter):
        os.makedirs(output_dir, exist_ok=True)

        doc = Document(docx_path)
        tables_meta = {}  # {csv_filename: header_text}

        table_counter = 0
        last_paragraphs = []  # keep a small history of paragraphs
        last_single_row_table = []

        for block in iter_block_items(doc):
            if isinstance(block, Paragraph):
                # Keep track of the last N paragraphs
                text = block.text.strip()
                if text:
                    last_paragraphs.append(text)
                    if len(last_paragraphs) > 3:  # keep only last 3
                        last_paragraphs.pop(0)

            elif isinstance(block, Table):
                # Extract all rows
                rows = [[cell.text.strip() for cell in row.cells] for row in block.rows]

                # Handle single-row tables: save for later search
                if len(rows) == 1:
                    last_single_row_table = rows
                    continue

                # Skip empty multi-row tables
                if all(not any(cell for cell in row) for row in rows):
                    continue

                # Default: first row’s first cell
                table_header = rows[0][0] if rows[0] else f"Unnamed table {table_counter+1}"

                # If "לוח" is not in the first cell, look back at paragraphs
                if "לוח" not in table_header:
                    for prev_text in reversed(last_paragraphs):
                        if "לוח" in prev_text:
                            table_header = prev_text
                            print(f"[{table_counter+1}] Table header → {table_header}")
                            break

                # If still not found, check previous single-row table
                if "לוח" not in table_header and last_single_row_table:
                    for cell in last_single_row_table[0]:
                        if " לוח" in cell:
                            table_header = cell
                            print(f"[{table_counter}] Table header ← from single-row table → {table_header}")
                            break

                # As an extra fallback: search within the table itself (all cells)
                if "לוח" not in table_header:
                    for row in rows[:2]:  # check first 2 rows
                        for cell in row:
                            if "לוח" in cell:
                                table_header = cell
                                break

                # Remove first row (header) and check if the rest is empty
                table_content = rows[1:]  # skip header

                # Skip table if it has no real content - graphs
                if not table_content or all(all(not cell for cell in row) for row in table_content) or "תרשים" in table_header:
                    continue

                if not "לוח" in table_header:
                    table_header = "unnamed"
                    self.unnamed_count +=1

                print(f"[{table_counter+1}] Table header → {table_header}")

                # Remove first row from the actual table content
                max_cols = max(len(row) for row in rows)
                table_content = rows[1:]


                for i, row in enumerate(table_content):
                    if len(row) < max_cols:
                        table_content[i] += [""] * (max_cols - len(row))
                    elif len(row) > max_cols:
                        table_content[i] = row[:max_cols]


                # Skip tables that have no real content
                if not table_content:
                    continue

                table_counter += 1
                csv_filename = f"{table_counter}_{chapter}_{year}.csv"
                new_output_dir = os.path.join(output_dir, str(year))
                new_output_dir = os.path.join(new_output_dir,str(chapter))
                os.makedirs(new_output_dir, exist_ok=True)
                csv_path = os.path.join(new_output_dir, csv_filename)

                # Save actual table content to CSV
                with open(csv_path, "w", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f)
                    writer.writerows(table_content)

                # Save only the first cell as header in JSON
                table_id = f"{table_counter}_{chapter}_{year}"
                tables_meta[table_id] = table_header

        # Save headers JSON
        json_path = os.path.join(output_dir, "summaries.json")
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(tables_meta, f, ensure_ascii=False, indent=2)

        print(f"Extracted {table_counter} tables")
        print(f"Saved headers JSON to {json_path}")
        return tables_meta

    
    def process_years(self, years=None, chapters=None):
        """Process multiple years and chapters."""
        if years is None:
            years = [2019, 2021, 2022, 2023, 2024]
        if chapters is None:
            chapters = range(1, 16)
        
        all_summaries = {}
        
        # Year processing loop (lines 1060-1120)
        
        for year in years:
            year_dir = os.path.join(self.reports_dir, str(year))
            if not os.path.exists(year_dir):
                print(f"⚠️ Missing reports for {year}")
                continue

            # Loop over all docx files in that year
            for filename in sorted(os.listdir(year_dir)):
                if not filename.endswith(".docx"):
                    continue

                # Parse chapter number from filename (e.g., "01_01.docx" -> 1)
                chapter = filename.split("_")[0].lstrip("0")
                chapter = int(chapter) if chapter.isdigit() else chapter
                if chapter not in chapters:
                    continue

                docx_path = os.path.join(year_dir, filename)

                print(f"\n📄 Extracting {docx_path} → year={year}, chapter={chapter}")
                meta = self.extract_tables_with_headers(docx_path, self.tables_dir, year, chapter)

                # --- Save chapter summaries.json for the merger ---
                chapter_dir = os.path.join(self.tables_dir, str(year), str(chapter))
                os.makedirs(chapter_dir, exist_ok=True)
                chapter_summary_path = os.path.join(chapter_dir, "summaries.json")
                with open(chapter_summary_path, "w", encoding="utf-8") as f:
                    json.dump(meta, f, ensure_ascii=False, indent=2)

                # --- Run merger for this chapter ---
                print(f"🔄 Running continuation merger for {year}/{chapter}...")
                # merger = ContinuationMerger(chapter_dir)
                # merged_info = merger.combine_continuation_tables()

                # --- Reload updated summaries.json after merge ---
                with open(chapter_summary_path, "r", encoding="utf-8") as f:
                    updated_meta = json.load(f)

                # Merge into big dictionary
                all_summaries.update(updated_meta)
        
        return all_summaries
