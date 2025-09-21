"""
Statistics generation for extracted tables.
"""

# ========================================================================
# INSTRUCTIONS: PASTE YOUR CODE FROM NOTEBOOK
# ========================================================================
# FROM LINES: ~1300-1400 (approximately, near the end)
# CONTENT: The statistics generation code
# 
# FIND THE SECTION WITH:
# - Code that creates tables_stats.csv
# - Code that counts unnamed tables
# - Code that generates per_chapter_year statistics
#
# MODIFICATIONS NEEDED:
# 1. Wrap the code in the generate_statistics() function below
# 2. Return the stats dictionary
# ========================================================================

import json
import csv
import os
from collections import defaultdict
import logging

logger = logging.getLogger(__name__)


def generate_statistics(tables_dir):
    """
    Calculate statistics from the tables_summary.json file.
    
    Args:
        tables_dir: Directory containing tables
        
    Returns:
        dict: Statistics dictionary
    """
    
    # Path to summary file (one level up from tables_dir)
    summary_path = os.path.join(tables_dir, "..", "tables_summary.json")
    
    # Statistics generation code 
    
    
    unnamed = 0
    # Load summaries
    with open(summary_path, "r", encoding="utf-8") as f:
        summaries = json.load(f)

    # Nested dict: year → chapter → [total_tables, unnamed_tables]
    tables_stats = defaultdict(lambda: defaultdict(lambda: [0, 0]))

    for table_id, header in summaries.items():
        serial, chapter, year = table_id.split("_")
        year = int(year)
        chapter = int(chapter)

        # Count total tables
        tables_stats[year][chapter][0] += 1

        # Count unnamed tables
        if "unnamed" in header.lower():
            tables_stats[year][chapter][1] += 1
            unnamed += 1

    # Prepare CSV rows
    csv_rows = [["Year", "Chapter", "Total Tables", "Unnamed Tables"]]
    for year in sorted(tables_stats):
        for chapter in sorted(tables_stats[year]):
            total, unnamed_count = tables_stats[year][chapter]
            csv_rows.append([year, chapter, total, unnamed_count])


    # Write to CSV
    csv_path = os.path.join(tables_dir, "..", "tables_stats.csv")
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerows(csv_rows)

    print(f"✅ Table statistics saved to: {csv_path}")
    unnamed_tables_sum = sum(unnamed for year in tables_stats.values()
                                    for _, unnamed in year.values())

    print("🔢 Sum of all Unnamed Tables:", unnamed_tables_sum)


    tables_sum = sum(total for year in tables_stats.values()
                            for total, _ in year.values())

    print("🔢 Sum of all Tables:", tables_sum)

    
    # Make sure to return the stats dictionary at the end:
    return {'total': tables_sum, 'per_chapter_year': dict(tables_stats), 'unnamed_count': unnamed_tables_sum}
