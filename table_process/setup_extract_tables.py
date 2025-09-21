#!/usr/bin/env python3
"""
Script to set up the extract_tables repository structure.
Run this from the table_process/ directory:
    python setup_extract_tables.py
"""

import os
import sys

def create_file(path, content):
    """Create a file with given content."""
    dir_path = os.path.dirname(path)
    if dir_path:
        os.makedirs(dir_path, exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        f.write(content)
    print(f"Created: {path}")

def main():
    print("Setting up extract_tables repository structure...")
    print("=" * 60)
    
    # Create directory structure
    os.makedirs("extract_tables/src", exist_ok=True)
    os.makedirs("extract_tables/notebooks", exist_ok=True)
    print("✓ Created directory structure")
    
    # 1. Create main.py
    main_py = '''#!/usr/bin/env python3
"""
Main CLI entry point for table extraction pipeline.
Run from table_process/ root directory:
    python extract_tables/main.py --drive-folder-id YOUR_ID --years 2021 2022
"""

import argparse
import logging
import sys
import os
from pathlib import Path

# Add src to path
sys.path.append(str(Path(__file__).parent))

from src.drive_manager import GoogleDriveManager
from src.extractor_2001_2016 import TableExtractor2001_2016
from src.extractor_2017_2018_2020 import TableExtractor2017_2018_2020
from src.extractor_2019_2024 import TableExtractor2019_2024
from src.merger import GlobalContinuationMerger
from src.statistics import generate_statistics
from src.utils import load_config, setup_logging, validate_year_range

def main():
    parser = argparse.ArgumentParser(description='Extract tables from Hebrew Word documents')
    parser.add_argument('--drive-folder-id', required=True, help='Google Drive folder ID')
    parser.add_argument('--years', nargs='+', type=int, required=True, 
                        help='Years to process (e.g., 2021 2022 2023)')
    parser.add_argument('--chapters', nargs='+', type=int, default=list(range(1, 16)),
                        help='Chapters to process (default: 1-15)')
    parser.add_argument('--config', default='extract_tables/config.yaml',
                        help='Configuration file path')
    parser.add_argument('--download-only', action='store_true',
                        help='Only download files, do not extract')
    parser.add_argument('--skip-merge', action='store_true',
                        help='Skip continuation table merging')
    parser.add_argument('--verbose', action='store_true',
                        help='Enable verbose logging')
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(verbose=args.verbose)
    logger = logging.getLogger(__name__)
    
    # Load configuration
    config = load_config(args.config)
    
    # Authenticate if in Colab
    try:
        from google.colab import auth
        auth.authenticate_user()
        logger.info("Authenticated in Google Colab")
    except ImportError:
        pass  # Not in Colab
    
    # Set paths relative to table_process root
    reports_dir = config.get('reports_dir', 'extract_tables/temp/reports')
    tables_dir = config.get('tables_dir', 'chain/table-chain-matching/tables')
    
    # Ensure directories exist
    Path(reports_dir).mkdir(parents=True, exist_ok=True)
    Path(tables_dir).mkdir(parents=True, exist_ok=True)
    
    try:
        # Step 1: Download from Google Drive
        logger.info(f"Initializing Google Drive connection...")
        manager = GoogleDriveManager(args.drive_folder_id)
        
        logger.info(f"Downloading files for years {args.years}, chapters {args.chapters}")
        downloaded = manager.download_selective(
            years=args.years,
            chapters=args.chapters,
            download_dir=reports_dir
        )
        logger.info(f"Downloaded {len(downloaded)} files")
        
        if args.download_only:
            logger.info("Download complete (--download-only mode)")
            return
        
        # Step 2: Extract tables based on year ranges
        logger.info("Starting table extraction...")
        
        # Categorize years
        categorized = validate_year_range(args.years)
        
        all_summaries = {}
        
        # Extract 2001-2016
        if categorized['2001_2016']:
            logger.info(f"Processing years {categorized['2001_2016']} with 2001-2016 extractor")
            extractor = TableExtractor2001_2016(base_dir=reports_dir, out_dir=tables_dir)
            extractor.process_files(years=categorized['2001_2016'], chapters=args.chapters)
            if not args.skip_merge:
                extractor.combine_continuation_tables()
            
        # Extract 2019, 2021-2024  
        if categorized['2019_2024']:
            logger.info(f"Processing years {categorized['2019_2024']} with 2019-2024 extractor")
            extractor = TableExtractor2019_2024(reports_dir=reports_dir, tables_dir=tables_dir)
            summaries = extractor.process_years(years=categorized['2019_2024'], chapters=args.chapters)
            all_summaries.update(summaries)
            
        # Handle special years (2017, 2018, 2020)
        if categorized['special']:
            logger.info(f"Processing years {categorized['special']} with special extractor")
            extractor = TableExtractor2017_2018_2020(reports_dir=reports_dir, tables_dir=tables_dir)
            summaries = extractor.process_files(years=categorized['special'], chapters=args.chapters)
            if summaries:
                all_summaries.update(summaries)
        
        # Step 3: Global merge of continuation tables
        if not args.skip_merge:
            logger.info("Running global merge of continuation tables...")
            merger = GlobalContinuationMerger(tables_dir)
            merger.combine_continuation_tables()
        
        # Step 4: Generate statistics
        logger.info("Generating statistics...")
        stats = generate_statistics(tables_dir)
        logger.info(f"Extraction complete: {stats['total']} tables extracted")
        
    except Exception as e:
        logger.error(f"Extraction failed: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
'''
    create_file("extract_tables/main.py", main_py)
    
    # 2. Create src/__init__.py
    create_file("extract_tables/src/__init__.py", '"""Table extraction package."""\n')
    
    # 3. Create src/drive_manager.py
    drive_manager_py = '''"""
Google Drive manager for downloading Word documents.
"""

# ========================================================================
# INSTRUCTIONS: PASTE YOUR CODE FROM NOTEBOOK
# ========================================================================
# FROM LINES: ~25-350
# CONTENT: The entire GoogleDriveManager class
# 
# MODIFICATIONS NEEDED:
# 1. Remove the line: auth.authenticate_user() from __init__ or _authenticate()
# 2. Remove the line: self._authenticate() from __init__
# 3. Change _authenticate() to _build_service() and just build the drive service
# ========================================================================

import os
import io
import logging
import pandas as pd
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

logger = logging.getLogger(__name__)

# >>>>>>> PASTE START: GoogleDriveManager class (lines 25-350) <<<<<<<

# DELETE THE LINES BELOW AND PASTE YOUR CLASS HERE
class GoogleDriveManager:
    def __init__(self, folder_id):
        raise NotImplementedError("PASTE GoogleDriveManager class from notebook lines 25-350")
        
# >>>>>>> PASTE END <<<<<<<
'''
    create_file("extract_tables/src/drive_manager.py", drive_manager_py)
    
    # 4. Create src/extractor_2001_2016.py
    extractor_2001_2016_py = '''"""
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

# >>>>>>> PASTE START: First TableExtractor class (lines 350-650) <<<<<<<

# DELETE THE LINES BELOW AND PASTE YOUR CLASS HERE
class TableExtractor2001_2016:
    def __init__(self, base_dir="/content/reports", out_dir="/content/tables"):
        raise NotImplementedError("PASTE TableExtractor class from notebook lines 350-650 and rename to TableExtractor2001_2016")
        
# >>>>>>> PASTE END <<<<<<<
'''
    create_file("extract_tables/src/extractor_2001_2016.py", extractor_2001_2016_py)
    
    # 5. Create src/extractor_2017_2018_2020.py
    extractor_special_py = '''"""
Table extractor for special years 2017, 2018, 2020.
"""

# ========================================================================
# INSTRUCTIONS: TO BE IMPLEMENTED
# ========================================================================
# These years require a different extraction method.
# This file is a placeholder for future implementation.
# ========================================================================

import os
import json
import logging
from docx import Document

logger = logging.getLogger(__name__)


class TableExtractor2017_2018_2020:
    """
    Extracts tables from Word documents for years 2017, 2018, 2020.
    These years have a different format that requires special handling.
    """
    
    def __init__(self, reports_dir="/content/reports", tables_dir="/content/tables"):
        self.reports_dir = reports_dir
        self.tables_dir = tables_dir
        self.encoding = "utf-8-sig"
        
    def process_files(self, years=None, chapters=None):
        """
        Process Word documents for special years 2017, 2018, 2020.
        
        TO BE IMPLEMENTED: Add extraction logic specific to these years.
        """
        if years is None:
            years = [2017, 2018, 2020]
        
        logger.warning(f"Extraction for years {years} is not yet implemented")
        logger.warning("Placeholder for future implementation")
        
        # Return empty summaries for now
        return {}
    
    def extract_tables(self, docx_path, year, chapter):
        """
        Extract tables from a single document.
        
        TO BE IMPLEMENTED: Add the specific extraction logic.
        """
        raise NotImplementedError("Extraction method for 2017, 2018, 2020 not yet implemented")
'''
    create_file("extract_tables/src/extractor_2017_2018_2020.py", extractor_special_py)
    
    # 6. Create src/extractor_2019_2024.py
    extractor_2019_2024_py = '''"""
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

# >>>>>>> PASTE START: iter_block_items function (lines ~865-870) <<<<<<<

def iter_block_items(parent):
    """DELETE THIS AND PASTE YOUR FUNCTION FROM LINE ~865"""
    raise NotImplementedError("PASTE iter_block_items function here")

# >>>>>>> PASTE END <<<<<<<


class TableExtractor2019_2024:
    """Extracts tables from Word documents for years 2019, 2021-2024."""
    
    def __init__(self, reports_dir="/content/reports", tables_dir="/content/tables"):
        self.reports_dir = reports_dir
        self.tables_dir = tables_dir
        self.unnamed_count = 0
        self.encoding = "utf-8-sig"
    
    # >>>>>>> PASTE START: extract_tables_with_headers (lines 870-1050) <<<<<<<
    
    def extract_tables_with_headers(self, docx_path, output_dir, year, chapter):
        """DELETE THIS AND PASTE YOUR FUNCTION FROM LINES 870-1050
        Remember to:
        1. Add 'self' as first parameter
        2. Use self.unnamed_count instead of global unnamed"""
        raise NotImplementedError("PASTE extract_tables_with_headers function here")
    
    # >>>>>>> PASTE END <<<<<<<
    
    def process_years(self, years=None, chapters=None):
        """Process multiple years and chapters."""
        if years is None:
            years = [2019, 2021, 2022, 2023, 2024]
        if chapters is None:
            chapters = range(1, 16)
        
        all_summaries = {}
        
        # >>>>>>> PASTE START: Year processing loop (lines 1060-1120) <<<<<<<
        
        # DELETE THE LINE BELOW AND PASTE YOUR PROCESSING LOOP HERE
        # This is the loop starting with 'for year in years:'
        raise NotImplementedError("PASTE year processing loop from lines 1060-1120")
        
        # >>>>>>> PASTE END <<<<<<<
        
        return all_summaries
'''
    create_file("extract_tables/src/extractor_2019_2024.py", extractor_2019_2024_py)
    
    # 6. Create src/merger.py
    merger_py = '''"""
Continuation table merger for combining split tables.
"""

# ========================================================================
# INSTRUCTIONS: PASTE YOUR CODE FROM NOTEBOOK
# ========================================================================
# FROM LINES: ~1150-1250
# CONTENT: GlobalContinuationMerger class (and ContinuationMerger if exists)
# 
# MODIFICATIONS NEEDED: None, paste as-is
# ========================================================================

import os
import json
import pandas as pd
from collections import defaultdict
import logging

logger = logging.getLogger(__name__)

# >>>>>>> PASTE START: ContinuationMerger class (if exists) <<<<<<<

# If you have a ContinuationMerger class in your notebook, paste it here
# Otherwise, delete this comment

# >>>>>>> PASTE END <<<<<<<


# >>>>>>> PASTE START: GlobalContinuationMerger class (lines ~1150-1250) <<<<<<<

# DELETE THE LINES BELOW AND PASTE YOUR CLASS HERE
class GlobalContinuationMerger:
    def __init__(self, base_dir):
        raise NotImplementedError("PASTE GlobalContinuationMerger class from notebook lines 1150-1250")

# >>>>>>> PASTE END <<<<<<<
'''
    create_file("extract_tables/src/merger.py", merger_py)
    
    # 7. Create src/statistics.py
    statistics_py = '''"""
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
    
    # >>>>>>> PASTE START: Statistics generation code (lines ~1300-1400) <<<<<<<
    
    # DELETE THE LINES BELOW AND PASTE YOUR STATISTICS CODE HERE
    # The code that:
    # 1. Loads summaries from JSON
    # 2. Counts total tables and unnamed tables
    # 3. Creates per_chapter_year statistics
    # 4. Writes tables_stats.csv
    
    raise NotImplementedError("PASTE statistics generation code from notebook lines ~1300-1400")
    
    # >>>>>>> PASTE END <<<<<<<
    
    # Make sure to return the stats dictionary at the end:
    # return {'total': total, 'per_chapter_year': per_chapter_year_dict, 'unnamed_count': unnamed_count}
'''
    create_file("extract_tables/src/statistics.py", statistics_py)
    
    # 8. Create src/utils.py
    utils_py = '''"""
Utility functions for configuration and common operations.
"""

import os
import yaml
import logging
import json
from pathlib import Path


def load_config(config_path="extract_tables/config.yaml"):
    """Load configuration from YAML file."""
    if not os.path.exists(config_path):
        return get_default_config()
    
    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    return config


def get_default_config():
    """Get default configuration."""
    return {
        'reports_dir': 'extract_tables/temp/reports',
        'tables_dir': 'chain/table-chain-matching/tables',
        'encoding': 'utf-8-sig',
        'table_marker': 'לוח',
        'exclude_marker': 'תרשים',
    }


def setup_logging(verbose=False):
    """Setup logging configuration."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )


def validate_year_range(years):
    """Categorize years by extraction method."""
    categorized = {
        '2001_2016': [],
        '2019_2024': [],
        'special': [],
        'invalid': []
    }
    
    for year in years:
        if 2001 <= year <= 2016:
            categorized['2001_2016'].append(year)
        elif year in [2019, 2021, 2022, 2023, 2024]:
            categorized['2019_2024'].append(year)
        elif year in [2017, 2018, 2020]:
            categorized['special'].append(year)
        else:
            categorized['invalid'].append(year)
    
    return categorized
'''
    create_file("extract_tables/src/utils.py", utils_py)
    
    # 9. Create requirements.txt
    requirements = '''pandas>=1.3.0
numpy>=1.21.0
python-docx>=0.8.11
google-api-python-client>=2.0.0
google-auth>=2.0.0
google-auth-httplib2>=0.1.0
google-auth-oauthlib>=0.4.0
PyYAML>=5.4.0
openpyxl>=3.0.0
'''
    create_file("extract_tables/requirements.txt", requirements)
    
    # 10. Create config.yaml
    config = '''# Configuration for table extraction

# Paths (relative to table_process root)
reports_dir: "extract_tables/temp/reports"
tables_dir: "chain/table-chain-matching/tables"

# Extraction settings
encoding: "utf-8-sig"
table_marker: "לוח"
exclude_marker: "תרשים"

# Year ranges
years_2001_2016: [2001, 2002, 2003, 2004, 2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016]
years_2019_2024: [2019, 2021, 2022, 2023, 2024]
years_special: [2017, 2018, 2020]
'''
    create_file("extract_tables/config.yaml", config)
    
    # 11. Create README.md
    readme = '''# Hebrew Table Extraction Pipeline

Extracts statistical tables from Hebrew Word documents stored in Google Drive.

## Installation

1. Install dependencies:
```bash
pip install -r extract_tables/requirements.txt
```

## Usage

**IMPORTANT: Always run from the `table_process/` root directory**

### Basic Usage

```bash
python extract_tables/main.py --drive-folder-id YOUR_FOLDER_ID --years 2021 2022 2023
```

### Parameters

- `--drive-folder-id`: Google Drive folder ID (required)
- `--years`: Years to process (required)
- `--chapters`: Chapters to process (default: 1-15)
- `--download-only`: Only download files
- `--skip-merge`: Skip continuation table merging
- `--verbose`: Enable detailed logging

### Output

- Tables saved to: `chain/table-chain-matching/tables/`
- Summary file: `chain/table-chain-matching/tables_summary.json`

### Google Colab Usage

```python
from google.colab import auth
auth.authenticate_user()

!python extract_tables/main.py --drive-folder-id YOUR_ID --years 2021 2022
```

## Year Ranges

- **2001-2016**: Standard extraction
- **2017, 2018, 2020**: Special method (placeholder - to be implemented)
- **2019, 2021-2024**: Enhanced extraction
'''
    create_file("extract_tables/README.md", readme)
    
    # 12. Create .gitignore
    gitignore = '''# Temporary files
temp/
*.pyc
__pycache__/
.DS_Store

# Downloaded reports
reports/

# IDE
.vscode/
.idea/

# Environment
.env
venv/
'''
    create_file("extract_tables/.gitignore", gitignore)
    
    print("\n" + "=" * 60)
    print("SETUP COMPLETE!")
    print("=" * 60)
    print("\nNEXT STEPS:")
    print("\n1. Open each file and paste the code from your notebook:")
    print("   - src/drive_manager.py         → Lines 25-350 (GoogleDriveManager class)")
    print("   - src/extractor_2001_2016.py    → Lines 350-650 (First TableExtractor class)")
    print("   - src/extractor_2017_2018_2020.py → TO BE IMPLEMENTED (placeholder for special years)")
    print("   - src/extractor_2019_2024.py    → Lines 865-1120 (iter_block_items + extract_tables_with_headers)")
    print("   - src/merger.py                → Lines 1150-1250 (GlobalContinuationMerger class)")
    print("   - src/statistics.py            → Lines ~1300-1400 (Statistics generation code)")
    print("\n2. Follow the modification instructions in each file")
    print("\n3. Install dependencies:")
    print("   pip install -r extract_tables/requirements.txt")
    print("\n4. Run the extraction:")
    print("   python extract_tables/main.py --drive-folder-id YOUR_ID --years 2021 2022")
    print("\n" + "=" * 60)

if __name__ == "__main__":
    main()
