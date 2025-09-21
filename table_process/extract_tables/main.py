#!/usr/bin/env python3
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
