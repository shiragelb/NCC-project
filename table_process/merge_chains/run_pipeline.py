#!/usr/bin/env python3
"""
Main orchestrator for Table Chain Merger pipeline
Run this script to process chains for specific chapters
"""

import argparse
import json
import logging
from pathlib import Path
from datetime import datetime

# These imports will work once you paste the code into the module files
from main_pipeline import TableChainMerger

def setup_logging(verbose=False):
    """Setup logging configuration"""
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f"pipeline_{timestamp}.log"
    
    level = logging.DEBUG if verbose else logging.INFO
    
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

def validate_environment():
    """Validate that necessary files and directories exist"""
    issues = []
    
    # Check for chain JSON files in chain_configs directory
    chain_files = list(Path("chain_configs").glob("chains_chapter_*.json"))
    if not chain_files:
        issues.append("No chain configuration files found in chain_configs/ folder")
    
    return issues

def main():
    """Main orchestrator function"""
    parser = argparse.ArgumentParser(
        description='Table Chain Merger Pipeline - Process statistical table chains'
    )
    parser.add_argument(
        '--chapters', 
        type=int, 
        nargs='+',
        required=True,
        help='Chapter numbers to process (e.g., 1 2 3)'
    )
    parser.add_argument(
        '--chains',
        type=str,
        nargs='*',
        help='Specific chain IDs to process (optional)'
    )
    parser.add_argument(
        '--config',
        type=str,
        help='Path to custom config JSON file'
    )
    parser.add_argument(
        '--years',
        type=int,
        nargs='+',
        help='Limit processing to specific years (e.g., 2001 2002)'
    )
    parser.add_argument(
        '--validate',
        action='store_true',
        help='Run validation on outputs after processing'
    )
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose logging'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be processed without actually running'
    )
    
    args = parser.parse_args()
    
    # Setup logging
    logger = setup_logging(args.verbose)
    logger.info("=" * 60)
    logger.info("TABLE CHAIN MERGER PIPELINE")
    logger.info("=" * 60)
    
    # Validate environment
    issues = validate_environment()
    if issues and not args.dry_run:
        logger.error("Environment validation failed:")
        for issue in issues:
            logger.error(f"  - {issue}")
        return 1
    
    # Load or create config
    if args.config:
        logger.info(f"Using custom config: {args.config}")
        config_path = args.config
    else:
        # Create default config if needed
        config_path = None
        logger.info("Using default configuration")
    
    # Initialize merger
    try:
        merger = TableChainMerger(config_path)
        logger.info("Initialized Table Chain Merger")
    except Exception as e:
        logger.error(f"Failed to initialize merger: {e}")
        return 1
    
    # Process each chapter
    all_results = []
    for chapter in args.chapters:
        logger.info(f"\nProcessing Chapter {chapter}")
        logger.info("-" * 40)
        
        if args.dry_run:
            # Just show what would be processed
            try:
                chains = merger.loader.load_chain_config(chapter)
                if args.chains:
                    # Filter chains with partial matching for merged chains
                    filtered_chains = {}
                    for k, v in chains.items():
                        for chain_id in args.chains:
                            if (k == chain_id or 
                                f"merged_{chain_id}_chain" in k or 
                                f"_chain_{chain_id}" in k or
                                chain_id in k):
                                filtered_chains[k] = v
                                break
                    chains = filtered_chains
                
                logger.info(f"Would process {len(chains)} chains:")
                for chain_id, chain_config in chains.items():
                    years = chain_config.get('years', [])
                    if args.years:
                        years = [y for y in years if y in args.years]
                    logger.info(f"  - {chain_id}: {len(years)} years")
            except Exception as e:
                logger.error(f"Error loading chains for chapter {chapter}: {e}")
            continue
        
        # Actually process the chapter
        try:
            # Apply year filter if specified
            if args.years:
                # Monkey-patch to filter years
                original_process = merger.process_single_chain
                def filtered_process(chain_id, chain_config):
                    indices = [i for i, year in enumerate(chain_config['years']) 
                              if year in args.years]
                    if not indices:
                        logger.warning(f"No matching years for chain {chain_id}")
                        return {'chain_id': chain_id, 'success': False, 
                               'error': 'No matching years'}
                    
                    chain_config['years'] = [chain_config['years'][i] for i in indices]
                    chain_config['tables'] = [chain_config['tables'][i] for i in indices]
                    if 'mask_references' in chain_config:
                        chain_config['mask_references'] = [
                            chain_config['mask_references'][i] 
                            for i in indices 
                            if i < len(chain_config['mask_references'])
                        ]
                    return original_process(chain_id, chain_config)
                
                merger.process_single_chain = filtered_process
                logger.info(f"Filtering to years: {args.years}")
            
            # Process the chapter
            # Filter chains with partial matching for merged chains
            if args.chains:
                all_chains = merger.loader.load_chain_config(chapter)
                filtered_chain_ids = []
                for k in all_chains.keys():
                    for chain_id in args.chains:
                        if (k == chain_id or 
                            f"merged_{chain_id}_chain" in k or 
                            f"_chain_{chain_id}" in k or
                            chain_id in k):
                            filtered_chain_ids.append(k)
                            break
                results = merger.process_chapter(chapter, filtered_chain_ids)
            else:
                results = merger.process_chapter(chapter, args.chains)
            all_results.extend(results)
            
            # Report results
            successful = len([r for r in results if r.get('success')])
            logger.info(f"Chapter {chapter} complete: {successful}/{len(results)} chains processed")
            
        except Exception as e:
            logger.error(f"Failed to process chapter {chapter}: {e}")
            continue
    
    # Validation if requested
    if args.validate and not args.dry_run:
        logger.info("\nRunning validation...")
        from main_pipeline import validate_outputs
        validation_results = validate_outputs()
        valid_count = len([v for v in validation_results if v.get('valid')])
        logger.info(f"Validation complete: {valid_count}/{len(validation_results)} files valid")
    
    # Final summary
    if not args.dry_run:
        logger.info("\n" + "=" * 60)
        logger.info("PIPELINE COMPLETE")
        logger.info(f"Total chains processed: {len(all_results)}")
        successful = len([r for r in all_results if r.get('success')])
        logger.info(f"Successful: {successful}/{len(all_results)}")
        logger.info("=" * 60)
    
    return 0

if __name__ == "__main__":
    exit(main())
