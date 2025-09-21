#!/usr/bin/env python3
"""
Main Orchestrator for Table Chain Matching System
Run this script to execute the complete pipeline
"""

import os
import sys
import json
from pathlib import Path

# Add src directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

def setup_environment():
    """Setup environment variables and paths"""
    # Load configuration
    if os.path.exists('config.json'):
        with open('config.json', 'r') as f:
            config = json.load(f)
            
        # Set API key if provided
        if 'CLAUDE_API_KEY' in config and config['CLAUDE_API_KEY']:
            os.environ['CLAUDE_API_KEY'] = config['CLAUDE_API_KEY']
    
    # Create necessary directories
    directories = ['output', 'tables', 'mask', 'cache', 'chain_storage']
    for dir_path in directories:
        os.makedirs(dir_path, exist_ok=True)

def main():
    """Main execution function"""
    print("="*60)
    print("TABLE CHAIN MATCHING SYSTEM")
    print("="*60)
    
    # Setup environment
    print("\nSetting up environment...")
    setup_environment()
    
    # Import the main processor
    try:
        from final_complete_processor import process_table_chains_final_complete
    except ImportError as e:
        print(f"Error importing modules: {e}")
        print("Make sure all module files are populated with code from the notebook")
        sys.exit(1)
    
    # Check for required files
    if not os.path.exists('tables_summary.json'):
        print("\nWarning: tables_summary.json not found!")
        print("Please ensure this file exists before running the pipeline")
        response = input("Continue anyway? (y/n): ")
        if response.lower() != 'y':
            sys.exit(0)
    
    # Run the main pipeline
    print("\nStarting pipeline execution...")
    try:
        chains, statistics = process_table_chains_final_complete()
        
        # Display results
        if chains and statistics:
            print("\n✅ Pipeline completed successfully!")
            print(f"Total chains processed: {sum(len(ch) for ch in chains.values())}")
            
            # Save summary
            with open('output/pipeline_summary.json', 'w') as f:
                json.dump({
                    'total_chapters': len(chains),
                    'statistics': statistics
                }, f, indent=2)
                
            print("Results saved to output/ directory")
        else:
            print("\n⚠️ Pipeline completed but no results generated")
            
    except Exception as e:
        print(f"\n❌ Error during pipeline execution: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
