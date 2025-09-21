#!/usr/bin/env python3
"""Quick test to verify the pipeline setup"""

import sys
from pathlib import Path

def check_setup():
    """Check if the repository is properly set up"""
    print("Checking repository setup...")
    
    required_files = [
        "config.py",
        "chain_loader.py", 
        "table_normalizer.py",
        "column_matcher.py",
        "merger_engine.py",
        "output_generator.py",
        "main_pipeline.py",
        "embeddings_handler.py"
    ]
    
    missing = []
    for file in required_files:
        if not Path(file).exists():
            missing.append(file)
    
    if missing:
        print(f"✗ Missing files: {', '.join(missing)}")
        return False
    
    print("✓ All required files present")
    
    # Try importing
    try:
        from main_pipeline import TableChainMerger
        print("✓ Imports successful")
        return True
    except ImportError as e:
        print(f"✗ Import error: {e}")
        print("  Make sure to paste the notebook code into the Python files")
        return False

if __name__ == "__main__":
    success = check_setup()
    sys.exit(0 if success else 1)
