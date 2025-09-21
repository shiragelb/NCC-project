#!/usr/bin/env python3
"""
Sanity check script for Table Chain Matching System
Run this to verify your setup is correct
"""

import os
import sys
import json

def check_setup():
    print("="*60)
    print("SANITY CHECK - Table Chain Matching System")
    print("="*60)
    
    errors = []
    warnings = []
    success = []
    
    # 1. Check directory structure
    print("\n1. Checking directory structure...")
    required_dirs = ['src', 'output', 'tables', 'mask', 'cache', 'chain_storage']
    for dir_name in required_dirs:
        if os.path.exists(dir_name):
            success.append(f"✓ Directory exists: {dir_name}/")
        else:
            warnings.append(f"✗ Missing directory: {dir_name}/ (will be created)")
    
    # 2. Check required files
    print("\n2. Checking required files...")
    required_files = [
        ('main.py', True),
        ('config.json', True),
        ('requirements.txt', True),
        ('tables_summary.json', True),
        ('README.md', False)
    ]
    
    for file_name, critical in required_files:
        if os.path.exists(file_name):
            success.append(f"✓ File exists: {file_name}")
        else:
            if critical:
                errors.append(f"✗ CRITICAL: Missing {file_name}")
            else:
                warnings.append(f"✗ Missing {file_name} (not critical)")
    
    # 3. Check Python modules in src/
    print("\n3. Checking Python modules...")
    required_modules = [
        'config.py', 'hebrew_processor.py', 'table_loader.py', 
        'similarity.py', 'hungarian.py', 'split_merge.py', 
        'chains.py', 'report_gen.py', 'real_embeddings.py',
        'api_validator.py', 'gap_handler.py', 'storage_manager.py',
        'statistics_tracker.py', 'complex_relationships.py', 
        'networkx_builder.py', 'conflict_resolver.py',
        'response_handler.py', 'parameter_tuner.py', 
        'test_suite.py', 'final_complete_processor.py',
        'visualization.py'
    ]
    
    for module in required_modules:
        module_path = os.path.join('src', module)
        if os.path.exists(module_path):
            # Check if file has actual code (not just template)
            with open(module_path, 'r') as f:
                content = f.read()
                if len(content) > 100 and 'class' in content or 'def' in content:
                    success.append(f"✓ Module populated: {module}")
                else:
                    warnings.append(f"✗ Module empty/template: {module}")
        else:
            errors.append(f"✗ Missing module: src/{module}")
    
    # 4. Try importing modules
    print("\n4. Testing imports...")
    sys.path.insert(0, 'src')
    
    critical_imports = [
        'config', 'table_loader', 'chains', 
        'final_complete_processor'
    ]
    
    for module_name in critical_imports:
        try:
            __import__(module_name)
            success.append(f"✓ Import successful: {module_name}")
        except ImportError as e:
            errors.append(f"✗ Import failed: {module_name} - {str(e)}")
        except Exception as e:
            warnings.append(f"✗ Import error: {module_name} - {str(e)}")
    
    # 5. Check config.json
    print("\n5. Checking configuration...")
    if os.path.exists('config.json'):
        try:
            with open('config.json', 'r') as f:
                config = json.load(f)
            
            required_keys = ['tables_dir', 'reference_json', 'similarity_threshold']
            for key in required_keys:
                if key in config:
                    success.append(f"✓ Config has: {key}")
                else:
                    warnings.append(f"✗ Config missing: {key}")
            
            if config.get('use_api_validation') and not config.get('CLAUDE_API_KEY'):
                warnings.append("✗ API validation enabled but no API key set")
                
        except json.JSONDecodeError:
            errors.append("✗ config.json is not valid JSON")
    
    # 6. Check for required packages
    print("\n6. Checking Python packages...")
    required_packages = [
        'pandas', 'numpy', 'scipy', 'plotly'
    ]
    
    for package in required_packages:
        try:
            __import__(package)
            success.append(f"✓ Package installed: {package}")
        except ImportError:
            warnings.append(f"✗ Package missing: {package} (run: pip install -r requirements.txt)")
    
    # 7. Try minimal functionality test
    print("\n7. Testing basic functionality...")
    try:
        from config import MatchingConfig
        conf = MatchingConfig()
        success.append("✓ Configuration class works")
    except Exception as e:
        errors.append(f"✗ Configuration class failed: {e}")
    
    try:
        from hebrew_processor import HebrewProcessor
        hp = HebrewProcessor()
        test_text = "test לוח 1.1"
        result = hp.process_header(test_text)
        success.append("✓ Hebrew processor works")
    except Exception as e:
        warnings.append(f"✗ Hebrew processor failed: {e}")
    
    # Print summary
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    
    print(f"\n✓ SUCCESS ({len(success)} items):")
    for item in success[:5]:  # Show first 5
        print(f"  {item}")
    if len(success) > 5:
        print(f"  ... and {len(success)-5} more")
    
    if warnings:
        print(f"\n⚠ WARNINGS ({len(warnings)} items):")
        for item in warnings:
            print(f"  {item}")
    
    if errors:
        print(f"\n✗ ERRORS ({len(errors)} items):")
        for item in errors:
            print(f"  {item}")
    
    # Final verdict
    print("\n" + "="*60)
    if errors:
        print("❌ SETUP HAS CRITICAL ERRORS - Fix these before running main.py")
        return False
    elif warnings:
        print("⚠️  SETUP OK WITH WARNINGS - System should run but check warnings")
        return True
    else:
        print("✅ SETUP COMPLETE - Ready to run: python main.py")
        return True

if __name__ == "__main__":
    result = check_setup()
    sys.exit(0 if result else 1)
