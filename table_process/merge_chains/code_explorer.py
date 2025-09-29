#!/usr/bin/env python3
"""
Code Explorer: Analyzes project structure to identify potential redundancy
Run this from your merge_chains directory
"""

import os
import ast
import hashlib
from pathlib import Path
from collections import defaultdict
import json

def get_file_hash(filepath):
    """Calculate MD5 hash of file content"""
    try:
        with open(filepath, 'rb') as f:
            return hashlib.md5(f.read()).hexdigest()
    except:
        return None

def extract_python_structure(filepath):
    """Extract classes, functions, and imports from Python file"""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
        tree = ast.parse(content)
        
        structure = {
            'classes': [],
            'functions': [],
            'imports': []
        }
        
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef):
                structure['classes'].append(node.name)
            elif isinstance(node, ast.FunctionDef):
                if not node.name.startswith('_'):  # Skip private functions for brevity
                    structure['functions'].append(node.name)
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    structure['imports'].append(alias.name)
            elif isinstance(node, ast.ImportFrom):
                if node.module:
                    structure['imports'].append(node.module)
        
        return structure
    except:
        return None

def analyze_directory(root_path='.'):
    """Main analysis function"""
    root_path = Path(root_path)
    
    # Data collection structures
    file_hashes = defaultdict(list)
    file_sizes = {}
    python_structures = {}
    directory_tree = defaultdict(list)
    similar_names = defaultdict(list)
    
    # Patterns to skip
    skip_dirs = {'__pycache__', '.git', 'venv', 'env', '.pytest_cache', 'node_modules'}
    
    print("=" * 80)
    print("PROJECT STRUCTURE ANALYSIS")
    print("=" * 80)
    
    # 1. Build directory tree and collect file info
    print("\n1. DIRECTORY STRUCTURE:")
    print("-" * 40)
    
    for dirpath, dirnames, filenames in os.walk(root_path):
        # Skip unwanted directories
        dirnames[:] = [d for d in dirnames if d not in skip_dirs]
        
        rel_path = Path(dirpath).relative_to(root_path)
        level = len(rel_path.parts)
        
        # Print directory with indentation
        if str(rel_path) == '.':
            print("./")
        else:
            indent = "  " * (level - 1) + "├── "
            print(f"{indent}{rel_path.name}/")
        
        # Process files in this directory
        for filename in sorted(filenames):
            if filename.startswith('.'):
                continue
                
            filepath = Path(dirpath) / filename
            rel_filepath = filepath.relative_to(root_path)
            
            # Get file size
            try:
                size = os.path.getsize(filepath)
                file_sizes[str(rel_filepath)] = size
                size_str = f"{size:,} bytes"
            except:
                size_str = "N/A"
            
            # Calculate hash for duplicate detection
            file_hash = get_file_hash(filepath)
            if file_hash:
                file_hashes[file_hash].append(str(rel_filepath))
            
            # Extract Python structure
            if filename.endswith('.py'):
                structure = extract_python_structure(filepath)
                if structure:
                    python_structures[str(rel_filepath)] = structure
            
            # Group similar filenames
            base_name = filename.rsplit('.', 1)[0] if '.' in filename else filename
            similar_names[base_name].append(str(rel_filepath))
            
            # Print file
            indent = "  " * level + "├── "
            print(f"{indent}{filename} ({size_str})")
    
    # 2. Identify duplicate files
    print("\n2. DUPLICATE FILES (same content):")
    print("-" * 40)
    duplicates_found = False
    for file_hash, files in file_hashes.items():
        if len(files) > 1:
            duplicates_found = True
            print(f"Hash {file_hash[:8]}...:")
            for f in files:
                size = file_sizes.get(f, 0)
                print(f"  - {f} ({size:,} bytes)")
    if not duplicates_found:
        print("No exact duplicates found")
    
    # 3. Similar named files
    print("\n3. SIMILARLY NAMED FILES:")
    print("-" * 40)
    similar_found = False
    for base_name, files in similar_names.items():
        if len(files) > 1:
            similar_found = True
            print(f"'{base_name}' pattern:")
            for f in files:
                print(f"  - {f}")
    if not similar_found:
        print("No similar names found")
    
    # 4. Backup and temp directories
    print("\n4. BACKUP/TEMP DIRECTORIES:")
    print("-" * 40)
    backup_dirs = []
    for dirpath, dirnames, filenames in os.walk(root_path):
        for dirname in dirnames:
            if any(pattern in dirname.lower() for pattern in ['backup', 'bak', 'old', 'temp', 'tmp', 'archive']):
                rel_path = Path(dirpath, dirname).relative_to(root_path)
                backup_dirs.append(str(rel_path))
    
    if backup_dirs:
        for d in backup_dirs:
            # Count files in backup dir
            backup_path = root_path / d
            file_count = sum(1 for _ in backup_path.rglob('*') if _.is_file())
            print(f"  - {d}/ ({file_count} files)")
    else:
        print("No backup/temp directories found")
    
    # 5. Python module analysis
    print("\n5. PYTHON MODULE OVERVIEW:")
    print("-" * 40)
    
    # Main entry points
    entry_points = ['main_pipeline.py', 'run_pipeline.py', 'test_setup.py']
    print("Potential entry points:")
    for ep in entry_points:
        if ep in python_structures:
            struct = python_structures[ep]
            print(f"\n  {ep}:")
            if struct['classes']:
                print(f"    Classes: {', '.join(struct['classes'][:5])}")
            if struct['functions']:
                print(f"    Main functions: {', '.join(struct['functions'][:5])}")
    
    # Core modules
    print("\n\nCore modules:")
    core_modules = ['merger_engine.py', 'chain_loader.py', 'column_matcher.py', 
                    'embeddings_handler.py', 'output_generator.py', 'table_normalizer.py']
    for module in core_modules:
        if module in python_structures:
            struct = python_structures[module]
            print(f"\n  {module}:")
            if struct['classes']:
                print(f"    Classes: {', '.join(struct['classes'])}")
            if struct['functions']:
                funcs = [f for f in struct['functions'] if not f.startswith('test_')]
                print(f"    Key functions: {', '.join(funcs[:5])}")
    
    # 6. Check for redundant imports
    print("\n6. IMPORT ANALYSIS:")
    print("-" * 40)
    import_count = defaultdict(int)
    for filepath, structure in python_structures.items():
        if structure:
            for imp in structure['imports']:
                import_count[imp] += 1
    
    # Show most common imports
    common_imports = sorted(import_count.items(), key=lambda x: x[1], reverse=True)[:10]
    print("Most imported modules:")
    for module, count in common_imports:
        print(f"  - {module}: used in {count} files")
    
    # 7. Config files analysis
    print("\n7. CONFIGURATION FILES:")
    print("-" * 40)
    config_files = []
    for filepath in Path(root_path).rglob('*'):
        if filepath.is_file():
            name = filepath.name
            if any(pattern in name.lower() for pattern in ['config', 'settings', '.json', '.yaml', '.yml', '.ini']):
                rel_path = filepath.relative_to(root_path)
                size = file_sizes.get(str(rel_path), 0)
                config_files.append((str(rel_path), size))
    
    if config_files:
        for cf, size in config_files:
            print(f"  - {cf} ({size:,} bytes)")
    else:
        print("No configuration files found")
    
    # 8. Test files
    print("\n8. TEST FILES:")
    print("-" * 40)
    test_files = []
    for filepath, structure in python_structures.items():
        if 'test' in filepath.lower() or filepath.startswith('tests/'):
            test_files.append(filepath)
    
    if test_files:
        for tf in test_files:
            print(f"  - {tf}")
    else:
        print("No test files found")
    
    # 9. Summary statistics
    print("\n" + "=" * 80)
    print("SUMMARY STATISTICS:")
    print("=" * 80)
    total_files = len(file_sizes)
    total_size = sum(file_sizes.values())
    py_files = len(python_structures)
    
    print(f"Total files: {total_files}")
    print(f"Total size: {total_size:,} bytes ({total_size/1024/1024:.2f} MB)")
    print(f"Python files: {py_files}")
    print(f"Backup directories: {len(backup_dirs)}")
    
    # Check for potential redundancy indicators
    print("\n" + "=" * 80)
    print("REDUNDANCY INDICATORS:")
    print("=" * 80)
    
    redundancy_score = 0
    indicators = []
    
    if len(backup_dirs) > 0:
        redundancy_score += len(backup_dirs) * 2
        indicators.append(f"Found {len(backup_dirs)} backup/temp directories")
    
    if duplicates_found:
        dup_count = sum(1 for files in file_hashes.values() if len(files) > 1)
        redundancy_score += dup_count * 3
        indicators.append(f"Found {dup_count} sets of duplicate files")
    
    # Check for multiple entry points
    existing_entry_points = [ep for ep in entry_points if ep in python_structures]
    if len(existing_entry_points) > 1:
        redundancy_score += 2
        indicators.append(f"Multiple entry points: {', '.join(existing_entry_points)}")
    
    # Check for similar functionality files
    if 'config.py' in python_structures and Path('config').exists():
        redundancy_score += 2
        indicators.append("Both config.py and config/ directory exist")
    
    print(f"Redundancy Score: {redundancy_score}/10")
    for ind in indicators:
        print(f"  ⚠ {ind}")
    
    if redundancy_score > 5:
        print("\n🔴 High redundancy detected - significant cleanup opportunity")
    elif redundancy_score > 2:
        print("\n🟡 Moderate redundancy detected - some cleanup recommended")
    else:
        print("\n🟢 Low redundancy - codebase is relatively clean")

if __name__ == "__main__":
    print("Starting code exploration for redundancy analysis...")
    print("This may take a moment...\n")
    analyze_directory('.')
    print("\n" + "=" * 80)
    print("Analysis complete!")
    print("=" * 80)
