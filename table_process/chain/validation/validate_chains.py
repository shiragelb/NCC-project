#!/usr/bin/env python3
"""
Interactive Chain Validation Tool - Sample 40 chains
Run from directory containing chains_chapter_*.json files
"""

import json
import random
import glob
import os
from typing import Dict, List, Tuple, Any
import math

class ChainValidator:
    def __init__(self, target_samples=40):
        self.chains_data = {}
        self.clean_chains = []
        self.contaminated_chains = []
        self.sampled_chains = set()
        self.target_samples = target_samples
        self.load_chains()
    
    def load_chains(self):
        """Load all chain JSON files from current directory"""
        chain_files = glob.glob("chains_chapter_*.json")
        
        if not chain_files:
            print("Error: No chains_chapter_*.json files found in current directory!")
            exit(1)
        
        print(f"Loading {len(chain_files)} chapter files...")
        
        for filepath in sorted(chain_files):
            # Extract chapter number from filename
            chapter_num = filepath.replace("chains_chapter_", "").replace(".json", "")
            
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.chains_data[int(chapter_num)] = data
                    print(f"  Loaded Chapter {chapter_num}: {len(data)} chains")
            except Exception as e:
                print(f"  Error loading {filepath}: {e}")
        
        total_chains = sum(len(chains) for chains in self.chains_data.values())
        print(f"\nTotal chapters loaded: {len(self.chains_data)}")
        print(f"Total chains available: {total_chains}")
        print(f"Target samples: {self.target_samples}")
        print("-" * 60)
    
    def get_random_chain(self) -> Tuple[int, str, Any]:
        """Get a random chain that hasn't been sampled yet"""
        available_chains = []
        
        for chapter_num, chains in self.chains_data.items():
            for chain_id, chain_data in chains.items():
                if (chapter_num, chain_id) not in self.sampled_chains:
                    available_chains.append((chapter_num, chain_id, chain_data))
        
        if not available_chains:
            print("\nNo more chains available for sampling!")
            return None, None, None
        
        chapter, chain_id, chain_data = random.choice(available_chains)
        self.sampled_chains.add((chapter, chain_id))
        
        return chapter, chain_id, chain_data
    
    def display_chain(self, chapter: int, chain_id: str, chain_data: Any):
        """Display chain information"""
        print(f"\n{'='*60}")
        print(f"Chain Sample: Chapter {chapter}, Chain {chain_id}")
        print(f"{'='*60}")
        
        # Handle different possible structures
        tables = []
        
        # If chain_data is a list of tables
        if isinstance(chain_data, list):
            for table_info in chain_data:
                if isinstance(table_info, dict):
                    year = table_info.get('year', 'Unknown')
                    # Try different possible field names for header
                    header = (table_info.get('header_text') or 
                             table_info.get('table_name') or 
                             table_info.get('header') or 
                             table_info.get('name') or 
                             'No header')
                    tables.append((year, header))
        
        # If chain_data is a dict with 'tables' key
        elif isinstance(chain_data, dict):
            if 'tables' in chain_data:
                # Check if tables is a list or dict
                if isinstance(chain_data['tables'], list):
                    # Check if we have parallel lists structure
                    if 'years' in chain_data and 'headers' in chain_data:
                        # Parallel lists structure
                        table_ids = chain_data['tables']
                        years = chain_data.get('years', [])
                        headers = chain_data.get('headers', [])
                        
                        # Zip them together
                        for i, table_id in enumerate(table_ids):
                            year = years[i] if i < len(years) else 'Unknown'
                            header = headers[i] if i < len(headers) else f'Table {table_id}'
                            tables.append((year, header))
                    else:
                        # Tables list contains dict objects
                        for table_info in chain_data['tables']:
                            if isinstance(table_info, dict):
                                year = table_info.get('year', 'Unknown')
                                header = (table_info.get('header_text') or 
                                         table_info.get('table_name') or 
                                         table_info.get('header') or 
                                         table_info.get('name') or 
                                         'No header')
                                tables.append((year, header))
                elif isinstance(chain_data['tables'], dict):
                    for table_id, table_info in chain_data['tables'].items():
                        year = table_info.get('year', 'Unknown')
                        header = (table_info.get('header_text') or 
                                 table_info.get('table_name') or 
                                 table_info.get('header') or 
                                 'No header')
                        tables.append((year, header))
            else:
                # Try to extract tables from the dictionary itself
                # Each key might be a table ID with table info as value
                for key, value in chain_data.items():
                    if isinstance(value, dict):
                        # Check if this looks like table data
                        if 'year' in value or 'header_text' in value or 'table_name' in value:
                            year = value.get('year', 'Unknown')
                            header = (value.get('header_text') or 
                                     value.get('table_name') or 
                                     value.get('header') or 
                                     value.get('name') or 
                                     f'Table {key}')
                            tables.append((year, header))
                        # Maybe nested structure with table info
                        elif 'table' in value or 'data' in value:
                            nested = value.get('table') or value.get('data')
                            if isinstance(nested, dict):
                                year = nested.get('year', value.get('year', 'Unknown'))
                                header = (nested.get('header_text') or 
                                         nested.get('table_name') or 
                                         nested.get('header') or 
                                         f'Table {key}')
                                tables.append((year, header))
        
        # Sort tables by year
        tables.sort(key=lambda x: x[0] if isinstance(x[0], (int, float)) else 9999)
        
        print(f"Headers in chain ({len(tables)} tables):")
        print("-" * 60)
        for year, header in tables:
            # Truncate very long headers for display
            if len(str(header)) > 100:
                header = str(header)[:97] + "..."
            print(f"{year} - {header}")
        
        if len(tables) == 0:
            print("WARNING: No tables found in this chain!")
            print("This might be an error in the chain data structure.")
        
        print("-" * 60)
    
    def calculate_confidence_interval(self, successes: int, total: int) -> Tuple[float, float]:
        """Calculate 95% confidence interval using Wilson score interval"""
        if total == 0:
            return 0.0, 1.0
        
        p_hat = successes / total
        z = 1.96  # 95% confidence
        
        # Wilson score interval
        denominator = 1 + z**2 / total
        center = (p_hat + z**2 / (2 * total)) / denominator
        margin = z * math.sqrt((p_hat * (1 - p_hat) + z**2 / (4 * total)) / total) / denominator
        
        lower = max(0, center - margin)
        upper = min(1, center + margin)
        
        return lower * 100, upper * 100
    
    def display_statistics(self):
        """Display current statistics"""
        total_sampled = len(self.clean_chains) + len(self.contaminated_chains)
        
        if total_sampled == 0:
            print("\nNo chains validated yet.")
            return
        
        clean_count = len(self.clean_chains)
        contaminated_count = len(self.contaminated_chains)
        clean_rate = (clean_count / total_sampled) * 100
        
        ci_lower, ci_upper = self.calculate_confidence_interval(clean_count, total_sampled)
        
        print(f"\n{'='*60}")
        print("Current Statistics:")
        print(f"  Clean chains: {clean_count}")
        print(f"  Chains with false positives: {contaminated_count}")
        print(f"  Total validated: {total_sampled}/{self.target_samples}")
        print(f"  Clean rate: {clean_rate:.1f}%")
        print(f"  95% Confidence Interval: [{ci_lower:.1f}%, {ci_upper:.1f}%]")
        
        # Calculate standard error
        if total_sampled > 1:
            p = clean_count / total_sampled
            se = math.sqrt(p * (1 - p) / total_sampled) * 100
            print(f"  Standard Error: {se:.2f}%")
        
        print(f"{'='*60}")
    
    def run(self):
        """Main interaction loop"""
        print("\nChain Validation Tool")
        print("Instructions:")
        print("  - Review each chain's headers")
        print("  - Answer 'y' if chain contains any false positives")
        print("  - Answer 'n' if chain is clean (no false positives)")
        print("  - Type 'skip' to skip current chain")
        print("  - Type 'stats' to see statistics only")
        print("  - Type 'quit' to exit")
        print("-" * 60)
        
        validated_count = 0
        
        while validated_count < self.target_samples:
            chapter, chain_id, chain_data = self.get_random_chain()
            
            if chapter is None:
                print("\nNo more chains to sample!")
                break
            
            self.display_chain(chapter, chain_id, chain_data)
            
            print(f"\n[Progress: {validated_count}/{self.target_samples}]")
            
            while True:
                response = input("Does this chain contain any false positives? (y/n/skip/stats/quit): ").strip().lower()
                
                if response == 'quit' or response == 'q':
                    print("\nExiting...")
                    self.display_statistics()
                    self.save_results()
                    return
                
                elif response == 'stats' or response == 's':
                    self.display_statistics()
                    # Don't break, let user still answer for this chain
                    continue
                
                elif response == 'skip':
                    print("Skipping this chain...")
                    self.sampled_chains.remove((chapter, chain_id))  # Allow resampling
                    break
                
                elif response == 'n' or response == 'no':
                    self.clean_chains.append((chapter, chain_id))
                    validated_count += 1
                    print("✓ Marked as CLEAN chain")
                    self.display_statistics()
                    break
                
                elif response == 'y' or response == 'yes':
                    self.contaminated_chains.append((chapter, chain_id))
                    validated_count += 1
                    print("✗ Marked as chain with FALSE POSITIVES")
                    self.display_statistics()
                    break
                
                else:
                    print("Invalid response. Please enter y/n/skip/stats/quit")
        
        print(f"\n{'='*60}")
        print(f"Completed {self.target_samples} validations!")
        self.display_statistics()
        self.save_results()
    
    def save_results(self):
        """Save validation results to file"""
        if len(self.clean_chains) + len(self.contaminated_chains) == 0:
            return
        
        results = {
            'clean_chains': [{'chapter': c, 'chain_id': i} for c, i in self.clean_chains],
            'contaminated_chains': [{'chapter': c, 'chain_id': i} for c, i in self.contaminated_chains],
            'statistics': {
                'total_sampled': len(self.clean_chains) + len(self.contaminated_chains),
                'clean_count': len(self.clean_chains),
                'contaminated_count': len(self.contaminated_chains),
                'clean_rate': len(self.clean_chains) / (len(self.clean_chains) + len(self.contaminated_chains)) if (len(self.clean_chains) + len(self.contaminated_chains)) > 0 else 0
            }
        }
        
        with open('validation_results.json', 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        
        print(f"Results saved to validation_results.json")

if __name__ == "__main__":
    validator = ChainValidator(target_samples=40)
    validator.run()
