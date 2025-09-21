#!/usr/bin/env python3
"""
Chain Merger - Iteratively merge complementary chains with semantic similarity checking
Usage: python merge_chains_iterative.py --chapters 1 2 3 --output-dir results
"""
import json
import os
import sys
import argparse
from typing import Dict, List, Set, Tuple
from datetime import datetime
import numpy as np
import torch
from transformers import AutoTokenizer, AutoModel
import anthropic
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class IterativeChainMerger:
    def __init__(self, verbose=False, similarity_threshold=0.7):
        api_key = os.getenv("ANTHROPIC_API_KEY")
        if not api_key:
            raise ValueError("ANTHROPIC_API_KEY not found in .env file")
        
        self.client = anthropic.Anthropic(api_key=api_key)
        self.model = "claude-sonnet-4-20250514"
        self.api_cache = {}
        self.merge_history = []
        self.iteration_reports = []
        self.verbose = verbose
        self.total_api_calls = 0
        self.similarity_threshold = similarity_threshold
        self.pairs_pre_screened_out = 0
        
        # Initialize AlephBERT
        print("Loading AlephBERT model...")
        self.tokenizer = AutoTokenizer.from_pretrained('onlplab/alephbert-base')
        self.bert_model = AutoModel.from_pretrained('onlplab/alephbert-base')
        self.bert_model.eval()
        self.embeddings_cache = {}
        print("AlephBERT loaded successfully")
        
    def get_embedding(self, text: str) -> np.ndarray:
        """Get AlephBERT embedding for text"""
        if text in self.embeddings_cache:
            return self.embeddings_cache[text]
        
        # Tokenize and get embeddings
        with torch.no_grad():
            inputs = self.tokenizer(text, return_tensors='pt', truncation=True, 
                                   max_length=512, padding=True)
            outputs = self.bert_model(**inputs)
            # Use mean pooling of last hidden states
            embeddings = outputs.last_hidden_state.mean(dim=1).squeeze().numpy()
        
        self.embeddings_cache[text] = embeddings
        return embeddings
    
    def calculate_cosine_similarity(self, text1: str, text2: str) -> float:
        """Calculate cosine similarity between two texts using AlephBERT"""
        if not text1 or not text2:
            return 0.0
        
        emb1 = self.get_embedding(text1)
        emb2 = self.get_embedding(text2)
        
        # Cosine similarity
        dot_product = np.dot(emb1, emb2)
        norm1 = np.linalg.norm(emb1)
        norm2 = np.linalg.norm(emb2)
        
        if norm1 == 0 or norm2 == 0:
            return 0.0
        
        similarity = dot_product / (norm1 * norm2)
        return float(similarity)
        
    def load_chains_from_chapter(self, chapter_num: int) -> Dict:
        """Load chains from a specific chapter file"""
        filename = f"chains_chapter_{chapter_num}.json"
        if not os.path.exists(filename):
            raise FileNotFoundError(f"File {filename} not found")
            
        with open(filename, 'r', encoding='utf-8') as f:
            data = json.load(f)
            if self.verbose:
                print(f"✓ Loaded {len(data)} chains from chapter {chapter_num}")
            return data
    
    def load_multiple_chapters(self, chapter_nums: List[int]) -> Dict:
        """Load and combine chains from multiple chapters"""
        all_chains = {}
        for chapter_num in chapter_nums:
            chains = self.load_chains_from_chapter(chapter_num)
            # Prefix chain IDs with chapter number to avoid conflicts
            for chain_id, chain_data in chains.items():
                new_id = f"ch{chapter_num}_{chain_id}"
                chain_data['id'] = new_id
                chain_data['source_chapter'] = chapter_num
                all_chains[new_id] = chain_data
        return all_chains
    
    def analyze_year_coverage(self, chains: Dict) -> Dict:
        """Analyze year coverage for all chains"""
        coverage_map = {}
        for chain_id, chain_data in chains.items():
            years = chain_data['years']
            gaps = chain_data.get('gaps', [])
            coverage_map[chain_id] = {
                'min_year': min(years) if years else 0,
                'max_year': max(years) if years else 0,
                'covered_years': set(years),
                'gaps': set(gaps),
                'completeness': len(years) / (max(years) - min(years) + 1) if years else 0,
                'total_years': len(years)
            }
        return coverage_map
    
    def find_best_complement(self, chains: Dict) -> List:
        """Find complementary pairs that actually improve coverage, sorted by improvement"""
        coverage_map = self.analyze_year_coverage(chains)
        candidates = []
        
        chain_ids = list(chains.keys())
        for i, chain1_id in enumerate(chain_ids):
            for chain2_id in chain_ids[i+1:]:
                coverage1 = coverage_map[chain1_id]['covered_years']
                coverage2 = coverage_map[chain2_id]['covered_years']
                
                # Calculate combined coverage
                combined_years = coverage1 | coverage2
                
                # Calculate improvement - this is the KEY metric
                improvement = len(combined_years) - max(len(coverage1), len(coverage2))
                
                # Only include pairs that actually improve coverage
                if improvement > 0:  # Must add at least 1 new year
                    min_year = min(combined_years)
                    max_year = max(combined_years)
                    span = (max_year - min_year + 1)
                    completeness = len(combined_years) / span
                    
                    # Calculate other metrics
                    overlap = len(coverage1 & coverage2)
                    new_years_from_2 = len(coverage2 - coverage1)
                    new_years_from_1 = len(coverage1 - coverage2)
                    
                    candidates.append({
                        'chain1': chain1_id,
                        'chain2': chain2_id,
                        'completeness': completeness,
                        'combined_years': sorted(list(combined_years)),
                        'year_range': f"{min_year}-{max_year}",
                        'total_years': len(combined_years),
                        'improvement': improvement,
                        'overlap': overlap,
                        'new_years_added': improvement  # This is the actual number of new years
                    })
        
        # Sort by IMPROVEMENT first (how many new years gained), then by completeness
        return sorted(candidates, key=lambda x: (x['improvement'], x['completeness']), reverse=True)
    
    def get_representative_headers(self, chain_data: Dict) -> str:
        """Get a representative header from the chain"""
        headers = chain_data.get("headers", [])
        if headers:
            header = headers[0] if headers[0] else (headers[1] if len(headers) > 1 else "")
            if header:
                lines = header.split("\n")
                unique = []
                for line in lines:
                    if line.strip() and line.strip() not in unique:
                        unique.append(line.strip())
                        if len(unique) >= 3:
                            break
                return " | ".join(unique) if unique else header[:200]
        return ""
    
    def check_semantic_similarity(self, chain1_data: Dict, chain2_data: Dict) -> Tuple[bool, str]:
        """Check if two chains are about the same topic using Claude API"""
        header1 = self.get_representative_headers(chain1_data)
        header2 = self.get_representative_headers(chain2_data)
        
        if not header1 or not header2:
            return False, "Missing headers"
        
        # PRE-SCREEN with AlephBERT cosine similarity
        cosine_sim = self.calculate_cosine_similarity(header1, header2)
        
        if cosine_sim < self.similarity_threshold:
            self.pairs_pre_screened_out += 1
            if self.verbose:
                print(f"    Pre-screened out (cosine={cosine_sim:.3f} < {self.similarity_threshold})")
            return False, f"Low cosine similarity: {cosine_sim:.3f}"
        
        # Check cache
        cache_key = f"{chain1_data['id']}_{chain2_data['id']}"
        cache_key_rev = f"{chain2_data['id']}_{chain1_data['id']}"
        
        if cache_key in self.api_cache:
            return self.api_cache[cache_key]
        if cache_key_rev in self.api_cache:
            return self.api_cache[cache_key_rev]
        
        try:
            self.total_api_calls += 1
            response = self.client.messages.create(
                model=self.model,
                max_tokens=300,
                messages=[{
                    "role": "user",
                    "content": f"""Determine if these Hebrew table headers describe the same statistical dataset that continues across years.

Header 1: {header1[:500]}
Header 2: {header2[:500]}

Answer YES only if ALL conditions are met:
1. They measure the same core variable (e.g., both "children", both "births")
2. They use the same primary categorization (e.g., both "by religion", both "by district")
3. They use the same type of measurement:
   - Both percentages/rates (אחוז, שיעור) OR
   - Both absolute numbers (מספרים, אלפים)
   - Do NOT merge percentages with absolute numbers

Answer NO if:
- Different variables (children vs births)
- Different categorizations (by religion vs by district)  
- Different measurement types (percentages vs absolute numbers)
- One is data and the other is methodology notes

Answer: [YES/NO]
Brief reason: [One line explanation]"""
                }]
            )
            
            # Parse response
            response_text = response.content[0].text if hasattr(response.content[0], 'text') else str(response.content)
            is_similar = "YES" in response_text.upper()[:20]  # Check early in response
            
            # Extract reason if possible
            reason = "Semantic check"
            if "Brief reason:" in response_text:
                reason = response_text.split("Brief reason:")[-1].strip()[:100]
            elif "NO" in response_text.upper()[:20]:
                if "Different" in response_text:
                    reason = response_text.split("Different")[1].split("\n")[0][:100]
                else:
                    reason = "Different statistical measures"
            
            # Add cosine similarity to reason
            reason = f"{reason} (cosine={cosine_sim:.3f})"
            
            result = (is_similar, reason)
            
            # Cache result
            self.api_cache[cache_key] = result
            return result
            
        except Exception as e:
            print(f"API error: {e}")
            return False, f"API error: {str(e)}"
    
    def merge_chains(self, chain1: Dict, chain2: Dict) -> Dict:
        """Merge two chains into one"""
        # Combine years and tables, avoiding duplicates
        year_to_table = {}
        year_to_mask = {}
        year_to_header = {}
        
        # Add chain1 data
        for i, year in enumerate(chain1['years']):
            year_to_table[year] = chain1['tables'][i]
            year_to_mask[year] = chain1['mask_references'][i]
            if i < len(chain1['headers']):
                year_to_header[year] = chain1['headers'][i]
        
        # Add chain2 data (only for years not in chain1)
        for i, year in enumerate(chain2['years']):
            if year not in year_to_table:
                year_to_table[year] = chain2['tables'][i]
                year_to_mask[year] = chain2['mask_references'][i]
                if i < len(chain2['headers']):
                    year_to_header[year] = chain2['headers'][i]
        
        # Build sorted lists
        combined_years = sorted(year_to_table.keys())
        combined_tables = [year_to_table[y] for y in combined_years]
        combined_masks = [year_to_mask[y] for y in combined_years]
        combined_headers = [year_to_header.get(y, "") for y in combined_years]  # THIS IS THE FIX
        
        # Calculate gaps
        all_gaps = []
        if combined_years:
            for year in range(min(combined_years), max(combined_years) + 1):
                if year not in combined_years:
                    all_gaps.append(year)
        
        # Create merged chain
        merged = {
            'id': f"merged_{chain1['id']}_{chain2['id']}",
            'tables': combined_tables,
            'years': combined_years,
            'headers': combined_headers,  # Now preserves all headers in year order
            'mask_references': combined_masks,
            'status': 'merged',
            'gaps': all_gaps,
            'source_chains': [chain1['id'], chain2['id']],
            'source_chapters': list(set([
                chain1.get('source_chapter', 0),
                chain2.get('source_chapter', 0)
            ]))
        }
        
        return merged
    
    def iterative_merge(self, chains: Dict, max_iterations: int = 10) -> Tuple[Dict, List]:
        """Iteratively merge chains until no more beneficial merges exist"""
        working_chains = chains.copy()
        all_reports = []
        
        for iteration in range(max_iterations):
            print(f"\n{'='*60}")
            print(f"ITERATION {iteration + 1}")
            print(f"{'='*60}")
            print(f"Current chains: {len(working_chains)}")
            
            # Find candidates that actually improve coverage
            candidates = self.find_best_complement(working_chains)
            
            if not candidates:
                print("No more pairs that improve coverage found")
                break
            
            print(f"Found {len(candidates)} candidate pairs that improve coverage")
            print(f"Best improvement: {candidates[0]['improvement']} years" if candidates else "")
            
            # Try to merge candidates IN ORDER
            merged_count = 0
            iteration_merges = []
            already_merged = set()
            checked_count = 0
            pre_screened_count = 0
            
            for i, candidate in enumerate(candidates):
                # Skip if either chain was already merged this iteration
                if candidate['chain1'] in already_merged or candidate['chain2'] in already_merged:
                    continue
                    
                chain1 = working_chains.get(candidate['chain1'])
                chain2 = working_chains.get(candidate['chain2'])
                
                if not chain1 or not chain2:
                    continue
                
                checked_count += 1
                
                # Show progress periodically
                if checked_count % 10 == 0:
                    print(f"\nProgress: Checked {checked_count} candidates, found {merged_count} merges...")
                
                # Show details for candidates with good improvement
                if candidate['improvement'] >= 2 or self.verbose:
                    print(f"\nCandidate {i+1}: {candidate['chain1']} + {candidate['chain2']}")
                    print(f"  Improvement: +{candidate['improvement']} years, Completeness: {candidate['completeness']:.2%}")
                
                # Check semantic similarity (includes pre-screening)
                is_similar, reason = self.check_semantic_similarity(chain1, chain2)
                
                if is_similar:
                    print(f"  ✓ MATCH FOUND: {reason}")
                    
                    # Perform merge
                    merged = self.merge_chains(chain1, chain2)
                    
                    # Mark as merged and remove from working set
                    already_merged.add(candidate['chain1'])
                    already_merged.add(candidate['chain2'])
                    del working_chains[candidate['chain1']]
                    del working_chains[candidate['chain2']]
                    working_chains[merged['id']] = merged
                    
                    # Record merge
                    merge_record = {
                        'iteration': iteration + 1,
                        'candidate_position': i + 1,
                        'chain1': candidate['chain1'],
                        'chain2': candidate['chain2'],
                        'improvement': candidate['improvement'],
                        'completeness': candidate['completeness'],
                        'year_range': candidate['year_range'],
                        'total_years': candidate['total_years'],
                        'reason': reason
                    }
                    iteration_merges.append(merge_record)
                    merged_count += 1
                    
                    print(f"  → Merged! New range: {candidate['year_range']} ({candidate['total_years']} years)")
                elif "Low cosine similarity" in reason:
                    pre_screened_count += 1
                    if self.verbose:
                        print(f"  ✗ Pre-screened: {reason}")
                elif candidate['improvement'] >= 2:  # Only show rejection for good candidates
                    print(f"  ✗ No match: {reason[:80]}...")
            
            # Record iteration report
            iteration_report = {
                'iteration': iteration + 1,
                'candidates_available': len(candidates),
                'candidates_checked': checked_count,
                'pre_screened_out': pre_screened_count,
                'chains_at_start': len(working_chains) + merged_count * 2,
                'merges_performed': merged_count,
                'chains_remaining': len(working_chains),
                'api_calls_in_iteration': checked_count - pre_screened_count,
                'merges': iteration_merges
            }
            all_reports.append(iteration_report)
            
            print(f"\n{'='*40}")
            print(f"Iteration {iteration + 1} summary:")
            print(f"  - Candidates with improvement: {len(candidates)}")
            print(f"  - Candidates checked: {checked_count}")
            print(f"  - Pre-screened out: {pre_screened_count}")
            print(f"  - API calls made: {checked_count - pre_screened_count}")
            print(f"  - Valid merges found: {merged_count}")
            print(f"  - Chains remaining: {len(working_chains)}")
            
            if merged_count == 0:
                print("\nNo valid semantic matches found - stopping iterations")
                break
        
        return working_chains, all_reports
    
    def generate_report(self, original_chains: Dict, merged_chains: Dict, 
                       iteration_reports: List, output_dir: str, chapter_nums: List[int]):
        """Generate a detailed merge report"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        chapters_str = "_".join(map(str, chapter_nums))
        report_file = os.path.join(output_dir, f"merge_report_ch{chapters_str}_{timestamp}.json")
        
        # Calculate statistics
        original_years = set()
        merged_years = set()
        
        for chain in original_chains.values():
            original_years.update(chain['years'])
        
        for chain in merged_chains.values():
            merged_years.update(chain['years'])
        
        # Find successful merges
        successful_merges = []
        for chain_id, chain in merged_chains.items():
            if 'source_chains' in chain:
                successful_merges.append({
                    'merged_chain_id': chain_id,
                    'source_chains': chain['source_chains'],
                    'years': chain['years'],
                    'year_count': len(chain['years']),
                    'year_range': f"{min(chain['years'])}-{max(chain['years'])}"
                })
        
        report = {
            'metadata': {
                'timestamp': timestamp,
                'chapters_processed': chapter_nums,
                'api_model': self.model,
                'total_api_calls': self.total_api_calls,
                'similarity_threshold': self.similarity_threshold,
                'pairs_pre_screened_out': self.pairs_pre_screened_out
            },
            'summary': {
                'original_chains': len(original_chains),
                'final_chains': len(merged_chains),
                'chains_reduced': len(original_chains) - len(merged_chains),
                'reduction_percentage': ((len(original_chains) - len(merged_chains)) / len(original_chains) * 100) if len(original_chains) > 0 else 0,
                'successful_merges': len(successful_merges),
                'unique_years_before': len(original_years),
                'unique_years_after': len(merged_years),
                'year_coverage_maintained': len(merged_years) == len(original_years)
            },
            'successful_merges': successful_merges,
            'iteration_details': iteration_reports,
            'final_chains_summary': {}
        }
        
        # Add summary of final chains
        for chain_id, chain in merged_chains.items():
            report['final_chains_summary'][chain_id] = {
                'years': chain['years'],
                'year_count': len(chain['years']),
                'year_range': f"{min(chain['years'])}-{max(chain['years'])}" if chain['years'] else "N/A",
                'gaps': chain.get('gaps', []),
                'is_merged': 'source_chains' in chain,
                'source_chains': chain.get('source_chains', [chain_id])
            }
        
        # Write report
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        
        print(f"\n✓ Report saved to: {report_file}")
        
        # Print summary to console
        print("\n" + "="*60)
        print("MERGE SUMMARY")
        print("="*60)
        print(f"Chapters processed: {chapter_nums}")
        print(f"Original chains: {len(original_chains)}")
        print(f"Final chains: {len(merged_chains)}")
        print(f"Successful merges: {len(successful_merges)}")
        print(f"Reduction: {len(original_chains) - len(merged_chains)} chains ({report['summary']['reduction_percentage']:.1f}%)")
        print(f"Total API calls: {self.total_api_calls}")
        print(f"Pairs pre-screened out: {self.pairs_pre_screened_out}")
        print(f"AlephBERT threshold: {self.similarity_threshold}")
        print(f"Year coverage maintained: {report['summary']['year_coverage_maintained']}")
        
        if successful_merges:
            print("\nSuccessful merges:")
            for merge in successful_merges[:5]:  # Show first 5
                print(f"  • {merge['source_chains'][0]} + {merge['source_chains'][1]} → {merge['year_range']}")
            if len(successful_merges) > 5:
                print(f"  ... and {len(successful_merges) - 5} more")
        
        return report_file
    
    def save_merged_chains(self, merged_chains: Dict, output_dir: str, chapter_nums: List[int]):
        """Save the merged chains to a JSON file"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        chapters_str = "_".join(map(str, chapter_nums))
        output_file = os.path.join(output_dir, f"merged_chains_ch{chapters_str}_{timestamp}.json")
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(merged_chains, f, ensure_ascii=False, indent=2)
        
        # Save copy to merge_chains folder
        # Save copy to merge_chains folder
        if len(chapter_nums) == 1:
            alt_filename = f"chains_chapter_{chapter_nums[0]}.json"
        else:
            alt_filename = f"chains_chapter_{chapters_str}.json"
        alt_output_file = os.path.join("../../merge_chains", alt_filename)
        os.makedirs(os.path.dirname(alt_output_file), exist_ok=True)
        with open(alt_output_file, 'w', encoding='utf-8') as f:
            json.dump(merged_chains, f, ensure_ascii=False, indent=2)
        print(f"✓ Copy saved to: {alt_output_file}")    
        
        print(f"✓ Merged chains saved to: {output_file}")
        return output_file
    
    def process_chapters(self, chapter_nums: List[int], output_dir: str):
        """Main processing function"""
        # Create output directory if needed
        os.makedirs(output_dir, exist_ok=True)
        
        # Load chains
        if len(chapter_nums) == 1:
            print(f"\nProcessing chapter {chapter_nums[0]}...")
            original_chains = self.load_chains_from_chapter(chapter_nums[0])
        else:
            print(f"\nProcessing chapters {chapter_nums}...")
            original_chains = self.load_multiple_chapters(chapter_nums)
        
        print(f"Total chains loaded: {len(original_chains)}")
        print(f"AlephBERT similarity threshold: {self.similarity_threshold}")
        
        # Perform iterative merging
        merged_chains, iteration_reports = self.iterative_merge(original_chains)
        
        # Save results
        merged_file = self.save_merged_chains(merged_chains, output_dir, chapter_nums)
        report_file = self.generate_report(original_chains, merged_chains, 
                                          iteration_reports, output_dir, chapter_nums)
        
        return merged_file, report_file

def main():
    parser = argparse.ArgumentParser(
        description='Iteratively merge complementary chains with semantic similarity checking'
    )
    parser.add_argument(
        '--chapters', 
        nargs='+', 
        type=int, 
        required=True,
        help='Chapter number(s) to process (e.g., --chapters 1 or --chapters 1 2 3)'
    )
    parser.add_argument(
        '--output-dir', 
        type=str, 
        default='merged_results',
        help='Output directory for results (default: merged_results)'
    )
    parser.add_argument(
        '--threshold',
        type=float,
        default=0.7,
        help='AlephBERT cosine similarity threshold (0.0-1.0, default: 0.7)'
    )
    parser.add_argument(
        '--verbose', 
        action='store_true',
        help='Enable verbose output'
    )
    
    args = parser.parse_args()
    
    # Validate chapters
    for chapter in args.chapters:
        if chapter < 1 or chapter > 15:
            print(f"Error: Chapter {chapter} out of range (1-15)")
            sys.exit(1)
    
    # Validate threshold
    if not 0.0 <= args.threshold <= 1.0:
        print(f"Error: Threshold {args.threshold} out of range (0.0-1.0)")
        sys.exit(1)
    
    # Run merger
    try:
        merger = IterativeChainMerger(
            verbose=args.verbose,
            similarity_threshold=args.threshold
        )
        merged_file, report_file = merger.process_chapters(args.chapters, args.output_dir)
        
        print("\n" + "="*60)
        print("PROCESSING COMPLETE")
        print("="*60)
        print(f"Merged chains: {merged_file}")
        print(f"Report: {report_file}")
        
    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()