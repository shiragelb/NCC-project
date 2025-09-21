# Chain Matching and Merging Implementation Plan

## Overview
This plan outlines the implementation of a system to intelligently merge chains of tables that cover complementary years and discuss the same subject matter, using Claude Sonnet 4 for semantic verification.

## Problem Statement
- Multiple chains exist covering different year ranges
- Some chains have gaps that could be filled by other chains
- Chains must be semantically similar to be merged (e.g., both about "children by religion" not mixing "poisoning incidents" with "traffic accidents")
- Goal: Create maximally complete time series by merging compatible chains

## Implementation Steps

### Step 1: Data Loading and Initial Setup
**Action:**
- Load the JSON file containing all chains
- Initialize Anthropic API client with Claude Sonnet 4
- Set up configuration parameters (similarity thresholds, etc.)
- Prepare logging and error handling

**Deliverable:**
- Loaded chains dictionary
- Configured API client
- Initialized data structures for processing

**Code Structure:**
```python
import json
import anthropic
from typing import Dict, List, Tuple

class ChainMatcher:
    def __init__(self, api_key: str, chains_file: str):
        self.client = anthropic.Anthropic(api_key=api_key)
        self.model = "claude-sonnet-4-20250514"
        self.chains = self.load_chains(chains_file)
        self.api_cache = {}  # Cache API responses
        self.merge_history = []
```

---

### Step 2: Build Comprehensive Year Coverage Analysis
**Action:**
- Create a mapping of all years across all chains
- Identify coverage gaps within each chain
- Calculate overlap and complementarity between chains
- Build a "completeness potential" matrix

**Algorithm:**
```python
def analyze_year_coverage(chains):
    coverage_map = {}
    for chain_id, chain_data in chains.items():
        years = chain_data['years']
        gaps = chain_data['gaps']
        coverage_map[chain_id] = {
            'min_year': min(years),
            'max_year': max(years),
            'covered_years': set(years),
            'gaps': set(gaps),
            'completeness': len(years) / (max(years) - min(years) + 1)
        }
    return coverage_map
```

**Deliverable:**
- Year coverage matrix
- Gap analysis report
- Completeness scores for each chain

---

### Step 3: Generate and Rank Chain Pair Candidates
**Action:**
- Identify all chain pairs where one could fill the other's gaps
- Calculate completeness score for potential merges
- Rank pairs by achievable completeness

**Scoring Formula:**
```
completeness_score = total_years_covered / (max_year - min_year + 1)
```

**Priority Criteria:**
1. Maximum completeness achieved (closest to 1.0)
2. Minimum remaining gaps after merge
3. Continuous year coverage preferred

**Algorithm:**
```python
def find_complementary_pairs(coverage_map):
    candidates = []
    for chain1_id in coverage_map:
        for chain2_id in coverage_map:
            if chain1_id >= chain2_id:  # Avoid duplicates
                continue
            
            # Check if they could complement each other
            combined_years = (coverage_map[chain1_id]['covered_years'] | 
                            coverage_map[chain2_id]['covered_years'])
            
            if len(combined_years) > max(len(coverage_map[chain1_id]['covered_years']),
                                        len(coverage_map[chain2_id]['covered_years'])):
                min_year = min(combined_years)
                max_year = max(combined_years)
                completeness = len(combined_years) / (max_year - min_year + 1)
                
                candidates.append({
                    'chain1': chain1_id,
                    'chain2': chain2_id,
                    'completeness': completeness,
                    'combined_years': combined_years,
                    'gaps_filled': len(combined_years) - len(coverage_map[chain1_id]['covered_years'])
                })
    
    return sorted(candidates, key=lambda x: x['completeness'], reverse=True)
```

**Deliverable:**
- Ranked list of candidate pairs with completeness scores

---

### Step 4: Semantic Similarity Verification via Claude Sonnet 4
**Action:**
- Extract representative headers from each chain
- Use Claude Sonnet 4 to verify semantic similarity
- Cache results to minimize API calls

**API Implementation:**
```python
def check_semantic_similarity(self, chain1_data, chain2_data):
    # Get representative headers (first, middle, last)
    headers1 = self.get_representative_headers(chain1_data)
    headers2 = self.get_representative_headers(chain2_data)
    
    # Check cache first
    cache_key = f"{chain1_data['id']}_{chain2_data['id']}"
    if cache_key in self.api_cache:
        return self.api_cache[cache_key]
    
    # API call
    response = self.client.messages.create(
        model=self.model,
        max_tokens=150,
        messages=[{
            "role": "user",
            "content": f"""Determine if these Hebrew table headers describe the same statistical dataset topic.
            
Consider the specific data being measured, not just the general subject area.
For example, "children poisoning incidents" vs "children traffic accidents" would be NO even though both involve children and accidents.

Headers from Chain 1:
{headers1[0]}

Headers from Chain 2:
{headers2[0]}

Answer YES if they measure the same specific statistical phenomenon.
Answer NO if they measure different phenomena.

Response format:
Answer: [YES/NO]
Topic: [Brief description in English of what's being measured]"""
        }]
    )
    
    # Parse and cache response
    result = self.parse_api_response(response.content)
    self.api_cache[cache_key] = result
    return result
```

**Cost Optimization:**
- Use representative headers only (not all headers)
- Cache all API responses
- Skip pairs with no year overlap potential
- Batch similar comparisons when possible

**Deliverable:**
- Filtered list of semantically compatible chain pairs
- API response cache for auditing

---

### Step 5: Chain Merging Algorithm
**Action:**
- Merge approved chain pairs
- Handle duplicate detection
- Update all metadata appropriately

**Merge Process:**
```python
def merge_chains(self, chain1, chain2):
    # Check for duplicate tables in same year
    years1 = set(zip(chain1['years'], chain1['tables']))
    years2 = set(zip(chain2['years'], chain2['tables']))
    
    for year in set(chain1['years']) & set(chain2['years']):
        tables1 = [t for y, t in years1 if y == year]
        tables2 = [t for y, t in years2 if y == year]
        if tables1[0] != tables2[0]:
            raise ValueError(f"Different tables for year {year}: {tables1[0]} vs {tables2[0]}")
    
    # Create merged chain
    merged = {
        'id': f"merged_{chain1['id']}_{chain2['id']}",
        'tables': chain1['tables'] + [t for t in chain2['tables'] if t not in chain1['tables']],
        'years': sorted(list(set(chain1['years'] + chain2['years']))),
        'headers': chain1['headers'] + chain2['headers'],
        'mask_references': chain1['mask_references'] + chain2['mask_references'],
        'status': 'merged',
        'gaps': self.calculate_gaps(merged_years),
        'similarities': chain1['similarities'] + chain2['similarities'],
        'api_validated': chain1['api_validated'] + chain2['api_validated'],
        'merge_history': [chain1['id'], chain2['id']]
    }
    
    return merged
```

**Deliverable:**
- Merged chain objects with complete metadata

---

### Step 6: Iterative Merging Process
**Action:**
- After initial merges, re-run steps 3-5 on the merged chains
- Continue until no more beneficial merges exist
- Track merge history for debugging

**Algorithm:**
```python
def iterative_merge(self):
    iteration = 0
    chains_to_process = self.chains.copy()
    
    while iteration < 10:  # Safety limit
        candidates = self.find_complementary_pairs(chains_to_process)
        merged_in_iteration = False
        
        for candidate in candidates:
            if candidate['completeness'] < 0.8:  # Threshold for merging
                break
                
            # Check semantic similarity
            if self.check_semantic_similarity(
                chains_to_process[candidate['chain1']], 
                chains_to_process[candidate['chain2']]
            ):
                merged = self.merge_chains(
                    chains_to_process[candidate['chain1']], 
                    chains_to_process[candidate['chain2']]
                )
                
                # Remove originals, add merged
                del chains_to_process[candidate['chain1']]
                del chains_to_process[candidate['chain2']]
                chains_to_process[merged['id']] = merged
                merged_in_iteration = True
                
                self.merge_history.append({
                    'iteration': iteration,
                    'merged': [candidate['chain1'], candidate['chain2']],
                    'completeness': candidate['completeness']
                })
        
        if not merged_in_iteration:
            break
            
        iteration += 1
    
    return chains_to_process
```

**Deliverable:**
- Maximally consolidated chain set
- Complete merge history log

---

### Step 7: Validation and Quality Checks
**Action:**
- Verify no year has multiple different tables
- Check gap calculations are correct
- Ensure all original tables are preserved
- Validate similarity scores make sense

**Validation Checks:**
```python
def validate_merged_chains(self, merged_chains, original_chains):
    # Check 1: No lost tables
    original_tables = set()
    for chain in original_chains.values():
        original_tables.update(chain['tables'])
    
    merged_tables = set()
    for chain in merged_chains.values():
        merged_tables.update(chain['tables'])
    
    assert original_tables == merged_tables, "Tables lost during merge"
    
    # Check 2: No duplicate tables in same year
    for chain in merged_chains.values():
        year_table_pairs = list(zip(chain['years'], chain['tables']))
        assert len(year_table_pairs) == len(set(year_table_pairs)), "Duplicate year-table pairs"
    
    # Check 3: Gap calculations
    for chain in merged_chains.values():
        calculated_gaps = self.calculate_gaps(chain['years'])
        assert calculated_gaps == chain['gaps'], f"Gap mismatch in {chain['id']}"
    
    return True
```

**Deliverable:**
- Validation report
- Error log if any issues found

---

### Step 8: Output Generation
**Action:**
- Generate new JSON with merged chains
- Include merge metadata
- Create summary report

**Output Format:**
```python
def generate_output(self, merged_chains, output_file):
    output = {
        'merged_chains': merged_chains,
        'metadata': {
            'merge_date': datetime.now().isoformat(),
            'api_model': self.model,
            'merge_history': self.merge_history,
            'statistics': {
                'original_chain_count': len(self.chains),
                'merged_chain_count': len(merged_chains),
                'total_api_calls': len(self.api_cache),
                'average_completeness': self.calculate_average_completeness(merged_chains)
            }
        }
    }
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    
    return output
```

**Deliverable:**
- Final JSON file with merged chains
- Merge summary report
- API usage statistics

---

## Error Handling

### API Errors
- Implement exponential backoff for rate limits
- Retry failed requests up to 3 times
- Log all API errors with context

### Data Validation Errors
- Check for missing required fields
- Validate year formats
- Handle malformed headers gracefully

### Merge Conflicts
- Log conflicts with detailed information
- Skip problematic merges rather than failing entirely
- Provide manual review list for conflicts

---

## Testing Strategy

### Unit Tests
- Test year coverage calculation
- Test completeness scoring
- Test merge logic with edge cases

### Integration Tests
- Test API integration with mock responses
- Test full merge pipeline with sample data

### Validation Tests
- Ensure no data loss
- Verify merge correctness
- Check output format compliance

---

## Performance Considerations

### API Optimization
- Estimated API calls: 500-1000 for ~100 chains
- Cost estimate: $2-5 total with Claude Sonnet 4
- Cache persistence between runs

### Memory Management
- Process chains in batches if dataset is large
- Clear intermediate data structures
- Use generators for large iterations

---

## Next Steps
1. Implement the ChainMatcher class
2. Run on sample data for testing
3. Process full dataset
4. Review merge results
5. Manual validation of edge cases
6. Generate final consolidated dataset