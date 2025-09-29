# Chain Merger - Iterative Table Chain Consolidation

## What It Does

This tool merges complementary chains of statistical tables from Hebrew yearbooks. It finds chains that cover different years but measure the same statistical phenomenon, then combines them to create more complete time series.

## Requirements

```bash
pip install torch transformers anthropic python-dotenv numpy
```

Create a `.env` file with your Anthropic API key:
```
ANTHROPIC_API_KEY=your_key_here
```

## Input Files

Place chain JSON files in the working directory named as:
- `chains_chapter_1.json`
- `chains_chapter_2.json`
- etc.

## Usage

### Single Chapter
```bash
python merge_chains_iterative.py --chapters 1 --output-dir results
```

### Multiple Chapters
```bash
python merge_chains_iterative.py --chapters 1 2 3 --output-dir results
```

### Options
- `--chapters`: Chapter numbers to process (required)
- `--output-dir`: Where to save results (default: `merged_results`)
- `--threshold`: AlephBERT similarity threshold 0-1 (default: 0.7)
- `--verbose`: Show detailed progress

## How It Works

1. **Loads chains** from specified chapter files
2. **Identifies pairs** that could fill each other's year gaps
3. **Pre-screens** using AlephBERT Hebrew embeddings (cosine similarity)
4. **Validates** semantically similar pairs via Claude Sonnet 4 API
5. **Merges** validated pairs into single chains
6. **Iterates** until no more beneficial merges exist
7. **Saves** consolidated chains and detailed report

## Merge Criteria

Chains are merged only if they:
- Cover complementary years (filling gaps)
- Pass AlephBERT similarity threshold
- Are verified by Claude to measure the same statistical variable
- Use the same measurement type (percentages vs absolute numbers)

## Output

### Merged Chains File
`merged_chains_ch{X}_{timestamp}.json` - The consolidated chains

### Report File  
`merge_report_ch{X}_{timestamp}.json` - Contains:
- Number of original vs final chains
- List of successful merges
- API call statistics
- Complete merge history

### Alternative Location
A copy is also saved to `../../merge_chains/chains_chapter_{X}.json`

## Example Output Summary

```
MERGE SUMMARY
============================================================
Chapters processed: [1, 2, 3]
Original chains: 127
Final chains: 89
Successful merges: 38
Reduction: 38 chains (29.9%)
Total API calls: 142
Pairs pre-screened out: 287
AlephBERT threshold: 0.7
Year coverage maintained: True
```

## FUTURE WORK
This part was relatively budget heavy, around 50$ on API calls. we believe that with further time and budget, due to its itterative nature, this algorithm can keep improving the merging of small chains into bigger ones. 