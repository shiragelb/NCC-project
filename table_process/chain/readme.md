# Table Chain Processing Pipeline

## Overview

This pipeline tracks and consolidates Hebrew statistical tables across multiple years (2001-2024), creating temporal chains that show how tables evolve over time. It consists of two sequential stages:

1. **Chain Creation** (`table-chain-matching/`): Tracks individual tables across years within each chapter
2. **Chain Merging** (`chain-api-expantion/`): Consolidates related chains that represent the same statistical data

## Quick Start

### Prerequisites

```bash
# Python 3.8+
pip install pandas numpy scipy
pip install sentence-transformers torch transformers
pip install anthropic python-dotenv
pip install plotly
```

### Basic Setup

1. **Configure Chain Creation** (`table-chain-matching/config.json`):
```json
{
    "tables_dir": "tables",
    "reference_json": "tables_summary.json",
    "similarity_threshold": 0.85,
    "use_api_validation": false
}
```

2. **Configure Chain Merging** (`chain-api-expantion/.env`):
```
ANTHROPIC_API_KEY=your_api_key_here
```

### Running the Pipeline

```bash
# Stage 1: Create chains from tables
cd table-chain-matching
python main.py

# Stage 2: Merge related chains
cd ../chain-api-expantion
python merge_chains_iterative.py --chapters 1 2 3 --threshold 0.7
```

## Directory Structure

```
chain/
├── table-chain-matching/          # Stage 1: Chain creation
│   ├── main.py                    # Run this first
│   ├── config.json                # Configuration
│   ├── tables_summary.json        # Required: Maps table IDs to Hebrew headers
│   ├── src/                       # Core modules
│   ├── tables/                    # Input CSVs (year/chapter structure)
│   └── output/                    # Processing logs
│
├── chain-api-expantion/           # Stage 2: Chain merging
│   ├── merge_chains_iterative.py  # Run this second
│   ├── chains_chapter_*.json      # Input from Stage 1
│   ├── .env                       # API configuration
│   └── merged_results/            # Consolidated outputs
│
└── validation/                    # Additional validation tools (see internal docs)
```

## Stage 1: Chain Creation

Creates chains tracking how each table evolves across years within its chapter.

**Required Input**:
- `tables_summary.json`: Maps table identifiers to their Hebrew headers
- Tables in CSV format (optional, for full processing)

**Run**:
```bash
cd table-chain-matching
python main.py
```

**Output**:
- Creates `chains_chapter_1.json` through `chains_chapter_15.json` in `../chain-api-expantion/`
- Each file contains chains showing table evolution from 2001-2024

## Stage 2: Chain Merging

Merges chains that represent the same statistical data but were split across chapters or years.

**Required Input**:
- Chain files from Stage 1
- Anthropic API key in `.env` file

**Run**:
```bash
cd chain-api-expantion

# Process specific chapters
python merge_chains_iterative.py --chapters 1 2 3

# Adjust similarity threshold if needed (default 0.7)
python merge_chains_iterative.py --chapters 1 --threshold 0.75
```

**Parameters**:
- `--chapters`: Which chapters to process (1-15)
- `--threshold`: Similarity threshold for merging (0.0-1.0)
- `--output-dir`: Where to save results (default: `merged_results`)
- `--verbose`: Show detailed progress

**Output**:
- `merged_results/merged_chains_ch*_timestamp.json`: Consolidated chains
- `merged_results/merge_report_ch*_timestamp.json`: Processing statistics
- Clean copies in `../../merge_chains/chains_chapter_*.json`

## Understanding the Output

### Chain Structure
```json
{
  "chain_id": {
    "id": "chain_1_01_2001",
    "tables": ["1_01_2001", "1_01_2002", ...],
    "years": [2001, 2002, ...],
    "headers": ["Column 1", "Column 2", ...],
    "gaps": [2005, 2007],
    "similarities": [0.95, 0.87, ...]
  }
}
```

### Key Fields
- `tables`: List of table IDs in the chain
- `years`: Years covered by the chain
- `headers`: Hebrew column headers (normalized)
- `gaps`: Years where table was missing
- `similarities`: Match confidence scores

## Input Data Requirements

### Tables Summary JSON
Must map table IDs to their headers:
```json
{
  "1_01_2001": ["header1", "header2", ...],
  "2_01_2001": ["header1", "header2", ...]
}
```

### Table Naming Convention
Tables should follow the pattern: `TABLE#_CHAPTER_YEAR.csv`
- Example: `1_01_2001.csv` (Table 1, Chapter 01, Year 2001)

## Common Use Cases

### Process Single Chapter
```bash
# Create chains for chapter 1
cd table-chain-matching
python main.py  # Processes all chapters by default

# Merge within chapter 1
cd ../chain-api-expantion
python merge_chains_iterative.py --chapters 1
```

### Process Multiple Chapters with Different Thresholds
```bash
# Stricter matching for chapters 1-5 (more similar content)
python merge_chains_iterative.py --chapters 1 2 3 4 5 --threshold 0.75

# Looser matching for chapters 6-10 (more varied content)
python merge_chains_iterative.py --chapters 6 7 8 9 10 --threshold 0.65
```

## Troubleshooting

### No `tables_summary.json`
This file is required and must contain the mapping of all table IDs to their Hebrew headers.

### API Key Issues
Ensure `.env` file exists in `chain-api-expantion/` with valid `ANTHROPIC_API_KEY`.

### No Merges Found
- Try lowering the threshold (e.g., 0.6 instead of 0.7)
- Check that chains have complementary year coverage
- Enable `--verbose` to see what's being compared

### Memory Issues
Process chapters individually rather than all at once.

## Notes

- The pipeline is optimized for Hebrew text processing using AlephBERT embeddings
- Stage 2 uses Claude API for semantic validation of uncertain matches
- Processing all 15 chapters typically takes 10-30 minutes depending on API usage
- The `validation/` folder contains additional tools for result validation (see documentation within)

## Output Locations

Final processed chains will be in:
1. `chain-api-expantion/merged_results/` - Timestamped results with reports
2. `../../merge_chains/` - Clean output files for downstream processing