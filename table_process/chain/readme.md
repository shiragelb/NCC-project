# Table Chain Processing Pipeline

## Overview

This pipeline processes Hebrew statistical tables across multiple years, creating temporal chains and merging related datasets. The system consists of two main stages:

1. **Chain Creation** (`table-chain-matching/`): Builds chains from tables across years
2. **Chain Merging** (`chain-api-expantion/`): Merges complementary chains using semantic validation

## Directory Structure

```
chain/
├── table-chain-matching/     # Stage 1: Chain creation
│   ├── src/                  # Core modules
│   ├── main.py               # Main pipeline
│   └── tables_summary.json   # Required input
│
└── chain-api-expantion/       # Stage 2: Chain merging
    ├── chains_chapter_*.json  # Input chains (from Stage 1)
    ├── merge_chains_iterative.py
    └── merged_results/        # Merge outputs
```

## Pipeline Workflow

### Stage 1: Chain Creation

**Location**: `table-chain-matching/`

**Purpose**: Creates chains of related tables across years (2001-2024) for each chapter.

**Input Requirements**:
- `tables_summary.json`: Mapping of table identifiers to headers
- Table CSV files in `tables/year/chapter/` structure (optional)

**Process**:
```bash
cd table-chain-matching
python main.py
```

**Outputs**: 
- `../chain-api-expantion/chains_chapter_1.json` through `chains_chapter_15.json`
- Each file contains chains tracking table evolution across years

**Key Features**:
- Hebrew text normalization
- AlephBERT embeddings for semantic similarity
- Hungarian algorithm for optimal matching
- Gap handling for missing years
- Split/merge detection

### Stage 2: Chain Merging

**Location**: `chain-api-expantion/`

**Purpose**: Merges complementary chains that represent the same dataset split across chapters.

**Input Requirements**:
- Chain files from Stage 1 (`chains_chapter_*.json`)
- `.env` file with `ANTHROPIC_API_KEY`
- AlephBERT model (auto-downloads)

**Process**:
```bash
cd chain-api-expantion

# Single chapter
python merge_chains_iterative.py --chapters 1

# Multiple chapters
python merge_chains_iterative.py --chapters 1 2 3 --threshold 0.7
```

**Parameters**:
- `--chapters`: Chapter numbers to process (1-15)
- `--threshold`: Cosine similarity threshold (0.0-1.0, default 0.7)
- `--output-dir`: Output directory (default: merged_results)
- `--verbose`: Enable detailed output

**Outputs**:
- `merged_results/merged_chains_ch*_timestamp.json`: Timestamped results
- `merged_results/merge_report_ch*_timestamp.json`: Processing report
- `../../merge_chains/chains_chapter_*.json`: Clean output for downstream use

## Installation

### Prerequisites
```bash
# Python 3.8+
pip install pandas numpy scipy
pip install sentence-transformers torch transformers
pip install anthropic python-dotenv
pip install plotly networkx
```

### Setup

1. **Clone and setup Stage 1**:
```bash
cd table-chain-matching
pip install -r requirements.txt
```

2. **Configure Stage 1**:
Edit `config.json`:
```json
{
    "tables_dir": "/path/to/tables",
    "similarity_threshold": 0.78,
    "use_api_validation": false
}
```

3. **Setup Stage 2**:
Create `.env` in `chain-api-expantion/`:
```
ANTHROPIC_API_KEY=your_api_key_here
```

## Complete Pipeline Example

```bash
# Stage 1: Create chains
cd table-chain-matching
python main.py
# Creates: ../chain-api-expantion/chains_chapter_1.json ... chains_chapter_15.json

# Stage 2: Merge chains
cd ../chain-api-expantion

# Merge within single chapter
python merge_chains_iterative.py --chapters 1

# Merge across multiple chapters
python merge_chains_iterative.py --chapters 1 2 3 --threshold 0.75

# Results saved to:
# - merged_results/ (timestamped files)
# - ../../merge_chains/ (clean output)
```

## Output Files

### Chain Format
```json
{
  "chain_id": {
    "id": "chain_1_01_2001",
    "tables": ["1_01_2001", "1_01_2002", ...],
    "years": [2001, 2002, ...],
    "headers": ["header1", "header2", ...],
    "status": "active|dormant|merged",
    "gaps": [2005, 2007],
    "similarities": [0.95, 0.87, ...]
  }
}
```

### Merge Report
- Original vs final chain count
- Successful merges with year ranges
- API calls and pre-screening statistics
- Year coverage analysis

## Key Algorithms

1. **Similarity Matching**: Cosine similarity with AlephBERT embeddings
2. **Hungarian Algorithm**: Optimal bipartite matching
3. **Gap Handling**: Dormancy and reactivation logic
4. **Semantic Validation**: Claude API for uncertain matches
5. **Iterative Merging**: Greedy optimization by coverage improvement

## Troubleshooting

### Common Issues

1. **Missing tables_summary.json**:
   - Required for Stage 1
   - Maps table IDs to Hebrew headers

2. **API Key errors**:
   - Ensure `.env` file exists with valid `ANTHROPIC_API_KEY`
   - Check API quota limits

3. **Memory issues**:
   - Process chapters individually
   - Increase threshold to reduce API calls

4. **No merges found**:
   - Lower similarity threshold
   - Check if chains have complementary years

### Performance Tips

- Process chapters 1-5 separately from 6-15 (different content types)
- Use threshold 0.7-0.75 for balanced precision/recall
- Enable verbose mode for debugging

## Data Flow

```
tables_summary.json → Chain Creation → chains_chapter_*.json
                           ↓
                    Chain Merging (API validation)
                           ↓
                    merged_chains_*.json
                           ↓
                    Final output in merge_chains/
```

## License

MIT

## Support

For issues or questions, check:
- Chain creation logs in `table-chain-matching/output/`
- Merge reports in `chain-api-expantion/merged_results/`
- API usage in merge reports for cost tracking
