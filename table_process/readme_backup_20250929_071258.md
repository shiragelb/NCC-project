# Hebrew Statistical Table Processing Pipeline

A comprehensive system for extracting, tracking, and merging Hebrew statistical tables from multi-year government reports (2001-2024) to create continuous temporal datasets.

## 🎯 Overview

This pipeline processes Hebrew statistical documents organized by year and chapter, extracting tables from Word documents, creating temporal chains that track how tables evolve across years, and merging related datasets to produce complete time series data.

## 📊 Pipeline Architecture

The system consists of three main stages:

```
Stage 1: EXTRACT TABLES (extract_tables/)
  ↓ Extracts tables from Word docs → CSV files + metadata
Stage 2A: CREATE CHAINS (chain/table-chain-matching/)
  ↓ Matches tables across years → temporal chains
Stage 2B: MERGE CHAINS (chain/chain-api-expantion/)
  ↓ Combines complementary chains → consolidated chains
Stage 3: FINAL PROCESSING (merge_chains/)
  ↓ Aligns columns & normalizes → final datasets
```

## 📁 Complete Data Flow & File Locations

### 🔵 Stage 1: Table Extraction (`extract_tables/`)

**What it does:** Downloads Word documents from Google Drive and extracts all tables with their Hebrew headers.

**Input Required:**
- Google Drive folder ID containing Word documents organized as:
  ```
  Drive Folder/
  ├── 2021/
  │   ├── 01_chapter_name.docx
  │   ├── 02_chapter_name.docx
  │   └── ...
  └── 2022/
      └── ...
  ```

**What gets saved where:**
```
chain/table-chain-matching/
├── tables/                           # Main output directory
│   ├── 2001/
│   │   ├── 01/                      # Chapter 01
│   │   │   ├── 1_01_2001.csv       # Table 1, Chapter 1, Year 2001
│   │   │   ├── 2_01_2001.csv       # Table 2, Chapter 1, Year 2001
│   │   │   └── summaries.json      # Chapter-level summary
│   │   ├── 02/                      # Chapter 02
│   │   │   ├── 1_02_2001.csv
│   │   │   └── ...
│   │   └── ...
│   ├── 2002/
│   └── ... (all years)
│
├── tables_summary.json               # CRITICAL FILE: Maps all table IDs to Hebrew headers
│                                    # Format: {"1_01_2001": "לוח 1.1 - אוכלוסייה", ...}
│
└── tables_stats.csv                 # Statistics: count of tables per chapter/year
```

### 🔵 Stage 2A: Chain Creation (`chain/table-chain-matching/`)

**What it does:** Analyzes tables across all years to create temporal chains - tracking how each table evolves, splits, or merges over time.

**Input Required:**
- `tables_summary.json` (from Stage 1)
- Table CSV files in `tables/` directory (optional, for validation)

**What gets saved where:**
```
chain/
├── table-chain-matching/
│   ├── output/                       # General outputs
│   ├── chain_storage/               # Embeddings and checkpoints
│   └── output_chapter_X/            # Per-chapter results
│       ├── chains_chapter_X.json    # Chain definitions
│       ├── report_chapter_X.html    # Visual report
│       └── sankey_chapter_X.html    # Flow diagram
│
└── chain-api-expantion/              # Output location
    ├── chains_chapter_1.json        # MAIN OUTPUT: Chains for chapter 1
    ├── chains_chapter_2.json        # Chains for chapter 2
    └── ... (chapters 1-15)          # Each file contains all chains for that chapter
```

**Chain file structure:**
```json
{
  "chain_1_01_2001": {
    "id": "chain_1_01_2001",
    "tables": ["1_01_2001", "1_01_2002", "1_01_2003", ...],
    "years": [2001, 2002, 2003, ...],
    "headers": ["לוח 1.1 - אוכלוסייה לפי מחוז", ...],
    "gaps": [2005, 2007],  // Missing years
    "status": "active",
    "similarities": [0.95, 0.87, ...]
  }
}
```

### 🔵 Stage 2B: Chain Merging (`chain/chain-api-expantion/`)

**What it does:** Merges complementary chains that represent the same dataset split across chapters or time periods. Uses Claude API for semantic validation.

**Input Required:**
- Chain files from Stage 2A (`chains_chapter_*.json`)
- `.env` file with `ANTHROPIC_API_KEY`

**What gets saved where:**
```
chain/chain-api-expantion/
├── INPUT FILES (from Stage 2A):
│   ├── chains_chapter_1.json
│   └── ...
│
├── merged_results/                   # Timestamped outputs
│   ├── merged_chains_ch1_20240315_143022.json   # Merged chains with timestamp
│   ├── merge_report_ch1_20240315_143022.json    # Processing statistics
│   └── ...
│
└── FINAL OUTPUT (copied to):
    ../../merge_chains/               # Clean output for Stage 3
    ├── chains_chapter_1.json        # Final merged chains
    └── ...
```

### 🔵 Stage 3: Final Processing (`merge_chains/`)

**What it does:** Takes the merged chains and original table data to create final, column-aligned datasets with temporal tracking.

**Input Required:**
- Merged chain files from Stage 2B
- Original table CSV files from Stage 1
- Optional: mask files for data validation

**What gets saved where:**
```
merge_chains/
├── INPUT FILES:
│   ├── chains_chapter_*.json        # From Stage 2B
│   ├── tables/                      # Symlink/copy from Stage 1
│   └── masks/                       # Optional validation masks
│
└── output/                           # FINAL OUTPUTS
    ├── merged_chain_1_01_2001.csv   # Final dataset with meta_year column
    ├── metadata_1_01_2001.json      # Processing metadata
    ├── report_chapter_1.txt         # Validation report
    └── summary_chapter_1.json       # Processing summary
```

**Final CSV structure:**
```csv
meta_year,column1,column2,column3,...
2001,value1,value2,value3,...
2002,value1,value2,value3,...
```

## 🚀 Step-by-Step Usage Guide

### Prerequisites

1. **Install Python 3.8+**
2. **Install dependencies:**
```bash
cd table_process
pip install -r requirements.txt
pip install -r extract_tables/requirements.txt
pip install -r chain/table-chain-matching/requirements.txt
pip install -r merge_chains/requirements.txt
pip install anthropic  # For Claude API
```

3. **Set up API keys:**
```bash
# For extraction (years 2017, 2018, 2020 only)
export ANTHROPIC_API_KEY='sk-ant-api-your-key-here'

# For chain merging (Stage 2B)
echo "ANTHROPIC_API_KEY=sk-ant-api-your-key-here" > chain/chain-api-expantion/.env
```

### Step 1: Extract Tables from Documents

```bash
# Basic extraction for standard years (2001-2016, 2019, 2021-2024)
python extract_tables/main.py \
    --drive-folder-id YOUR_GOOGLE_DRIVE_FOLDER_ID \
    --years 2021 2022 2023 \
    --verbose

# For years requiring Claude API (2017, 2018, 2020)
export ANTHROPIC_API_KEY='your-api-key'
python extract_tables/main.py \
    --drive-folder-id YOUR_GOOGLE_DRIVE_FOLDER_ID \
    --years 2017 2018 2020

# Extract specific chapters only
python extract_tables/main.py \
    --drive-folder-id YOUR_ID \
    --years 2021 2022 \
    --chapters 1 2 3

# Check outputs
ls chain/table-chain-matching/tables/
cat chain/table-chain-matching/tables_summary.json | head
```

### Step 2A: Create Temporal Chains

```bash
cd chain/table-chain-matching

# Create configuration
cat > config.json << EOF
{
    "tables_dir": "tables",
    "reference_json": "tables_summary.json",
    "similarity_threshold": 0.78,
    "use_api_validation": false
}
EOF

# Run chain creation
python main.py

# Check outputs (chains created in adjacent directory)
ls ../chain-api-expantion/chains_chapter_*.json
```

### Step 2B: Merge Complementary Chains

```bash
cd ../chain-api-expantion

# Ensure .env file exists with API key
cat .env  # Should show: ANTHROPIC_API_KEY=sk-ant-api...

# Merge single chapter
python merge_chains_iterative.py --chapters 1

# Merge multiple chapters
python merge_chains_iterative.py --chapters 1 2 3 --threshold 0.75

# Merge all chapters (1-15)
python merge_chains_iterative.py --chapters $(seq 1 15)

# Check outputs
ls merged_results/
ls ../../merge_chains/chains_chapter_*.json
```

### Step 3: Final Processing and Output

```bash
cd ../../merge_chains

# Process specific chapters
python run_pipeline.py --chapters 1 2 3

# With validation
python run_pipeline.py --chapters 1 --validate

# Dry run to see what would be processed
python run_pipeline.py --chapters 1 --dry-run

# Process specific chains only
python run_pipeline.py --chapters 1 --chains chain_1_01_2001 chain_1_02_2002

# Check final outputs
ls output/
head output/merged_chain_1_01_2001.csv
```

## 🎛️ Configuration Files

### `extract_tables/config.yaml`
```yaml
reports_dir: "extract_tables/temp/reports"
tables_dir: "chain/table-chain-matching/tables"
table_marker: "לוח"          # Hebrew for "table"
exclude_marker: "תרשים"      # Hebrew for "diagram" (excluded)
encoding: "utf-8-sig"
```

### `chain/table-chain-matching/config.json`
```json
{
    "tables_dir": "tables",
    "reference_json": "tables_summary.json",
    "similarity_threshold": 0.78,
    "use_api_validation": false,
    "max_gap_years": 3
}
```

### `chain/chain-api-expantion/.env`
```
ANTHROPIC_API_KEY=sk-ant-api-your-key-here
```

## 📈 Processing Statistics

### Typical Processing Times
- **Extract (Stage 1):** ~30 seconds per document
- **Chain Creation (Stage 2A):** ~2-5 minutes per chapter
- **Chain Merging (Stage 2B):** ~1-3 minutes per chapter
- **Final Processing (Stage 3):** ~1 minute per chapter

### API Costs (when applicable)
- **Extraction (2017, 2018, 2020):** ~$0.01-0.02 per document
- **Chain Merging:** ~$0.10-0.30 per chapter (depending on matches)
- **Total for full pipeline:** ~$3-5 for all years and chapters

## 🐛 Troubleshooting

### Common Issues and Solutions

#### Stage 1: Extraction Issues
```bash
# Problem: No tables extracted
# Solution: Check document format and Hebrew markers
python extract_tables/main.py --years 2021 --verbose

# Problem: Authentication error in Google Colab
# Solution: Run auth first
from google.colab import auth
auth.authenticate_user()
```

#### Stage 2A: Chain Creation Issues
```bash
# Problem: tables_summary.json not found
# Solution: Ensure Stage 1 completed successfully
ls chain/table-chain-matching/tables_summary.json

# Problem: No chains created
# Solution: Lower similarity threshold
# Edit config.json: "similarity_threshold": 0.65
```

#### Stage 2B: Chain Merging Issues
```bash
# Problem: No merges found
# Solution: Lower threshold
python merge_chains_iterative.py --chapters 1 --threshold 0.6

# Problem: API key error
# Solution: Check .env file
cat chain/chain-api-expantion/.env
```

#### Stage 3: Final Processing Issues
```bash
# Problem: Missing chains
# Solution: Verify Stage 2B output exists
ls merge_chains/chains_chapter_*.json

# Problem: Memory issues
# Solution: Process fewer chapters at once
python run_pipeline.py --chapters 1
```

## 📝 File Naming Conventions

- **Table CSVs:** `{serial}_{chapter}_{year}.csv`
  - Example: `1_01_2021.csv` = Table 1, Chapter 1, Year 2021
- **Chain IDs:** `chain_{serial}_{chapter}_{year}`
  - Example: `chain_1_01_2001` = Chain starting from table 1_01_2001
- **Merged outputs:** `merged_chain_{id}.csv`
  - Includes `meta_year` column for temporal tracking

## 🔍 Verifying Pipeline Success

After running the complete pipeline, verify:

1. **Tables extracted:** 
   ```bash
   find chain/table-chain-matching/tables -name "*.csv" | wc -l
   ```

2. **Chains created:**
   ```bash
   ls chain/chain-api-expantion/chains_chapter_*.json | wc -l  # Should be 15
   ```

3. **Chains merged:**
   ```bash
   ls merge_chains/chains_chapter_*.json | wc -l
   ```

4. **Final outputs:**
   ```bash
   ls merge_chains/output/*.csv
   ```

## 📚 Additional Resources

- Each subdirectory contains its own detailed README
- Processing logs are saved with timestamps
- HTML reports provide visual validation
- API usage is tracked in merge reports

## 🤝 Contributing

When adding new features:
1. Update the appropriate stage's README
2. Ensure backward compatibility
3. Document any new output files
4. Update this main README if data flow changes

