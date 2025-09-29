# Table Chain Merger - BigQuery Integration
A comprehensive pipeline for merging the ncc tables across multiple years with temporal tracking, column alignment, and BigQuery integration.

## 🔄 Data Flow Overview

The pipeline performs a complex transformation cycle:
1. **Retrieves** long-format data from BigQuery (one row per cell)
2. **Pivots** to wide format (traditional table structure) 
3. **Normalizes** table structures using masks to identify headers vs data
4. **Aligns** columns across years using cosine similarity (alephBERT embeddings)
5. **Stacks** aligned tables with meta_year tracking
6. **Converts** back to long format for BigQuery storage
7. **Saves** both as local CSV (wide format) and BigQuery (long format)

## 📊 Data Architecture

### Input Sources (BigQuery)
- **`chains_metadata`**: Basic chain information
- **`tables_data`**: Table content in long format (row_index, col_index, cell_value)
- **`masks_data`**: Cell classification (is_feature: true=header, false=data)
- **Chain Configs**: Local JSON files in `chain_configs/` with detailed chain definitions

### Output Destinations
- **Local CSV**: Wide format with merged data (`output/merged_*.csv`)
- **BigQuery `merged_chains`**: Long format with merge metadata

## 🏗️ Pipeline Components

### 1. **ChainLoader** (`chain_loader.py`)
- Loads chain configurations from `chain_configs/*.json`
- Queries BigQuery for table data and pivots from long to wide format
- Applies year-specific skiprows logic (2001-2016: skip 2 rows)
- Reconstructs masks from BigQuery's boolean format to categorical

### 2. **TableNormalizer** (`table_normalizer.py`)
Handles four table structure patterns:
- **STANDARD**: Simple header rows followed by data
- **TABLE_GOES_DOWN**: Multiple header batches (tables stacked vertically)
- **HAMSHECH**: Tables with המשך (continuation) markers
- **DISTORTED**: Inconsistent column counts

Uses masks to separate headers from data, with special logic for first rows based on year.

### 3. **ColumnMatcher** (`column_matcher.py`)
Matches columns across years using:
- **Semantic similarity** via Hebrew BERT model (AlephBERT)
- **Edit distance** as fallback
- **Dual threshold system**:
  - `auto_accept_threshold` (0.85): Auto-match high confidence
  - `manual_review_threshold` (0.5): Minimum for consideration
  - Currently both set to same value to avoid API calls - when having the right masks could be useful to involve api calls

### 4. **MergerEngine** (`merger_engine.py`)
Core merging logic:
- Creates schema tracking column evolution across years
- Assigns UUIDs to columns for consistent tracking
- Stacks tables vertically with `meta_year` column
- Handles missing years with null rows
- **Note**: Currently loads BERT model once per chain (efficiency improvement implemented)

### 5. **OutputGenerator** (`output_generator.py`)
Dual output system:
- Saves merged wide-format table as CSV
- Converts to long format and inserts to BigQuery
- Updates merge status (pending → completed/failed)
- Handles batch insertions (500 rows at a time)

## 🚀 Usage Examples

### Process Single Chain
```bash
# Process specific chain (supports partial matching for merged chains)
python run_pipeline.py --chapters 1 --chains chain_1_01_2001

# This will match:
# - Exact: chain_1_01_2001
# - Merged: merged_chain_1_01_2001_chain_1_01_2005
# - Any chain containing the ID
```

### Process Multiple Chapters
```bash
# Process all chains in chapters 1, 2, and 3
python run_pipeline.py --chapters 1 2 3

# Process specific years only
python run_pipeline.py --chapters 1 --years 2001 2002 2003
```

### Validation and Testing
```bash
# Dry run - see what would be processed
python run_pipeline.py --chapters 1 --dry-run

# Run with validation
python run_pipeline.py --chapters 1 --validate
```

## 🔄 Format Transformations

### Long → Wide (Loading from BigQuery)
```python
# BigQuery stores:
table_id | row_index | col_index | cell_value
---------|-----------|-----------|------------
1_01_2001|     0     |     0     |   "מחוז"
1_01_2001|     0     |     1     |   "2001"
1_01_2001|     1     |     0     |   "צפון"
1_01_2001|     1     |     1     |   "1234"

# Pivots to:
   0        1
0  מחוז    2001
1  צפון    1234
```

### Wide → Long (Saving to BigQuery)
```python
# Merged table:
meta_year | מחוז  | אוכלוסייה
----------|-------|----------
2001      | צפון  | 1234
2002      | צפון  | 1245

# Converts to:
chain_id | meta_year | row_index | column_name | cell_value
---------|-----------|-----------|-------------|------------
chain_1  | 2001      |     0     | מחוז        | צפון
chain_1  | 2001      |     0     | אוכלוסייה   | 1234
chain_1  | 2002      |     1     | מחוז        | צפון
chain_1  | 2002      |     1     | אוכלוסייה   | 1245
```

## 🔧 Configuration

### Key Parameters in `config.py`
```python
'matching': {
    'auto_accept_threshold': 0.85,      # High confidence matches
    'manual_review_threshold': 0.5,     # Minimum similarity
    'use_semantic_matching': True,      # Enable BERT embeddings
    'semantic_similarity_alpha': 0.7,   # Not currently used
}
```

### Environment Setup
```bash
# Required
export GCP_PROJECT_ID="ncc-data-bigquery"

# Optional (removes warnings)
export GOOGLE_CLOUD_PROJECT="ncc-data-bigquery"

# Authentication
gcloud auth application-default login
```

## 🎯 Potential Improvements

### Mask Enhancement via Chain Context
The current mask generation operates on individual tables without chain context. The merging stage could improve masks by:
- If a row matches a "feature" row from all previous years → likely a feature

**Note**: We haven't implemented this as mask generation itself needs improvements first.

### API Integration for Ambiguous Matches
Currently, the dual threshold system is set up but not utilized (both thresholds = 0.85):
- Future implementation could call Claude/GPT for matches between 0.5-0.85
- Would provide human-like judgment for Hebrew column name variations
- Disabled to avoid API costs during development

### Performance Optimizations
- BERT model now loads once per chain (previously per year)
- Consider caching embeddings for repeated column names
- Batch processing for BigQuery insertions already implemented

## 📝 Processing Flow for a Single Chain

1. **Load Configuration**: Read chain definition from `chain_configs/chains_chapter_X.json`
2. **For Each Year in Chain**:
   - Query table from BigQuery (long format)
   - Pivot to wide format DataFrame
   - Load corresponding mask
   - Normalize structure (separate headers from data)
3. **Build Unified Schema**: 
   - Match columns across years using BERT
   - Track column name evolution
   - Assign unique IDs to columns
4. **Stack Tables**: Create single table with meta_year column
5. **Output Results**:
   - Save CSV locally (wide format)
   - Convert to long format
   - Insert to BigQuery with status tracking



### BigQuery Warnings
```bash
# Install storage API for faster reads (optional)
pip install google-cloud-bigquery-storage pyarrow
```

### Memory Issues with Large Chains
- Process chapters individually
- Reduce batch size in OutputGenerator
- Consider processing year ranges

## 📊 Verifying Results

### Check BigQuery Output
```sql
-- View merge status
SELECT chain_id, merge_status, COUNT(*) as rows
FROM `ncc-data-bigquery.chains_dataset.merged_chains`
WHERE chain_id LIKE '%1_01_2001%'
GROUP BY chain_id, merge_status;

-- Sample merged data
SELECT meta_year, column_name, cell_value
FROM `ncc-data-bigquery.chains_dataset.merged_chains`
WHERE chain_id = 'merged_chain_1_01_2001_chain_1_01_2005'
  AND row_index < 5
ORDER BY meta_year, row_index, column_name;
```

### Check Local Output
```bash
# View first few lines of merged CSV
head output/merged_chain_1_01_2001.csv

# Check summary
cat output/summary_chapter_1.json | jq '.statistics'
```

## 🏗️ Architecture Notes

The pipeline elegantly handles the impedance mismatch between:
- **Storage format** (long, normalized for BigQuery)
- **Processing format** (wide, traditional tables)
- **Hebrew text challenges** (RTL, variations, abbreviations)
- **Temporal evolution** (column names changing over years)

By maintaining both formats and using semantic similarity for matching, it achieves robust merging of evolving table structures across decades of Hebrew statistical data.