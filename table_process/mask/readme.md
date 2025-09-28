# Feature-Datapoint Classification Pipeline

A comprehensive system for automatically classifying cells in tabular data as either "features" (headers/labels) or "data-points" (values) using a hybrid approach combining hard-coded rules and LLM-based classification.

## Overview

This pipeline processes CSV tables to create classification masks that identify each cell as:
- **feature**: Headers, labels, or descriptive text
- **data-point**: Actual values, measurements, or specific instances  
- **None**: Missing or invalid data
- **undecided**: Cells requiring further analysis

## Pipeline Architecture

The system uses a two-stage classification approach:

```
Stage 1: HARD RULE CLASSIFICATION (HardRuleClassifier)
  ↓ Applies deterministic rules → partial mask with "None", "feature", "data-point", "undecided"

Stage 2: LLM CLASSIFICATION (LLMClassifier) 
  ↓ Uses Claude API to classify remaining "undecided" cells → final mask
```

## Complete Data Flow & Components

### Stage 1: Hard Rule Classification

**What it does:** Applies deterministic rules to classify cells based on patterns, missing values, and row consistency.

**Classification Rules:**
1. **Missing Value Detection**: Empty cells, "N/A", "null", etc. → "None"
2. **Row Identity Check**: Rows with ≥80% identical values → "feature" 
3. **Numeric Pattern Matching**: Numbers, decimals, percentages → "data-point"
4. **Row Consistency**: Enforces majority classification within rows

**Input:** Raw CSV table
**Output:** Partial mask with deterministic classifications

### Stage 2: LLM Classification  

**What it does:** Uses Claude API to classify remaining "undecided" cells with contextual understanding.

**Smart Model Selection:**
- Tables ≤30 rows: Uses Haiku models for cost efficiency
- Tables >30 rows: Uses Sonnet models for reliability
- Automatic fallback on dimensional failures

**Input:** Table data + partial mask from Stage 1
**Output:** Complete classification mask

## File Structure & Classes

### Core Classes

```
TableLoader
├── load_and_clean()           # Loads CSV and removes artifacts
└── Handles year-based headers # Pre-2017 vs post-2017 formats

HardRuleClassifier  
├── classify()                 # Main classification pipeline
├── _is_missing_value()        # Detects missing data patterns
├── _check_row_identity()      # Row similarity analysis
├── _is_numeric_pattern()      # Numeric pattern matching
└── _enforce_row_consistency() # Row-level majority enforcement

LLMClassifier
├── classify_undecided()       # Main LLM classification
├── _build_prompt()            # Creates classification prompt
├── _parse_response()          # Validates LLM output dimensions
├── get_usage_summary()        # API cost tracking
└── Smart model selection     # Cost/reliability optimization

TableClassifier
├── classify_table()           # Complete pipeline orchestration
├── process_single_table()     # Individual table processing
├── process_all_tables()       # Batch processing with parallelization
└── load_table_names()         # Loads Hebrew table names for context
```

## Step-by-Step Usage Guide

### Prerequisites

1. **Install Python 3.8+**
2. **Install dependencies:**
```python
pip install pandas anthropic pathlib concurrent.futures
```

3. **Set up API key:**
```bash
export ANTHROPIC_API_KEY='sk-ant-api-your-key-here'
```

### Basic Usage

```python
from table_classifier import TableClassifier

# Initialize classifier
classifier = TableClassifier(api_key="your-api-key")  # Optional: uses env var

# Process single table
mask = classifier.classify_table(
    csv_path="path/to/table.csv",
    table_name="Population by District"  # Hebrew context for LLM
)

# Save result
mask.to_csv("output_mask.csv", index=False)
```

### Batch Processing

```python
# Process all tables in directory structure
classifier.process_all_tables(
    input_dir="/path/to/tables/",           # Organized as year/chapter/
    output_dir="/path/to/masks/",           # Same structure for outputs
    summary_path="/path/to/tables_summary.json",  # Table name mappings
    years=range(2001, 2025),               # Years to process
    chapters=range(1, 16),                 # Chapters to process
    parallel=True,                         # Enable parallelization
    max_workers=4,                         # Parallel worker count
    skip_existing=True                     # Skip already processed files
)
```

### Advanced Configuration

```python
# Custom hard rule thresholds
from hard_rule_classifier import HardRuleClassifier

hard_classifier = HardRuleClassifier(
    threshold=0.8,              # Row identity threshold
    consistency_threshold=0.3   # Row consistency enforcement
)

# Custom LLM model selection
from llm_classifier import LLMClassifier

llm_classifier = LLMClassifier(api_key="your-key")

# Force specific model
mask = llm_classifier.classify_undecided(
    table_name="Population Data",
    table_df=df,
    partial_mask=partial_mask,
    model="sonnet_4"  # Force Sonnet usage
)
```

## Configuration Parameters

### HardRuleClassifier Settings
```python
threshold: float = 0.8                    # Row identity detection sensitivity
consistency_threshold: float = 0.3        # Row majority enforcement threshold
```

### LLMClassifier Model Selection
```python
# Automatic selection based on table size
≤30 rows: ['haiku_3', 'haiku_3_5', 'sonnet_4']     # Cost-optimized
>30 rows: ['sonnet_4', 'haiku_3_5', 'haiku_3']     # Reliability-optimized

# Pricing (per 1M tokens)
haiku_3: $0.25 input, $1.25 output
haiku_3_5: $1.00 input, $5.00 output  
sonnet_4: $3.00 input, $15.00 output
```

### Processing Options
```python
parallel: bool = False                    # Enable parallel processing
max_workers: int = 4                      # Parallel worker count
skip_existing: bool = True                # Skip processed files
years: range = range(2001, 2025)          # Year range to process
chapters: range = range(1, 16)            # Chapter range to process
```

## Input/Output Specifications

### Required Input Structure
```
input_dir/
├── 2001/
│   ├── 01/
│   │   ├── 1_01_2001.csv              # Table 1, Chapter 1, Year 2001
│   │   ├── 2_01_2001.csv              # Table 2, Chapter 1, Year 2001
│   │   └── ...
│   ├── 02/
│   └── ...
├── 2002/
└── ...

tables_summary.json                      # Table name mappings
{
  "1_01_2001": "לוח 1.1 - אוכלוסייה לפי מחוז",
  "2_01_2001": "לוח 1.2 - צפיפות אוכלוסין"
}
```

### Output Structure  
```
output_dir/
├── 2001/
│   ├── 01/
│   │   ├── 1_01_2001.csv              # Classification mask
│   │   ├── 2_01_2001.csv              # Same structure as input
│   │   └── ...
│   └── ...
└── ...

# Mask CSV format:
feature,feature,feature
data-point,data-point,data-point
None,data-point,feature
```

## Processing Statistics & Monitoring

### Usage Tracking
```python
# Get comprehensive API usage summary
classifier.usage_summary()

# Output includes:
# - Token usage by model
# - Cost breakdown per model
# - Success rates by strategy
# - Processing time statistics
# - Dimensional failure analysis
```

### Typical Performance Metrics
- **Hard Rules Coverage**: ~60-80% of cells classified deterministically
- **LLM Processing**: ~20-40% of cells require LLM classification
- **Success Rate**: >95% dimensional accuracy with smart model selection
- **Processing Speed**: ~2-5 seconds per table (depending on size)

### Cost Optimization
- **Small tables (≤30 rows)**: ~$0.001-0.005 per table
- **Large tables (>30 rows)**: ~$0.01-0.03 per table  
- **Emergency fallbacks**: Automatic Sonnet retry for dimensional failures

## Pattern Recognition Examples

### Hard Rule Classifications

**Missing Values → "None"**
```
"", "N/A", "null", "---", None
```

**Numeric Patterns → "data-point"**
```
"123.45", "1,234", "1,234.56", "145" (excluding 0-9, 1900-2030)
```

**Row Identity → "feature"**
```
["Region", "Region", "Region", "Region"]     # 100% identical
["2001", "2002", "2003", "2004"]           # Sequential years  
```

### LLM Classification Logic

**Headers/Labels → "feature"**
- Table titles and column headers
- Geographic regions, time periods
- Category labels and descriptions

**Values/Data → "data-point"**  
- Population counts, statistics
- Measurements and calculations
- Specific instances and observations

## Troubleshooting

### Common Issues and Solutions

#### Hard Rule Classification Issues
```python
# Problem: Too many cells left "undecided"
# Solution: Lower identity threshold
classifier = HardRuleClassifier(threshold=0.7)

# Problem: Row consistency too aggressive  
# Solution: Raise consistency threshold
classifier = HardRuleClassifier(consistency_threshold=0.5)
```

#### LLM Classification Issues
```python
# Problem: Dimensional mismatch errors
# Solution: Enable automatic Sonnet fallback (default behavior)

# Problem: API rate limiting
# Solution: Add delays between calls
import time
time.sleep(1)  # Add between table processing

# Problem: High API costs
# Solution: Use hard rules more aggressively
classifier = HardRuleClassifier(threshold=0.6, consistency_threshold=0.2)
```

#### Processing Issues
```python
# Problem: Memory issues with large batches
# Solution: Reduce parallel workers or disable parallelization
classifier.process_all_tables(parallel=False)

# Problem: Missing table names
# Solution: Verify tables_summary.json exists and is valid
import json
with open("tables_summary.json") as f:
    names = json.load(f)
    print(f"Loaded {len(names)} table names")
```

## File Naming Conventions

- **Input CSVs**: `{serial}_{chapter}_{year}.csv`
  - Example: `1_01_2021.csv` = Table 1, Chapter 1, Year 2021
- **Output Masks**: Same filename as input with classification values
- **Summary Files**: `tables_summary.json` for table name mappings

## Validation & Quality Assurance

### Automatic Validation
- Dimensional consistency checking
- None value preservation
- Row-level consistency enforcement  
- API response validation

### Manual Verification
```python
# Check classification distribution
mask = pd.read_csv("output_mask.csv")
print(mask.value_counts().to_dict())

# Verify no "None" values were added inappropriately
original_none_count = (partial_mask == "None").sum().sum()
final_none_count = (mask == "None").sum().sum() 
assert original_none_count == final_none_count
```

## Integration with Other Pipelines

This classification system is designed to integrate with:
- Table extraction pipelines (provides classification for extracted tables)
- Data validation workflows (masks identify data vs metadata)
- ML preprocessing (separate feature engineering for headers vs data)

The classification masks enable downstream processing to handle tabular data more intelligently by distinguishing between structural elements and actual data values.
