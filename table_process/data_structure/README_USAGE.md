# NCC BigQuery Data - Usage Guide for Downstream Team

## Dataset Overview
- **Project**: ncc-data-bigquery
- **Dataset**: chains_dataset
- **Total Data**: ~600 chains, ~6,000 tables across 15 chapters

## 🗂️ Hierarchical Organization
```
NCC Statistical Yearbooks/
│
├── Chapter 1: מאפיינים דמוגרפיים/
│   ├── chain_1_01_2001/
│   │   ├── table_1_01_2001.csv (year 2001)
│   │   ├── table_1_01_2002.csv (year 2002)
│   │   └── table_1_01_2003.csv (year 2003)
│   │
│   └── chain_2_01_2001/
│       ├── table_2_01_2001.csv
│       └── table_2_01_2002.csv
│
└── Chapter 2: משפחה/
    └── [similar structure...]
```

## Table Structure

### chains_metadata
Contains chain-level information with Hebrew names
```yaml
chapter_id:    INTEGER       # Chapter number (1-15)
chapter_name:  STRING        # Hebrew chapter name
chain_id:      STRING        # e.g., "chain_1_01_2001"
chain_name:    STRING        # Hebrew chain description  
table_count:   INTEGER       # Number of tables in chain
years:         ARRAY<INT>    # Available years
gaps:          ARRAY<INT>    # Missing years in sequence
```

### tables_data
All table cells in normalized format:
```yaml
chapter_id:    INTEGER       # Chapter number (1-15)
chain_id:      STRING        # Unique chain identifier
table_id:      STRING        # Original table name
table_name:    STRING        # Hebrew table description
year:          INTEGER       # Year from table_id
row_index:     INTEGER       # Row position (0-based)
col_index:     INTEGER       # Column position (0-based)
cell_value:    STRING        # Actual data (Hebrew text preserved)
```

### masks_data
Feature/data-point identification (needs improvement)
```yaml
chapter_id:    INTEGER       # Chapter number
chain_id:      STRING        # Chain identifier
table_id:      STRING        # Associated table
mask_name:     STRING        # Mask description (Hebrew)
row_index:     INTEGER       # Row position (0-based)
col_index:     INTEGER       # Column position (0-based)
is_feature:    BOOLEAN       # TRUE = feature/header, FALSE = data value
```

## 🔄 Long Format Transformation

### Original CSV Table Structure
```
┌──────────┬──────────┬──────────┐
│ ירושלים  │  2,547   │   15%    │  Row 0
├──────────┼──────────┼──────────┤
│ תל אביב  │  3,892   │   23%    │  Row 1
├──────────┼──────────┼──────────┤
│  חיפה    │  1,234   │    8%    │  Row 2
└──────────┴──────────┴──────────┘
   Col 0      Col 1      Col 2
```

### ⬇️ Transforms to BigQuery Long Format

```
| chapter_id | chain_id         | table_id   | row | col | cell_value |
|------------|------------------|------------|-----|-----|------------|
| 1          | chain_1_01_2001  | 1_01_2001  | 0   | 0   | ירושלים    |
| 1          | chain_1_01_2001  | 1_01_2001  | 0   | 1   | 2,547      |
| 1          | chain_1_01_2001  | 1_01_2001  | 0   | 2   | 15%        |
| 1          | chain_1_01_2001  | 1_01_2001  | 1   | 0   | תל אביב    |
| 1          | chain_1_01_2001  | 1_01_2001  | 1   | 1   | 3,892      |
| 1          | chain_1_01_2001  | 1_01_2001  | 1   | 2   | 23%        |
| ...        | ...              | ...        | ... | ... | ...        |
```

**Key Point:** Each cell in the original CSV becomes a single row in BigQuery

## 🎭 Mask Data Structure

### Mask CSV (identifies features vs data points)
```
┌──────────┬──────────┬──────────┐
│ FEATURE  │ FEATURE  │ FEATURE  │  ← Column headers
├──────────┼──────────┼──────────┤
│   data   │   data   │   data   │  ← Row header + data
├──────────┼──────────┼──────────┤
│   data   │   data   │   data   │  ← Row header + data
└──────────┴──────────┴──────────┘
```

### ⬇️ Transforms to masks_data table

```
| table_id   | row | col | is_feature |
|------------|-----|-----|------------|
| 1_01_2001  | 0   | 0   | TRUE       |  ← Corner header
| 1_01_2001  | 0   | 1   | TRUE       |  ← Column header
| 1_01_2001  | 0   | 2   | TRUE       |  ← Column header
| 1_01_2001  | 1   | 0   | FALSE      |  ← Row header
| 1_01_2001  | 1   | 1   | FALSE      |  ← Data value
| 1_01_2001  | 1   | 2   | FALSE      |  ← Data value
| 1_01_2001  | 2   | 0   | FALSE       |  ← Row header
| 1_01_2001  | 2   | 1   | FALSE      |  ← Data value
| 1_01_2001  | 2   | 2   | FALSE      |  ← Data value
```

## Common Query Patterns

```sql
-- Get all tables in a chain
SELECT DISTINCT table_id, year 
FROM `ncc-data-bigquery.chains_dataset.tables_data`
WHERE chain_id = 'chain_2_01_2001'
ORDER BY year;

-- Reconstruct original table
SELECT cell_value
FROM `ncc-data-bigquery.chains_dataset.tables_data`
WHERE table_id = '2_01_2001'
ORDER BY row_index, col_index;

-- Get data by chapter topic
SELECT * 
FROM `ncc-data-bigquery.chains_dataset.tables_data`
WHERE chapter_id = 10  -- "ילדים וכלכלה"

-- Get only data values (exclude headers)
SELECT t.cell_value
FROM `ncc-data-bigquery.chains_dataset.tables_data` t
JOIN `ncc-data-bigquery.chains_dataset.masks_data` m
  ON t.table_id = m.table_id 
  AND t.row_index = m.row_index 
  AND t.col_index = m.col_index
WHERE m.is_feature = FALSE
  AND t.chain_id = 'chain_1_01_2001';

-- Track values across years for specific cell
SELECT year, cell_value
FROM `ncc-data-bigquery.chains_dataset.tables_data`
WHERE chain_id = 'chain_1_01_2001'
  AND row_index = 5 
  AND col_index = 3
ORDER BY year;
```

## Python Access Example

```python
from google.cloud import bigquery

client = bigquery.Client(project='ncc-data-bigquery')

def get_chain_as_dataframe(chain_id):
    query = f"""
    SELECT * FROM `ncc-data-bigquery.chains_dataset.tables_data`
    WHERE chain_id = '{chain_id}'
    ORDER BY table_id, row_index, col_index
    """
    return client.query(query).to_dataframe()

def reconstruct_table(table_id):
    """Reconstruct original table structure from long format"""
    query = f"""
    SELECT row_index, col_index, cell_value
    FROM `ncc-data-bigquery.chains_dataset.tables_data`
    WHERE table_id = '{table_id}'
    ORDER BY row_index, col_index
    """
    df = client.query(query).to_dataframe()
    
    # Pivot back to wide format
    return df.pivot(index='row_index', 
                    columns='col_index', 
                    values='cell_value')

def get_data_only(chain_id):
    """Get only data values, excluding headers"""
    query = f"""
    SELECT t.table_id, t.row_index, t.col_index, t.cell_value
    FROM `ncc-data-bigquery.chains_dataset.tables_data` t
    JOIN `ncc-data-bigquery.chains_dataset.masks_data` m
      ON t.table_id = m.table_id 
      AND t.row_index = m.row_index 
      AND t.col_index = m.col_index
    WHERE m.is_feature = FALSE
      AND t.chain_id = '{chain_id}'
    ORDER BY t.table_id, t.row_index, t.col_index
    """
    return client.query(query).to_dataframe()
```

## Important Notes
- Hebrew text is UTF-8 encoded
- Masks need refinement (not all are proper feature/data-point values)
- Data is static (no regular updates)
- Each cell in original tables becomes a separate row in BigQuery (long format)
- Use JOIN with masks_data to filter headers vs data values