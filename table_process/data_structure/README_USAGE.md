# NCC BigQuery Data - Usage Guide for Downstream Team

## Dataset Overview
- **Project**: ncc-data-bigquery
- **Dataset**: chains_dataset
- **Total Data**: ~600 chains, ~6,000 tables across 15 chapters

## Table Structure

### chains_metadata
Contains chain-level information with Hebrew names

### tables_data
All table cells in normalized format:
- chapter_id: Chapter number (1-15)
- chain_id: Unique chain identifier
- table_id: Original table name
- row_index, col_index: Cell position
- cell_value: Actual data (Hebrew text preserved)

### masks_data
Feature/data-point identification (needs improvement)

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
```

## Important Notes
- Hebrew text is UTF-8 encoded
- Masks need refinement (not all are proper feature/data-point values)
- Data is static (no regular updates)
