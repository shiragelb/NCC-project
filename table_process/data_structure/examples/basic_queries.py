"""Basic query examples for NCC BigQuery data"""

from google.cloud import bigquery

client = bigquery.Client(project='ncc-data-bigquery')

# Example 1: Get chain summary
query = """
SELECT chapter_id, chain_id, chain_name, table_count
FROM `ncc-data-bigquery.chains_dataset.chains_metadata`
LIMIT 10
"""
df = client.query(query).to_dataframe()
print(df)
