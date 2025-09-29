-- Create BigQuery dataset for chains
CREATE SCHEMA IF NOT EXISTS chains_dataset
OPTIONS(
  description="NCC-data tables from PDFs",
  location="EU"
);
