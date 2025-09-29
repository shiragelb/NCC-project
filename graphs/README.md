# Israeli Education Reports - Graph Extraction Pipeline

## Project Overview

This project extracts and processes graphs and tables from Israeli Ministry of Education annual reports spanning 2001-2023. The pipeline converts unstructured data from Word documents into structured CSV files, handling different document formats and using AI-powered redundancy detection to filter relevant graphs.

## Data Coverage

- **2001-2016**: Old Word format (.doc) files
- **2019, 2021-2023**: Modern Word format (.docx) files with embedded Excel
- **Excluded years**: 2017-2018, 2020, 2024 (due to PDF format or data quality issues)

## Pipeline Architecture

### Phase 1: Document Processing (2001-2016)

**Challenge**: Old Word documents (.doc) without embedded Excel files

**Solution**:
1. Convert documents to high-resolution images using LibreOffice
2. Extract graph images from tables containing "תרשים" (chart/diagram)
3. Use AI (Claude Sonnet) to convert graph images to CSV format

### Phase 2: Document Processing (2019, 2021-2023)

**Challenge**: Modern Word documents (.docx) with embedded Excel files in non-sequential order

**Solution**:
1. Extract all embedded Excel files by chapter
2. Parse table headers from Word documents
3. Use interactive alignment tool to match Excel files with their correct headers
4. Convert Excel files to CSV format

### Phase 3: Redundancy Detection

**Purpose**: Identify graphs that merely visualize existing table data

**Process**:
1. Calculate semantic similarity between graph headers and table headers
2. Use AI to determine if graphs are redundant (≈80% redundancy rate found)
3. Filter out redundant graphs to keep only unique data

## Repository Structure

```
project/
├── graphs_extraction.ipynb          # Main extraction pipeline
├── redundant_graphs_removal.ipynb   # Redundancy detection system
├── alignment_of_tables_and_headers.ipynb  # Interactive alignment tool
├── extracting_graphs_to_bigquery.ipynb    # BigQuery upload script
├── docs/                            # Documentation
├── reports/                         # Original Word documents
├── images/                          # Extracted graph images (2001-2016)
├── extracted_files/                 # Extracted Excel/CSV files (2019-2023)
├── graphs_to_csvs_2001-2016/      # Converted CSVs from images
├── relevant_graphs/                 # Non-redundant graphs only
└── json_mappings/                   # Various mapping files
```

## Key Scripts

### 1. `graphs_extraction.ipynb`
- Downloads reports from Google Drive
- Extracts graphs from 2001-2016 using LibreOffice conversion
- Extracts embedded Excel files from 2019-2023 documents
- Converts graph images to CSV using Claude API

### 2. `redundant_graphs_removal.ipynb`
- Implements semantic similarity using Hebrew BERT embeddings
- Uses Claude API for redundancy classification
- Generates validation samples for quality control
- Produces statistics: ~20% of graphs contain unique information

### 3. `alignment_of_tables_and_headers.ipynb`
- Interactive tool for matching Excel files to their headers
- Handles cases where file names are non-informative
- Creates mapping JSON files for proper identification

### 4. `extracting_graphs_to_bigquery.ipynb`
- Uploads all processed CSV files to Google BigQuery
- Organizes data by year and chapter for easy querying

## Output Files

### Primary Outputs
- **CSV files**: Structured data extracted from graphs
- **graphs_summary.json**: Mapping of graph IDs to Hebrew headers
- **redundant_graphs.json**: Classification of each graph (YES/NO for redundancy)
- **aligned_mapping.json**: Mapping between Excel files and their proper names

### BigQuery Tables
- Dataset: `ncc-data-bigquery.graphs`
- Tables named by pattern: `{serial}_{chapter}_{year}`
- Query examples available for loading data by chapter or year

## Requirements

### Python Libraries
```python
pip install python-docx
pip install sentence-transformers
pip install anthropic
pip install google-cloud-bigquery
pip install pandas
pip install Pillow
```

### System Dependencies
- LibreOffice (for .doc to PNG conversion)
- Google Colab environment (or equivalent with Drive API access)

### API Keys
- Anthropic Claude API key (for AI processing)
- Google Cloud credentials (for Drive and BigQuery access)

## Usage

### Quick Start

1. **Setup Environment**
```python
# Authenticate Google services
from google.colab import auth
auth.authenticate_user()

# Set API keys
ANTHROPIC_API_KEY = "your-key-here"
```

2. **Download Reports**
```python
# Downloads all reports to /content/reports/
download_all_chapters()
```

3. **Extract Graphs (2001-2016)**
```python
for year in range(2001, 2017):
    for chapter in range(1, 16):
        extract_images_from_docx_tables(docx_path, output_dir, chapter, year)
```

4. **Process Modern Documents (2019-2023)**
```python
# Extract Excel files and create mappings
extract_docx_tables_and_excels(docx_path, year, chapter)
```

5. **Filter Redundant Graphs**
```python
analyze_graph_redundancy(year, chapter_id, table_json_url, graph_json_url)
```

### BigQuery Access

To query the processed data:

```sql
-- Get all graphs from 2022
SELECT * FROM `ncc-data-bigquery.graphs.*` 
WHERE _TABLE_SUFFIX LIKE '%_2022'

-- Get specific chapter across years
SELECT * FROM `ncc-data-bigquery.graphs.*` 
WHERE _TABLE_SUFFIX LIKE '%_5_%'
```

## Technical Decisions

### Why LibreOffice for Image Conversion?
- Preserves Hebrew text accurately
- Maintains graph quality at high resolution
- Handles WMF/EMF formats common in old Word documents

### Why Interactive Alignment?
- Excel file names in .docx were non-informative (e.g., "Microsoft_Excel_Worksheet1.xlsx")
- Semantic similarity alone was insufficient due to similar table names
- Manual verification ensures 100% accuracy in mapping

### Why Redundancy Detection?
- ~80% of graphs merely visualized existing table data
- Removing redundant graphs reduces processing costs and storage
- Focuses analysis on unique information sources

## Validation

- **Manual Sampling**: 40 random YES classifications reviewed for accuracy
- **Cross-validation**: Table presence verified across chapters
- **Size Checks**: Images validated for minimum readable dimensions (200px)

## Known Limitations

1. **Missing Years**: 2017-2018, 2020, 2024 not processed due to format issues
2. **OCR Accuracy**: Some complex Hebrew tables may have extraction errors
3. **API Costs**: Full processing requires ~$7-10 in Claude API credits
4. **Manual Steps**: Alignment for 2019-2023 requires human intervention

## Future Improvements

- Automate the alignment process using advanced NLP
- Add support for PDF extraction (2017-2018, 2020)
- Implement quality scoring for extracted CSVs
- Create automated validation pipeline

## Contact & Contributing

For questions or contributions, please open an issue or submit a pull request.

## License

[Specify your license here]

---

*Note: This project processes publicly available government reports. All extracted data maintains original Hebrew text without translation.*
