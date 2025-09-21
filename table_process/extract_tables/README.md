# Hebrew Table Extraction Pipeline

Extracts statistical tables from Hebrew Word documents stored in Google Drive, preparing them for temporal chain analysis.

## Overview

This tool processes Hebrew statistical reports (Word documents) organized by year and chapter, extracting tables and their headers into CSV format. It handles continuation tables (marked with "המשך") and creates a unified summary JSON for downstream processing.

## Prerequisites

- Python 3.7 or higher
- Google Colab account (recommended) OR local Google Cloud credentials
- **For years 2017, 2018, 2020**: Anthropic API key (Claude)
- Google Drive folder with documents organized as:
  ```
  Drive Folder/
  ├── 2021/
  │   ├── 01_chapter_name.docx
  │   ├── 02_chapter_name.docx
  │   └── ...
  ├── 2022/
  │   └── ...
  ```

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd table_process
```

2. Install dependencies:
```bash
pip install -r extract_tables/requirements.txt

# For years 2017, 2018, 2020 (Claude API):
pip install anthropic
```

3. Set up Claude API (for years 2017, 2018, 2020):
```bash
# Linux/Mac
export ANTHROPIC_API_KEY='your-api-key-here'

# Or in Python/Colab
import os
os.environ['ANTHROPIC_API_KEY'] = 'your-api-key-here'
```

## Usage

**IMPORTANT: Always run from the `table_process/` root directory**

### Basic Usage

Extract all chapters for specific years:
```bash
python extract_tables/main.py \
    --drive-folder-id 1e0eA-AIsz_BSwVHOppJMXECX42hBfG4J \
    --years 2021 2022 2023
```

### Advanced Usage

Extract specific chapters with verbose logging:
```bash
python extract_tables/main.py \
    --drive-folder-id YOUR_FOLDER_ID \
    --years 2021 2022 2023 2024 \
    --chapters 1 2 3 4 5 \
    --verbose
```

Download only (no extraction):
```bash
python extract_tables/main.py \
    --drive-folder-id YOUR_FOLDER_ID \
    --years 2021 \
    --download-only
```

### Parameters

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `--drive-folder-id` | Yes | - | Google Drive folder ID containing Word documents |
| `--years` | Yes | - | Space-separated years to process (e.g., 2021 2022) |
| `--chapters` | No | 1-15 | Chapter numbers to process |
| `--download-only` | No | False | Only download files, skip extraction |
| `--skip-merge` | No | False | Skip merging continuation tables |
| `--verbose` | No | False | Enable detailed logging |
| `--config` | No | extract_tables/config.yaml | Custom configuration file |

## Output Structure

The pipeline creates the following structure:

```
chain/table-chain-matching/
├── tables/
│   ├── 2021/
│   │   ├── 1/
│   │   │   ├── 1_1_2021.csv     # Table 1, Chapter 1, Year 2021
│   │   │   ├── 2_1_2021.csv
│   │   │   └── summaries.json   # Chapter-level summary
│   │   ├── 2/
│   │   └── ...
│   ├── 2022/
│   └── ...
├── tables_summary.json           # Global summary of all tables
└── tables_stats.csv             # Statistics report
```

### File Naming Convention
- CSV files: `{serial}_{chapter}_{year}.csv`
- Serial: Sequential table number within chapter
- Chapter: Chapter number (1-15)  
- Year: Document year

## Google Colab Usage

Run in Google Colab for automatic authentication:

```python
# Cell 1: Clone repository
!git clone <repository-url>
%cd table_process

# Cell 2: Install dependencies
!pip install -r extract_tables/requirements.txt
!pip install anthropic  # For years 2017, 2018, 2020

# Cell 3: Authenticate (automatic in Colab)
from google.colab import auth
auth.authenticate_user()

# Cell 4: Set Claude API key (if processing 2017, 2018, 2020)
import os
os.environ['ANTHROPIC_API_KEY'] = 'your-anthropic-api-key-here'

# Cell 5: Run extraction
!python extract_tables/main.py \
    --drive-folder-id YOUR_FOLDER_ID \
    --years 2021 2022 2023 \
    --verbose

# Or for Claude API years:
!python extract_tables/main.py \
    --drive-folder-id YOUR_FOLDER_ID \
    --years 2017 2018 2020 \
    --verbose
```

## Year-Specific Extractors

The pipeline uses different extraction methods based on document format changes:

| Years | Method | Status | Cost |
|-------|--------|--------|------|
| 2001-2016 | Standard extractor | ✅ Implemented | Free |
| 2017, 2018, 2020 | Claude API extractor | ✅ Implemented | ~$0.01-0.02 per document |
| 2019, 2021-2024 | Enhanced extractor (better header detection) | ✅ Implemented | Free |

### Claude API Integration (2017, 2018, 2020)

These years have complex document formats that require AI-powered extraction using Claude:

#### Setup
1. **Get an Anthropic API key** from [console.anthropic.com](https://console.anthropic.com/)
2. **Set the environment variable**:
   ```bash
   # Before running extraction
   export ANTHROPIC_API_KEY='sk-ant-api...'
   ```
3. **Run extraction** (same command as other years):
   ```bash
   python extract_tables/main.py --drive-folder-id YOUR_ID --years 2017 2018 2020
   ```

#### Features
- **Automatic table detection** using Claude 3.5 Sonnet
- **Continuation table handling** - merges tables marked with "(המשך)"
- **Cost tracking** - displays total API cost after processing
- **Rate limiting protection** - 1-second delay between API calls
- **Debug output** - saves raw Claude responses for troubleshooting

#### Cost Estimation
- **Per document**: $0.01-0.02 (depending on document size)
- **Per year (15 chapters)**: ~$0.15-0.30
- **All three years**: ~$0.45-0.90

#### Troubleshooting Claude API
- **No API key**: Set `ANTHROPIC_API_KEY` environment variable
- **Rate limits**: Script includes automatic delays
- **JSON parsing errors**: Check debug files in output directory
- **High costs**: Process chapters individually to monitor spending

## Expected Input Format

### Google Drive Structure
Documents must be organized in folders by year:
- Year folders: `2021/`, `2022/`, etc.
- Document naming: `{chapter_number}_{any_name}.docx`
  - Example: `01_population_statistics.docx` for Chapter 1
  - Chapter numbers should be 01-15 (zero-padded)

### Document Format
- Word documents (.docx) containing tables
- Tables identified by Hebrew marker "לוח" (table)
- Diagrams with "תרשים" are excluded
- Continuation tables marked with "(המשך)"

## How It Works

1. **Download Phase**: Fetches Word documents from Google Drive
2. **Extraction Phase**: 
   - Parses each document to find tables
   - Identifies table headers (searching for "לוח" marker)
   - Extracts table data to CSV files
3. **Merging Phase**: 
   - Combines continuation tables
   - Renumbers tables sequentially
   - Creates unified summary JSON
4. **Statistics Generation**: 
   - Counts total and unnamed tables
   - Generates per-chapter-year breakdown

## Output Files

### tables_summary.json
Maps table identifiers to their Hebrew headers:
```json
{
  "1_01_2021": "לוח 1.1 - אוכלוסייה לפי מחוז",
  "2_01_2021": "לוח 1.2 - התפלגות גילאים",
  ...
}
```

### tables_stats.csv
Statistical summary of extraction:
```csv
Year,Chapter,Total Tables,Unnamed Tables
2021,1,15,2
2021,2,12,1
...
```

## Troubleshooting

### Common Issues

1. **Authentication Error in Colab**: 
   - Ensure `auth.authenticate_user()` is run before extraction
   - Restart runtime and try again

2. **Missing Files**:
   - Verify Google Drive folder ID is correct
   - Check folder permissions (must be accessible)
   - Ensure files follow naming convention

3. **No Tables Extracted**:
   - Check if documents contain Hebrew table marker "לוח"
   - Verify documents are .docx format (not .doc)
   - Run with `--verbose` for detailed logs

4. **Encoding Issues**:
   - Pipeline uses UTF-8-SIG for Hebrew text
   - If issues persist, check document encoding

### Getting Help

Check logs for detailed error messages:
```bash
python extract_tables/main.py --drive-folder-id YOUR_ID --years 2021 --verbose
```

## Configuration

Edit `extract_tables/config.yaml` to customize:
```yaml
# Paths (relative to table_process root)
reports_dir: "extract_tables/temp/reports"
tables_dir: "chain/table-chain-matching/tables"

# Hebrew markers
table_marker: "לוח"      # Table identifier
exclude_marker: "תרשים"  # Diagram identifier (excluded)

# Encoding
encoding: "utf-8-sig"
```

## Development

### Project Structure
```
extract_tables/
├── main.py                       # CLI entry point
├── config.yaml                   # Configuration
├── requirements.txt              # Python dependencies
├── README.md                     # This file
└── src/
    ├── drive_manager.py          # Google Drive operations
    ├── extractor_2001_2016.py    # Standard extractor
    ├── extractor_2017_2018_2020.py # Claude API extractor
    ├── extractor_2019_2024.py    # Enhanced extractor
    ├── merger.py                 # Continuation table merger
    ├── statistics.py             # Statistics generation
    └── utils.py                  # Utility functions
```

### Adding Support for New Years

If document formats change in future years:
1. Create a new extractor in `src/extractor_YEARS.py`
2. Implement the `process_files()` method
3. Update `main.py` to use the new extractor
4. Update `utils.py` to categorize the new years
