# Table Chain Merger

A pipeline for merging Hebrew statistical tables across multiple years with temporal tracking.

## Setup

1. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Prepare data:**
   - Place chain JSON files (`chains_chapter_*.json`) in the root directory
   - Organize tables in `tables/year/chapter/` structure
   - Place mask files in `masks/` directory

3. **Copy code from notebook:**
   - Open each Python file and paste the corresponding cell content from your notebook
   - The files are marked with comments showing where to paste

## Usage

### Process specific chapters:
```bash
python run_pipeline.py --chapters 1 2 3
```

### Process specific chains:
```bash
python run_pipeline.py --chapters 1 --chains chain_1_01_2001 chain_1_02_2002
```

### Process specific years only:
```bash
python run_pipeline.py --chapters 1 --years 2001 2002 2003
```

### With custom configuration:
```bash
python run_pipeline.py --chapters 1 --config my_config.json
```

### Dry run (see what would be processed):
```bash
python run_pipeline.py --chapters 1 --dry-run
```

### With validation:
```bash
python run_pipeline.py --chapters 1 --validate
```

## Output

Results are saved in the `output/` directory:
- `merged_chain_*.csv` - Merged table data with meta_year column
- `metadata_*.json` - Processing metadata
- `report_*.txt` - Validation reports
- `summary_chapter_*.json` - Chapter processing summary

## Configuration

Create a custom config JSON file to override defaults:
```json
{
    "matching": {
        "auto_accept_threshold": 0.85,
        "manual_review_threshold": 0.5,
        "use_semantic_matching": true,
        "semantic_similarity_alpha": 0.7
    },
    "output": {
        "format": "csv",
        "include_metadata": true,
        "include_validation_report": true
    }
}
```

## Architecture

See the included architecture diagram for the complete flow.
