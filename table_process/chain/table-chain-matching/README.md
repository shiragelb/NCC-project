# Table Chain Matching System

A sophisticated system for matching and tracking tables across multiple years and chapters using advanced NLP techniques and Hungarian algorithm optimization.

## Features

- **Hebrew Text Processing**: Specialized handling of Hebrew headers and text
- **Smart Embedding Generation**: Using sentence transformers for semantic similarity
- **Hungarian Algorithm Matching**: Optimal assignment of tables to chains
- **Split/Merge Detection**: Identifies when tables split or merge across years
- **Gap Handling**: Manages dormant and reactivated chains
- **API Validation**: Optional Claude API integration for uncertain matches
- **Comprehensive Reporting**: HTML reports, Sankey diagrams, and JSON outputs

## Installation

1. Clone this repository:
```bash
git clone https://github.com/yourusername/table-chain-matching.git
cd table-chain-matching
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Configure settings:
- Edit `config.json` with your parameters
- Add your Claude API key if using API validation

## Usage

### Basic Usage

Run the main pipeline:
```bash
python main.py
```

### Configuration

Edit `config.json` to customize:
- `tables_dir`: Directory containing table CSV files
- `reference_json`: Path to tables summary JSON
- `similarity_threshold`: Threshold for matching (0.78 default)
- `use_api_validation`: Enable/disable API validation
- `max_gap_years`: Maximum years before chain ends

### Input Data Structure

Expected directory structure:
```
tables/
├── 2001/
│   ├── 01/
│   │   └── 1_01_2001.csv
│   └── 02/
│       └── 2_02_2001.csv
└── 2002/
    └── ...
```

## Output

The system generates:
- `output_chapter_X/`: Chapter-specific results
  - `chains_chapter_X.json`: Chain definitions
  - `report_chapter_X.html`: HTML report
  - `sankey_chapter_X.html`: Sankey visualization
- `chain_storage/`: Checkpoints and embeddings
- `output/`: General outputs and summaries

## Components

- **config.py**: Configuration management
- **hebrew_processor.py**: Hebrew text normalization
- **table_loader.py**: Table data loading
- **real_embeddings.py**: Embedding generation
- **hungarian.py**: Hungarian algorithm implementation
- **chains.py**: Chain management logic
- **split_merge.py**: Split/merge detection
- **gap_handler.py**: Gap and dormancy handling
- **api_validator.py**: Claude API integration
- **visualization.py**: Visualization generation
- **report_gen.py**: Report generation

## Testing

Run tests:
```bash
python -m pytest tests/
```

## License

MIT License

## Contributing

Pull requests are welcome. For major changes, please open an issue first.

## structure 
table-chain-matching/
├── src/                    # All Python modules
│   ├── config.py
│   ├── hebrew_processor.py
│   ├── table_loader.py
│   └── ... (18 more modules)
├── main.py                 # Main orchestrator
├── requirements.txt        # Dependencies
├── README.md              # Documentation
├── config.json            # Configuration
├── .gitignore             # Git ignore rules
├── output/                # Output directory
├── tables/                # Input tables
├── mask/                  # Mask files
├── cache/                 # Cache directory
└── chain_storage/         # Storage for chains
