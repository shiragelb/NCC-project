# Table Chain Matching System

A sophisticated system for tracking and matching tables across multiple years and chapters using advanced NLP techniques, Hebrew text processing, and Hungarian algorithm optimization.

## 🎯 Purpose

This system creates "chains" that track the same statistical table across multiple years in Hebrew government/statistical publications. Each table may evolve slightly over time (headers change, metrics update), but represents the same core data. The system:

- Identifies which tables from year N correspond to tables from year N+1
- Handles tables that split, merge, or go dormant across years
- Creates continuous chains for downstream data unification
- Processes chapters independently for better accuracy

## 📊 System Architecture

### Core Pipeline Flow

```
1. Table Loading → 2. Hebrew Processing → 3. Embedding Generation
       ↓                                            ↓
4. Chapter Organization → 5. Year-by-Year Matching → 6. Chain Updates
       ↓                                            ↓
7. Gap Handling → 8. API Validation → 9. Report Generation
```

### Key Components

#### **Core Processing Modules**

- **`final_complete_processor.py`**: Main orchestrator that runs the complete pipeline
  - Processes tables chapter-by-chapter, year-by-year
  - Coordinates all other modules
  - Handles API validation decisions

- **`table_loader.py`**: Loads table data and metadata
  - Reads CSV tables from directory structure
  - Loads reference JSON with table headers
  - Associates mask files for feature/data point definitions

- **`hebrew_processor.py`**: Specialized Hebrew text processing
  - Normalizes Hebrew headers
  - Removes repetitions and year markers
  - Handles right-to-left text properly
  - Optional Claude API integration for intelligent deduplication

#### **Matching & Chain Management**

- **`chains.py`**: Core chain data structure and management
  - Initializes chains from first year
  - Updates chains with new matches
  - Tracks similarity scores and API validation usage
  - Maintains mask references for each table

- **`real_embeddings.py`**: Generates semantic embeddings
  - Uses sentence-transformers (AlephBERT for Hebrew)
  - Caches embeddings for efficiency
  - Falls back to deterministic random if transformer unavailable

- **`similarity.py`**: Computes similarity matrices
  - Cosine similarity between chain and table embeddings
  - Normalizes scores to [0,1] range

- **`hungarian.py`**: Optimal assignment algorithm
  - Finds best 1:1 matching between chains and new tables
  - Configurable similarity threshold (default 0.85)

#### **Advanced Features**

- **`split_merge.py`**: Detects complex relationships
  - Identifies when one table splits into multiple
  - Detects when multiple tables merge into one
  - Handles N:N relationships

- **`gap_handler.py`**: Manages discontinuous chains
  - Marks chains as dormant when no match found
  - Attempts reactivation of dormant chains
  - Ends chains after configurable gap (default 3 years)

- **`conflict_resolver.py`**: Resolves multiple claims
  - Detects when multiple chains claim same table
  - Uses API validation or highest similarity

- **`api_validator.py`**: Claude API integration
  - Validates uncertain matches (similarity 0.85-0.97)
  - Provides intelligent decisions on edge cases
  - Tracks API usage for cost management

#### **Output & Visualization**

- **`report_gen.py`**: Generates comprehensive reports
  - JSON export of all chains
  - HTML reports with chain details
  - Summary statistics

- **`visualization.py`**: Creates interactive visualizations
  - Sankey diagrams showing table flow across years
  - Network graphs of chain relationships

## 📁 Directory Structure

```
table-chain-matching/
├── main.py                 # Entry point
├── config.json            # Configuration
├── tables_summary.json    # Table headers reference
│
├── src/                   # All Python modules
│   ├── final_complete_processor.py
│   ├── hebrew_processor.py
│   ├── table_loader.py
│   └── ... (18+ modules)
│
├── tables/               # Input CSV tables
│   ├── 2001/
│   │   ├── 01/          # Chapter 01
│   │   │   ├── 1_01_2001.csv
│   │   │   └── 2_01_2001.csv
│   │   └── 02/          # Chapter 02
│   └── 2002/
│       └── ...
│
├── mask/                # Mask files (feature definitions)
│   └── [same structure as tables/]
│
├── chain-api-expantion/ # Output directory
│   ├── chains_chapter_1.json
│   ├── report_chapter_1.html
│   └── sankey_chapter_1.html
│
├── cache/              # Embedding cache
└── chain_storage/      # Checkpoints
```

## 🚀 Installation & Setup

### Prerequisites

```bash
# Python 3.7+
pip install numpy scipy
pip install sentence-transformers  # For embeddings
pip install plotly  # For visualizations
pip install anthropic  # For API validation (optional)
```

### Configuration

Create `config.json`:

```json
{
  "tables_dir": "tables",
  "reference_json": "tables_summary.json",
  "mask_dir": "mask",
  "similarity_threshold": 0.85,
  "use_api_validation": true,
  "CLAUDE_API_KEY": "your-api-key-here",
  "max_gap_years": 3,
  "chapters_to_process": "all"
}
```

### Input Data Preparation

1. **Tables Directory**: CSV files organized as `tables/YEAR/CHAPTER/TABLE_CHAPTER_YEAR.csv`
2. **Reference JSON**: Maps table IDs to Hebrew headers (see example in original post)
3. **Mask Files** (optional): Define which cells are features vs data points

## 🔧 Usage

### Basic Usage

```bash
python main.py
```

The system will:
1. Load all tables and metadata
2. Process each chapter independently
3. Match tables year-by-year within each chapter
4. Generate reports and visualizations

### Understanding the Matching Process

For each chapter:
1. **Year 1 (e.g., 2001)**: Initialize chains from all tables
2. **Year 2+**: For each subsequent year:
   - Generate embeddings for new tables
   - Compute similarity with existing chains
   - Apply Hungarian algorithm for optimal matching
   - Validate uncertain matches with API (if configured)
   - Update chains or create new ones
   - Handle gaps and dormant chains

### Matching Thresholds

- **≥ 0.97**: High confidence - automatic match
- **0.85-0.97**: Uncertain - triggers API validation
- **< 0.85**: Rejected unless API strongly confirms

### Output Interpretation

#### Chain JSON Structure
```json
{
  "chain_2_01_2001": {
    "id": "chain_2_01_2001",
    "tables": ["2_01_2001", "2_01_2002", ...],
    "years": [2001, 2002, ...],
    "headers": ["Hebrew header 1", "Hebrew header 2", ...],
    "mask_references": ["../mask/2001/01/2_01_2001.csv", ...],
    "gaps": [2007],  // Years where chain was dormant
    "similarities": [1.0, 0.981, ...],  // Match confidence scores
    "api_validated": [false, false, true, ...]  // Which matches used API
  }
}
```

#### Chain States
- **Active**: Currently matching tables
- **Dormant**: No match for 1-2 years (may reactivate)
- **Ended**: No match for 3+ years

## 🔍 Advanced Features

### API Validation

When enabled, the system uses Claude API to:
- Validate uncertain matches (similarity 0.85-0.97)
- Resolve conflicts when multiple chains claim a table
- Provide intelligent decisions on edge cases

### Split/Merge Detection

The system can identify:
- **Splits**: One table becoming multiple tables
- **Merges**: Multiple tables combining into one
- **Complex N:N relationships**

### Reactivation Logic

Dormant chains attempt reactivation when:
- A new unmatched table appears
- Similarity exceeds reactivation threshold (0.90)
- API validation confirms the match

## 📈 Performance Optimization

- **Embedding Cache**: Reuses computed embeddings
- **Batch Processing**: Processes tables in batches
- **Chapter Parallelization**: Each chapter processed independently
- **Checkpointing**: Saves progress for large datasets

## 🧪 Testing

```bash
python -m pytest tests/
```

Or run built-in tests:
```python
from test_suite import run_all_tests
run_all_tests()
```

## 📊 Statistics & Monitoring

The system tracks:
- Match rates per year
- API validation usage
- Processing times
- Chain lifecycle (active/dormant/ended)
- Confidence score distributions

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Submit a pull request

## 📝 Notes

- **Hebrew Text**: System specifically optimized for Hebrew headers
- **Year Range**: Currently processes 2001-2024
- **Chapters**: Processes each chapter independently for accuracy
- **API Costs**: Monitor API validation usage (tracked in statistics)

## ⚠️ Troubleshooting

### Common Issues

1. **"No module named sentence_transformers"**
   - Install: `pip install sentence-transformers`

2. **API validation not working**
   - Check CLAUDE_API_KEY in config.json
   - Verify API credits available

3. **Memory issues with large datasets**
   - Process fewer chapters at once
   - Reduce batch size in embeddings

4. **Poor matching accuracy**
   - Adjust similarity_threshold
   - Enable API validation
   - Check Hebrew text normalization

## 📚 Algorithm Details

### Hungarian Algorithm
- Solves optimal assignment problem
- Ensures 1:1 matching between chains and tables
- Minimizes total "cost" (maximizes similarity)

### Cosine Similarity
- Measures semantic similarity between embeddings
- Normalized to [0,1] range
- Robust to text length variations

### AlephBERT Embeddings
- Hebrew-specific BERT model
- Captures semantic meaning beyond keywords
- 768-dimensional dense vectors

