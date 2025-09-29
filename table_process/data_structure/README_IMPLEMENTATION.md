# BigQuery Migration Implementation Guide
## Project Overview
Complete migration of NCC (National Council for Children) data tables to Google BigQuery, processing ~600 chains across 15 chapters with ~6,000 tables total.

## Google Cloud Project Configuration
- **Project Name**: ncc-data-bigquery
- **Project ID**: ncc-data-bigquery  
- **Region**: EU (optimal for Israel)
- **APIs Enabled**: BigQuery API, Google Drive API
- **Budget Protection**: $1 alert with 50% threshold
- **Expected Cost**: $0 (within free tier for ~1GB data)

## Repository Structure
```
data_structure/
├── config/
│   ├── service-account-key.json    # GCP credentials (DO NOT COMMIT)
│   ├── chapter_mapping.json        # Hebrew chapter topic names  
│   └── chains_chapter_*.json       # 15 chapter files with chain definitions
├── utils/                          # Helper functions
├── sql/                            # BigQuery schemas
├── docs/                           # Documentation
├── examples/                       # Usage examples
├── .env                           # Environment variables (DO NOT COMMIT)
├── requirements.txt               # Python dependencies
└── final_migrate.py              # Working migration script
```

## Chapter Organization
Data organized into 15 thematic chapters:
1. מאפיינים דמוגרפיים של אוכלוסיית הילדים
2. ילדים עולים
3. משפחות, משקי בית ותנאי גדילה של ילדים
4. ילדים החיים מחוץ למשפחתם
5. ילדים וחינוך
6. ילדים ועולם הפנאי
7. ילדים עם צרכים מיוחדים
8. ילדים ובריאות
9. ילדים ותאונות
10. ילדים וכלכלה
11. ילדים בעולם המשפט
12. ילדים עוברי חוק
13. ילדים נפגעי עבירה
14. ילדים במצבי מצוקה
15. ילדים בישראל ובעולם

## Environment Setup

### 1. Service Account
- **Name**: bigquery-migration
- **Email**: bigquery-migration@ncc-data-bigquery.iam.gserviceaccount.com
- **Roles**: BigQuery Admin, Storage Admin
- **Key File**: `config/service-account-key.json`

### 2. Environment Variables (.env)
```
GCP_PROJECT_ID=ncc-data-bigquery
GOOGLE_APPLICATION_CREDENTIALS=config/service-account-key.json
CHAINS_JSON_PATH=config/
DRIVE_FOLDER_ID=1zN7fvpc0bMoMIpGmWSkUxtPy8F1nq-s-
```

### 3. Python Dependencies (requirements.txt)
- google-cloud-bigquery==3.11.0
- pandas==2.0.3
- python-dotenv==1.0.0
- google-api-python-client
- google-auth
- google-auth-oauthlib
- google-auth-httplib2

## Google Drive Configuration

### Drive Structure
- **Folder**: My Drive/ncc-tables/
- **Contents**: Shortcuts to "tables" and "mask" shared folders from the common drive (make sure to shortcut the unzipped ones)
- **Authentication**: Personal OAuth (gcloud auth)
- **Note**: Service accounts cannot access Drive shortcuts, personal authentication required. Thus, every new user must replicate these shortcuts manualy in a new "ncc-tables" folder in their local drive whish should look just like:
├── ncc-tables/
│   ├── tables (shortcut to the original tables as currently constructed)
│   ├── masks (same with the original mask)


### Setup for New Team Members
1. Get access to original shared "tables" and "mask" folders
2. Create "ncc-tables" folder in your My Drive
3. Add shortcuts of both folders inside "ncc-tables"
4. Note folder ID from URL
5. Authenticate: `gcloud auth application-default login`

## Migration Execution

### Migration Script
- **File**: `final_migrate.py`
- **Dataset**: `ncc-data-bigquery.chains_dataset`
- **Features**:
  - Creates BigQuery dataset with chapter partitioning
  - Processes all 15 chapters with ~600 chains
  - Downloads CSVs from Google Drive
  - Maintains Hebrew text integrity

### How to Run
```bash
# 1. Authenticate with Google
gcloud auth application-default login

# 2. Test with single chain (recommended)
python3 final_migrate.py --test

# 3. Run full migration
python3 final_migrate.py
```

### Estimated Runtime
- Single chain: ~3 minutes
- Full migration: ~4 hours

## Migration Results (Completed September 28, 2025)
- **Chapters**: 15 processed
- **Chains**: ~600 migrated
- **Tables**: ~6,000 loaded
- **Location**: `ncc-data-bigquery.chains_dataset.tables_data`

## Querying the Data

```sql
-- Get all data for a specific chain
SELECT * FROM `ncc-data-bigquery.chains_dataset.tables_data` 
WHERE chain_id = 'chain_2_01_2001'

-- Summary statistics by chapter
SELECT 
  chapter_id,
  COUNT(DISTINCT chain_id) as chains,
  COUNT(DISTINCT table_id) as tables,
  COUNT(*) as total_rows
FROM `ncc-data-bigquery.chains_dataset.tables_data`
GROUP BY chapter_id

-- Search for specific Hebrew terms
SELECT * FROM `ncc-data-bigquery.chains_dataset.tables_data`
WHERE chapter_name LIKE '%חינוך%'
```

## Key Technical Notes
- Personal authentication required for Drive shortcuts access
- Service accounts work for BigQuery but not Drive shortcuts
- Hebrew text encoding preserved throughout pipeline
- Data partitioned by chapter for query optimization
- All data within Google Cloud free tier limits

## Maintenance & Support
- Re-run migrations: Use `final_migrate.py`
- Add new chapters: Update `config/chains_chapter_*.json`
- Monitor costs: Check GCP billing dashboard
- Query optimization: Use chapter_id partitioning