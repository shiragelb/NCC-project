# BigQuery Migration Implementation Guide

## ✅ Completed Setup (September 28, 2025)

### 1. Google Cloud Project Configuration
- **Project Name**: ncc-data-bigquery
- **Project ID**: ncc-data-bigquery
- **Region**: EU (for optimal performance from Israel)
- **APIs Enabled**: 
  - BigQuery API ✓
  - Google Drive API ✓

### 2. Service Account Created
- **Name**: bigquery-migration
- **Email**: bigquery-migration@ncc-data-bigquery.iam.gserviceaccount.com
- **Roles Assigned**:
  - BigQuery Admin
  - Storage Admin
- **Key File**: `config/service-account-key.json` (JSON key downloaded and stored)

### 3. Repository Structure Created
```
data_structure/
├── config/
│   ├── service-account-key.json    # GCP credentials (DO NOT COMMIT)
│   ├── chapter_mapping.json        # Hebrew chapter topic names  
│   └── chains_chapter_*.json       # 15 chapter files with chain definitions
├── utils/                          # Helper functions (to be implemented)
├── sql/                            # BigQuery schemas
├── docs/                           # Additional documentation
├── examples/                       # Usage examples
├── .env                           # Environment variables (DO NOT COMMIT)
├── requirements.txt               # Python dependencies
└── migrate_to_bigquery.py        # Main migration script (to be implemented)
```

### 4. Chapter Organization
Data organized into 15 chapters by topic:
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

### 5. Environment Configuration Complete
`.env` file configured with:
```
GCP_PROJECT_ID=ncc-data-bigquery
GOOGLE_APPLICATION_CREDENTIALS=config/service-account-key.json
CHAINS_JSON_PATH=config/
```

### 6. Dependencies Defined
`requirements.txt` includes:
- google-cloud-bigquery==3.11.0
- pandas==2.0.3
- python-dotenv==1.0.0
- Additional Google Cloud libraries

---
## Next Steps (TO BE IMPLEMENTED)
- [ ] Test BigQuery connection
- [ ] Create BigQuery dataset and tables
- [ ] Set up Google Drive access
- [ ] Build migration pipeline
- [ ] Execute migration
- [ ] Validate data integrity

## Google Drive Setup (COMPLETED)
### Drive Shortcut Created
- Shortcut name: "ncc-tables"
- Folder ID: 1zN7fvpc0bMoMIpGmWSkUxtPy8F1nq-s-
- Location: My Drive
- Contains: "mask" and "tables" folders

### For Team Members to Reproduce:
1. Open Google Drive
2. Find shared "ncc-tables" folder in "Shared with me"
3. Right-click → "Add shortcut to Drive" → "My Drive"
4. Get folder ID from URL when opened
5. Update .env with DRIVE_FOLDER_ID

## Drive Access Configuration (COMPLETED)
- Service account granted viewer access to ncc-tables folder
- No individual authentication needed for future users
- Access automated through service account

## Budget Protection (COMPLETED)
- Budget alert set at $1 with 50% threshold
- Monitors: ncc-data-bigquery project
- Expected cost: $0 (within free tier for ~1GB data)

## Migration Script (READY)
- Script: migrate_to_bigquery.py
- Creates BigQuery dataset with chapter partitioning
- Processes all 15 chapters with ~600 chains
- Downloads CSVs from Google Drive
- Loads data maintaining Hebrew text integrity

## How to Run Migration
1. Ensure all config files are in place
2. Test with single chain first (recommended)
3. Run full migration: `python3 migrate_to_bigquery.py`

## Estimated Runtime
- Single chain: ~1 minute
- Full migration: ~2-3 hours for 6000 tables

## Google Drive Access Setup (Personal Authentication Method)

### How the Drive Structure Was Created
1. Created "ncc-tables" folder in My Drive
2. Added shortcuts to two shared folders:
   - "tables" folder (shortcut from shared source)
   - "mask" folder (shortcut from shared source)
3. Result: My Drive/ncc-tables/ contains both shortcuts

### For Future Team Members to Reproduce
1. Get access to the original shared "tables" and "mask" folders
2. Create a folder in your My Drive called "ncc-tables"
3. Add shortcuts of both folders inside "ncc-tables"
4. Note your folder ID from the URL
5. Use personal OAuth authentication (not service account)

### Why Personal Auth Instead of Service Account
- Service accounts cannot access Drive shortcuts
- Shortcuts are personal references, not actual permissions
- Personal auth allows access to your shortcuts

## ✅ FINAL MIGRATION COMPLETED (September 28, 2025)

### Working Solution
- **Script**: `final_migrate.py` 
- **Method**: Personal Google authentication (gcloud auth)
- **Issue Resolved**: Drive API now works with shortcuts after permission refresh

### Migration Statistics
- 15 chapters processed
- ~600 chains migrated
- ~6,000 tables loaded to BigQuery
- Data accessible at: `ncc-data-bigquery.chains_dataset`

### How to Query Data
```sql
-- Example: Get all data for a chain
SELECT * FROM `ncc-data-bigquery.chains_dataset.tables_data` 
WHERE chain_id = 'chain_2_01_2001'

-- Example: Summary by chapter
SELECT 
  chapter_id,
  COUNT(DISTINCT chain_id) as chains,
  COUNT(DISTINCT table_id) as tables,
  COUNT(*) as total_rows
FROM `ncc-data-bigquery.chains_dataset.tables_data`
GROUP BY chapter_id
```

### For Future Users
1. Use `final_migrate.py` for any re-runs
2. Authenticate with: `gcloud auth application-default login`
3. Ensure Drive shortcuts are in place
4. Run: `python3 final_migrate.py`

### Key Learning
- Service accounts cannot access Google Drive shortcuts
- Personal auth with gcloud works best for Drive shortcuts
- Always test with one chain before full migration
