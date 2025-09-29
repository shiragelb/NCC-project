#!/usr/bin/env python3
import os
import json
import pandas as pd
import re
import io
from google.cloud import bigquery
from google.auth import default
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from dotenv import load_dotenv
from tqdm import tqdm
import logging

logging.basicConfig(level=logging.INFO)
load_dotenv()

creds, _ = default()
drive = build('drive', 'v3', credentials=creds)
bq_client = bigquery.Client(project=os.getenv('GCP_PROJECT_ID'))

def download_csv(file_path, folder_id):
    try:
        # Determine shortcut
        if 'mask/' in file_path:
            shortcut_name = 'mask'
            file_path = file_path.replace('mask/', '').replace('../', '')
        else:
            shortcut_name = 'tables'
            file_path = file_path.replace('tables/', '').replace('../', '')
        
        # Get shortcut target
        results = drive.files().list(
            q=f"'{folder_id}' in parents and name='{shortcut_name}'",
            fields="files(id,shortcutDetails)"
        ).execute()
        
        if not results['files']:
            return None
            
        current_id = results['files'][0].get('shortcutDetails', {}).get('targetId', results['files'][0]['id'])
        
        # Navigate path
        for part in file_path.split('/')[:-1]:
            results = drive.files().list(
                q=f"'{current_id}' in parents and name='{part}'",
                fields="files(id)"
            ).execute()
            if results['files']:
                current_id = results['files'][0]['id']
            else:
                return None
        
        # Get file
        filename = file_path.split('/')[-1]
        results = drive.files().list(
            q=f"'{current_id}' in parents and name='{filename}'",
            fields="files(id)"
        ).execute()
        
        if not results['files']:
            return None
            
        # Download
        request = drive.files().get_media(fileId=results['files'][0]['id'])
        buffer = io.BytesIO()
        downloader = MediaIoBaseDownload(buffer, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        buffer.seek(0)
        return buffer
        
    except Exception as e:
        logging.error(f"Error: {e}")
        return None

def clean_text(text):
    if not text: 
        return "unnamed"
    text = re.sub(r'\d{4}-\d{4}', '', text)
    text = re.sub(r'לוח\s+\d+\.?\d*', '', text)
    return ' '.join(text.split())[:200]

def test_one_chain():
    # Test with first chain from Chapter 1
    with open('config/chains_chapter_1.json', 'r', encoding='utf-8') as f:
        chains = json.load(f)
    
    chain_id = list(chains.keys())[0]
    chain_data = chains[chain_id]
    
    print(f"\nTesting with chain: {chain_id}")
    print(f"Tables: {len(chain_data['tables'])}")
    
    # Insert metadata
    metadata = {
        'chapter_id': 1,
        'chapter_name': "מאפיינים דמוגרפיים של אוכלוסיית הילדים",
        'chain_id': chain_id,
        'chain_name': clean_text(chain_data['headers'][0]),
        'table_count': len(chain_data['tables']),
        'years': chain_data.get('years', []),
        'gaps': chain_data.get('gaps', [])
    }
    
    bq_client.insert_rows_json(f"{os.getenv('GCP_PROJECT_ID')}.chains_dataset.chains_metadata", [metadata])
    
    # Process first 2 tables only
    for i, table_name in enumerate(chain_data['tables'][:2]):
        print(f"Processing table: {table_name}")
        parts = table_name.split('_')
        if len(parts) >= 3:
            csv_buffer = download_csv(f"tables/{parts[2]}/{parts[1]}/{table_name}.csv", os.getenv('DRIVE_FOLDER_ID'))
            if csv_buffer:
                df = pd.read_csv(csv_buffer, encoding='utf-8-sig', header=None)
                print(f"  Loaded {len(df)} rows x {len(df.columns)} columns")
                
                # Just insert first 10 rows as test
                rows = []
                for row_idx in range(min(10, len(df))):
                    for col_idx in range(len(df.columns)):
                        rows.append({
                            'chapter_id': 1,
                            'chain_id': chain_id,
                            'table_id': table_name,
                            'table_name': clean_text(chain_data['headers'][i] if i < len(chain_data['headers']) else ""),
                            'year': int(parts[2]),
                            'row_index': row_idx,
                            'col_index': col_idx,
                            'cell_value': str(df.iloc[row_idx, col_idx]) if pd.notna(df.iloc[row_idx, col_idx]) else None
                        })
                
                table_ref = f"{os.getenv('GCP_PROJECT_ID')}.chains_dataset.tables_data"
                bq_client.insert_rows_json(table_ref, rows)
                print(f"  Inserted {len(rows)} cells to BigQuery")
    
    # Check results
    query = f"SELECT COUNT(*) as count FROM `{os.getenv('GCP_PROJECT_ID')}.chains_dataset.tables_data` WHERE chain_id = '{chain_id}'"
    result = list(bq_client.query(query))[0]
    print(f"\n✓ Test complete! {result.count} rows in BigQuery")

def full_migration():
    # Load chapter mapping
    with open('config/chapter_mapping.json', 'r', encoding='utf-8') as f:
        chapter_mapping = json.load(f)
    
    for chapter_num in range(1, 16):
        with open(f'config/chains_chapter_{chapter_num}.json', 'r', encoding='utf-8') as f:
            chains = json.load(f)
        
        chapter_name = chapter_mapping[str(chapter_num)]
        print(f"\nChapter {chapter_num}: {len(chains)} chains")
        
        for chain_id, chain_data in tqdm(chains.items(), desc=f"Chapter {chapter_num}"):
            try:
                # Insert chain metadata
                metadata = {
                    'chapter_id': chapter_num,
                    'chapter_name': chapter_name,
                    'chain_id': chain_id,
                    'chain_name': clean_text(chain_data['headers'][0] if chain_data['headers'] else ""),
                    'table_count': len(chain_data['tables']),
                    'years': chain_data.get('years', []),
                    'gaps': chain_data.get('gaps', [])
                }
                
                bq_client.insert_rows_json(
                    f"{os.getenv('GCP_PROJECT_ID')}.chains_dataset.chains_metadata", 
                    [metadata]
                )
                
                # Process ALL tables in the chain
                for i, table_name in enumerate(chain_data['tables']):
                    parts = table_name.split('_')
                    if len(parts) >= 3:
                        csv_buffer = download_csv(
                            f"tables/{parts[2]}/{parts[1]}/{table_name}.csv", 
                            os.getenv('DRIVE_FOLDER_ID')
                        )
                        
                        if csv_buffer:
                            df = pd.read_csv(csv_buffer, encoding='utf-8-sig', header=None)
                            
                            # Process ALL rows (not just 10 like in test)
                            rows = []
                            for row_idx in range(len(df)):
                                for col_idx in range(len(df.columns)):
                                    rows.append({
                                        'chapter_id': chapter_num,
                                        'chain_id': chain_id,
                                        'table_id': table_name,
                                        'table_name': clean_text(
                                            chain_data['headers'][i] if i < len(chain_data['headers']) else ""
                                        ),
                                        'year': int(parts[2]),
                                        'row_index': row_idx,
                                        'col_index': col_idx,
                                        'cell_value': str(df.iloc[row_idx, col_idx]) if pd.notna(df.iloc[row_idx, col_idx]) else None
                                    })
                            
                            # Insert to BigQuery in batches of 500
                            table_ref = f"{os.getenv('GCP_PROJECT_ID')}.chains_dataset.tables_data"
                            for batch_start in range(0, len(rows), 500):
                                batch = rows[batch_start:batch_start + 500]
                                errors = bq_client.insert_rows_json(table_ref, batch)
                                if errors:
                                    logging.error(f"Insert errors for {table_name}: {errors}")
                        
                        # Also process masks if they exist
                        if i < len(chain_data.get('mask_references', [])):
                            mask_path = chain_data['mask_references'][i].replace('../', '')
                            mask_buffer = download_csv(mask_path, os.getenv('DRIVE_FOLDER_ID'))
                            
                            if mask_buffer:
                                mask_df = pd.read_csv(mask_buffer, encoding='utf-8-sig', header=None)
                                
                                mask_rows = []
                                for row_idx in range(len(mask_df)):
                                    for col_idx in range(len(mask_df.columns)):
                                        mask_rows.append({
                                            'chapter_id': chapter_num,
                                            'chain_id': chain_id,
                                            'table_id': table_name,
                                            'mask_name': f"mask - {clean_text(chain_data['headers'][i] if i < len(chain_data['headers']) else '')}",
                                            'row_index': row_idx,
                                            'col_index': col_idx,
                                            'is_feature': str(mask_df.iloc[row_idx, col_idx]).lower() == 'feature' if pd.notna(mask_df.iloc[row_idx, col_idx]) else False
                                        })
                                
                                # Insert mask data in batches
                                mask_table_ref = f"{os.getenv('GCP_PROJECT_ID')}.chains_dataset.masks_data"
                                for batch_start in range(0, len(mask_rows), 500):
                                    batch = mask_rows[batch_start:batch_start + 500]
                                    errors = bq_client.insert_rows_json(mask_table_ref, batch)
                                    if errors:
                                        logging.error(f"Mask insert errors: {errors}")
            
            except Exception as e:
                logging.error(f"Error processing chain {chain_id}: {e}")
                continue

# Main execution
if __name__ == "__main__":
    print("Testing...")
    test_file = download_csv("tables/2001/01/1_01_2001.csv", os.getenv('DRIVE_FOLDER_ID'))
    if test_file:
        print("✓ Successfully accessed Drive files!")
        print("Ready to migrate. This will take time...")
    else:
        print("❌ Could not access files. Check Drive permissions.")
    
    test_one_chain()
    
    if input("\nRun FULL migration? (y/n): ") == 'y':
        full_migration()
        print("\n✓ Migration complete!")