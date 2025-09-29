#!/usr/bin/env python3
"""
NCC Data Migration using Personal Google Authentication
This script uses your personal Google account to access Drive shortcuts
"""

import os
import json
import pandas as pd
import re
import io
import pickle
from pathlib import Path
from google.cloud import bigquery
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from dotenv import load_dotenv
from tqdm import tqdm
import logging
from datetime import datetime

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('migration_personal.log'),
        logging.StreamHandler()
    ]
)

load_dotenv()

# Google Drive scope
SCOPES = ['https://www.googleapis.com/auth/drive.readonly']

def authenticate_personal_drive():
    """Authenticate using personal Google account"""
    creds = None
    token_file = 'token.pickle'
    
    # Load existing token
    if os.path.exists(token_file):
        with open(token_file, 'rb') as token:
            creds = pickle.load(token)
    
    # If there are no (valid) credentials, authenticate
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            # Create a simple client configuration
            client_config = {
                "installed": {
                    "client_id": "YOUR_CLIENT_ID",
                    "client_secret": "YOUR_CLIENT_SECRET",
                    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                    "token_uri": "https://oauth2.googleapis.com/token",
                    "redirect_uris": ["http://localhost"]
                }
            }
            
            # For now, we'll use a simpler approach
            print("\n" + "="*60)
            print("PERSONAL AUTHENTICATION REQUIRED")
            print("="*60)
            print("\nOption 1 (Recommended): Use Google Colab method")
            print("1. Run this in a new Python script:")
            print("""
from google.colab import auth
auth.authenticate_user()
import pickle
from google.auth import default
creds, _ = default()
with open('token.pickle', 'wb') as f:
    pickle.dump(creds, f)
print('Token saved!')
            """)
            print("\nOption 2: Create OAuth credentials")
            print("1. Go to: https://console.cloud.google.com/apis/credentials")
            print("2. Create OAuth 2.0 Client ID")
            print("3. Download JSON as 'credentials.json'")
            print("="*60)
            
            if os.path.exists('credentials.json'):
                flow = InstalledAppFlow.from_client_secrets_file(
                    'credentials.json', SCOPES)
                creds = flow.run_local_server(port=0)
            else:
                raise Exception("Please set up authentication first (see instructions above)")
        
        # Save the credentials for the next run
        with open(token_file, 'wb') as token:
            pickle.dump(creds, token)
    
    return build('drive', 'v3', credentials=creds)

def clean_hebrew_text(text):
    """Clean Hebrew text for names"""
    if not text:
        return "unnamed"
    # Remove year ranges
    text = re.sub(r'\d{4}-\d{4}', '', text)
    # Remove table numbers  
    text = re.sub(r'לוח\s+\d+\.?\d*', '', text)
    # Clean whitespace
    text = ' '.join(text.split())
    return text[:200]

def download_csv_from_drive(service, file_path, folder_id):
    """Download CSV file from Google Drive"""
    try:
        # Clean the path
        file_path = file_path.replace('../', '')
        path_parts = file_path.split('/')
        
        # Navigate through folders
        current_folder_id = folder_id
        
        for folder_name in path_parts[:-1]:
            # Find folder
            query = f"'{current_folder_id}' in parents and name = '{folder_name}' and mimeType = 'application/vnd.google-apps.folder'"
            response = service.files().list(q=query, fields="files(id, name)").execute()
            folders = response.get('files', [])
            
            if folders:
                current_folder_id = folders[0]['id']
            else:
                logging.warning(f"Folder not found: {folder_name}")
                return None
        
        # Get the file
        filename = path_parts[-1]
        query = f"'{current_folder_id}' in parents and name = '{filename}'"
        response = service.files().list(q=query, fields="files(id, name)").execute()
        files = response.get('files', [])
        
        if not files:
            logging.warning(f"File not found: {filename}")
            return None
        
        # Download file content
        file_id = files[0]['id']
        request = service.files().get_media(fileId=file_id)
        file_buffer = io.BytesIO()
        downloader = MediaIoBaseDownload(file_buffer, request)
        
        done = False
        while not done:
            status, done = downloader.next_chunk()
        
        file_buffer.seek(0)
        return file_buffer
        
    except Exception as e:
        logging.error(f"Error downloading {file_path}: {e}")
        return None

def migrate_chapter_data(chapter_num, drive_service, bq_client):
    """Migrate a single chapter's data"""
    
    # Load chapter configuration
    chapter_file = f"config/chains_chapter_{chapter_num}.json"
    with open('config/chapter_mapping.json', 'r', encoding='utf-8') as f:
        chapter_mapping = json.load(f)
    
    chapter_name = chapter_mapping[str(chapter_num)]
    
    logging.info(f"Processing Chapter {chapter_num}: {chapter_name}")
    
    # Load chains for this chapter
    with open(chapter_file, 'r', encoding='utf-8') as f:
        chains = json.load(f)
    
    # Process each chain
    successful_chains = 0
    failed_chains = 0
    
    for chain_id, chain_data in tqdm(chains.items(), desc=f"Chapter {chapter_num}"):
        try:
            # Clean chain name from first header
            chain_name = clean_hebrew_text(chain_data['headers'][0])
            
            # Insert chain metadata
            chain_metadata = {
                'chapter_id': chapter_num,
                'chapter_name': chapter_name,
                'chain_id': chain_id,
                'chain_name': chain_name,
                'table_count': len(chain_data['tables']),
                'years': chain_data.get('years', []),
                'gaps': chain_data.get('gaps', [])
            }
            
            # Insert to chains_metadata table
            table_id = f"{os.getenv('GCP_PROJECT_ID')}.chains_dataset.chains_metadata"
            errors = bq_client.insert_rows_json(table_id, [chain_metadata])
            
            if errors:
                logging.error(f"Error inserting chain metadata: {errors}")
                continue
            
            # Process tables in this chain
            tables_processed = 0
            
            for i, table_id_str in enumerate(chain_data['tables']):
                try:
                    year = chain_data['years'][i] if i < len(chain_data['years']) else None
                    header = chain_data['headers'][i] if i < len(chain_data['headers']) else ""
                    table_name = clean_hebrew_text(header)
                    
                    # Build file path
                    # Expected format: tables/YYYY/MM/X_MM_YYYY.csv
                    parts = table_id_str.split('_')
                    if len(parts) >= 3:
                        month = parts[1]
                        year_str = parts[2]
                        table_path = f"tables/{year_str}/{month}/{table_id_str}.csv"
                    else:
                        logging.warning(f"Unexpected table ID format: {table_id_str}")
                        continue
                    
                    # Download CSV
                    csv_content = download_csv_from_drive(
                        drive_service, 
                        table_path, 
                        os.getenv('DRIVE_FOLDER_ID')
                    )
                    
                    if csv_content:
                        # Read CSV
                        df = pd.read_csv(csv_content, encoding='utf-8-sig', header=None)
                        
                        # Convert to BigQuery rows
                        rows = []
                        for row_idx, row in df.iterrows():
                            for col_idx, value in enumerate(row):
                                rows.append({
                                    'chapter_id': chapter_num,
                                    'chain_id': chain_id,
                                    'table_id': table_id_str,
                                    'table_name': table_name,
                                    'year': int(year) if year else 0,
                                    'row_index': row_idx,
                                    'col_index': col_idx,
                                    'cell_value': str(value) if pd.notna(value) else None
                                })
                        
                        # Insert to BigQuery in batches
                        if rows:
                            table_ref = f"{os.getenv('GCP_PROJECT_ID')}.chains_dataset.tables_data"
                            for batch_start in range(0, len(rows), 500):
                                batch = rows[batch_start:batch_start + 500]
                                errors = bq_client.insert_rows_json(table_ref, batch)
                                if errors:
                                    logging.error(f"Error inserting table data: {errors}")
                        
                        tables_processed += 1
                    
                    # Process mask if exists
                    if i < len(chain_data.get('mask_references', [])):
                        mask_path = chain_data['mask_references'][i].replace('../', '')
                        mask_content = download_csv_from_drive(
                            drive_service,
                            mask_path,
                            os.getenv('DRIVE_FOLDER_ID')
                        )
                        
                        if mask_content:
                            # Read mask CSV
                            mask_df = pd.read_csv(mask_content, encoding='utf-8-sig', header=None)
                            
                            # Convert to BigQuery rows
                            mask_rows = []
                            for row_idx, row in mask_df.iterrows():
                                for col_idx, value in enumerate(row):
                                    # Check if it's a feature
                                    is_feature = str(value).lower() == 'feature' if pd.notna(value) else False
                                    
                                    mask_rows.append({
                                        'chapter_id': chapter_num,
                                        'chain_id': chain_id,
                                        'table_id': table_id_str,
                                        'mask_name': f"mask - {table_name}",
                                        'row_index': row_idx,
                                        'col_index': col_idx,
                                        'is_feature': is_feature
                                    })
                            
                            # Insert mask data
                            if mask_rows:
                                table_ref = f"{os.getenv('GCP_PROJECT_ID')}.chains_dataset.masks_data"
                                for batch_start in range(0, len(mask_rows), 500):
                                    batch = mask_rows[batch_start:batch_start + 500]
                                    errors = bq_client.insert_rows_json(table_ref, batch)
                                    if errors:
                                        logging.error(f"Error inserting mask data: {errors}")
                
                except Exception as e:
                    logging.error(f"Error processing table {table_id_str}: {e}")
                    continue
            
            logging.info(f"Chain {chain_id}: {tables_processed}/{len(chain_data['tables'])} tables processed")
            successful_chains += 1
            
        except Exception as e:
            logging.error(f"Error processing chain {chain_id}: {e}")
            failed_chains += 1
            continue
    
    logging.info(f"Chapter {chapter_num} complete: {successful_chains} successful, {failed_chains} failed")
    return successful_chains, failed_chains

def main():
    """Main migration function"""
    start_time = datetime.now()
    
    print("="*60)
    print("NCC DATA MIGRATION TO BIGQUERY")
    print("Using Personal Authentication")
    print("="*60)
    
    # Initialize BigQuery client
    bq_client = bigquery.Client(project=os.getenv('GCP_PROJECT_ID'))
    logging.info("BigQuery client initialized")
    
    # Authenticate with Google Drive
    print("\nAuthenticating with Google Drive...")
    drive_service = authenticate_personal_drive()
    logging.info("Drive authentication successful")
    
    # Test connection
    print("\nTesting Drive access...")
    try:
        # List files in the ncc-tables folder
        query = f"'{os.getenv('DRIVE_FOLDER_ID')}' in parents"
        results = drive_service.files().list(q=query, pageSize=10, fields="files(id, name)").execute()
        items = results.get('files', [])
        print(f"Found {len(items)} items in ncc-tables folder")
        for item in items:
            print(f"  - {item['name']}")
    except Exception as e:
        print(f"Error accessing Drive: {e}")
        return
    
    # Process chapters
    print("\n" + "="*60)
    print("STARTING MIGRATION")
    print("="*60)
    
    total_successful = 0
    total_failed = 0
    
    # You can test with one chapter first
    test_mode = input("\nTest with Chapter 1 only? (y/n): ").lower() == 'y'
    
    if test_mode:
        chapters_to_process = [1]
    else:
        chapters_to_process = range(1, 16)
    
    for chapter_num in chapters_to_process:
        successful, failed = migrate_chapter_data(chapter_num, drive_service, bq_client)
        total_successful += successful
        total_failed += failed
    
    # Final summary
    end_time = datetime.now()
    duration = end_time - start_time
    
    print("\n" + "="*60)
    print("MIGRATION COMPLETE")
    print("="*60)
    print(f"Total chains processed: {total_successful}")
    print(f"Failed chains: {total_failed}")
    print(f"Duration: {duration}")
    print(f"Check migration_personal.log for details")
    
    # Verify in BigQuery
    query = """
    SELECT 
        chapter_id,
        COUNT(DISTINCT chain_id) as chains,
        COUNT(DISTINCT table_id) as tables,
        COUNT(*) as total_rows
    FROM `ncc-data-bigquery.chains_dataset.tables_data`
    GROUP BY chapter_id
    ORDER BY chapter_id
    """
    
    print("\nData in BigQuery:")
    for row in bq_client.query(query):
        print(f"  Chapter {row.chapter_id}: {row.chains} chains, {row.tables} tables, {row.total_rows} rows")

if __name__ == "__main__":
    main()
