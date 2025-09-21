"""
Google Drive manager for downloading Word documents.
"""

# ========================================================================
# INSTRUCTIONS: PASTE YOUR CODE FROM NOTEBOOK
# ========================================================================
# FROM LINES: ~25-350
# CONTENT: The entire GoogleDriveManager class
# 
# MODIFICATIONS NEEDED:
# 1. Remove the line: auth.authenticate_user() from __init__ or _authenticate()
# 2. Remove the line: self._authenticate() from __init__
# 3. Change _authenticate() to _build_service() and just build the drive service
# ========================================================================

import os
import io
import logging
import pandas as pd
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

logger = logging.getLogger(__name__)

# GoogleDriveManager class
class GoogleDriveManager:
    """
    Manages Google Drive operations including listing, filtering, downloading, and uploading files.
    """

    def __init__(self, folder_id):
        """
        Initialize the GoogleDriveManager with authentication and folder ID.

        Args:
            folder_id: The Google Drive folder ID to work with
        """
        self.folder_id = folder_id
        self.drive_service = None
        self.files_df = None  # Cache for file listings

        # Authenticate and build service
        self._authenticate()

    def _authenticate(self):
        """Authenticate with Google Drive and build the service object."""
        try:
            # auth.authenticate_user() # no need for that here handeled in colab
            self.drive_service = build('drive', 'v3')
            logger.info("✅ Successfully authenticated with Google Drive")
        except Exception as e:
            logger.error(f"❌ Authentication failed: {e}")
            raise

    def list_all_files(self, force_refresh=False):
        """
        Recursively list all files in the folder and subfolders.

        Args:
            force_refresh: If True, force a new listing even if cached data exists

        Returns:
            pd.DataFrame: DataFrame with columns [file_name, file_path, file_id, file_url]
        """
        if self.files_df is not None and not force_refresh:
            logger.info("📋 Using cached file list")
            return self.files_df

        logger.info("🔍 Listing all files in folder...")
        all_files = self._list_files_recursive(self.folder_id)

        # Convert to DataFrame
        if all_files:
            self.files_df = pd.DataFrame(all_files)

            # Deduplicate by folder+name (file_path already encodes folder)
            self.files_df = self.files_df.drop_duplicates(
                subset=["file_path", "file_name"], keep="first"
            )

            logger.info(f"✅ Found {len(self.files_df)} unique files")
        else:
            self.files_df = pd.DataFrame(columns=['file_name', 'file_path', 'file_id', 'file_url'])
            logger.info("📁 No files found in folder")

        return self.files_df

    def _list_files_recursive(self, parent_id, parent_path=""):
        """
        Recursively list files in a folder.

        Args:
            parent_id: Google Drive folder ID
            parent_path: Path string for tracking folder hierarchy

        Returns:
            list: List of file dictionaries
        """
        all_files = []
        query = f"'{parent_id}' in parents and trashed=false"
        page_token = None

        while True:
            try:
                response = self.drive_service.files().list(
                    q=query,
                    spaces='drive',
                    fields='nextPageToken, files(id, name, mimeType)',
                    pageToken=page_token
                ).execute()

                for item in response.get('files', []):
                    item_path = f"{parent_path}/{item['name']}" if parent_path else item['name']

                    if item['mimeType'] == 'application/vnd.google-apps.folder':
                        # Recurse into subfolder
                        all_files.extend(self._list_files_recursive(item['id'], item_path))
                    else:
                        all_files.append({
                            "file_name": item['name'],
                            "file_path": item_path,
                            "file_id": item['id'],
                            "file_url": f"https://drive.google.com/file/d/{item['id']}/view?usp=sharing"
                        })

                page_token = response.get('nextPageToken', None)
                if page_token is None:
                    break

            except Exception as e:
                logger.error(f"❌ Error listing files in {parent_path}: {e}")
                break

        return all_files

    def filter_files(self, df=None, years=None, chapters=None):
        """
        Filter files based on specified years and chapters using exact matching.

        Args:
            df: DataFrame to filter (if None, uses cached files_df)
            years: List of years to include (e.g., [2021, 2022, 2023])
            chapters: List of chapter numbers to include (e.g., [1, 2, 5, 10])

        Returns:
            pd.DataFrame: Filtered DataFrame containing only requested files
        """
        # Use provided df or cached one
        if df is None:
            if self.files_df is None:
                logger.warning("⚠️ No files listed yet. Running list_all_files() first.")
                self.list_all_files()
            df = self.files_df.copy()
        else:
            df = df.copy()

        if df.empty:
            logger.warning("⚠️ No files to filter")
            return df

        # Apply year filter
        if years is not None:
            year_strings = [str(year) for year in years]
            # Exact match: year must be a folder in the path
            year_mask = df['file_path'].apply(
                lambda path: any(f"/{year}/" in f"/{path}" or path.startswith(f"{year}/")
                               for year in year_strings)
            )
            df = df[year_mask]
            logger.info(f"📅 Filtered for years: {years} - {len(df)} files")

        # Apply chapter filter
        if chapters is not None:
            # Exact match for filename pattern: 01.docx, 02.docx, etc.
            chapter_filenames = [f"{ch:02d}.docx" for ch in chapters]
            chapter_mask = df['file_name'].apply(
                lambda name: name in chapter_filenames
            )
            df = df[chapter_mask]
            logger.info(f"📖 Filtered for chapters: {chapters} - {len(df)} files")

        return df

    def download_files(self, filtered_df, download_dir="/content/reports"):
        """
        Download files from a filtered DataFrame.

        Args:
            filtered_df: DataFrame containing files to download
            download_dir: Base directory for downloads

        Returns:
            dict: Dictionary mapping file paths to local paths
        """
        if filtered_df is None or filtered_df.empty:
            logger.warning("⚠️ No files to download")
            return {}

        downloaded_files = {}
        total_files = len(filtered_df)

        logger.info(f"📥 Starting download of {total_files} files...")

        for idx, row in filtered_df.iterrows():
            file_id = row['file_id']
            file_name = row['file_name']
            file_path = row['file_path']

            # Extract year from path (assuming structure: year/filename)
            path_parts = file_path.split('/')
            if len(path_parts) >= 2:
                year = path_parts[0]
                local_path = os.path.join(download_dir, year, file_name)
            else:
                local_path = os.path.join(download_dir, file_name)

            # Ensure directory exists
            os.makedirs(os.path.dirname(local_path), exist_ok=True)

            try:
                # Download file
                request = self.drive_service.files().get_media(fileId=file_id)
                fh = io.FileIO(local_path, "wb")
                downloader = MediaIoBaseDownload(fh, request)

                done = False
                while not done:
                    status, done = downloader.next_chunk()
                    if status:
                        progress = int(status.progress() * 100)
                        print(f"⬇️  Downloading {file_name}: {progress}%", end='\r')

                logger.info(f"✅ Downloaded {file_name} to {local_path}")
                downloaded_files[file_path] = local_path

            except Exception as e:
                logger.warning(f"⚠️ Failed to download {file_name}: {e}")
                continue

        logger.info(f"✅ Download complete: {len(downloaded_files)}/{total_files} files")
        return downloaded_files

    def download_selective(self, years=None, chapters=None, download_dir="/content/reports"):
        """
        Convenience method to list, filter, and download files in one operation.

        Args:
            years: List of years to download (e.g., [2021, 2022, 2023])
            chapters: List of chapter numbers to download (e.g., [1, 2, 5, 10])
            download_dir: Base directory for downloads

        Returns:
            dict: Dictionary mapping file paths to local paths

        Example:
            # Download chapters 1-5 for years 2021-2023
            manager.download_selective(
                years=range(2021, 2024),
                chapters=range(1, 6),
                download_dir="/content/reports"
            )
        """
        # Step 1: List all files
        logger.info("🚀 Starting selective download workflow...")
        all_files = self.list_all_files()

        # Step 2: Filter files
        filtered_files = self.filter_files(all_files, years=years, chapters=chapters)

        if filtered_files is None or filtered_files.empty:
            logger.warning("⚠️ No files match the specified criteria")
            return {}

        logger.info(f"📊 Found {len(filtered_files)} files matching criteria")

        # Step 3: Download filtered files
        downloaded = self.download_files(filtered_files, download_dir)

        return downloaded

    def get_summary(self, df=None):
        """
        Get summary statistics about the files.

        Args:
            df: DataFrame to summarize (if None, uses cached files_df)

        Returns:
            dict: Summary statistics
        """
        if df is None:
            if self.files_df is None:
                logger.warning("⚠️ No files listed yet. Running list_all_files() first.")
                self.list_all_files()
            df = self.files_df

        if df is None or df.empty:
            return {"total_files": 0, "years": [], "chapters": []}

        # Extract years from paths
        years = df['file_path'].apply(lambda x: x.split('/')[0] if '/' in x else None)
        years = sorted(years.dropna().unique())

        # Extract chapters from filenames (assuming pattern: 01.docx, 02.docx)
        chapters = df['file_name'].apply(
            lambda x: int(x[:2]) if x[:2].isdigit() and x.endswith('.docx') else None
        )
        chapters = sorted(chapters.dropna().unique())

        summary = {
            "total_files": len(df),
            "years": years,
            "year_count": len(years),
            "chapters": chapters,
            "chapter_count": len(chapters),
            "file_types": df['file_name'].apply(lambda x: x.split('.')[-1]).value_counts().to_dict()
        }

        return summary

    def preview_files(self, df=None, n=10):
        """
        Preview first n files from the DataFrame.

        Args:
            df: DataFrame to preview (if None, uses cached files_df)
            n: Number of files to preview
        """
        if df is None:
            if self.files_df is None:
                logger.warning("⚠️ No files listed yet. Running list_all_files() first.")
                self.list_all_files()
            df = self.files_df

        if df is None or df.empty:
            logger.info("No files to preview")
            return

        preview = df.head(n)[['file_name', 'file_path']]
        logger.info(f"\n📋 Preview of first {min(n, len(df))} files:")
        for idx, row in preview.iterrows():
            logger.info(f"  {row['file_path']}")

    def check_missing_files(self, years, chapters):
        """
        Check which year/chapter combinations are missing.

        Args:
            years: List of years to check
            chapters: List of chapter numbers to check

        Returns:
            list: List of missing (year, chapter) tuples
        """
        if self.files_df is None:
            self.list_all_files()

        missing = []

        for year in years:
            for chapter in chapters:
                # Check if this combination exists
                filtered = self.filter_files(
                    self.files_df,
                    years=[year],
                    chapters=[chapter]
                )

                if filtered is None or filtered.empty:
                    missing.append((year, chapter))
                    logger.warning(f"⚠️ Missing: Year {year}, Chapter {chapter:02d}")

        if missing:
            logger.info(f"📊 Total missing files: {len(missing)}")
        else:
            logger.info("✅ All requested files are present")

        return missing