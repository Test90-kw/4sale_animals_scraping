import os
import json
from google.oauth2.service_account import Credentials  # For authenticating using service account JSON
from googleapiclient.discovery import build            # To build the Google Drive API service
from googleapiclient.http import MediaFileUpload       # For handling file upload to Drive
from datetime import datetime, timedelta               # Not used in this class but may be useful for timestamps

class SavingOnDrive:
    def __init__(self, credentials_dict):
        self.credentials_dict = credentials_dict  # Dictionary containing service account credentials
        self.scopes = ['https://www.googleapis.com/auth/drive']  # Full access to Google Drive
        self.service = None  # Will be initialized after authentication
        self.parent_folder_id = '1Qqo4x3i-iE6RYKWKFagtwGSjCPNk4kTo'  # ID of the root folder in Drive where all uploads go

    # Authenticate with Google Drive using service account credentials
    def authenticate(self):
        try:
            # Load credentials from the provided dictionary and set scopes
            creds = Credentials.from_service_account_info(self.credentials_dict, scopes=self.scopes)
            # Build the Drive API service client
            self.service = build('drive', 'v3', credentials=creds)
        except Exception as e:
            # Print and raise any errors during authentication
            print(f"Authentication error: {e}")
            raise

    # Retrieve the ID of a subfolder (if exists) under the parent folder by its name
    def get_folder_id(self, folder_name):
        try:
            # Google Drive query to find the folder with matching name under the parent folder
            query = (f"name='{folder_name}' and "
                     f"'{self.parent_folder_id}' in parents and "
                     f"mimeType='application/vnd.google-apps.folder' and "
                     f"trashed=false")

            # Execute the query using the Drive API
            results = self.service.files().list(
                q=query,
                spaces='drive',
                fields='files(id, name)'  # Limit response to folder ID and name
            ).execute()

            files = results.get('files', [])
            # Return folder ID if found; otherwise, return None
            return files[0]['id'] if files else None
        except Exception as e:
            print(f"Error getting folder ID: {e}")
            return None

    # Create a new folder under the parent folder and return its ID
    def create_folder(self, folder_name):
        try:
            # Metadata describing the folder
            file_metadata = {
                'name': folder_name,
                'mimeType': 'application/vnd.google-apps.folder',
                'parents': [self.parent_folder_id]  # Set the parent folder
            }

            # Call Drive API to create the folder
            folder = self.service.files().create(
                body=file_metadata,
                fields='id'  # Only return the new folder's ID
            ).execute()
            return folder.get('id')  # Return the created folder ID
        except Exception as e:
            print(f"Error creating folder: {e}")
            raise

    # Upload a file to a specified Drive folder and return its file ID
    def upload_file(self, file_name, folder_id):
        try:
            # Metadata for the file to be uploaded (name and parent folder)
            file_metadata = {
                'name': os.path.basename(file_name),  # Use only the file's base name
                'parents': [folder_id]  # Destination folder ID
            }

            # Prepare the file upload using MediaFileUpload
            media = MediaFileUpload(file_name, resumable=True)

            # Upload the file to Google Drive
            file = self.service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id'  # Return only the file ID
            ).execute()
            return file.get('id')  # Return the uploaded file's ID
        except Exception as e:
            print(f"Error uploading file: {e}")
            raise

    # Upload multiple files to the specified folder
    def save_files(self, files, folder_id=None):
        try:
            # Loop through each file path provided and upload
            for file_name in files:
                self.upload_file(file_name, folder_id)
            print("Files uploaded successfully.")
        except Exception as e:
            print(f"Error saving files: {e}")
            raise
