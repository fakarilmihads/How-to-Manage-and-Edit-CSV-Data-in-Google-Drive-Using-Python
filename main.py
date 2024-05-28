import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
import io
import os
import datetime

# Setup the API clients
SCOPES = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/spreadsheets']
CREDS = Credentials.from_service_account_file(r'C:\Users\hadif\PycharmProjects\Google Drive search and find\credentials2.json', scopes=SCOPES)

drive_service = build('drive', 'v3', credentials=CREDS)
gc = gspread.authorize(CREDS)

class GoogleDriveManager:
    def __init__(self, drive_service):
        self.drive_service = drive_service

    def download_file(self, file_id, output_path):
        request = self.drive_service.files().get_media(fileId=file_id)
        fh = io.FileIO(output_path, 'wb')
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
            print(f"Download {int(status.progress() * 100)}%")
        fh.close()

    def upload_file(self, file_path, folder_id):
        file_metadata = {
            'name': os.path.basename(file_path),
            'parents': [folder_id]
        }
        media = MediaFileUpload(file_path, mimetype='text/csv')
        file = self.drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()
        return file.get('id')

    def get_latest_file_id(self, folder_id, base_name):
        query = f"'{folder_id}' in parents and name contains '{base_name}' and mimeType='text/csv'"
        results = self.drive_service.files().list(q=query, orderBy="modifiedTime desc", pageSize=1, fields="files(id, name)").execute()
        items = results.get('files', [])
        if not items:
            raise FileNotFoundError(f"No files found in folder with id: {folder_id}")
        return items[0]['id'], items[0]['name']

def generate_output_filename(base_name, folder_path):
    date_str = datetime.datetime.now().strftime("%Y%m%d")
    base_filename = f"{base_name}_{date_str}"
    counter = 1
    while True:
        if counter == 1:
            output_filename = f"{base_filename}.csv"
        else:
            output_filename = f"{base_filename}_{counter}.csv"
        if not os.path.exists(os.path.join(folder_path, output_filename)):
            break
        counter += 1
    return os.path.join(folder_path, output_filename)

def main():
    # Replace with the appropriate Google Sheets file ID and Google Drive folder ID
    folder_id = '1dtG_SsQnel38PDR4Z84D18eMPU_oPtgy'
    base_name = 'MOCK_DATA'
    folder_path = r'C:\Users\hadif\PycharmProjects\Google Drive search and find'

    drive_manager = GoogleDriveManager(drive_service)
    edited_rows = set()  # To track edited rows
    changes_made = False  # To track changes

    # Get the latest file ID and name
    sheet_id, latest_file_name = drive_manager.get_latest_file_id(folder_id, base_name)

    # Generate output file name based on the latest file name
    local_csv_path = os.path.join(folder_path, latest_file_name)

    # Download the latest file from Google Drive
    drive_manager.download_file(sheet_id, local_csv_path)
    print(f"Downloaded file from Google Drive: {local_csv_path}")

    while True:
        # Open and edit data in the terminal
        df = pd.read_csv(local_csv_path)

        # Search function
        def search_data(df, column, value):
            return df[df[column].str.contains(value, case=False, na=False)]

        # Edit data in the terminal
        action = input("Do you want to search, edit a specific row, edit the last row, or add a new row? (search/edit row/edit last/add/view edited/exit): ")
        if action.lower() == 'exit':
            break
        elif action.lower() == 'search':
            column = input("Search by (first_name/last_name/email): ").strip().lower()
            if column not in ['first_name', 'last_name', 'email']:
                print("Invalid column.")
                continue
            value = input(f"Enter the value to search for in {column}: ").strip()
            results = search_data(df, column, value)
            print("Search results:")
            print(results)
        elif action.lower() == 'add':
            new_row = input("Enter the new data (comma separated): ")
            new_data = new_row.split(',')
            if len(new_data) == len(df.columns):
                df.loc[len(df)] = new_data
                changes_made = True
                edited_rows.add(len(df) - 1)
            else:
                print(f"The number of values entered does not match the number of columns ({len(df.columns)} columns).")
        elif action.lower() == 'edit row':
            try:
                row_index = int(input(f"Enter the row index to edit (0 to {len(df) - 1}): "))
                column_name = input(f"Enter the column name to edit (choose from {list(df.columns)}): ")
                if column_name in df.columns:
                    new_value = input("Enter the new value: ")
                    df.at[row_index, column_name] = new_value
                    changes_made = True
                    edited_rows.add(row_index)
                else:
                    print(f"Column {column_name} not found.")
            except ValueError:
                print("Invalid row index or column name.")
            except KeyError:
                print("Column not found.")
            except IndexError:
                print("Invalid row index.")
        elif action.lower() == 'edit last':
            try:
                row_index = len(df) - 1  # Index of the last row
                column_name = input(f"Enter the column name to edit (choose from {list(df.columns)}): ")
                if column_name in df.columns:
                    new_value = input("Enter the new value: ")
                    df.at[row_index, column_name] = new_value
                    changes_made = True
                    edited_rows.add(row_index)
                else:
                    print(f"Column {column_name} not found.")
            except ValueError:
                print("Invalid column name.")
            except KeyError:
                print("Column not found.")
            except IndexError:
                print("Invalid row index.")
        elif action.lower() == 'view edited':
            print("Edited rows:")
            for row in edited_rows:
                print(df.iloc[row])

        if changes_made:
            upload = input("Do you want to upload the changes to Google Drive? (yes/no): ")
            if upload.lower() == 'yes':
                # Save the edited CSV file
                df.to_csv(local_csv_path, index=False)

                # Upload the edited CSV file to Google Drive
                updated_file_id = drive_manager.upload_file(local_csv_path, folder_id)
                print(f"The edited file was successfully uploaded to Google Drive with ID: {updated_file_id}")
                changes_made = False
            else:
                print("Changes were not uploaded to Google Drive.")

if __name__ == "__main__":
    main()
