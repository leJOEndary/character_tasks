import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build


class GoogleSheetsService:
    def __init__(self, credentials_file_path, scopes):
        self.credentials_file_path = credentials_file_path
        self.scopes = scopes
        self.service = self._build_service()

    def _build_service(self):
        creds = service_account.Credentials.from_service_account_file(
            self.credentials_file_path, scopes=self.scopes)
        return build('sheets', 'v4', credentials=creds)

    def get_sheets_list(self, spreadsheet_id):
        spreadsheet_metadata = self.service.spreadsheets().get(
            spreadsheetId=spreadsheet_id).execute()
        sheets = spreadsheet_metadata.get('sheets', '')
        sheet_list = [{'title': sheet['properties']['title'],
                       'sheetId': sheet['properties']['sheetId']} for sheet in sheets]
        return sheet_list

    def get_sheet_titles(self, spreadsheet_id):
        sheets = self.get_sheets_list(spreadsheet_id)
        return [sheet['title'] for sheet in sheets]

    def ensure_sheet_exists(self, spreadsheet_id, sheet_title):
        sheets = self.get_sheets_list(spreadsheet_id)
        if any(sheet['title'] == sheet_title for sheet in sheets):
            print(f"Sheet '{sheet_title}' already exists.")
            return

        batch_update_request = {
            'requests': [
                {
                    'addSheet': {
                        'properties': {
                            'title': sheet_title
                        }
                    }
                }
            ]
        }
        self.service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body=batch_update_request
        ).execute()
        print(f"Created new sheet: '{sheet_title}'")

    def update_or_append_data_to_sheet(self, spreadsheet_id, sheet_title, values):
        range_ = f"{sheet_title}!A1"
        body = {'values': values}

        # Check if the sheet is empty to decide between append and update
        result = self.service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id, range=range_
        ).execute()

        if result.get('values'):
            request = self.service.spreadsheets().values().append(
                spreadsheetId=spreadsheet_id,
                range=range_,
                valueInputOption='USER_ENTERED',
                insertDataOption='INSERT_ROWS',
                body=body
            )
        else:
            request = self.service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=range_,
                valueInputOption='USER_ENTERED',
                body=body
            )

        response = request.execute()
        print(f"Updated or appended data to '{sheet_title}'")
        return response
    
    
    def delete_rows_in_list_of_values(self, spreadsheet_id, sheet_title, list_of_values):
        # Get the current values in the sheet
        cleaned_sheet_title = f"'{sheet_title}'"
        range_ = f"{cleaned_sheet_title}!A:H"  # This is the variable that probably caused the crash
        result = self.service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id, range=range_
        ).execute()
        current_values = result.get('values', [])
        
        # Find the rows to delete
        rows_to_delete = []
        for i, row in enumerate(current_values):
            if row in list_of_values:
                rows_to_delete.append(i + 1)  # Sheet rows are 1-indexed
        
        print(f"Rows to delete: {rows_to_delete}")
        
        # Correctly handling row deletion to account for shifting indices
        requests = []
        for i, row in enumerate(sorted(rows_to_delete, reverse=True)):  # Reverse sort to delete from bottom to top
            requests.append({
                'deleteDimension': {
                    'range': {
                        'sheetId': self.get_sheet_id(spreadsheet_id, sheet_title),
                        'dimension': 'ROWS',
                        'startIndex': row - 1 - i,  # Adjust for 0-indexing and already deleted rows
                        'endIndex': row - i  # No adjustment needed here since endIndex is exclusive
                    }
                }
            })

        if requests and False:
            self.service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={'requests': requests}
            ).execute()
        
        print(f"Deleted {len(rows_to_delete)} rows from '{sheet_title}'")




def download_sheet_as_df(service_account_path, sheet_id, sheet_name):
    # Authenticate with the service account
    scopes = ['https://www.googleapis.com/auth/spreadsheets.readonly']
    creds = service_account.Credentials.from_service_account_file(
        service_account_path, scopes=scopes)
    service = build('sheets', 'v4', credentials=creds)

    # Construct the range to read
    sheet_range = f"{sheet_name}!A:Z"  # Adjust the range A:Z as needed

    # Make the API request
    result = service.spreadsheets().values().get(
        spreadsheetId=sheet_id, range=sheet_range).execute()
    values = result.get('values', [])

    # Convert to a DataFrame
    if not values:
        print("No data found.")
        return pd.DataFrame()
    else:
        return pd.DataFrame(values[1:], columns=values[0])


def upload_df_to_sheet(service_account_path, sheet_id, sheet_name, df):
    """
    Uploads headers and data from a DataFrame to a Google Sheet.
    """
    # Authenticate with the service account
    scopes = ['https://www.googleapis.com/auth/spreadsheets']
    creds = service_account.Credentials.from_service_account_file(
        service_account_path, scopes=scopes)
    service = build('sheets', 'v4', credentials=creds)

    # Construct the range to write
    sheet_range = f"{sheet_name}!A:Z"  # Adjust the range A:Z as needed

    # Convert the DataFrame to a 2D list of values
    values = [df.columns.tolist()] + df.values.tolist()

    # Make the API request
    body = {'values': values}
    service.spreadsheets().values().update(
        spreadsheetId=sheet_id, range=sheet_range,
        valueInputOption='USER_ENTERED', body=body).execute()


def create_new_sheet_from_df(service_account_path, sheet_id, sheet_name, df):
    """
    Creates a new sheet and populates it with headers and data from a DataFrame.
    """
    # Authenticate with the service account
    scopes = ['https://www.googleapis.com/auth/spreadsheets']
    creds = service_account.Credentials.from_service_account_file(
        service_account_path, scopes=scopes)
    service = build('sheets', 'v4', credentials=creds)

    # Convert the DataFrame to a 2D list of values
    values = [df.columns.tolist()] + df.values.tolist()

    # Make the API request
    body = {'values': values}
    service.spreadsheets().values().append(
        spreadsheetId=sheet_id, range=sheet_name,
        valueInputOption='USER_ENTERED', body=body).execute()
