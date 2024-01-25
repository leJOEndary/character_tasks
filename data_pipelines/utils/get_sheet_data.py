import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials

def download_google_spreadsheet(credentials_dict, spreadsheet_key, sheet_name):
    """
    Download data from a Google Spreadsheet and return it as a DataFrame.

    Parameters:
    - credentials_dict: A dictionary containing Google API credentials.
    - spreadsheet_key: The key of the Google Spreadsheet (found in the URL).
    - sheet_name: The name of the sheet in the Google Spreadsheet.

    Returns:
    - pandas DataFrame containing the data from the specified sheet.
    """

    # Set up credentials
    credentials = ServiceAccountCredentials.from_json_keyfile_dict(credentials_dict, ['https://spreadsheets.google.com/feeds'])
    gc = gspread.authorize(credentials)

    # Open the Google Spreadsheet using its key
    spreadsheet = gc.open_by_key(spreadsheet_key)

    # Select the specified sheet
    sheet = spreadsheet.worksheet(sheet_name)

    # Get all values from the sheet
    data = sheet.get_all_values()

    # Convert the data to a DataFrame
    df = pd.DataFrame(data[1:], columns=data[0])

    return df