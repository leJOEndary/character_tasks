# Selected_rows_df is a pandas dataframe containing Timestamp and Colab_link.
# Last commented code is to make changes in the whole sheet.

from googleapiclient.discovery import build
from google.oauth2 import service_account
from datetime import datetime, timezone
import pandas as pd

# Use your own service account file
service_account_file = 'creds/google__sa.json'

# Define the scopes
SCOPES = ['https://www.googleapis.com/auth/drive']

# Create a service account credential
creds = service_account.Credentials.from_service_account_file(service_account_file, scopes=SCOPES)

# Build the service
drive_service = build('drive', 'v3', credentials=creds)

def get_revision_before_timestamp(file_id, timestamp):
    try:
        # Get the revisions of the file
        revisions = drive_service.revisions().list(fileId=file_id).execute()
    except Exception as e:
        print(f"File not found: {file_id}")
        return None
    
    if timestamp == "":
        timestamp = datetime.now().replace(tzinfo=None)
    else:
        timestamp = pd.to_datetime(timestamp)
    timestamp = pd.to_datetime(timestamp) - pd.Timedelta(hours=2)

    for revision in reversed(revisions['revisions']):
        # Convert the modifiedTime of the revision to a datetime object
        modified_time = pd.to_datetime(revision['modifiedTime'])
        modified_time = str(modified_time)
        # Convert it to a datetime object
        dt_object = datetime.fromisoformat(modified_time)
        
        # Format it to a string with your desired format
        formatted_string = dt_object.strftime("%Y-%m-%d %H:%M:%S")
        modified_time = pd.to_datetime(formatted_string)

        if modified_time <= timestamp:
            return revision
    return revisions['revisions'][0]

# Example usage:
file_id = '1KtSOMVTE5kj8VuId-zm-y_TZVVMQwYwZ'
timestamp = selected_rows_df.iloc[12]['Timestamp'] 
if timestamp == "":
    timestamp = datetime.now().replace(tzinfo=None)
else:
    timestamp = pd.to_datetime(timestamp)
revision = get_revision_before_timestamp(file_id, timestamp)


# def get_file_id_from_task_link(task_link):
#     try:
#         file_id = task_link.split("/")[-1]
#         if '#scrollTo' not in file_id:
#             return file_id
#         else:
#             return file_id.split("#")[0]
#     except Exception as e:
#         print('ERROR' + '='*60)
#         print(task_link)
#         return None

# # Add a new column to the DataFrame for the file IDs
# selected_rows_df['file_id'] = selected_rows_df['colab_link'].apply(get_file_id_from_task_link)

# # Apply the function to each row in the DataFrame
# selected_rows_df['revision'] = selected_rows_df.apply(lambda row: get_revision_before_timestamp(row['file_id'], row['Timestamp']) if row['file_id'] is not None else None, axis=1)
