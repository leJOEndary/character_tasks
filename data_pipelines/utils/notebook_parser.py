import io
import threading

import nbformat
from datetime import datetime

from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from oauth2client.service_account import ServiceAccountCredentials


def notebook_parser(notebook):
    """
    Parse a notebook and extract the message objects.

    :param notebook: The notebook object.
    """
 
    for cell in notebook.cells[2:]:
        if cell["cell_type"] == "markdown":
            markdown_headers = ["**User**"]
            lines = cell["source"].split("\n")
            if lines is not None and len(lines) >=3:

                return(lines[2])
            
   


def download_and_parse_notebook(credentials_dict, file_id, completion_date):
    try:
    
        credentials = ServiceAccountCredentials.from_json_keyfile_dict(credentials_dict, ['https://www.googleapis.com/auth/drive'])
        service = build('drive', 'v3', credentials=credentials)

        # Request to download the file
        request = service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)

    

        # Download the file
        done = False
        while not done:
            status, done = downloader.next_chunk()

        # Move the buffer's pointer to the beginning
        fh.seek(0)

        # Open the notebook
        notebook = nbformat.read(fh, as_version=4)

        # Parse the notebook
        messages = notebook_parser(notebook)

        # Get the list of revisions
        if completion_date != "":
            revisions = service.revisions().list(fileId=file_id).execute().get('revisions', [])
            date_formats = ["%m/%d/%Y", "%Y/%m/%d", "%d/%m/%Y"]

            for format in date_formats:
                try:
                    completion_date = datetime.strptime(completion_date, format).strftime("%Y-%m-%d")
                    break
                except ValueError:
                    pass
        
            revisions_on_date = [revision for revision in revisions if str(datetime.fromisoformat(revision['modifiedTime'][:-1]).date()) == completion_date]

            if revisions_on_date:
            # Find the latest revision on the specific date
                latest_revision_on_date = max(revisions_on_date, key=lambda x: x['modifiedTime'])
            else :
                latest_revision_on_date= {"id": None, "modifiedTime" : None}
        else :
                latest_revision_on_date= {"id": None, "modifiedTime" : None}

        return messages, latest_revision_on_date['id'], latest_revision_on_date['modifiedTime']
    except :
        return "none", "none", "none"

    


def parse_notebook(credentials_dict, row):

    file_id = row["task_link"].split("/")[-1]
    completion_date = row["completion_date"]
  
    return download_and_parse_notebook(credentials_dict, file_id, completion_date)
