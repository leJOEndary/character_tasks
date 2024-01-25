import sys

from data_pipelines.utils.bigquery import BigQuery
import json
from data_pipelines.utils.get_sheet_data import download_google_spreadsheet
from data_pipelines.utils.access_key import get_secret_version



def load_contributors(project_id):
    api_key = json.loads(get_secret_version(project="turing-gpt", secret_id="character_ai_dev", version_id=1))

    bq_client = BigQuery(project_id=project_id)
    tracking_sheet_id = "14bcgtOEh5ClIYhH7rebN8c_AXEQ6ZcDVLRgwn3ZQW4E"

    contributors_df=download_google_spreadsheet(api_key,  tracking_sheet_id, "Character.ai_Devs")

# Set dataset and table name
    table_id = 'character_ai.contributors'
    # Set job config. In this example old data will be removed and new data will be added.
    
    bq_client.store_data(
        contributors_df,
        table_id,
        write_disposition='WRITE_TRUNCATE'
    )


