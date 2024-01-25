import sys
from data_pipelines.utils.bigquery import BigQuery
import json
import pandas as pd
import hashlib

from src.sheets_utils import download_sheet_as_df
from data_pipelines.utils.get_sheet_data import download_google_spreadsheet
from data_pipelines.utils.access_key import get_secret_version
from data_pipelines.utils.notebook_parser import  parse_notebook
from datetime import datetime


def load_conversations(project_id):
    api_key = json.loads(get_secret_version(project="turing-gpt", secret_id="character_ai_dev", version_id=1))
    bq_client = BigQuery(project_id=project_id)
    tracking_sheet_id = "1qBU7Kvuuij2fxbqPxebReKMxWgIBmOIE5Gi4ZuX0j_4"
    sheet_name="Conversations_Batch_"
    conversation_df=pd.DataFrame()

    for i in range(1, 7):

        df=download_google_spreadsheet(api_key,  tracking_sheet_id, f'{sheet_name}{i}')
        df["batch_id"] =  i
        conversation_df=pd.concat([conversation_df, df])

    conversation_df["conversation_id"] =    conversation_df.apply(lambda row: hashlib.md5((row['task_link']+ row['metadata__topic']).encode()).hexdigest(), axis=1)
    conversation_df["problem_statement"] = conversation_df.apply(lambda row:  parse_notebook(api_key, row)[0], axis=1)
    conversation_df["colab_revision_id"] = conversation_df.apply(lambda row:  parse_notebook(api_key, row)[1], axis=1)
    conversation_df["last_modified_at"]  = conversation_df.apply(lambda row:  parse_notebook(api_key, row)[2], axis=1)
    conversation_df.rename(columns={'assigned_to_email': 'current_user'}, inplace=True)
    conversation_df.rename(columns={'task_link': 'colab_link'}, inplace=True)
    conversation_df["user_role"] =  None
    conversation_df["assistant_role"] = None
    conversation_df["no_of_turns"] =  None
    conversation_df["min_turns"] = None
    conversation_df["max_turns"] = None
    conversation_df.rename(columns={'completion_status': 'current_status'}, inplace=True)
    conversation_df["conversation_seed_id"] = None
    conversation_df["created_at"] = datetime.utcnow()

    
    conversations = conversation_df[["conversation_id", "problem_statement", "current_user", "user_role", "assistant_role", "colab_revision_id", 
                    "colab_link", "no_of_turns", "batch_id", "min_turns", "max_turns", "current_status", "completion_date", "duration_mins", "conversation_seed_id", "created_at"]]
    



    # Set dataset and table name
    table_id = 'character_ai.conversations'
    # Set job config. In this example old data will be removed and new data will be added.
    
    bq_client.store_data(
        conversations,
        table_id,
        write_disposition='WRITE_TRUNCATE'
    )






