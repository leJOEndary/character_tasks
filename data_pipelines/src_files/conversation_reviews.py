import sys
sys.path.append('../..')
import hashlib

from data_pipelines.utils.bigquery import BigQuery
import json
import pandas as pd

from src.sheets_utils import download_sheet_as_df
from data_pipelines.utils.get_sheet_data import download_google_spreadsheet
from data_pipelines.utils.access_key import get_secret_version
from datetime import datetime


def load_conversations_reviews(project_id):
    def find_conv_id(row, conv_df):
        try: 
            return conv_df[conv_df["colab_link"] == row["Task Link [Google Colab]"]].conversation_id.values[0]
        except:
            pass
    #api_key = json.loads(get_secret_version(project="turing-gpt", secret_id="character_ai_dev", version_id=1))
    api_key={
    "type": "service_account",
    "project_id": "turing-gpt",
    "private_key_id": "b0ce9cc3b199b4e250de23629fcec79b44d5fdc6",
    "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQDFkp6vgP81SRET\neDSsTH4KBiWe4HaXT3LGSqpCrLCauq2XyIAq/CXOd8pNf/atMwLjncoKSOSiVPGx\nm3bjdkkr/JNpaxwO3HSgsG+D7ieFGleVwp09a2U8Nqj9r05EFa1/KvkvDBf++LNG\nAJU3nUNo9VW3KJ1n4GAgIFGidMshISh3ese0zozkAhW0Z3Lkj69PXOb+3CHGjbg8\nuM3saYgfzdg12TLKA7nxOCvM/WIMpWvZGSp2vy6+tkk90EqkujmGvgwirAg5cVNL\npsZL2txwrYkEmGGljtOIVg5i79Hy+54vpcgYS4ZcuNL2UWsQdpF66ce0WyI6B/CC\nccK2KSRDAgMBAAECggEAEtHYfGN4+Le49C+4y/BnXlR494bUu3oNnUmi3HU8FCkT\n4xSHibcS2A8W5EX8Fqh4YUhel/2a8pAApW60Wig8AeaHzJYvo7mOzhJ9NN+vjQeJ\nPN8/M7wL3LrG/MJ0d2c3OEwjVPHXRIOmxzrD+/qARgdK/tNUFxjk6/p+iQKMdnjz\nUTfu4DwGEYJXA6wErMwbsXYX8Y5UKgOKBSUnJJZIltxKjGGN3YO7f4iy0mxnvvkM\nVmqIj2ux4npPuy6daJslC2rgidyLo8w13N3CkgNY5u4ZGIn4TngTg8uW+yQSBE95\n21GjXycjrKnYbYsLkfcuW4dgCco9/UVrTw2DSqvvBQKBgQD6INlSCcJ2mRP57MKQ\nEr0/crNyWgJ84DRzoR0iXHOcq/keFvLUyTfolfNuXbJhKJ0P+XVmVD6nbvSWtJY7\nP9Jt0ZR9D2DY0Z2OzaeeE8z+DZpsHxjrFY6cBxIQ1R2NrMJgrBKtuBa2wHnCeCTv\nj5m8BSYGhb0tugNs3hQacN9F1wKBgQDKNe/ocUAt5kvgT0nmXbzQWtPO8xLpLRuR\n+mQH+NVIU0mzomdliqkkGG4MjfH9J+SL14zPlxTGp0nD7YFwWSiaP+UwL4QXfwcF\nVba2B4uS3Rsf8BAczpJHhyRPCATILlyEQcPueBF+bUiFbWcS79gkPnXHii0YD8Mj\n5/qRbxpvdQKBgDspO92wm0NL77Kkclx7YjG4ooMAkSgSK7XRvL9c2KeM8Y5RZTw5\nH7UuinZnSQK8BXI60a7TbEJT9xwSOJpo/Q+mi5WM5p5B7h9RuzyjeE/6zbSXImCw\nJ5v1/CrpDWVuIEeXS4+1RAITwnjhy/Kxp24WVakjRtyi6557ZIvgeJ7fAoGAQI9P\n9WqTDxSEDS3DbmVnLy6QiXZaB1B7iPPklBcCIYFV+qBTWrwWnIugNynqpOTjtzIu\npuiZnYMzrfZaaUBOElFsHyJMjiPkXfOrzhpCmbU2P9AplMjvMx8WnJT20eLbbu6A\nW9bB3xeNG+x+CDHDVG8Ms5SOKSx/JSufeGd8jTECgYBFoeni3rvmMP2iumvHJn16\ndJfStnxy08RfZyTVx7PbjsJYGToDolXm1M5DGorfmjh5EiWk3aflfsx0q9kYTEu3\nfgDU1OAl7LlXI8gveGyyDxJ/IXQejupt9Ahy1fPccapLgc60yoJfGDUiuJYfnUKk\nicJe7Xola6lPoMF4vXEAjA==\n-----END PRIVATE KEY-----\n",
    "client_email": "char-automations@turing-gpt.iam.gserviceaccount.com",
    "client_id": "102308741245049500041",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/char-automations%40turing-gpt.iam.gserviceaccount.com",
    "universe_domain": "googleapis.com"
  }

    bq_client = BigQuery(project_id=project_id)
    tracking_sheet_id = "1qBU7Kvuuij2fxbqPxebReKMxWgIBmOIE5Gi4ZuX0j_4"
    sheet_name="Reviews"
    df = download_google_spreadsheet(api_key,  tracking_sheet_id, f'{sheet_name}')[0:5]
    conv_df = bq_client.get_data("SELECT colab_link, conversation_id FROM turing-gpt.character_ai.conversations")
     
    df["review_id"] =    df.apply(lambda row: hashlib.md5((row['Task Link [Google Colab]'].split("#", 1)[0]+ row['Email Address'].split("@", 1)[0]).encode()).hexdigest(), axis=1)

    df["conversation_id"] = df.apply(lambda row: find_conv_id(row, conv_df) , axis=1)
 
    
    df.rename(columns={'Feedback for the Trainer': 'review'}, inplace=True)
    df["follow_up_required"] = df.apply(lambda row: 1 if int(row['Code Quality'] if row['Code Quality'] != '' else 0 ) <5 or  int(row['Language Quality'] if row['Language Quality'] !='' else 0  ) <5 else 0, axis=1)

    df["code_score"] = df.apply(lambda row:  int(row['Code Quality'] if row['Code Quality'].isdigit() else 0 ) , axis=1)
    df["language_score"] = df.apply(lambda row:  int(row['Language Quality'] if row['Language Quality'].isdigit() else 0 ) , axis=1)
    df.rename(columns={'Timestamp': 'submitted_at'}, inplace=True)
    df.rename(columns={'Email Address': 'reviewed_by'}, inplace=True)
    df["reviewed_role"] = "Assistant"


    conversation_reviews = df[["review_id", "conversation_id", "review", "code_score", "language_score", "follow_up_required", 
                    "submitted_at", "reviewed_by", "reviewed_role"]]


    # Set dataset and table name
    table_id = 'character_ai.conversations_reviews'
    # Set job config. In this example old data will be removed and new data will be added.
    
    bq_client.store_data(
        conversation_reviews,
        table_id,
        write_disposition='WRITE_TRUNCATE'
    )






