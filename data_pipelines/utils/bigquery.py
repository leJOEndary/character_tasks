"""The module contains the bigquery class and its functions."""
from typing import Optional

import pandas as pd
from google.cloud import bigquery


class BigQuery:
    """Base class for the bigquery interfacing."""

    def __init__(self, project_id: str):
        """__init__ Initialization function.

        Args:
            project_id (str): bigquery project id

        """
        client_config = bigquery.QueryJobConfig(priority=bigquery.QueryPriority.BATCH)
        self._bq_client = bigquery.Client(project=project_id, default_query_job_config=client_config)

    def get_data(self, query: str) -> pd.DataFrame:
        """get_data creates a dataframe from biquery sql.

        Args:
            query (str): query as string value

        Returns:
            pd.DataFrame: result dataframe

        """
        result_df = pd.DataFrame()
        query_job = self._bq_client.query(query=query)
        result_df = query_job.result().to_dataframe(create_bqstorage_client=True)
        return result_df

    def store_data(
        self,
        data_frame: pd.DataFrame,
        table: str,
        write_disposition: str,
        table_schema: Optional[list[dict[str, str]]] = None,
    ) -> None:
        """store_data stores the data to the bigquery table.

        Args:
            data_frame (pd.DataFrame): dataframe to be loaded to bigquery
            table (str): table name
            write_disposition (str): table refresh type
            table_schema (List[Dict[str,str]]): schema definition as list

        """
        if table_schema:
            schema = [
                bigquery.SchemaField(value["name"], eval(f"bigquery.enums.SqlTypeNames.{value['type']}"))
                for value in table_schema
            ]
            job_config = bigquery.LoadJobConfig(
                schema=schema,
                write_disposition=write_disposition,
            )
        else:
            job_config = bigquery.LoadJobConfig(
                write_disposition=write_disposition,
            )
        query_job = self._bq_client.load_table_from_dataframe(data_frame, table, job_config=job_config)
        query_job.result()

    def run_query(self, query: str) -> None:
        """run_query executes sql query.

        Args:
            query(str): sql query as a string value.

        """
        if query:
            self._bq_client.query(query)
