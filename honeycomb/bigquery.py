import json

import pandas as pd


def run_gbq_query(query, project_id='places-clickstream', credentials=None):
    """
    BigQuery-specific query function

    Args:
        query (str): The query to submit to BigQuery
        project_id (str):
            The GCP project to run the query under. Required even if
            a dataset is accessible from multiple projects
    """
    try:
        from google.oauth2.service_account import Credentials
    except ModuleNotFoundError:
        raise ImportError('Package "google-auth" is required to use '
                          'honeycomb\'s "bigquery" module.')
    if credentials is not None:
        if isinstance(credentials, dict):
            credentials = Credentials.from_service_account_info(credentials)
        elif isinstance(credentials, str):
            credentials = Credentials.from_service_account_info(
                json.loads(credentials))
        if not isinstance(credentials, Credentials):
            raise TypeError(
                'Credentials passed to "run_gbq_query" must be a JSON string, '
                'a dictionary, or google.oauth2.service_account.Credentials.'
            )

    df = pd.read_gbq(query, project_id=project_id, credentials=credentials)
    return df
