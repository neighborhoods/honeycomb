import pandas as pd


def gbq_query(query, project_id='places-clickstream'):
    """
    BigQuery-specific query function

    Args:
        query (str): The query to submit to BigQuery
        project_id (str):
            The GCP project to run the query under. Required even if
            a dataset is accessible from multiple projects
    """
    df = pd.read_gbq(query, project_id=project_id)
    return df
