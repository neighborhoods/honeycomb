import pandas as pd


def gbq_query(query, project_id):
    """
    BigQuery-specific query function
    """
    df = pd.read_gbq(query, project_id=project_id)
    return df
