import pandas as pd
from pyhive import hive, presto


def clean_col_names(df):
    """
    Removes table prefixes from Hive-queried tables
    """
    df.columns = df.columns.str.replace(r'^.*\.', '')
    return df


def run_query(query, engine="presto"):
    """
    General wrapper function around querying with different engines
    """
    query_fn = query_fns[engine]
    df = query_fn(query)
    return df


def hive_query(query):
    """
    Hive-specific query function
    Note: uses an actual connection, rather than a connection cursor
    """
    with hive.connect("localhost") as conn:
        df = pd.read_sql(query, conn)
    return clean_col_names(df)


def presto_query(query):
    """
    Presto-specific query function
    Note: uses an actual connection, rather than a connection cursor
    """
    with presto.connect("localhost") as conn:
        df = pd.read_sql(query, conn)
    return df


def gbq_query(query, project_id):
    """
    BigQuery-specific query function
    """
    df = pd.read_gbq(query, project_id="")
    return df


query_fns = {
    "presto": presto_query,
    "hive": hive_query,
    "gbq": gbq_query
}
