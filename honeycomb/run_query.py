import pandas as pd
from pyhive import hive, presto


def run_query(query, engine='presto'):
    """
    General wrapper function around querying with different engines
    """
    query_fn = query_fns[engine]
    df = query_fn(query)
    return df


def _clean_col_names(df):
    """
    Removes table prefixes from Hive-queried tables
    """
    df.columns = df.columns.str.replace(r'^.*\.', '')
    return df


def _hive_query(query):
    """
    Hive-specific query function
    Note: uses an actual connection, rather than a connection cursor
    """
    with hive.connect('localhost') as conn:
        df = pd.read_sql(query, conn)
    return _clean_col_names(df)


def _presto_query(query):
    """
    Presto-specific query function
    Note: uses an actual connection, rather than a connection cursor
    """
    # Presto does not have a notion of a persistent connection, so closing
    # is unnecessary
    conn = presto.connect('localhost')
    df = pd.read_sql(query, conn)
    return df


def _gbq_query(query, project_id):
    """
    BigQuery-specific query function
    """
    df = pd.read_gbq(query, project_id=project_id)
    return df


query_fns = {
    'presto': _presto_query,
    'hive': _hive_query,
    'gbq': _gbq_query
}
