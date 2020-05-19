import pandas as pd

from honeycomb.connection import get_db_connection


def run_query(query, engine='presto'):
    """
    General wrapper function around querying with different engines
    """
    query_fn = query_fns[engine]
    df = query_fn(query)
    return df


def _hive_query(query):
    """
    Hive-specific query function
    Note: uses an actual connection, rather than a connection cursor
    """
    with get_db_connection('hive', cursor=False) as conn:
        df = pd.read_sql(query, conn)
    # Cleans table prefixes from column names, which are added by Hive
    df.columns = df.columns.str.replace(r'^.*\.', '')
    return df


def _presto_query(query):
    """
    Presto-specific query function
    Note: uses an actual connection, rather than a connection cursor
    """
    # Presto does not have a notion of a persistent connection, so closing
    # is unnecessary
    conn = get_db_connection('presto', cursor=False)
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
