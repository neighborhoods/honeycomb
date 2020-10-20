import os

import pandas as pd

from honeycomb.connection import get_db_connection


def run_lake_query(query, engine='presto'):
    """
    General wrapper function around querying with different engines

    Args:
        query (str): The query to be executed in the lake
        engine (str):
            The querying engine to run the query through
            Use 'presto' for faster, ad-hoc/experimental querie
            Use 'hive' for slower but more robust queries
    """
    addr = os.getenv('HC_LAKE_ADDRESS', 'localhost')

    query_fns = {
        'presto': _presto_query,
        'hive': _hive_query,
    }
    query_fn = query_fns[engine]
    df = query_fn(query, addr)
    return df


def _query_returns_df(query):
    """
    Based on the type of query being run, states whether
    a given query should return a dataframe
    """
    keywords_that_return = ['SELECT', 'DESCRIBE', 'SHOW']
    if query.split(' ')[0].upper() in keywords_that_return:
        return True
    return False


def _hive_query(query, addr):
    """
    Hive-specific query function
    Note: uses an actual connection, rather than a connection cursor
    """
    if _query_returns_df(query):
        with get_db_connection('hive', addr=addr, cursor=False) as conn:
            df = pd.read_sql(query, conn)
            # Cleans table prefixes from column names, which are added by Hive
            df.columns = df.columns.str.replace(r'^.*\.', '')
            return df
    else:
        with get_db_connection('hive', addr=addr, cursor=True) as conn:
            conn.execute(query)


def _presto_query(query, addr):
    """
    Presto-specific query function
    Note: uses an actual connection, rather than a connection cursor
    """
    # Presto does not have a notion of a persistent connection, so closing
    # is unnecessary
    if _query_returns_df(query):
        conn = get_db_connection('presto', addr=addr, cursor=False)
        df = pd.read_sql(query, conn)
        return df
    else:
        conn = get_db_connection('presto', addr=addr, cursor=True)
        conn.execute(query)
