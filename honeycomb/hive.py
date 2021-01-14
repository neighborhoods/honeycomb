import os

import pandas as pd

from honeycomb.connection import get_db_connection


def run_lake_query(query, engine='hive'):
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
    if query.strip().split(' ')[0].strip().upper() in keywords_that_return:
        return True
    return False


def _hive_query(query, addr):
    """
    Hive-specific query function
    Note: uses an actual connection, rather than a connection cursor
    """
    col_prefix_regex = r'^.*\.'
    if _query_returns_df(query):
        with get_db_connection('hive', addr=addr, cursor=False) as conn:
            df = pd.read_sql(query, conn)
            if 'join' not in query.lower():
                # Cleans table prefixes from all column names,
                # which are added by Hive even in non-join queries
                df.columns = df.columns.str.replace(col_prefix_regex, '')
            else:
                # Cleans table prefixes from any non-duplicated column names
                cols_wo_prefix = df.columns.str.replace(col_prefix_regex, '')
                duplicated_cols = cols_wo_prefix.duplicated(keep=False)
                cols_to_rename = dict(zip(df.columns[~duplicated_cols],
                                          cols_wo_prefix[~duplicated_cols]))

                df = df.rename(columns=cols_to_rename)

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
