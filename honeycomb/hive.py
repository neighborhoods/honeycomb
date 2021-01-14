import os

import pandas as pd

from honeycomb.connection import get_db_connection


col_prefix_regex = r'^.*\.'

hive_vector_option_name = 'hive.vectorized.execution.enabled'
false_str = 'false'


def run_lake_query(query, engine='hive', has_complex_cols_and_joins=False):
    """
    General wrapper function around querying with different engines

    Args:
        query (str): The query to be executed in the lake
        engine (str):
            The querying engine to run the query through
            Use 'presto' for faster, ad-hoc/experimental querie
            Use 'hive' for slower but more robust queries
        has_complex_cols_and_joins (bool, default False):
            Whether the query involves both complex cols and joins. Indicating
            this beforehand will save query time later, as it allows for
            avoiding error handling associated with running a query like that
            without special treatment. Caused by a hive bug
    """
    if has_complex_cols_and_joins:
        configuration = {hive_vector_option_name: false_str}
    else:
        configuration = None

    addr = os.getenv('HC_LAKE_ADDRESS', 'localhost')

    query_fns = {
        'presto': _presto_query,
        'hive': _hive_query,
    }
    query_fn = query_fns[engine]
    df = query_fn(query, addr, configuration)
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


def _hive_query(query, addr, configuration):
    """
    Hive-specific query function
    Note: uses an actual connection, rather than a connection cursor
    """
    if _query_returns_df(query):
        if 'join' in query.lower():
            df = _hive_handle_join_query(query, addr, configuration)

        else:
            df = _hive_run_pd_query_w_context(query, addr, configuration)
            # Cleans table prefixes from all column names,
            # which are added by Hive even in non-join queries
            df.columns = df.columns.str.replace(col_prefix_regex, '')

        return df
    else:
        with get_db_connection('hive', addr=addr, cursor=True,
                               configuration=configuration) as conn:
            conn.execute(query)


def _hive_handle_join_query(query, addr, configuration):
    """
    Applies the special behaviors needed for queries involving
    the `JOIN` keyword

    Currently, due to a bug in hive 3.1.2, `JOIN` queries if the underlying
    storage types of the involved tables are the same and complex-type columns
    are involved. This is because hive is supposed to disable query
    vectorization if complex columns are involved, but for some reason this
    is not applied on JOINs involving tables of the same storage type

    1. If the user did not specify that complex columns were involved in
       the query, this function will catch the related error and retry
       with vectorization manually disabled. If a query fails for a different
       reason than expected, the error will be raised normally
    2. This function also removes the prefixed table name from all column names
       except those that would have a naming conflict post-join
    """
    try:
        df = _hive_run_pd_query_w_context(query, addr, configuration)

    except pd.io.sql.DatabaseError as e:

        if (isinstance(configuration, dict) and
                configuration[hive_vector_option_name] == 'true'):
            # This means that the query failed even though vectorization
            # was disabled, and the failure is caused by something else
            raise e

        complex_col_err_substring = (
            'cannot be cast to org.apache.hadoop.'
            'hive.serde2.objectinspector.PrimitiveObjectInspector'
        )
        if complex_col_err_substring in e.args[0]:
            print('Query involves selecting complex type columns from a '
                  'joined table. Due to a hive bug, extra options must be '
                  'set for this scenario. To speed up query time, set '
                  '\'has_complex_cols_and_joins\' to True in '
                  '\'hc.run_lake_query\'if you run such a query again.')
            configuration[hive_vector_option_name] = false_str
            return _hive_handle_join_query(query, addr, configuration)

    # Cleans table prefixes from any non-duplicated column names
    cols_wo_prefix = df.columns.str.replace(col_prefix_regex, '')
    duplicated_cols = cols_wo_prefix.duplicated(keep=False)
    cols_to_rename = dict(zip(df.columns[~duplicated_cols],
                              cols_wo_prefix[~duplicated_cols]))

    df = df.rename(columns=cols_to_rename)

    return df


def _hive_run_pd_query_w_context(query, addr, configuration):
    """Runs a hive query within a connection context manager"""
    with get_db_connection('hive', addr=addr, cursor=False,
                           configuration=configuration) as conn:
        df = pd.read_sql(query, conn)
    return df


def _presto_query(query, addr, configuration):
    """
    Presto-specific query function
    Note: uses an actual connection, rather than a connection cursor

    The 'configuration' parameter is included solely as a pass-through for
    compatibility reasons. If it is not 'None' it will raise errors in
    get_db_connection
    """
    # Presto does not have a notion of a persistent connection, so closing
    # is unnecessary
    if _query_returns_df(query):
        conn = get_db_connection('presto', addr=addr, cursor=False,
                                 configuration=configuration)
        df = pd.read_sql(query, conn)
        return df
    else:
        conn = get_db_connection('presto', addr=addr, cursor=True,
                                 configuration=configuration)
        conn.execute(query)
