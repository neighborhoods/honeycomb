import logging
import os
import re

import pandas as pd

from honeycomb.connection import get_db_connection
from honeycomb.meta import get_table_s3_location


col_prefix_regex = r'^.*\.'
hive_vector_option_name = 'hive.vectorized.execution.enabled'


def run_lake_query(query, engine='hive', complex_join=False):
    """
    General wrapper function around querying with different engines

    Args:
        query (str): The query to be executed in the lake
        engine (str):
            The querying engine to run the query through
            Use 'presto' for faster, ad-hoc/experimental querie
            Use 'hive' for slower but more robust queries
        complex_join (bool, default False):
            Whether the query involves both complex cols and joins. Indicating
            this beforehand will save query time later, as it allows for
            avoiding error handling associated with running a query like that
            without special treatment. Caused by a hive bug
    """
    if complex_join:
        configuration = _hive_get_nonvectorized_config()
    else:
        configuration = None

    # INSERT OVERWRITE commands on external tables can cause file deletion in
    # S3. As a result, we check that the path being overwritten into is not
    # the root of the bucket
    insert_overwrite_pattern = r'INSERT *OVERWRITE'
    if re.match(insert_overwrite_pattern, query, flags=re.IGNORECASE):
        # Multi-Insert statements have unknown behavior currently, so we
        # disallow them
        if len(re.findall(insert_overwrite_pattern, query,
                          flags=re.IGNORECASE)) > 1:
            raise ValueError(
                'Multi-Insert Statements are currently not supported by '
                'honeycomb for bucket integrity reasons.'
            )
        schema, table_name = re.search(
            r'INSERT *OVERWRITE *TABLE *(\w+)\.(\w+)', query,
            flags=re.IGNORECASE).groups()

        _, table_s3_path = get_table_s3_location(table_name, schema)
        if not _hive_check_valid_table_path(table_s3_path):
            raise ValueError(
                'The path of the table to be written into makes using an '
                'INSERT OVERWRITE command unsafe. Please recreate the target '
                'table using a safe S3 path and try again.')

    addr = os.getenv('HC_LAKE_ADDRESS', 'localhost')

    query_fns = {
        'presto': _presto_query,
        'hive': _hive_query,
    }
    query_fn = query_fns[engine]
    df = query_fn(query, addr, configuration)
    return df


def _hive_get_nonvectorized_config(configuration=None):
    """
    Sets the Hive option for query vectorization to false
    """
    false_str = 'false'
    if isinstance(configuration, dict):
        configuration[hive_vector_option_name] = false_str
    else:
        configuration = {hive_vector_option_name: false_str}
    return configuration


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
    is_join_query = 'join' in query.lower()
    if _query_returns_df(query):
        conn = get_db_connection('hive', addr=addr, cursor=False,
                                 configuration=configuration)
        kwargs = {'sql': query, 'con': conn}
        query_fn = pd.read_sql
        should_return_df = True
    else:
        conn = get_db_connection('hive', addr=addr, cursor=True,
                                 configuration=configuration)
        kwargs = {'operation': query}
        query_fn = conn.execute
        should_return_df = False

    try:
        df = query_fn(**kwargs)
    except pd.io.sql.DatabaseError as e:
        return _hive_check_if_complex_join_error(query, addr, configuration,
                                                 e, is_join_query)

    if should_return_df:
        if not is_join_query:
            df.columns = df.columns.str.replace(col_prefix_regex, '')
        else:
            # Cleans table prefixes from any non-duplicated column names
            cols_wo_prefix = df.columns.str.replace(col_prefix_regex, '')
            duplicated_cols = cols_wo_prefix.duplicated(keep=False)
            cols_to_rename = dict(zip(df.columns[~duplicated_cols],
                                      cols_wo_prefix[~duplicated_cols]))

            df = df.rename(columns=cols_to_rename)
        return df


def _hive_check_if_complex_join_error(query, addr, configuration,
                                      e, is_join_query):
    """
    Checks if an error raised by _hive_query is the error caused by a hive bug
    where non-vectorizable queries being run as vectorized (described below).
    If the user did not specify that complex columns were involved in
    the query using 'complex_join=True', this function will catch the
    related error and retry with vectorization manually disabled. If a query
    fails for a different reason than expected, the error will be
    raised normally

    Currently, due to a bug in hive 3.1.2, `JOIN` queries raise errors if the
    underlying storage types of the involved tables are the same and
    complex-type columns are being selected. This is because hive is supposed
    to disable query vectorization if complex columns are involved, but for
    some reason this is not applied on JOINs involving tables of the same
    storage type. This is seen in honeycomb if both tables use Parquet or
    both use Avro

    Args:
        query (str): The query to be run
        addr (str): The address of the data lake to communicate with
        configuration (dict<str:str>):
            Optional settings to apply to the hive connection
        e (Exception): The exception raised by _hive_query
        is_join_query (bool):
            Whether the query being run involves JOINing. If it is not,
            then the original exception is immediately re-raised, as it
            is definitively not the error we are trying to catch.
    """
    if is_join_query:
        # This means that the query failed even though vectorization
        # was disabled, and the failure is caused by something else
        already_attempted_nonvectorization = (
            isinstance(configuration, dict) and
            configuration[hive_vector_option_name] == 'true')

        complex_join_err_substring = (
            'cannot be cast to org.apache.hadoop.'
            'hive.serde2.objectinspector.PrimitiveObjectInspector'
        )

        # This means the error raised matches that which is raised from
        # queries involving complex columns and table joining
        is_complex_join_err = complex_join_err_substring in e.args[0]

        # If the query has both not already been attempted with vectorization
        # disabled and the raised does contain the substring that is indicitave
        # of the error raised by the hive bug
        if not already_attempted_nonvectorization and is_complex_join_err:
            disabling_vectorization_msg = (
                 'Query involves selecting complex type columns from a '
                 'joined table. Due to a hive bug, extra options must be '
                 'set for this scenario. To speed up query time, set '
                 '\'complex_join\' to True in \'hc.run_lake_query\' '
                 'if you run such a query again.'
            )
            logging.warn(disabling_vectorization_msg)
            configuration = _hive_get_nonvectorized_config(configuration)
            return _hive_query(query, addr, configuration)
    else:
        raise e


def _hive_check_valid_table_path(path):
    valid_path_pattern = r'^\w+[-/\w]*$'
    return bool(re.match(valid_path_pattern, path, flags=re.ASCII))


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
