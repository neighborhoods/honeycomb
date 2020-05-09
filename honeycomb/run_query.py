import pandas as pd
from pyhive import hive, presto


def clean_col_names(df):
    df.columns = df.columns.str.replace(r'^.*\.', '')
    return df


def run_query(query, engine="presto"):
    """
    General wrapper function around querying with different engines
    """
    query_fn = query_fns[engine]
    df = query_fn(query)
    return df


def hive_query(query, should_convert_dtypes=False):
    """
    Hive-specific query function
    """
    with hive.connect("localhost") as conn:
        df = pd.read_sql(query, conn)
    # if should_convert_dtypes:
    #     df = convert_dtypes(df)
    return clean_col_names(df)


def presto_query(query, should_convert_dtypes=False):
    """
    Presto-specific query function
    """
    with presto.connect("localhost") as conn:
        df = pd.read_sql(query, conn)
    # if should_convert_dtypes:
    #     df = convert_dtypes(df)
    return df


def gbq_query(query, should_convert_dtypes=False):
    """
    BigQuery-specific query function
    """
    df = pd.read_gbq(query, project_id="")
    # if should_convert_dtypes:
    #     df = convert_dtypes(df)
    return df


query_fns = {
    "presto": presto_query,
    "hive": hive_query,
    "gbq": gbq_query
}
