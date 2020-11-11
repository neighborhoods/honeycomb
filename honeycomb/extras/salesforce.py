import boto3
import pandas as pd


def get_secret(key):
    client = boto3.client('ssm', 'us-east-1')
    resp = client.get_parameter(Name=key, WithDecryption=True)
    return resp['Parameter']['Value']


def get_salesforce_conn():
    """
    Function that returns a salesforce connection object by pulling creds
    from the environment. Must have SALESFORCE_USERNAME, SALESFORCE_PASSWORD,
    SALESFORCE_SECURITY_TOKEN available in the environment
    Returns: simple_salesforce.api.Salesforce: a Salesforce connection object.
    """
    # TODO (maybe): Check for correct version of 'simple-salesforce'?
    try:
        from simple_salesforce import Salesforce
    except ModuleNotFoundError:
        raise ImportError('Package "simple-salesforce" is required to use '
                          'honeycomb\'s "salesforce" module.')

    path = '/prod/lead-routing-serverless-functions/salesforce-data-science/'
    return Salesforce(
        username=get_secret(path + 'username'),
        password=get_secret(path + 'password'),
        security_token=get_secret(path + 'token')
        )


def run_sf_query(query, sf=None):
    """
    Queries Salesforce using the query text provided, removing the unnecessary
    Salesforce metadata
    Args:
        query (str): The SOQL query
        sf (simple_salesforce.Salesforce): The Salesforce API object
    Returns:
        pd.DataFrame: The dataframe directly queried from Salesforce
    """
    if sf is None:
        sf = get_salesforce_conn()
    df = sf.query_all(query)['records']
    return pd.DataFrame(df).drop('attributes', axis=1)


def sf_get_column_names(object_name, sf=None):
    """
    Queries salesforce and returns a list of all the column names in
    the object provided.
    Args:
        object_name (str): The name of the object (table) to get col names from
        sf (simple_salesforce.Salesforce): The Salesforce API object
    Returns:
        list<str>: A list of column names of the salesforce object
    """
    if sf is None:
        sf = get_salesforce_conn()
    desc = getattr(sf, object_name).describe()
    return [field['name'] for field in desc['fields']]


def sf_select_star(object_name, where_clause=None, limit_clause=None, sf=None):
    """
    Queries Salesforce using SELECT *, removing the unnecessary
    Salesforce metadata. An optional WHERE or LIMIT clause can be included,
    which is appended to the SOQL query, to limit the number of records
    returned.
    Args:
        object_name (str): The name of the object (table) to query
        where_clause (str): Optional, a WHERE clause to append to the query.
            Must start with WHERE and be valid SOQL e.g. "WHERE Id = 'ab12345'"
        limit_clause (str): Optional, a LIMIT clause to append to the query.
            Must start with LIMIT and be valid SOQL e.g. 'LIMIT 10'
        sf (simple_salesforce.Salesforce): The Salesforce API object
    Returns:
        pd.DataFrame: The SELECT * dataframe directly queried from Salesforce
    """
    if sf is None:
        sf = get_salesforce_conn()
    soql = gen_select_star_query(object_name, where_clause, limit_clause, sf)
    print(soql)

    return run_sf_query(soql, sf)


def gen_select_star_query(object_name, where_clause=None, limit_clause=None,
                          sf=None):
    if sf is None:
        sf = get_salesforce_conn()
    field_names = sf_get_column_names(object_name, sf)

    soql = 'SELECT\n    {}\nFROM {}'.format(',\n    '.join(field_names),
                                            object_name)
    soql = '\n'.join(filter(None, [soql, where_clause, limit_clause]))

    return soql
