from pyhive import hive, presto

from honeycomb.dtype_mapping import apply_spec_dtypes, map_pd_to_db_dtypes
from honeycomb import run_query, river as rv


# TODO logging instead of print
# TODO table/column comments
valid_schemas = [
    'landing',
    'staging',
    'experimental',
    'curated'
]

schema_to_zone_bucket_map = {
    'landing': 'nhds-data-lake-landing-zone',
    'staging': 'nhds-data-lake-staging-zone',
    'experimental': 'nhds-data-lake-experimental-zone',
    'curated': 'nhds-data-lake-curated-zone'
}


# TODO BigQuery connection?
def get_db_connection(engine='hive'):
    """
    Initializes and returns a connection to the specified database engine.

    Args:
        engine (str): String specifying which engine to return a connection for

    Returns:
        conn (str): Something of a misnomer - 'conn' is a cursor
        of a connection rather than a connection object itself.
    """
    if engine == 'hive':
        conn = hive.connect('localhost').cursor()
    elif engine == 'presto':
        conn = presto.connect('localhost').cursor()
    else:
        raise ValueError('Specified engine is not supported: ' + engine)
    return conn


def check_table_existence(schema_name, table_name):
    """
    Checks if a specific table exists in a specific schema

    Args:
        schema_name (str): Which schema to check for the table in
        table_name (str): The name of the table to check for

    Returns:
        bool: Whether or not the specified table exists
    """
    show_tables_query = (
        'SHOW TABLES IN {schema_name} LIKE \'{table_name}\''.format(
            schema_name=schema_name,
            table_name=table_name)
    )

    similar_tables = run_query.run_query(show_tables_query, engine='hive')
    if table_name in similar_tables['tab_name'].values:
        return True
    return False


# TODO filename generation
# TODO add presto support?
def create_table_from_df(df, table_name, schema_name='experimental',
                         dtypes=None, s3_filename=None, s3_folder=None):
    """
    Uploads a dataframe to S3 and establishes it as a new table in Hive.

    Args:
        df (pd.DataFrame): Which schema to check for the table in
        table_name (str): The name of the table to be created
        schema_name (str): The name of the schema to create the table in
        dtypes (dict<str:str>, optional): A dictionary specifying dtypes for
            specific columns to be cast to prior to uploading.
        s3_filename (str, optional): Filename to store CSV in S3 under
        s3_folder, (str, optional): S3 'folder' to prepend 's3_filename'
    """
    conn = get_db_connection(engine='hive')

    table_exists = check_table_existence(schema_name, table_name)
    if table_exists:
        raise ValueError(
            "Table {schema_name}.{table_name} already exists. ".format(
                schema_name=schema_name,
                table_name=table_name))

    if dtypes is not None:
        df = apply_spec_dtypes(df, dtypes)
    db_dtypes = map_pd_to_db_dtypes(df)

    # TODO replace with s3 tool
    s3_bucket = schema_to_zone_bucket_map[schema_name]
    s3_path = rv.store(df, s3_filename, s3_folder, s3_bucket)

    create_statement = """
    CREATE EXTERNAL TABLE {schema_name}.{table_name} (
    {columns_and_types}
    )
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\\n'
    LOCATION 's3://{s3_path}'
    """.format(
        schema_name=schema_name,
        table_name=table_name,
        columns_and_types=db_dtypes.to_string().replace('\n', ',\n'),
        s3_path=s3_path.rsplit('/', 1)[0] + '/'
    )
    print(create_statement)
    conn.execute(create_statement)


def append_table(df, table_name, schema_name='experimental', filename=None):
    """
    Uploads a dataframe to S3 and appends it to an already existing table.

    Args:
        df (pd.DataFrame): Which schema to check for the table in
        table_name (str): The name of the table to be created
        schema_name (str): The name of the schema to create the table in
        dtypes (dict<str:str>, optional): A dictionary specifying dtypes for
            specific columns to be cast to prior to uploading.
        s3_filename (str, optional): Filename to store CSV in S3 under
        s3_folder, (str, optional): S3 'folder' to prepend 's3_filename'
    """
    table_exists = check_table_existence(schema_name, table_name)
    if not table_exists:
        raise ValueError(
            "Table {schema_name}.{table_name} does not exist. ".format(
                schema_name=schema_name,
                table_name=table_name))
