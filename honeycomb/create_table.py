from datetime import datetime
import os

import river as rv

from honeycomb.dtype_mapping import apply_spec_dtypes, map_pd_to_db_dtypes
from honeycomb import run_query


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


def check_schema_existence(schema_name, engine='presto'):
    show_schemas_query = (
        'SHOW SCHEMAS LIKE \'{schema_name}\''.format(schema_name=schema_name)
    )

    similar_schemas = run_query.run_query(show_schemas_query, engine='hive')
    if similar_schemas is not None:
        # NOTE: 'database' and 'schema' are interchangeable terms in Hive
        if schema_name in similar_schemas['database_name'].values:
            return True
    return False


def check_table_existence(schema_name, table_name, engine='presto'):
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


def add_comments_to_col_defs(col_defs, comments):
    col_defs = (
        col_defs
        .to_frame(name='dtype')
    )

    for column, comment in comments.items():
        col_defs.loc[col_defs.index == column, 'comment'] = comment

    col_defs['comment'] = (
        ' COMMENT \'' + col_defs['comment'].astype(str) + '\'')
    return col_defs


# TODO add presto support?
def create_table_from_df(df, table_name, schema_name='experimental',
                         dtypes=None, path=None, filename=None,
                         table_comment=None, col_comments=None):
    """
    Uploads a dataframe to S3 and establishes it as a new table in Hive.

    Args:
        df (pd.DataFrame): The DataFrame to create the tabale from.
        table_name (str): The name of the table to be created
        schema_name (str): The name of the schema to create the table in
        dtypes (dict<str:str>, optional): A dictionary specifying dtypes for
            specific columns to be cast to prior to uploading.
        path (str, optional): Folder in S3 to store all files for this table in
        filename (str, optional):
            Name to store the file under. Used to determine storage format.
            Can be left blank if writing to the experimental zone,
            in which case a name will be generated and storage format will
            default to Parquet
        table_comment (str, optional): Documentation on the table's purpose
        col_comments (dict<str:str>, optional):
            Dictionary from column name keys to column descriptions.
    """
    if path is None:
        path = table_name
    if filename is None:
        filename = gen_filename_if_allowed(schema_name)

    if not path.endswith('/'):
        path += '/'
    path += filename

    table_exists = check_table_existence(
        schema_name, table_name, engine='hive')
    if table_exists:
        raise ValueError(
            'Table \'{schema_name}.{table_name}\' already exists. '.format(
                schema_name=schema_name,
                table_name=table_name))

    if dtypes is not None:
        df = apply_spec_dtypes(df, dtypes)
    col_defs = map_pd_to_db_dtypes(df)
    if col_comments is not None:
        col_defs = add_comments_to_col_defs(col_defs, col_comments)

    s3_bucket = schema_to_zone_bucket_map[schema_name]

    storage_type = os.path.splitext(filename)[-1][1:].lower()
    storage_settings = storage_type_config[storage_type]['settings']
    full_path = rv.write(df, path, s3_bucket, **storage_settings)

    create_statement = """
    CREATE EXTERNAL TABLE {schema_name}.{table_name} (
    {columns_and_types}
    )
    {table_comment}
    {storage_format_ddl}
    LOCATION 's3://{full_path}'
    """.format(
        schema_name=schema_name,
        table_name=table_name,
        columns_and_types=col_defs.to_string(
            header=False).replace('\n', ',\n'),
        table_comment=('COMMENT \'{table_comment}\''.format(
            table_comment=table_comment)) if table_comment else '',
        storage_format_ddl=storage_type_config[storage_type]['ddl'],
        full_path=full_path.rsplit('/', 1)[0] + '/'
    )
    print(create_statement)
    run_query.run_query(create_statement, engine='hive')


# TODO Sound alarm if appending will overwrite a file
# (waiting on river release)
def append_table(df, table_name, schema_name='experimental', filename=None):
    """
    Uploads a dataframe to S3 and appends it to an already existing table.
    Queries existing table metadata to

    Args:
        df (pd.DataFrame): Which schema to check for the table in
        table_name (str): The name of the table to be created
        schema_name (str, optional): Name of the schema to create the table in
        filename (str, optional):
            Name to store the file under. Can be left blank if writing to the
            experimental zone, in which case a name will be generated.
    """
    table_exists = check_table_existence(schema_name, table_name)
    if not table_exists:
        raise ValueError(
            'Table \'{schema_name}.{table_name}\' does not exist. '.format(
                schema_name=schema_name,
                table_name=table_name))

    metadata_query = "DESCRIBE FORMATTED {schema_name}.{table_name}".format(
        schema_name=schema_name,
        table_name=table_name
    )

    table_metadata = run_query.run_query(metadata_query, 'hive')
    # Columns from this query just take on the value in their first row -
    # can be confusing, so just setting it to numeric.
    table_metadata.columns = [0, 1, 2]

    full_path = table_metadata.loc[
        table_metadata[0].str.strip() == 'Location:', 1].values[0]

    prefix = 's3://'
    full_path = full_path[len(prefix):]

    bucket, path = full_path.split('/', 1)

    storage_type = get_table_storage_type(table_metadata)
    if filename is None:
        filename = gen_filename_if_allowed(schema_name, storage_type)
    if not path.endswith('/'):
        path += '/'
    path += filename

    storage_settings = storage_type_config[storage_type]['settings']
    rv.write(df, path, bucket, **storage_settings)


def gen_filename_if_allowed(schema_name, storage_type=None):
    """
    Pass-through to name generation fn, if writing to the experimental zone

    Args:
        schema_name (str):
            The name of the schema to be written to.
            Used to determine if a generated filename is permitted.
        storage_type (str, optional):
            Desired storage format of file to be stored. Passed through to
            'generate_s3_filename'
    """
    if schema_name == 'experimental':
        filename = generate_s3_filename(storage_type)
        return filename
    else:
        raise ValueError('A filename must be provided when writing '
                         'outside the experimental zone.')


def generate_s3_filename(storage_type=None):
    """
    Generates a filename based off a current timestamp and a storage format

    Args:
        storage_type (str, optional):
            Desired storage type of the file to be stored. Will be set to
            Parquet if left unspecified.
    """
    filename = datetime.strftime(datetime.now(), '%Y-%m-%d:%H-%M-%S')
    if storage_type is None:
        storage_type = 'pq'

    return '.'.join([filename, storage_type])


def get_table_storage_type(table_metadata):
    """
    Identifies the format a table's underlying files are stored in using
    the table's metadata.

    Args:
        table_metadata (pd.DataFrame): Metadata of the table being examined
    """
    hive_format_to_storage_type = {
        'org.apache.hadoop.mapred.TextInputFormat': 'csv',
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat': 'pq'
    }

    input_format = table_metadata.loc[
        table_metadata[0].str.strip() == 'InputFormat:', 1].values[0]

    return hive_format_to_storage_type[input_format]


storage_type_config = {
    'csv': {
        'settings': {
            'index': False,
            'header': False
        },
        'ddl': """
               ROW FORMAT DELIMITED
               FIELDS TERMINATED BY ','
               LINES TERMINATED BY '\\n'"""
    },
    'pq': {
        'settings': {},
        'ddl': 'STORED AS PARQUET'
    }
}
