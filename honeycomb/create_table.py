import os

import river as rv

from honeycomb.dtype_mapping import apply_spec_dtypes, map_pd_to_db_dtypes
from honeycomb import run_query


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


# TODO filename generation
# TODO add presto support?
def create_table_from_df(df, table_name, schema_name='experimental',
                         dtypes=None, s3_path=None,
                         table_comment=None, col_comments=None):
    """
    Uploads a dataframe to S3 and establishes it as a new table in Hive.

    Args:
        df (pd.DataFrame): The DataFrame to create the tabale from.
        table_name (str): The name of the table to be created
        schema_name (str): The name of the schema to create the table in
        dtypes (dict<str:str>, optional): A dictionary specifying dtypes for
            specific columns to be cast to prior to uploading.
        s3_path (str, optional): Filename to store CSV in S3 under
    """
    if s3_path is None:
        raise ValueError('Until S3 name generation is implemented, '
                         ' \'s3_path\' cannot be \'None\'.')

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

    # TODO replace with s3 tool
    s3_bucket = schema_to_zone_bucket_map[schema_name]

    filetype = os.path.splitext(s3_path)[-1][1:].lower()
    fn_defaults = dtype_fn_defaults[filetype]
    rv.write(df, s3_path, s3_bucket, **fn_defaults)
    s3_path = rv.write(df, s3_path, s3_bucket, index=False, header=False)

    create_statement = """
    CREATE EXTERNAL TABLE {schema_name}.{table_name} (
    {columns_and_types}
    )
    {table_comment}
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\\n'
    LOCATION 's3://{s3_path}'
    """.format(
        schema_name=schema_name,
        table_name=table_name,
        columns_and_types=col_defs.to_string(
            header=False).replace('\n', ',\n'),
        table_comment=('COMMENT \'{table_comment}\''.format(
            table_comment=table_comment)) if table_comment else '',
        s3_path=s3_path.rsplit('/', 1)[0] + '/'
    )
    print(create_statement)
    run_query.run_query(create_statement, engine='hive')


# TODO Sound alarm if appending will overwrite a file
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
            'Table \'{schema_name}.{table_name}\' does not exist. '.format(
                schema_name=schema_name,
                table_name=table_name))

    metadata_query = "DESCRIBE FORMATTED {schema_name}.{table_name}".format(
        schema_name=schema_name,
        table_name=table_name
    )

    table_metadata = run_query.run_query(metadata_query, 'hive',
                                         columns=['col1', 'col2', 'col3'])
    # Columns from this query just take on the value in their first row -
    # can be confusing, so just setting it to numeric.
    table_metadata.columns = [0, 1, 2]

    s3_location = table_metadata.loc[
        table_metadata['col1'].str.strip() == 'Location:',
        'col2'].values[0]

    prefix = 's3://'
    s3_location = s3_location[len(prefix):]

    bucket, path = s3_location.split('/', 1)
    if filename is None:
        filename = generate_s3_filename()

    if not path.endswith('/'):
        path += '/'
    path += filename

    # TODO defaults based on filetype
    filetype = get_table_storage_type()
    fn_defaults = dtype_fn_defaults[filetype]
    rv.write(df, path, bucket, **fn_defaults)


# TODO generate S3 filenames
def generate_s3_filename():
    pass


def get_table_storage_type():
    pass


dtype_fn_defaults = {
    'csv': {
        'index': False,
        'header': False
    }
}
