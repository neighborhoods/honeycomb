from datetime import datetime

from honeycomb import run_query as run
from honeycomb.describe_table import describe_table

storage_type_specs = {
    'csv': {
        'settings': {
            'index': False,
            'header': False
        },
        'ddl': """
               ROW FORMAT DELIMITED
               FIELDS TERMINATED BY ','
               LINES TERMINATED BY '\\n'
               """
    },
    'pq': {
        'settings': {},
        'ddl': 'STORED AS PARQUET'
    }
}


def prep_schema_and_table(table, schema):
    if schema is None:
        if '.' in table:
            schema, table = table.split('.')
        else:
            schema = 'experimental'
    return table, schema


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
    filename = datetime.strftime(datetime.now(), '%Y-%m-%d_%H-%M-%S')
    if storage_type is None:
        storage_type = 'pq'

    return '.'.join([filename, storage_type])


def get_table_column_order(table_name, schema):
    """
    Gets the order of columns in a data lake table

    Args:
        table_name (str): The table to get the column order of
        schema (str): The schema the table is in
    """
    description = describe_table(table_name, schema)
    return description['col_name'].to_list()


def get_table_metadata(table_name, schema_name):
    """
    Gets the metadata a data lake table

    Args:
        table_name (str): The table to get the metadata of
        schema (str): The schema the table is in
    """
    create_stmt_query = "SHOW CREATE TABLE {schema_name}.{table_name}".format(
        schema_name=schema_name,
        table_name=table_name
    )
    table_metadata = run.lake_query(create_stmt_query, 'hive')

    bucket, path = get_table_s3_location_from_metadata(table_metadata)
    storage_type = get_table_storage_type_from_metadata(table_metadata)

    metadata_dict = {
        'bucket': bucket,
        'path': path,
        'storage_type': storage_type
    }
    return metadata_dict


def get_table_s3_location_from_metadata(table_metadata):
    """
    Extracts the underlying S3 location a table uses from its metadata

    Args:
        table_metadata (pd.DataFrame):
            The metadata of a table in the lake as returned from
            'get_table_metadata'
    """
    loc_label_idx = table_metadata.index[
        table_metadata['createtab_stmt'].str.strip() == "LOCATION"].values[0]
    location = table_metadata.loc[
        loc_label_idx + 1, 'createtab_stmt'].strip()[1:-1]

    prefix = 's3://'
    full_path = location[len(prefix):]

    bucket, path = full_path.split('/', 1)
    return bucket, path


def get_table_storage_type_from_metadata(table_metadata):
    """
    Identifies the format a table's underlying files are stored in using
    the table's metadata.

    Args:
        table_metadata (pd.DataFrame): Metadata of the table being examined
    """
    hive_input_format_to_storage_type = {
        'org.apache.hadoop.mapred.TextInputFormat': 'csv',
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat': 'pq'
    }
    format_label_idx = table_metadata.index[
        table_metadata['createtab_stmt'].str.strip() ==
        "STORED AS INPUTFORMAT"].values[0]
    input_format = table_metadata.loc[
        format_label_idx + 1, 'createtab_stmt'].strip()[1:-1]

    return hive_input_format_to_storage_type[input_format]
