from datetime import datetime
import re

from honeycomb import hive
from honeycomb.describe_table import describe_table


storage_type_specs = {
    'avro': {
        'settings': {},
        'ddl': 'STORED AS AVRO'
    },
    'csv': {
        # If the index is written to the CSV, it will be treated by Hive
        # as a column.
        # If the header/column names are written to the CSV, they will be
        # treated by Hive as a row.
        'settings': {
            'index': False,
            'header': False
        },
        'ddl': ("ROW FORMAT DELIMITED\n"
                "FIELDS TERMINATED BY ','\n"
                "COLLECTION ITEMS TERMINATED BY '|'\n"
                "LINES TERMINATED BY '\\n'")
    },
    'json': {
        # Hive expects JSON data to be written differently than df.to_json()
        # writes it by default. This is handled by river.
        'settings': {'hive_format': True},
        'ddl': ("ROW FORMAT SERDE\n"
                "'org.apache.hadoop.hive.serde2.JsonSerDe'\n"
                "STORED AS TEXTFILE")
    },
    'parquet': {
        # Hive expects timestamps to be saved as 96-bit integers with Parquet,
        # even though this behavior is no longer the Parquet standard.
        'settings': {
            'engine': 'pyarrow',
            'compression': 'snappy',
            'use_deprecated_int96_timestamps': True
        },
        'ddl': 'STORED AS PARQUET'
    },
    'orc': {
        'settings': {},
        'ddl': 'STORED AS ORC'
    }
}

create_stmt_query_template = 'SHOW CREATE TABLE {schema}.{table_name}'


def prep_schema_and_table(table, schema):
    """
    If schema is provided in the table name string,
    it is split out. If none is provided, sets it to 'experimental'
    """
    if schema is None:
        if '.' in table:
            schema, table = table.split('.')
        else:
            schema = 'experimental'
    return table, schema


def ensure_path_ends_w_slash(path):
    """Ensures that a path string ends with a slash"""
    if not path.endswith('/'):
        path += '/'
    return path


def validate_table_path(path, table_name):
    """
    Ensures that the path provided for the table is valid, or assigns the path
    as the table name if no path was provided
    """
    if path is None:
        path = table_name
    path = ensure_path_ends_w_slash(path)
    if not re.match(r'^(\w+/)+$', path, flags=re.ASCII):
        raise ValueError('Invalid table path provided.')
    return path


def gen_filename_if_allowed(schema, storage_type=None):
    """
    Pass-through to name generation fn, if writing to the experimental zone

    Args:
        schema (str):
            The name of the schema to be written to.
            Used to determine if a generated filename is permitted.
        storage_type (str, optional):
            Desired storage format of file to be stored. Passed through to
            'generate_s3_filename'
    """
    if schema == 'experimental':
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
        storage_type = 'parquet'

    return '.'.join([filename, storage_type])


def get_table_column_order(table_name, schema, include_dtypes=False):
    """
    Gets the order of columns in a data lake table. Deliberately leaves
    out partition columns, because those should not be present in the original
    dataset.

    Args:
        table_name (str): The table to get the column order of
        schema (str): The schema the table is in
    """
    colname_col = 'col_name'
    dtype_col = 'data_type'
    description = describe_table(table_name, schema, include_metadata=True)
    colname_end = description.index[description[colname_col] == ''][0] - 1

    description = description.loc[:colname_end]
    if not include_dtypes:
        columns = description[colname_col]
        return columns.to_list()
    else:
        columns_w_types = description[[colname_col, dtype_col]]
        return columns_w_types.rename(columns={dtype_col: 'dtype'})


def get_table_metadata(table_name, schema):
    """
    Gets the metadata a data lake table

    Args:
        table_name (str): The table to get the metadata of
        schema (str): The schema the table is in
    """
    bucket, path = get_table_s3_location(table_name, schema)
    storage_type = get_table_storage_type(table_name, schema)

    metadata_dict = {
        'bucket': bucket,
        'path': path,
        'storage_type': storage_type
    }
    return metadata_dict


def get_table_s3_location(table_name, schema):
    """
    Extracts the underlying S3 location a table uses from its metadata

    Args:
        table_metadata (pd.DataFrame):
            The metadata of a table in the lake as returned from
            'get_table_metadata'
    """
    create_stmt_query = create_stmt_query_template.format(
        schema=schema,
        table_name=table_name
    )
    table_metadata = hive.run_lake_query(create_stmt_query)

    loc_label_idx = table_metadata.index[
        table_metadata['createtab_stmt'].str.strip() == "LOCATION"].values[0]
    location = table_metadata.loc[
        loc_label_idx + 1, 'createtab_stmt'].strip()[1:-1]

    prefix = 's3://'
    full_path = location[len(prefix):]

    bucket, path = full_path.split('/', 1)
    return bucket, path


def get_table_storage_type(table_name, schema):
    """
    Identifies the format a table's underlying files are stored in using
    the table's metadata.

    Args:
        table_metadata (pd.DataFrame): Metadata of the table being examined
    """
    create_stmt_query = create_stmt_query_template.format(
        schema=schema,
        table_name=table_name
    )
    table_metadata = hive.run_lake_query(create_stmt_query)

    hive_input_format_to_storage_type = {
        'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat': 'avro',
        'org.apache.hadoop.mapred.TextInputFormat': 'text',
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat':
            'parquet',
        'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat': 'orc'
    }
    format_label_idx = table_metadata.index[
        table_metadata['createtab_stmt'].str.strip() ==
        "STORED AS INPUTFORMAT"].values[0]
    input_format = table_metadata.loc[
        format_label_idx + 1, 'createtab_stmt'].strip()[1:-1]

    storage_format = hive_input_format_to_storage_type[input_format]
    if storage_format == 'text':
        # Both CSV and JSON tables will have a storage format of 'text',
        # so we must further differentiate them by checking the
        # serde type
        serde_label_idx = table_metadata.index[
            table_metadata['createtab_stmt'].str.strip() ==
            "ROW FORMAT SERDE"].values[0]
        serde_type = table_metadata.loc[
            serde_label_idx + 1, 'createtab_stmt'].strip()[1:-1]
        if serde_type == 'org.apache.hadoop.hive.serde2.JsonSerDe':
            storage_format = 'json'
        else:
            storage_format = 'csv'

    return storage_format


def is_partitioned_table(table_name, schema):
    desc = describe_table(table_name, schema)
    if any(desc['col_name'] == '# Partition Information'):
        return True
    return False


def get_partition_cols(table_name, schema):
    if not is_partitioned_table(table_name, schema):
        return None
    else:
        colname_col = 'col_name'
        desc = describe_table(table_name, schema)
        partition_col_start_idx = desc.index[
            desc[colname_col] == '# Partition Information'][0] + 2
        partition_cols = desc.loc[partition_col_start_idx:, colname_col]
        return partition_cols.to_list()
