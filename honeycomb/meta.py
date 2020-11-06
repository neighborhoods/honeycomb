from collections import OrderedDict
from datetime import datetime
import sys

from honeycomb import hive
from honeycomb.describe_table import describe_table


storage_type_specs = {
    'avro': {
        'settings': {},
        'ddl': 'STORED AS AVRO'
    },
    'csv': {
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
        'settings': {'hive_format': True},
        'ddl': ("ROW FORMAT SERDE\n"
                "'org.apache.hadoop.hive.serde2.JsonSerDe'\n"
                "STORED AS TEXTFILE")
    },
    'pq': {
        'settings': {
            'engine': 'pyarrow',
            'compression': 'snappy',
            'use_deprecated_int96_timestamps': True
        },
        'ddl': 'STORED AS PARQUET'
    }
}

hive_reserved_words = ['date', 'time', 'timestamp', 'datetime']


def prep_schema_and_table(table, schema):
    if schema is None:
        if '.' in table:
            schema, table = table.split('.')
        else:
            schema = 'experimental'
    return table, schema


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
        storage_type = 'pq'

    return '.'.join([filename, storage_type])


def get_table_column_order(table_name, schema):
    """
    Gets the order of columns in a data lake table

    Args:
        table_name (str): The table to get the column order of
        schema (str): The schema the table is in
    """
    colname_col = 'col_name'
    description = describe_table(table_name, schema, include_metadata=True)
    colname_end = description.index[description[colname_col] == ''][0] - 1
    columns = description.loc[:colname_end, colname_col]
    return columns.to_list()


def get_table_metadata(table_name, schema):
    """
    Gets the metadata a data lake table

    Args:
        table_name (str): The table to get the metadata of
        schema (str): The schema the table is in
    """
    create_stmt_query = "SHOW CREATE TABLE {schema}.{table_name}".format(
        schema=schema,
        table_name=table_name
    )
    table_metadata = hive.run_lake_query(create_stmt_query, 'hive')

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
        'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat': 'avro',
        'org.apache.hadoop.mapred.TextInputFormat': 'text',
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat': 'pq'
    }
    format_label_idx = table_metadata.index[
        table_metadata['createtab_stmt'].str.strip() ==
        "STORED AS INPUTFORMAT"].values[0]
    input_format = table_metadata.loc[
        format_label_idx + 1, 'createtab_stmt'].strip()[1:-1]

    storage_format = hive_input_format_to_storage_type[input_format]
    if storage_format == 'text':
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


def get_partition_key_order(table_name, schema):
    if not is_partitioned_table(table_name, schema):
        raise ValueError('Table {}.{} is not partitioned.'.format(
            schema, table_name))

    table_desc = describe_table(table_name, schema)
    partition_names_idx = table_desc.index[
        table_desc['col_name'].str.strip() == '# Partition Information'
    ].values[0] + 2
    partition_names = table_desc.loc[partition_names_idx:, 'col_name']

    return partition_names


def confirm_ordered_partition_dicts(obj_to_check):
    """
    If "obj_to_check" is a vanilla dictionary, checks if the Python version
    is at least 3.6, determining whether dictionaries are ordered.
    """
    if isinstance(obj_to_check, dict):
        python_version = sys.version_info
        if python_version.major >= 3:
            if python_version.minor >= 6:
                if python_version.minor == 6:
                    print(
                        'You are using Python 3.6. Dictionaries are ordered '
                        'in 3.6, but only as a side effect. It is recommended '
                        'to upgrade to 3.7 to have guaranteeably '
                        'ordered dicts.')
                confirmed = True
        confirmed = False

        if not confirmed:
            raise TypeError(
                'The order of partition dicts must be preserved, and '
                'dictionaries are not guaranteed to be order-preserving '
                'in Python versions < 3.7. Use a list of tuples or an '
                'OrderedDict, or upgrade your Python version.')
    elif isinstance(obj_to_check, list):
        obj_to_check = OrderedDict(obj_to_check)
    elif not isinstance(obj_to_check, OrderedDict):
        raise TypeError(
            'Partition data must be provided as a dict, a list of tuples, '
            'or an OrderedDict.')

    return obj_to_check
