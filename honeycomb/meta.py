from datetime import datetime


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


def get_table_s3_location(table_metadata):
    full_path = table_metadata.loc[
        table_metadata[0].str.strip() == 'Location:', 1].values[0]

    prefix = 's3://'
    full_path = full_path[len(prefix):]

    bucket, path = full_path.split('/', 1)
    return bucket, path


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
