import river as rv

from honeycomb import check, meta
from honeycomb.config import storage_type_specs


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
    table_exists = check.table_existence(schema_name, table_name)
    if not table_exists:
        raise ValueError(
            'Table \'{schema_name}.{table_name}\' does not exist. '.format(
                schema_name=schema_name,
                table_name=table_name))

    table_metadata = meta.get_table_metadata(table_name, schema_name)

    bucket = table_metadata['bucket']
    path = table_metadata['path']
    storage_type = table_metadata['storage_type']

    if filename is None:
        filename = meta.gen_filename_if_allowed(schema_name, storage_type)
    if not path.endswith('/'):
        path += '/'
    path += filename

    if rv.exists(path, bucket):
        raise KeyError('A file already exists at s3://' + bucket + path + ', '
                       'Which will be overwritten by this operation. '
                       'Specify a different filename to proceed.')

    storage_settings = storage_type_specs[storage_type]['settings']
    rv.write(df, path, bucket, **storage_settings)
