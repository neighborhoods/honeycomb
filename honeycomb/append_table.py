import river as rv

from honeycomb import check, meta
from honeycomb.config import storage_type_specs


# TODO Sound alarm if appending will overwrite a file
# (waiting on river release)
def append_table(df, table_name, schema='experimental', filename=None):
    """
    Uploads a dataframe to S3 and appends it to an already existing table.
    Queries existing table metadata to

    Args:
        df (pd.DataFrame): Which schema to check for the table in
        table_name (str): The name of the table to be created
        schema (str, optional): Name of the schema to create the table in
        filename (str, optional):
            Name to store the file under. Can be left blank if writing to the
            experimental zone, in which case a name will be generated.
    """
    table_exists = check.table_existence(schema, table_name)
    if not table_exists:
        raise ValueError(
            'Table \'{schema}.{table_name}\' does not exist. '.format(
                schema=schema,
                table_name=table_name))

    table_metadata = meta.get_table_metadata(table_name, schema)

    bucket = table_metadata['bucket']
    path = table_metadata['path']
    storage_type = table_metadata['storage_type']

    if filename is None:
        filename = meta.gen_filename_if_allowed(schema, storage_type)
    if not path.endswith('/'):
        path += '/'
    path += filename

    storage_settings = storage_type_specs[storage_type]['settings']
    rv.write(df, path, bucket, **storage_settings)
