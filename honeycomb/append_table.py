import river as rv

from honeycomb import check, meta, run_query
from honeycomb.config import storage_type_specs


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
    table_exists = check.table_existence(schema_name, table_name)
    if not table_exists:
        raise ValueError(
            'Table \'{schema_name}.{table_name}\' does not exist. '.format(
                schema_name=schema_name,
                table_name=table_name))

    metadata_query = "DESCRIBE FORMATTED {schema_name}.{table_name}".format(
        schema_name=schema_name,
        table_name=table_name
    )

    table_metadata = run_query(metadata_query, 'hive')
    # Columns from this query just set the value in the first row
    # as their name - can be confusing, so just setting it to numeric.
    table_metadata.columns = [0, 1, 2]

    bucket, path = meta.get_table_s3_location(table_metadata)
    storage_type = meta.get_table_storage_type(table_metadata)

    if filename is None:
        filename = meta.gen_filename_if_allowed(schema_name, storage_type)
    if not path.endswith('/'):
        path += '/'
    path += filename

    storage_settings = storage_type_specs[storage_type]['settings']
    rv.write(df, path, bucket, **storage_settings)
