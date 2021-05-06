import river as rv

from honeycomb import hive, meta
from honeycomb.alter_table import add_partition
from honeycomb.create_table.common import handle_avro_filetype
from honeycomb.ddl_building import build_create_table_ddl
from honeycomb.inform import inform


def build_and_run_ddl_stmt(df, table_name, schema, col_defs,
                           storage_type, bucket, path, filename,
                           col_comments=None, table_comment=None,
                           partitioned_by=None, partition_values=None,
                           auto_upload_df=True, avro_schema=None):
    """
    After preparation is performed in other calling functions,
    this function actually generates a CREATE TABLE command and runs it,
    optionally automatically uploading the DataFrame to the table as well

    Args:
        df (pd.DataFrame): The DataFrame to create the table from.
        table_name (str): The name of the table to be created
        schema (str): The name of the schema to create the table in
        col_defs (pd.DataFrame):
            A DataFrame with two columns, 'names' containing column names,
            and 'dtypes', containing a string representation of
            the column's dtype
        bucket (str): Bucket containing the table's files
        path (str): Path within bucket containing the table's files
        filename (str, optional):
            Name to store the file under. Used to determine storage format.
            Can be left blank if writing to the experimental zone,
            in which case a name will be generated and storage format will
            default to Parquet
        col_comments (dict<str:str>, optional):
            Dictionary from column name keys to column descriptions.
        table_comment (str, optional): Documentation on the table's purpose
        partitioned_by (dict<str:str>,
                        collections.OrderedDict<str:str>, or
                        list<tuple<str:str>>, optional):
            Dictionary or list of tuples containing a partition name and type.
            Cannot be a vanilla dictionary if using Python version < 3.6
        partition_values (dict<str:str>):
            Required if 'partitioned_by' is used and 'auto_upload_df' is True.
            List of tuples containing partition name and value to store
            the dataframe under
        auto_upload_df (bool, default True):
            Whether the df that the table's structure will be based off of
            should be automatically uploaded to the table
        avro_schema (dict, optional):
            Schema to use when writing a DataFrame to an Avro file. If not
            provided, one will be auto-generated.
    """
    # Gets settings to pass to river on how to write the files in a
    # Hive-readable format
    storage_settings = meta.storage_type_specs[storage_type]['settings']

    # tblproperties is for additional metadata to be provided to Hive
    # for the table. Generally, it is not needed
    tblproperties = {}

    if storage_type == 'avro':
        storage_settings, tblproperties = handle_avro_filetype(
            df, storage_settings, tblproperties, avro_schema, col_comments)

    full_path = '/'.join([bucket, path])
    create_table_ddl = build_create_table_ddl(table_name, schema, col_defs,
                                              col_comments, table_comment,
                                              storage_type, partitioned_by,
                                              full_path, tblproperties)
    inform(create_table_ddl)
    hive.run_lake_query(create_table_ddl, engine='hive')

    if partitioned_by and partition_values:
        path += add_partition(table_name, schema, partition_values)
    path += filename

    if auto_upload_df:
        # Creating the table doesn't populate it with data. Unless
        # auto_upload_df == False, we now need to write the DataFrame to a
        # file and upload it to S3
        _ = rv.write(df, path, bucket,
                     show_progressbar=False, **storage_settings)
