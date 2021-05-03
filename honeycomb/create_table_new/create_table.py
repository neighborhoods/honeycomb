import river as rv

from honeycomb import hive, meta
from honeycomb.alter_table import add_partition
from honeycomb.create_table_new.common import handle_avro_filetype
from honeycomb.ddl_building import build_create_table_ddl
from honeycomb.inform import inform


def create_table(df, table_name, schema, col_defs,
                 storage_type, bucket, path, filename,
                 col_comments=None, table_comment=None,
                 partitioned_by=None, partition_values=None,
                 auto_upload_df=True, avro_schema=None):
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
