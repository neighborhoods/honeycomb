from honeycomb import hive, meta

import river as rv


def __nuke_table(table_name, schema):
    """
    USE AT YOUR OWN RISK. THIS OPERATION IS NOT REVERSIBLE.

    Drop a table from the lake metastore and completely remove all of its
    underlying files from S3.

    Args:
        table_name (str): Name of the table to drop
        schema (str): Schema the table is in
    """
    table_metadata = meta.get_table_metadata(table_name, schema)
    current_bucket = table_metadata['bucket']
    current_path = table_metadata['path']
    hive.run_lake_query('DROP TABLE IF EXISTS {}.{}'.format(
        schema,
        table_name),
        engine='hive'
    )
    rv.delete(current_path, current_bucket, recursive=True)


def __nuke_partition(table_name, schema, partition_values):
    """
    USE AT YOUR OWN RISK. THIS OPERATION IS NOT REVERSIBLE.

    Drop a partition from a table and completely remove all of its
    underlying files from S3.

    Args:
        table_name (str): Name of the table to drop
        schema (str): Schema the table is in
        partition_values (dict<str:str>):
            Mapping from partition name to partition value, identifying the
            partition to be nuked
    """
    partition_string = ', '.join([
        '{}=\'{}\''.format(partition_key, partition_value)
        for partition_key, partition_value in partition_values.items()])
    print('DESCRIBE FORMATTED {}.{} PARTITION ({})'.format(
        schema, table_name, partition_string))
    partition_metadata = hive.run_lake_query(
        'DESCRIBE FORMATTED {}.{} PARTITION ({})'.format(
            schema, table_name, partition_string),
        engine='hive'
    )

    # The DataFrame returned by DESCRIBE queries are not organized like a
    # normal DataFrame, hence the inaccurate column names
    partition_location = partition_metadata.loc[
        partition_metadata['col_name'].str.strip() == 'Location:',
        'data_type'
    ].values[0]

    uri_prefix = 's3://'
    bucket, path = partition_location[len(uri_prefix):].split('/', 1)
    path += '/'

    hive.run_lake_query(
        'ALTER TABLE {}.{} DROP IF EXISTS PARTITION ({})'.format(
            schema, table_name, partition_string),
        engine='hive'
    )
    rv.delete(path, bucket, recursive=True)
