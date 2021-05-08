from datetime import datetime
import logging

from honeycomb import check, hive, meta
from honeycomb.inform import inform


def add_partition(table_name, schema, partition_values, partition_path=None):
    partition_strings = build_partition_strings(partition_values)
    if partition_path is None:
        # Datetimes cast to str will by default provide an invalid path
        partition_path = '/'.join(
            [val if not isinstance(val, datetime)
             else str(val.date()) for val in partition_values.values()]) + '/'
    else:
        partition_path = meta.validate_table_path(partition_path, table_name)

    if not check.partition_existence(table_name, schema, partition_values):
        add_partition_query = (
            'ALTER TABLE {}.{} ADD IF NOT EXISTS '
            'PARTITION ({}) LOCATION \'{}\''.format(
                schema,
                table_name,
                partition_strings,
                partition_path)
        )
        inform(add_partition_query)

        hive.run_lake_query(add_partition_query, engine='hive')
    else:
        logging.warn(
            'Partition ({}) already exists in table.'.format(
                partition_strings)
        )

    return partition_path


def build_partition_strings(partition_values):
    partition_strings = [
        '{}="{}"'.format(partition_key, str(partition_value))
        if str(partition_value) != '' else '{}'.format(partition_key)
        for partition_key, partition_value in partition_values.items()]
    return ', '.join(partition_strings)
