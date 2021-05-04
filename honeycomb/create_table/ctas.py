import os
import re

from honeycomb import hive, meta
from honeycomb.create_table.common import (
    check_for_allowed_overwrite, check_for_comments,
    handle_existing_table, schema_to_zone_bucket_map
)
from honeycomb.ddl_building import build_create_table_ddl
from honeycomb.describe_table import describe_table
from honeycomb.__danger import __nuke_table


def ctas(select_stmt, table_name, schema=None,
         path=None, table_comment=None, col_comments=None,
         storage_type='pq', overwrite=False):
    """
    Emulates the standard SQL 'CREATE TABLE AS SELECT' syntax.

    Under the hood, this function creates a view using the provided SELECT
    statement, and then performs an INSERT OVERWRITE from that view into the
    new table.

    Because this function uses INSERT OVERWRITE, there are considerable
    protections within this function to prevent accidental data loss.
    When an INSERT OVERWRITE command is done on an external table, all of the
    files in S3 at that table's path are deleted. If the table's path is,
    for example, the root of a bucket, there could be substantial data loss.
    As a result, we do our best to smartly assign table paths and prevent
    large-scale object deletion.

    Args:
        select_stmt (str):
            The select statement to build a new table from
        table_name (str):
            The name of the table to be created
        schema (str):
            The schema the new table should be created in
        path (str):
            The path that the new table's underlying files will be stored at.
            If left unset, it will be set to a folder with the same name
            as the table, which is generally recommended
        table_comment (str, optional): Documentation on the table's purpose
        col_comments (dict<str:str>, optional):
            Dictionary from column name keys to column descriptions.
        storage_type (str):
            The desired storage type of the new table
        overwrite (bool):
            Whether to overwrite or fail if a table already exists with
            the intended name of the new table in the selected schema
    """
    table_name, schema = meta.prep_schema_and_table(table_name, schema)

    if schema == 'curated':
        check_for_allowed_overwrite(overwrite)
        if not os.getenv('HC_PROD_ENV'):
            raise ValueError(
                'Non-production CTAS functionality is currently disabled in '
                'the curated zone. Contact Data Engineering for '
                'further information.'
            )

    bucket = schema_to_zone_bucket_map[schema]
    path = meta.validate_table_path(path, table_name)

    full_path = '/'.join([bucket, path])

    # If this function is used to overwrite a table that is being selected
    # from, we need to make sure that the original table is not dropped before
    # selecting from it (which happens at execution time of the INSERT)
    # In this case, we will temporarily rename the table. If any section of
    # the remainder of this function fails before the INSERT, the table
    # will be restored to its original name
    table_rename_template = 'ALTER TABLE {}.{} RENAME TO {}.{}'
    if '{}.{}'.format(schema, table_name) in select_stmt:
        if overwrite:
            source_table_name = table_name + '_temp_ctas_rename'
            select_stmt = re.sub(
                r'{}\.{}([\s,.]|$)'.format(schema, table_name),
                r'{}.{}\1'.format(schema, source_table_name),
                select_stmt)
            hive.run_lake_query(table_rename_template.format(
                schema, table_name, schema, source_table_name
            ))
            table_renamed = True
        else:
            raise ValueError(
                'CTAS functionality must have \'overwrite\' set to True '
                'in order to overwrite one of the source tables of the '
                'SELECT statement.'
            )
    # No rename needed
    else:
        source_table_name = table_name
        table_renamed = False

    try:
        temp_schema = 'experimental'
        view_name = '{}_temp_ctas_view'.format(table_name)
        create_view_stmt = 'CREATE VIEW {}.{} AS {}'.format(
            temp_schema, view_name, select_stmt)
        hive.run_lake_query(create_view_stmt)

        # If we DESCRIBE the view, we can get a list of all the columns
        # in the new table for building DDL and adding comments.
        # Useful in queries that involve JOINing, so you don't have to build
        # that column list yourself.
        col_defs = describe_table(view_name, schema=temp_schema)

        if schema == 'curated':
            check_for_comments(
                table_comment, col_defs['col_name'], col_comments)

        create_table_ddl = build_create_table_ddl(table_name, schema, col_defs,
                                                  col_comments, table_comment,
                                                  storage_type,
                                                  partitioned_by=None,
                                                  full_path=full_path)
        handle_existing_table(table_name, schema, overwrite)
        hive.run_lake_query(create_table_ddl)
        insert_overwrite_command = (
            'INSERT OVERWRITE TABLE {}.{} SELECT * FROM {}.{}').format(
                schema, table_name, temp_schema, view_name)
        hive.run_lake_query(insert_overwrite_command,
                            complex_join=True)
    except Exception as e:
        # If an error occurred at any point in the above and a source table
        # was renamed, restore its original name
        if table_renamed:
            hive.run_lake_query(table_rename_template.format(
                schema, source_table_name, schema, table_name
            ))
        raise e

    finally:
        # Regardless of success or failure of the above, we want to
        # drop the temporary view if it was created
        hive.run_lake_query(
            'DROP VIEW IF EXISTS {}.{}'.format(temp_schema, view_name))

    # If the source table had to be renamed, it would not have been dropped
    # by the call to 'handle_existing_table', so we have to handle it here.
    # If it still shares a storage location with the new table, we just
    # drop it. Otherwise, we nuke it.
    if table_renamed:
        source_metadata = meta.get_table_metadata(source_table_name,
                                                  schema)
        source_path = meta.ensure_path_ends_w_slash(
            source_metadata['path'])
        if source_path == path:
            hive.run_lake_query(
                'DROP TABLE {}.{}'.format(schema, source_table_name))
        else:
            __nuke_table(source_table_name, schema)
