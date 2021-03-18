from collections import OrderedDict
import logging
import json
import os
import re
import sys

import pandavro as pdx
import river as rv

from honeycomb import check, dtype_mapping, hive, inform, meta
from honeycomb.alter_table import add_partition
from honeycomb.describe_table import describe_table
from honeycomb.ddl_building import (build_create_table_ddl,
                                    restructure_comments_for_avro,
                                    add_comments_to_avro_schema)
from honeycomb.__danger import __nuke_table


schema_to_zone_bucket_map = {
    'landing': os.getenv('HC_LANDING_ZONE_BUCKET'),
    'staging': os.getenv('HC_STAGING_ZONE_BUCKET'),
    'experimental': os.getenv('HC_EXPERIMENTAL_ZONE_BUCKET'),
    'curated': os.getenv('HC_CURATED_ZONE_BUCKET')
}


def create_table_from_df(df, table_name, schema=None,
                         dtypes=None, path=None, filename=None,
                         table_comment=None, col_comments=None,
                         timezones=None, copy_df=True,
                         partitioned_by=None, partition_values=None,
                         overwrite=False, auto_upload_df=True,
                         avro_schema=None):
    """
    Uploads a dataframe to S3 and establishes it as a new table in Hive.

    Args:
        df (pd.DataFrame): The DataFrame to create the tabale from.
        table_name (str): The name of the table to be created
        schema (str): The name of the schema to create the table in
        dtypes (dict<str:str>, optional): A dictionary specifying dtypes for
            specific columns to be cast to prior to uploading.
        path (str, optional): Folder in S3 to store all files for this table in
        filename (str, optional):
            Name to store the file under. Used to determine storage format.
            Can be left blank if writing to the experimental zone,
            in which case a name will be generated and storage format will
            default to Parquet
        table_comment (str, optional): Documentation on the table's purpose
        col_comments (dict<str:str>, optional):
            Dictionary from column name keys to column descriptions.
        timezones (dict<str, str>):
            Dictionary from datetime columns to the timezone they
            represent. If the column is timezone-naive, it will have the
            timezone added to its metadata, leaving the times themselves
            unmodified. If the column is timezone-aware and is in a different
            timezone than the one that is specified, the column's timezone
            will be converted, modifying the original times.
        copy_df (bool):
            Whether the operations performed on df should be performed on the
            original or a copy. Keep in mind that if this is set to False,
            the original df passed in will be modified as well - twice as
            memory efficient, but may be undesirable if the df is needed
            again later
        partitioned_by (dict<str:str>,
                        collections.OrderedDict<str:str>, or
                        list<tuple<str:str>>, optional):
            Dictionary or list of tuples containing a partition name and type.
            Cannot be a vanilla dictionary if using Python version < 3.6
        partition_values (dict<str:str>):
            Required if 'partitioned_by' is used and 'auto_upload_df' is True.
            List of tuples containing partition name and value to store
            the dataframe under
        overwrite (bool, default False):
            Whether to overwrite the current table if one is already present
            at the specified name
        auto_upload_df (bool, default True):
            Whether the df that the table's structure will be based off of
            should be automatically uploaded to the table
        avro_schema (dict, optional):
            Schema to use when writing a DataFrame to an Avro file. If not
            provided, one will be auto-generated.
    """
    # Less memory efficient, but prevents modification of original df
    if copy_df:
        df = df.copy()

    table_name, schema = meta.prep_schema_and_table(table_name, schema)

    if partitioned_by:
        if isinstance(partitioned_by, dict) and not confirm_ordered_dicts():
            raise TypeError(
                'The order of "partitioned_by" must be preserved, and '
                'dictionaries are not guaranteed to be order-preserving '
                'in Python versions < 3.7. Use a list of tuples or an '
                'OrderedDict, or upgrade your Python version.')
        elif isinstance(partitioned_by, list):
            partitioned_by = OrderedDict(partitioned_by)
        if auto_upload_df and not partition_values:
            raise ValueError(
                'If using "partitioned_by" and "auto_upload_df" is True, '
                'values must be passed to "partition_values" as well.')

    if schema == 'curated':
        check_for_comments(table_comment, df.columns, col_comments)
        check_for_allowed_overwrite(overwrite)

    handle_existing_table(table_name, schema, overwrite)

    if filename is None:
        filename = meta.gen_filename_if_allowed(schema)
    path = meta.validate_table_path(path, table_name)

    bucket = schema_to_zone_bucket_map[schema]

    if rv.list_objects(path, bucket):
        raise KeyError((
            'Files are already present in s3://{}/{}. Creation of a new table '
            'requires a dedicated, empty folder. Either specify a different '
            'path for the table or ensure the directory is empty before '
            'attempting table creation.').format(bucket, path))

    storage_type = get_storage_type_from_filename(filename)
    df, col_defs = prep_df_and_col_defs(
        df, dtypes, timezones, schema, storage_type)

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

    if partitioned_by:
        path += add_partition(table_name, schema, partition_values)
    path += filename

    if auto_upload_df:
        # Creating the table doesn't populate it with data. Unless
        # auto_upload_df == False, we now need to write the DataFrame to a file
        # and upload it to S3
        _ = rv.write(df, path, bucket,
                     show_progressbar=False, **storage_settings)


def confirm_ordered_dicts():
    """
    Checks if the Python version is at least 3.6, determining whether
    dictionaries are ordered.
    """
    python_version = sys.version_info
    if python_version.major >= 3:
        if python_version.minor >= 6:
            if python_version.minor == 6:
                logging.info(
                    'You are using Python 3.6. Dictionaries are ordered in '
                    '3.6, but only as a side effect. It is recommended to '
                    'upgrade to 3.7 to have guaranteeably ordered dicts.')
            return True
    return False


def handle_existing_table(table_name, schema, overwrite):
    """
    Checks if a table name already exists in the lake. If it does,
    then depending on the value of overwrite, it will either nuke the table
    or raise an error.
    """
    table_exists = check.table_existence(table_name, schema)
    if table_exists:
        if not overwrite:
            raise ValueError(
                'Table \'{schema}.{table_name}\' already exists. '.format(
                    schema=schema,
                    table_name=table_name))
        else:
            __nuke_table(table_name, schema)


def check_for_comments(table_comment, columns, col_comments):
    """
    Checks that table and column comments are all present.
    Nested column comments cannot easily be checked for, so it is up to
    the user to utilize best practices regarding them.

    Args:
        table_comment (str): Value to be used as a table comment in a new table
        columns (pd.Index or pd.Series):
            Columns of the dataframe to be uploaded to the lake
        col_comments (dict<str, str>):
            Dictionary from column name keys to column comment values
    Raises:
        TypeError:
            * If either 'table_comment' is not a string
            * If any of the comment values in 'col_comments' are not strings
        ValueError:
            * If the table comment is 0-1 characters long
            (discourages cheating)
            * If not all columns present in the dataframe to be uploaded
            exist in 'col_comments'
            * If 'col_comments' contains columns that are not present in the
            dataframe
    """
    if table_comment is None:
        raise ValueError('"table_comment" is required when working outside '
                         'the experimental zone.')
    if not isinstance(table_comment, str):
        raise TypeError('"table_comment" must be a string.')

    if not len(table_comment) > 1:
        raise ValueError(
            'A table comment is required when creating a table outside of '
            'the experimental zone.')

    if col_comments is None:
        raise ValueError('"col_comments" is required when working outside '
                         'the experimental zone.')
    cols_missing_from_comments = columns[columns.isin(col_comments.keys())]
    if not all(columns.isin(col_comments.keys())):
        raise ValueError(
            'All columns must be present in the "col_comments" dictionary '
            'with a proper comment when writing outside the experimental '
            'zone. Columns missing: ' + ', '.join(cols_missing_from_comments))

    extra_comments_in_dict = set(columns).difference(set(col_comments.keys()))

    # Removing comments for nested fields - difficult/costly to
    # deterministically check for comments given the way that complex columns
    # are represented in Python
    extra_comments_in_dict = [comment for comment in extra_comments_in_dict
                              if '.' not in comment]
    if extra_comments_in_dict:
        raise ValueError('Columns present in "col_comments" that are not '
                         'present in the DataFrame. Extra columns: ' +
                         ', '.join(extra_comments_in_dict))

    cols_w_nonstring_comments = []
    cols_wo_comment = []
    for col, comment in col_comments.items():
        if not isinstance(comment, str):
            cols_w_nonstring_comments.append(str(col))
        if not len(comment) > 1:
            cols_wo_comment.append(str(col))

    if cols_w_nonstring_comments:
        raise TypeError(
            'Column comments must be strings. Columns with incorrect comment '
            'types: ' + ', '.join(cols_w_nonstring_comments))
    if cols_wo_comment:
        raise ValueError(
            'A column comment is required for each column when creating a '
            'table outside of the experimental zone. Columns that require '
            'comments: ' + ', '.join(cols_wo_comment))


def check_for_allowed_overwrite(overwrite):
    """
    For use with the curated zone, mostly. Checks if overwriting is allowed
    based on the value of an environment variable. If overwriting is being
    attempted when it is not allowed, will raise an error
    """
    if overwrite and not os.getenv('HC_PROD_ENV'):
        raise ValueError(
            'Overwrite functionality is only available in the '
            'experimental zone. Contact a lake administrator if '
            'modification of a non-experimental table is needed.')


def get_storage_type_from_filename(filename):
    """
    Gets the filetype of a file from its filename
    """
    return os.path.splitext(filename)[-1][1:].lower()


def prep_df_and_col_defs(df, dtypes, timezones, schema,
                         storage_type):
    """
    Applies any specified dtypes to df and any special handling that certain
    data types require. Also creates a mapping from the df's pandas dtypes
    to the corresponding hive dtypes
    """
    df = dtype_mapping.special_dtype_handling(df, dtypes, timezones, schema)
    col_defs = dtype_mapping.map_pd_to_db_dtypes(df, storage_type)
    return df, col_defs


def handle_avro_filetype(df, storage_settings, tblproperties,
                         avro_schema, col_comments):
    """
    Special behavior for DataFrames to be saved in the Avro format.
    Generates the Avro schema once and uses it twice, to avoid
    needing two separate generation processes.
    """
    if avro_schema is None:
        avro_schema = pdx.schema_infer(df)
    if col_comments is not None:
        avro_col_comments = restructure_comments_for_avro(col_comments)
        avro_schema = add_comments_to_avro_schema(avro_schema,
                                                  avro_col_comments)

    # Adding the Avro schema as a string literal to the tblproperties
    tblproperties['avro.schema.literal'] = json.dumps(
        avro_schema, indent=4).replace("'", "\\\\'")

    # So pandavro doesn't have to infer the schema a second time
    storage_settings['schema'] = avro_schema

    return storage_settings, tblproperties


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


def flash_update_table_from_df(df, table_name, schema=None, dtypes=None,
                               table_comment=None, col_comments=None,
                               timezones=None, copy_df=True):
    """
    Overwrites single-file table with minimal table downtime.
    Similar to 'create_table_from_df' with overwrite=True, but only usable
    when the table only consists of one underlying file

    Args:
        df (pd.DataFrame): The DataFrame to create the table from.
        table_name (str): The name of the table to be created
        schema (str): The name of the schema to create the table in
        dtypes (dict<str:str>, optional): A dictionary specifying dtypes for
            specific columns to be cast to prior to uploading.
        table_comment (str, optional): Documentation on the table's purpose
        col_comments (dict<str:str>, optional):
            Dictionary from column name keys to column descriptions.
        timezones (dict<str, str>):
            Dictionary from datetime columns to the timezone they
            represent. If the column is timezone-naive, it will have the
            timezone added to its metadata, leaving the times themselves
            unmodified. If the column is timezone-aware and is in a different
            timezone than the one that is specified, the column's timezone
            will be converted, modifying the original times.
        copy_df (bool):
            Whether the operations performed on df should be performed on the
            original or a copy. Keep in mind that if this is set to False,
            the original df passed in will be modified as well - twice as
            memory efficient, but may be undesirable if the df is needed
            again later
    """
    # Less memory efficient, but prevents modification of original df
    if copy_df:
        df = df.copy()

    table_name, schema = meta.prep_schema_and_table(table_name, schema)

    if schema == 'curated':
        check_for_comments(table_comment, df.columns, col_comments)
        if not os.getenv('HC_PROD_ENV'):
            raise ValueError(
                'Flash update functionality is only available in '
                'the experimental zone. Contact a lake administrator if '
                'modification of a non-experimental table is needed.')

    table_exists = check.table_existence(table_name, schema)
    if not table_exists:
        raise ValueError(
            'Table {}.{} does not exist.'.format(schema, table_name)
        )

    table_metadata = meta.get_table_metadata(table_name, schema)
    bucket = table_metadata['bucket']
    path = meta.ensure_path_ends_w_slash(table_metadata['path'])

    objects_present = rv.list_objects(path, bucket)

    if len(objects_present) > 1:
        # Flash updates are supposed to feel as close to atomic as possible.
        # Multi-file operations interfere with this.
        raise ValueError(
            'Flash update functionality is only available on '
            'tables that only consist of one underlying file.')
    if meta.is_partitioned_table(table_name, schema):
        # Difficult to deterministically restore partitions based on new data
        raise ValueError(
            'Flash update functionality is not available on '
            'partitioned tables.')

    if objects_present:
        filename = objects_present[0]
    else:
        filename = meta.gen_filename_if_allowed(schema)
    path += filename

    storage_type = get_storage_type_from_filename(filename)
    df, col_defs = prep_df_and_col_defs(
        df, dtypes, timezones, schema, storage_type)

    # Gets settings to pass to river on how to write the files in a
    # Hive-readable format
    storage_settings = meta.storage_type_specs[storage_type]['settings']

    # tblproperties is for additional metadata to be provided to Hive
    # for the table. Generally, it is not needed
    tblproperties = {}

    if storage_type == 'avro':
        storage_settings, tblproperties = handle_avro_filetype(
            df, storage_settings, tblproperties, col_comments)

    full_path = '/'.join([bucket, path])
    create_table_ddl = build_create_table_ddl(table_name, schema, col_defs,
                                              col_comments, table_comment,
                                              storage_type,
                                              partitioned_by=None,
                                              full_path=full_path,
                                              tblproperties=tblproperties)
    inform(create_table_ddl)
    drop_table_stmt = 'DROP TABLE IF EXISTS {}.{}'.format(schema, table_name)

    # Creating the table doesn't populate it with data. We now need to write
    # the DataFrame to a file and upload it to S3
    _ = rv.write(df, path, bucket, show_progressbar=False, **storage_settings)
    hive.run_lake_query(drop_table_stmt, engine='hive')
    hive.run_lake_query(create_table_ddl, engine='hive')
