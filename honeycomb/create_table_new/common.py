import json
import os

import pandavro as pdx

import river as rv

from honeycomb import check, dtype_mapping, hive, meta
from honeycomb.alter_table import add_partition
from honeycomb.ddl_building import (build_create_table_ddl,
                                    restructure_comments_for_avro,
                                    add_comments_to_avro_schema)
from honeycomb.inform import inform
from honeycomb.__danger import __nuke_table


schema_to_zone_bucket_map = {
    'landing': os.getenv('HC_LANDING_ZONE_BUCKET'),
    'staging': os.getenv('HC_STAGING_ZONE_BUCKET'),
    'experimental': os.getenv('HC_EXPERIMENTAL_ZONE_BUCKET'),
    'curated': os.getenv('HC_CURATED_ZONE_BUCKET')
}


def create_table_and_write(df, table_name, schema, col_defs,
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
