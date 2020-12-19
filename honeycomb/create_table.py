from collections import OrderedDict
import os
import pprint
import re
import sys

import pandavro as pdx
import river as rv

from honeycomb import check, dtype_mapping, hive, meta
from honeycomb.alter_table import add_partition
from honeycomb.__danger import __nuke_table


schema_to_zone_bucket_map = {
    'landing': 'nhds-data-lake-landing-zone',
    'staging': 'nhds-data-lake-staging-zone',
    'experimental': 'nhds-data-lake-experimental-zone',
    'curated': 'nhds-data-lake-curated-zone'
}


def add_comments_to_col_defs(col_defs, comments):
    for column, comment in comments.items():
        col_defs.loc[col_defs['col_name'] == column, 'comment'] = comment

    col_defs['comment'] = (
        ' COMMENT \'' + col_defs['comment'].astype(str) + '\'')
    return col_defs


def add_nested_col_comments(columns_and_types, nested_col_comments):
    print(columns_and_types)
    for col, comment in nested_col_comments.items():
        print(col)
        total_nesting_levels = col.count('.')
        current_nesting_level = 0
        block_start = 0
        block_end = -1
        columns_and_types = scan_ddl_level(col, comment, columns_and_types,
                                           block_start, block_end,
                                           current_nesting_level,
                                           total_nesting_levels)
    return columns_and_types


def scan_ddl_level(col, comment, columns_and_types,
                   block_start, block_end,
                   current_nesting_level, total_nesting_levels,):
    found = False
    while not found:
        col_at_level = col.split('.')[current_nesting_level]
        block_end = columns_and_types.find('<', block_start, block_end)
        next_block_start = find_matching_bracket(
            columns_and_types, block_end) + 1

        # TODO replace with regex
        if col_at_level in columns_and_types[block_start:block_end]:
            found = True

            if current_nesting_level == total_nesting_levels:
                print('hallo')
                col_loc = columns_and_types.find(col_at_level)
                def_end_idx = min(idx for idx in
                                  [
                                      columns_and_types[col_loc:].find(','),
                                      columns_and_types[col_loc:].find('>'),
                                      columns_and_types[col_loc:].find('\n')]
                                  if idx >= 0
                                  )
                col_def_end = col_loc + def_end_idx

                print(col_loc)
                print(col_def_end)
                print(columns_and_types[col_loc:col_def_end])
                columns_and_types = (columns_and_types[:col_def_end] +
                                     ' COMMENT \'{}\''.format(comment) +
                                     columns_and_types[col_def_end:])
                print(columns_and_types)
                return columns_and_types
            else:
                # TODO account for arrays of struct
                current_nesting_level += 1
                # Searching between the brackets that follow the just found col
                return scan_ddl_level(
                    col, comment, columns_and_types,
                    block_start=block_end + 1,
                    block_end=next_block_start - 1,
                    current_nesting_level=current_nesting_level,
                    total_nesting_levels=total_nesting_levels
                )
        else:
            if next_block_start != 0:
                block_start = next_block_start
                block_end = -1
            else:
                raise ValueError(
                    'Sub-field {} not found in definition for {}'.format(
                        col_at_level,
                        col
                    ))

    # col_types = describe_table(table_name, schema).set_index('col_name')
    # for col_name, comment in nested_col_comments.items():
    #     nesting_layers = col_name.count('.')
    #     top_level_field = col_name.split('.')[0]
    #     col_type = col_types.loc[top_level_field, 'data_type']
    #
    #     array_of_struct = 'array<struct<'
    #     array = 'array<'
    #     struct = 'struct<'
    #     for i in range(0, nesting_layers):
    #         if isinstance(col_type, str):
    #             if col_type.startswith(array_of_struct):
    #                 col_type = col_type[len(array_of_struct):-2]
    #                 col_type = col_type_str_to_dict(col_type)
    #             elif col_type.startswith(array):
    #                 col_type = col_type[len(array):-1]
    #             elif col_type.startswith(struct):
    #                 col_type = col_type[len(struct):-1]
    #         if isinstance(col_type, dict):
    #             current_level = col_name.split('.')[i + 1]
    #             col_type = col_type[current_level]
    #
    # #     add_comment_command = (
    #         'ALTER TABLE {}.{} CHANGE `{}` `{}` {} COMMENT \'{}\''.format(
    #             schema,
    #             table_name,
    #             col_name,
    #             col_name,
    #             col_type,
    #             comment
    #         ))
    #     print(add_comment_command)
    #     hive.run_lake_query(add_comment_command, engine='hive')


def find_matching_bracket(columns_and_types, start_ind):
    bracket_count = 0
    for i, c in enumerate(columns_and_types[start_ind:]):
        if c == '<':
            bracket_count += 1
        elif c == '>':
            bracket_count -= 1
        if bracket_count == 0:
            return i + start_ind


def col_type_str_to_dict(col_type):
    return {field.split(':')[0]: field.split(':')[1]
            for field in col_type.split(',')}


def build_create_table_ddl(table_name, schema, col_defs,
                           col_comments, table_comment, storage_type,
                           partitioned_by, full_path,
                           tblproperties=None):
    nested_col_comments = {key: value for key, value in col_comments.items()
                           if '.' in key}
    col_comments = {key: value for key, value in col_comments.items()
                    if '.' not in key}
    if col_comments is not None:
        col_defs = add_comments_to_col_defs(col_defs, col_comments)
    columns_and_types = col_defs.to_string(header=False, index=False)

    # Removing excess whitespace left by df.to_string()
    columns_and_types = re.sub(
        r' +',
        ' ',
        columns_and_types
    )

    columns_and_types = columns_and_types.replace('\n', ',\n    ')

    if nested_col_comments:
        columns_and_types = add_nested_col_comments(
            columns_and_types, nested_col_comments)

    create_table_ddl = """
CREATE EXTERNAL TABLE {schema}.{table_name} (
    {columns_and_types}
){table_comment}{partitioned_by}
{storage_format_ddl}
LOCATION 's3://{full_path}'{tblproperties}
    """.format(
        schema=schema,
        table_name=table_name,
        # BUG: pd.Series truncates long strings output by to_string,
        # have to cast to DataFrame first.
        columns_and_types=columns_and_types,
        table_comment=('\nCOMMENT \'{table_comment}\''.format(
            table_comment=table_comment)) if table_comment else '',
        partitioned_by=('\nPARTITIONED BY ({})'.format(', '.join(
            ['{} {}'.format(partition_name, partition_type)
             for partition_name, partition_type in partitioned_by.items()]))
            if partitioned_by else ''),
        storage_format_ddl=meta.storage_type_specs[storage_type]['ddl'],
        full_path=full_path.rsplit('/', 1)[0] + '/',
        tblproperties=('\nTBLPROPERTIES (\n  {}\n)'.format('\n  '.join([
            '\'{}\'=\'{}\''.format(prop_name, prop_val)
            for prop_name, prop_val in tblproperties.items()]))
            if tblproperties else '')
    )

    return create_table_ddl


def check_for_comments(table_comment, columns, col_comments):
    """
    Checks that table and column comments are all present.
    Args:
        table_comment (str): Value to be used as a table comment in a new table
        columns (pd.Index): Columns of the dataframe to be uploaded to the lake
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
    if not isinstance(table_comment, str):
        raise TypeError('"table_comment" must be a string.')

    if not len(table_comment) > 1:
        raise ValueError(
            'A table comment is required when creating a table outside of '
            'the experimental zone.')

    cols_missing_from_comments = columns[columns.isin(col_comments.keys())]
    if not all(columns.isin(col_comments.keys())):
        raise ValueError(
            'All columns must be present in the "col_comments" dictionary '
            'with a proper comment when writing outside the experimental '
            'zone. Columns missing: ' + ', '.join(cols_missing_from_comments))

    extra_comments_in_dict = set(columns).difference(set(col_comments.keys()))
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


def confirm_ordered_dicts():
    """
    Checks if the Python version is at least 3.6, determining whether
    dictionaries are ordered.
    """
    python_version = sys.version_info
    if python_version.major >= 3:
        if python_version.minor >= 6:
            if python_version.minor == 6:
                print(
                    'You are using Python 3.6. Dictionaries are ordered in '
                    '3.6, but only as a side effect. It is recommended to '
                    'upgrade to 3.7 to have guaranteeably ordered dicts.')
            return True
    return False


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

    if schema != 'experimental':
        check_for_comments(table_comment, df.columns, col_comments)
        if overwrite and not os.getenv('HC_PROD_ENV'):
            raise ValueError(
                'Overwrite functionality is only available in the '
                'experimental zone. Contact a lake administrator if '
                'modification of a non-experimental table is needed.')

    table_exists = check.table_existence(table_name, schema)
    if table_exists:
        if not overwrite:
            raise ValueError(
                'Table \'{schema}.{table_name}\' already exists. '.format(
                    schema=schema,
                    table_name=table_name))
        else:
            __nuke_table(table_name, schema)

    if path is None:
        path = table_name
    if filename is None:
        filename = meta.gen_filename_if_allowed(schema)
    if not path.endswith('/'):
        path += '/'

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

    storage_settings = meta.storage_type_specs[storage_type]['settings']

    tblproperties = {}
    if storage_type == 'avro':
        storage_settings, tblproperties = handle_avro_filetype(
            df, storage_settings, tblproperties, avro_schema)

    full_path = '/'.join([bucket, path])
    create_table_ddl = build_create_table_ddl(table_name, schema,
                                              col_defs,
                                              col_comments, table_comment,
                                              storage_type, partitioned_by,
                                              full_path, tblproperties)
    print(create_table_ddl)
    hive.run_lake_query(create_table_ddl, engine='hive')

    if partitioned_by:
        path += add_partition(table_name, schema, partition_values)
    path += filename

    if auto_upload_df:
        _ = rv.write(df, path, bucket,
                     show_progressbar=False, **storage_settings)


def get_storage_type_from_filename(filename):
    return os.path.splitext(filename)[-1][1:].lower()


def prep_df_and_col_defs(df, dtypes, timezones, schema,
                         storage_type):
    df = dtype_mapping.special_dtype_handling(df, dtypes, timezones, schema)
    col_defs = dtype_mapping.map_pd_to_db_dtypes(df, storage_type)
    return df, col_defs


def handle_avro_filetype(df, storage_settings, tblproperties, avro_schema):
    if avro_schema is None:
        avro_schema = pdx.schema_infer(df)
    tblproperties['avro.schema.literal'] = pprint.pformat(
        avro_schema).replace('\'', '"')
    # So pandavro doesn't have to infer the schema a second time
    storage_settings['schema'] = avro_schema

    return storage_settings, tblproperties


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
    if copy_df:
        df = df.copy()

    table_name, schema = meta.prep_schema_and_table(table_name, schema)

    if schema != 'experimental':
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
    path = table_metadata['path']
    if not path.endswith('/'):
        path += '/'

    objects_present = rv.list_objects(path, bucket)

    if len(objects_present) > 1:
        raise ValueError(
            'Flash update functionality is only available on '
            'tables that only consist of one underlying file.')
    if meta.is_partitioned_table(table_name, schema):
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
        df, dtypes, timezones, schema, storage_type, col_comments)

    storage_settings = meta.storage_type_specs[storage_type]['settings']

    tblproperties = {}
    if storage_type == 'avro':
        storage_settings, tblproperties = handle_avro_filetype(
            df, storage_settings, tblproperties)

    full_path = '/'.join([bucket, path])
    create_table_ddl = build_create_table_ddl(table_name, schema,
                                              col_defs, table_comment,
                                              storage_type,
                                              partitioned_by=None,
                                              full_path=full_path,
                                              tblproperties=tblproperties)
    print(create_table_ddl)
    drop_table_stmt = 'DROP TABLE IF EXISTS {}.{}'.format(schema, table_name)

    _ = rv.write(df, path, bucket, show_progressbar=False, **storage_settings)
    hive.run_lake_query(drop_table_stmt, engine='hive')
    hive.run_lake_query(create_table_ddl, engine='hive')
