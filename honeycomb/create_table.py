from collections import OrderedDict
import os
import subprocess
import sys

import river as rv

from honeycomb import check, dtype_mapping, hive, meta
from honeycomb.alter_table import add_partition


schema_to_zone_bucket_map = {
    'landing': 'nhds-data-lake-landing-zone',
    'staging': 'nhds-data-lake-staging-zone',
    'experimental': 'nhds-data-lake-experimental-zone',
    'curated': 'nhds-data-lake-curated-zone'
}


def add_comments_to_col_defs(col_defs, comments):
    col_defs = (
        col_defs
        .to_frame(name='dtype')
    )

    for column, comment in comments.items():
        col_defs.loc[col_defs.index == column, 'comment'] = comment

    col_defs['comment'] = (
        ' COMMENT \'' + col_defs['comment'].astype(str) + '\'')
    return col_defs


def build_create_table_ddl(table_name, schema, col_defs,
                           table_comment, storage_type,
                           partitioned_by, full_path):
    create_table_ddl = """
CREATE EXTERNAL TABLE {schema}.{table_name} (
    {columns_and_types}
){table_comment}{partitioned_by}
{storage_format_ddl}
LOCATION 's3://{full_path}'
    """.format(
        schema=schema,
        table_name=table_name,
        # BUG: pd.Series truncates long strings output by to_string,
        # have to cast to DataFrame first.
        columns_and_types=col_defs.to_frame().to_string(
            header=False).replace('\n', ',\n    '),
        table_comment=('\nCOMMENT \'{table_comment}\''.format(
            table_comment=table_comment)) if table_comment else '',
        partitioned_by=('\nPARTITIONED BY ({})'.format(', '.join(
            ['{} {}'.format(partition_name, partition_type)
             for partition_name, partition_type in partitioned_by.items()]))
            if partitioned_by else ''),
        storage_format_ddl=meta.storage_type_specs[storage_type]['ddl'],
        full_path=full_path.rsplit('/', 1)[0] + '/'
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
                         overwrite=False, auto_upload_df=True):
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
            List of tuples containing a partition name and type. Cannot be a
            vanilla dictionary if using Python version < 3.6
        partition_values (dict<str:str>):
            Required if 'partitioned_by' is used and 'auto_upload_df' is True.
            List of tuples containing partition name and value to store
            the dataframe under
        overwrite (bool, default False):
            Whether to overwrite the current table if one is already present
            at the specified name
        auto_upload_df (bool, optional):
            Whether the df that the table's structure will be based off of
            should be automatically uploaded to the table
    """
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
        if overwrite:
            raise ValueError(
                'Overwrite functionality is only available in the '
                'experimental zone. Contact a lake administrator if '
                'modification of a non-experimental table is needed.')

    if path is None:
        path = table_name
    if filename is None:
        filename = meta.gen_filename_if_allowed(schema)
    if not path.endswith('/'):
        path += '/'

    bucket = schema_to_zone_bucket_map[schema]

    table_exists = check.table_existence(table_name, schema)
    if table_exists:
        if not overwrite:
            raise ValueError(
                'Table \'{schema}.{table_name}\' already exists. '.format(
                    schema=schema,
                    table_name=table_name))
        else:
            __nuke_table(table_name, schema)

    if rv.list_objects(path, bucket):
        raise KeyError((
            'Files are already present in s3://{}/{}. Creation of a new table '
            'requires a dedicated, empty folder. Either specify a different '
            'path for the table or ensure the directory is empty before '
            'attempting table creation.').format(bucket, path))

    storage_type = os.path.splitext(filename)[-1][1:].lower()
    df = dtype_mapping.special_dtype_handling(
        df, dtypes, timezones, schema, copy_df)
    col_defs = dtype_mapping.map_pd_to_db_dtypes(df, storage_type)
    if col_comments is not None:
        col_defs = add_comments_to_col_defs(col_defs, col_comments)

    storage_settings = meta.storage_type_specs[storage_type]['settings']
    full_path = '/'.join([bucket, path])

    create_table_ddl = build_create_table_ddl(table_name, schema,
                                              col_defs, table_comment,
                                              storage_type, partitioned_by,
                                              full_path)
    print(create_table_ddl)
    hive.run_lake_query(create_table_ddl, engine='hive')

    if partitioned_by:
        path += add_partition(table_name, schema, partition_values)
    path += filename

    if auto_upload_df:
        _ = rv.write(df, path, bucket, **storage_settings)


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
        table_name
    ))
    rm_command = 'aws s3 rm --recursive s3://{}/{}'.format(
        current_bucket,
        current_path
    )
    print(rm_command)
    cp_process = subprocess.Popen(rm_command.split(),
                                  stdout=subprocess.PIPE)
    output, error = cp_process.communicate()
