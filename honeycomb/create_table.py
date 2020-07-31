import os
import subprocess

import river as rv

from honeycomb import check, meta, run_query as run
from honeycomb.dtype_mapping import apply_spec_dtypes, map_pd_to_db_dtypes


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


def build_create_table_ddl(schema, table_name, col_defs,
                           table_comment, storage_type, full_path):
    create_table_ddl = """
    CREATE EXTERNAL TABLE {schema}.{table_name} (
    {columns_and_types}
    )
    {table_comment}
    {storage_format_ddl}
    LOCATION 's3://{full_path}'
    """.format(
        schema=schema,
        table_name=table_name,
        # BUG: pd.Series truncates long strings output by to_string,
        # have to cast to DataFrame first.
        columns_and_types=col_defs.to_frame().to_string(
            header=False).replace('\n', ',\n'),
        table_comment=('COMMENT \'{table_comment}\''.format(
            table_comment=table_comment)) if table_comment else '',
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
        raise ValueError('A table comment is required when creating a table '
                         'outside of the experimental zone.')

    cols_missing_from_comments = columns[columns.isin(col_comments.keys())]
    if not all(columns.isin(col_comments.keys())):
        raise ValueError('All columns must be present in the "col_comments" '
                         'dictionary with a proper comment when writing '
                         'outside the experimental zone. Columns missing: ' +
                         ', '.join(cols_missing_from_comments))

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
        raise TypeError('Column comments must be strings. Columns with '
                        'incorrect comment types: ' +
                        ', '.join(cols_w_nonstring_comments))
    if cols_wo_comment:
        raise ValueError('A column comment is required for each column when '
                         'creating a table outside of the experimental zone. '
                         'Columns that require comments: ' +
                         ', '.join(cols_wo_comment))


def create_table_from_df(df, table_name, schema=None,
                         dtypes=None, path=None, filename=None,
                         table_comment=None, col_comments=None,
                         overwrite=False):
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
    """
    table_name, schema = meta.prep_schema_and_table(table_name, schema)

    if schema != 'experimental':
        check_for_comments(table_comment, df.columns, col_comments)
        if overwrite:
            raise ValueError('Overwrite functionality is only available in '
                             'the experimental zone. Contact a lake '
                             'administrator if modification of a non-'
                             'experimental table is needed.')

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
        raise KeyError(('Files are already present in s3://{}/{}. '
                        'Creation of a new table requires a dedicated, '
                        'empty folder.'
                        'If this is desired, set "overwrite" to True. '
                        'Otherwise, specify a different filename.').format(
                            bucket, path)
                       )

    path += filename

    if not overwrite and rv.exists(path, bucket):
        raise KeyError('A file already exists at s3://' + bucket + path + ', '
                       'Which will be overwritten by this operation. '
                       'Specify a different filename.')

    if dtypes is not None:
        df = apply_spec_dtypes(df, dtypes)
    col_defs = map_pd_to_db_dtypes(df)
    if col_comments is not None:
        col_defs = add_comments_to_col_defs(col_defs, col_comments)
    storage_type = os.path.splitext(filename)[-1][1:].lower()
    storage_settings = meta.storage_type_specs[storage_type]['settings']
    full_path = '/'.join([bucket, path])

    create_table_ddl = build_create_table_ddl(schema, table_name,
                                              col_defs, table_comment,
                                              storage_type, full_path)
    print(create_table_ddl)

    run.lake_query(create_table_ddl, engine='hive')
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
    run.lake_query('DROP TABLE IF EXISTS {}.{}'.format(
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
