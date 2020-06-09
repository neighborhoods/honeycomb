import os

import river as rv

from honeycomb import check, meta
from honeycomb.config import storage_type_specs
from honeycomb.dtype_mapping import apply_spec_dtypes, map_pd_to_db_dtypes
from honeycomb.run_query import run_query


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


def build_create_table_ddl(schema_name, table_name, col_defs,
                           table_comment, storage_type, full_path):
    create_table_ddl = """
    CREATE EXTERNAL TABLE {schema_name}.{table_name} (
    {columns_and_types}
    )
    {table_comment}
    {storage_format_ddl}
    LOCATION 's3://{full_path}'
    """.format(
        schema_name=schema_name,
        table_name=table_name,
        columns_and_types=col_defs.to_string(
            header=False).replace('\n', ',\n'),
        table_comment=('COMMENT \'{table_comment}\''.format(
            table_comment=table_comment)) if table_comment else '',
        storage_format_ddl=storage_type_specs[storage_type]['ddl'],
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


# TODO add presto support?
def create_table_from_df(df, table_name, schema_name='experimental',
                         dtypes=None, path=None, filename=None,
                         table_comment=None, col_comments=None):
    """
    Uploads a dataframe to S3 and establishes it as a new table in Hive.

    Args:
        df (pd.DataFrame): The DataFrame to create the tabale from.
        table_name (str): The name of the table to be created
        schema_name (str): The name of the schema to create the table in
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
    if schema_name != 'experimental':
        check_for_comments(table_comment, df.columns, col_comments)

    if path is None:
        path = table_name
    if filename is None:
        filename = meta.gen_filename_if_allowed(schema_name)

    if not path.endswith('/'):
        path += '/'
    path += filename

    table_exists = check.table_existence(
        schema_name, table_name, engine='hive')
    if table_exists:
        raise ValueError(
            'Table \'{schema_name}.{table_name}\' already exists. '.format(
                schema_name=schema_name,
                table_name=table_name))

    if dtypes is not None:
        df = apply_spec_dtypes(df, dtypes)
    col_defs = map_pd_to_db_dtypes(df)
    if col_comments is not None:
        col_defs = add_comments_to_col_defs(col_defs, col_comments)

    s3_bucket = schema_to_zone_bucket_map[schema_name]

    storage_type = os.path.splitext(filename)[-1][1:].lower()
    storage_settings = storage_type_specs[storage_type]['settings']
    full_path = rv.write(df, path, s3_bucket, **storage_settings)

    create_table_ddl = build_create_table_ddl(schema_name, table_name,
                                              col_defs, table_comment,
                                              storage_type, full_path)
    print(create_table_ddl)
    run_query(create_table_ddl, engine='hive')
