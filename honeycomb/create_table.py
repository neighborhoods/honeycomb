from tempfile import NamedTemporaryFile
import boto3

from honeycomb.config import dtype_map
from honeycomb import run_query
from pyhive import hive, presto

# TODO logging instead of print
# TODO table/column comments
valid_schemas = [
    'landing',
    'staging',
    'experimental',
    'curated'
]

schema_to_zone_bucket_map = {
    'landing': 'nhds-data-lake-landing-zone',
    'staging': 'nhds-data-lake-staging-zone',
    'experimental': 'nhds-data-lake-experimental-zone',
    'curated': 'nhds-data-lake-curated-zone'
}


def get_db_connection(engine='hive'):
    if engine == 'hive':
        conn = hive.connect('localhost').cursor()
    elif engine == 'presto':
        conn = presto.connect('localhost').cursor()
    # elif engine == 'gbq':
    else:
        raise ValueError('Specified engine is not supported: ' + engine)
    return conn


def check_table_existence(schema_name, table_name):
    show_tables_query = (
        'SHOW TABLES IN {schema_name} LIKE \'{table_name}\''.format(
            schema_name=schema_name,
            table_name=table_name)
    )

    similar_tables = run_query.run_query(show_tables_query, engine='hive')
    if table_name in similar_tables['tab_name'].values:
        return True
    return False


def apply_spec_dtypes(df, spec_dtypes):
    new_dtypes = df.dtypes
    for col_name, new_dtype in spec_dtypes:
        if col_name not in new_dtypes.keys():
            print('Additional dtype casting for failed: '
                  '{col_name} not in DataFrame.'.format(col_name=col_name))
        else:
            new_dtypes[col_name] = new_dtype

    try:
        df = df.astype(new_dtypes)
        return df
    except ValueError:
        print('Casting to default or specified dtypes failed.')


def map_pd_to_db_dtypes(df):
    db_dtypes = df.dtypes

    for orig_type, new_type in dtype_map.items():
        # dtypes can be compared to their string representations for equality
        db_dtypes[db_dtypes == orig_type] = new_type

    return db_dtypes


# TODO add s3_folder?
def upload_df(df, s3_filename, s3_bucket='nhds-data-lake-experimental-zone'):
    s3 = boto3.resource('s3')

    with NamedTemporaryFile() as temp:
        df.to_csv(temp.name, index=False, header=False)
        s3.meta.client.upload_file(temp.name, s3_bucket, s3_filename)

    return 's3://' + '/'.join([s3_bucket, s3_filename])


# TODO add presto support?
def create_table_from_df(df, table_name, schema_name='experimental',
                         dtypes=None, file_name=None, conn=None):
    if conn is None:
        conn = get_db_connection(engine='hive')

    table_exists = check_table_existence(schema_name, table_name)
    if table_exists:
        raise ValueError("Table {schema_name}.{table_name} already exists. "
                         "Cancelling...".format(
                             schema_name=schema_name,
                             table_name=table_name))
    if dtypes is not None:
        df = apply_spec_dtypes(df, dtypes)
    db_dtypes = map_pd_to_db_dtypes(df)

    # TODO replace with s3 tool
    s3_bucket = schema_to_zone_bucket_map[schema_name]
    s3_path = upload_df(df, file_name, s3_bucket=s3_bucket)

    create_statement = """
    CREATE EXTERNAL TABLE {schema_name}.{table_name} (
    {columns_and_types}
    )
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\\n'
    LOCATION '{s3_path}'
    """.format(
        schema_name=schema_name,
        table_name=table_name,
        columns_and_types=db_dtypes.to_string().replace('\n', ',\n'),
        s3_path=s3_path.rsplit('/', 1)[0] + '/'
    )
    print(create_statement)
    conn.execute(create_statement)
