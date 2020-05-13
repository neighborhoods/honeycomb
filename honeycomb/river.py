import boto3
import os
import pandas as pd
from tempfile import NamedTemporaryFile


def store(obj, filename, folder='',
          bucket='nhds-data-lake-experimental-zone'):
    """
    Uploads an object to S3, in a storage format decided by its file extension

    Args:
        obj (object): The object to be uploaded to S3
        filename (str): The filename to save 'obj' as
        folder (str, optional): The folder/prefix to save 'obj' under
        bucket (str, optional): The S3 bucket to save 'obj' in
    Returns:
        str: The full path to the object in S3, without the 's3://' prefix
    """
    filetype = os.path.splitext(filename)[-1][1:].lower()
    if filetype not in storage_fns.keys():
        raise ValueError('Storage type \'{storage_type}\' not supported.')
    storage_fn = storage_fns[filetype]

    if folder and not folder.endswith('/'):
        folder += '/'

    s3_path = folder + filename
    storage_fn(obj, s3_path, bucket)

    return '/'.join([bucket, s3_path])


def _upload_csv(obj, s3_path, bucket):
    """
    Saves a DataFrame to a CSV and uploads it to S3

    Args:
        obj (pd.DataFrame): The DataFrame to be uploaded
        s3_path (str): The full path (other than bucket) to the object in S3
        bucket (str): The S3 bucket to save 'obj' in

    Raises:
        TypeError: if 'obj' is not a DataFrame
    """
    if not isinstance(obj, pd.DataFrame):
        raise TypeError('Storage format of \'csv\' can only be used with '
                        'DataFrames.')

    s3 = boto3.resource('s3')
    with NamedTemporaryFile(dir='/tmp') as temp:
        obj.to_csv(temp.name, index=False, header=False)
        s3.meta.client.upload_file(temp.name, bucket, s3_path)


storage_fns = {
   'csv': _upload_csv
}
