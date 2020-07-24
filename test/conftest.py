import os
from tempfile import NamedTemporaryFile

import boto3
from moto import mock_s3
import numpy as np
import pandas as pd
import pytest


@pytest.fixture(autouse=True, scope='session')
def aws_credentials():
    """
    Sets AWS credentials to invalid values. Applied to all test functions and
    scoped to the entire testing session, so there's no chance of interfering
    with production buckets.
    """
    os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
    os.environ['AWS_SECURITY_TOKEN'] = 'testing'
    os.environ['AWS_SESSION_TOKEN'] = 'testing'


@pytest.fixture
def test_bucket():
    """Universal bucket name for use throughout testing"""
    return 'test_bucket'


@pytest.fixture
def test_schema():
    """Universal schema name for use throughout testing"""
    return 'test_schema'


@pytest.fixture
def test_df_key():
    """Universal S3 key for a DataFrame used throughout testing"""
    return 'test_df.csv'


@pytest.fixture
def test_df():
    """
    Dataframe for use throughout testing. Multiple data types
    used to test for proper encoding/decoding.
    """
    return pd.DataFrame({
        'intcol': [1, 2, 3],
        'strcol': ['four', 'five', 'six'],
        'floatcol': [7.0, 8.0, 9.0]
    })


@pytest.fixture
def test_df_all_types():
    """
    Dataframe for use throughout testing. Contains all data types
    that are compatible with hive that will also be supported by honeycomb.
    """
    return pd.DataFrame({
        'intcol': [1, 2],
        'strcol': ['three', 'four'],
        'floatcol': [5.0, 6.0],
        'boolcol': [True, False],
        'datetimecol': [
            np.datetime64('2020-01-01'), np.datetime64('2020-01-02')],
    })


@pytest.fixture
def mock_s3_client():
    """Mocks all s3 connections in any test or fixture that includes it"""
    with mock_s3():
        yield


@pytest.fixture
def setup_bucket_wo_contents(mock_s3_client, test_bucket):
    """Sets up a bucket with no contents."""
    s3 = boto3.client('s3')
    s3.create_bucket(Bucket=test_bucket)

    yield


@pytest.fixture
def setup_bucket_w_contents(mock_s3_client, test_bucket, test_df_key, test_df):
    """Sets up a bucket with no contents."""
    s3 = boto3.client('s3')
    s3.create_bucket(Bucket=test_bucket)

    with NamedTemporaryFile() as tmpfile:
        test_df.to_csv(tmpfile.name, index=False, header=False)
        s3.upload_file(tmpfile.name, test_bucket, test_df_key)

    yield
