import os

import boto3


def get_ssm_secret(key):
    """
    Gets a secret under the name 'key' from the AWS parameter store
    """
    client = boto3.client('ssm', os.getenv('AWS_DEFAULT_REGION'))
    resp = client.get_parameter(Name=key, WithDecryption=True)
    return resp['Parameter']['Value']
