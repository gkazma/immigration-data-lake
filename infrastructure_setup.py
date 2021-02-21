import pandas as pd
import boto3
import json
import configparser
import time
import logging
from botocore.exceptions import ClientError

def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True

config = configparser.ConfigParser()
config.read_file(open('creds.cfg'))

# AWS
KEY                    = config.get('AWS','KEY')
SECRET                 = config.get('AWS','SECRET')

s3_bucket = 'uda-data-eng'

s3 = boto3.resource('s3')
#s3.create_bucket(Bucket=s3_bucket)

# upload data to bucket
upload_file(file_name='data/i94_apr16_sub.csv', bucket=s3_bucket)
upload_file(file_name='data/code_to_country_mapping.csv', bucket=s3_bucket)
upload_file(file_name='data/us-cities-demographics.csv', bucket=s3_bucket)
upload_file(file_name='data/GlobalLandTemperaturesByState.csv', bucket=s3_bucket)
upload_file(file_name='data/airport-codes_csv.csv', bucket=s3_bucket)

# upload etl script to bucket
upload_file(file_name='scripts/spark_etl.py', bucket=s3_bucket)
