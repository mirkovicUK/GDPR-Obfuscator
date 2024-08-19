from urllib.parse import urlparse
from botocore.exceptions import ClientError
from io import StringIO
import boto3, csv


def gdpr_obfuscator(file_path:str, pii_fields:list):
    """
    Retrieve data ingested to AWS S3 and
    intercept personally identifiable information (PII).
    Return a byte stream object containing an exact 
    copy of the input file but with the sensitive
    data replaced with obfuscated strings.

    :param: file_path (str) S3 location of the data file for obfuscation
    :param: pii_fields list of the names of the fields that are 
        required to be obfuscated

    :return: TBC <-----
    """
    bucket, key = get_bucket_and_key(file_path)
    data_type = get_data_type(key)
    s3 = boto3.client('s3')
    
    
    if data_type == 'csv':
         pass


def get_bucket_and_key(s3_file_path:str):
    """
    Extract S3 bucket name and key of the object from s3 file path
    expect file path like 's3://my_backet/some_folder/file.txt
    
    :param: s3_file_path (str) path to data on AWS s3
    :retunr: bucket , key (str) 
    """
    
    o = urlparse(s3_file_path, allow_fragments=False)
    return o.netloc, o.path.lstrip('/')

class UnsupportedData(Exception):
        """Traps error where data is not supported"""
        pass

def get_data_type(key):
    """
    Extract data type from s3 object key
    Valid data type: csv, json, parquet
    
    :param: s3 object key
    :raise: UnsupportedData Exeption 
    :return: (string) data type
    """
    try:
        allowed_types = ['csv', 'json', 'parquet']
        data_type = key.split('.')[-1]
        if data_type not in allowed_types:
            raise UnsupportedData(f'Function supports only {", ".join(allowed_types)} types.')
        return data_type
    except:
        # Confirm expected required behaviour
        raise

def get_data(client, bucket, key):
    """
    Retrieve data from s3

    :param: client s3 boto client 
    :param: bucket (string) s3 bucket name
    :param: key (string) s3 data key 
    :return: bytestream representation of a file
    """
    try:
        response = client.get_object(
        Bucket = bucket,
        Key = key)
        return response['Body'].read()
    except ClientError as error:
        if error.response['Error']['Code'] == 'NoSuchKey':
            # Confirm expected required behaviour
            pass
        if error.response['Error']['Code'] == 'InvalidObjectState':
            #Object is archived and inaccessible until restored.
            # Confirm expected required behaviour
            pass
        raise