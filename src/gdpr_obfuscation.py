from urllib.parse import urlparse
from botocore.exceptions import ClientError
from io import StringIO
import boto3, csv, json


def gdpr_obfuscator(JSON_str:str):
    """
    Retrieve data ingested to AWS S3 and
    intercept personally identifiable information (PII).
    Return a byte stream object containing an exact 
    copy of the input file but with the sensitive
    data replaced with obfuscated strings.

    :param: JSON (string) containing:
        the S3 location of the required CSV file for obfuscation
        the names of the fields that are required to be obfuscated
    :return: bytestream representation of a file with obfuscated data fields
    """
    py_dict = json.loads(JSON_str)
    bucket, key = get_bucket_and_key(py_dict['file_to_obfuscate'])
    data_type = get_data_type(key)
    s3 = boto3.client('s3')
    data:bytes = get_data(s3, bucket, key)
    if data_type == 'csv':
        return obfuscate_csv(data.decode(), py_dict['pii_fields']).encode()


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
    
    :param: key s3 object key
    :raise: UnsupportedData Exeption 
    :return: (string) data type -> 'str'
    """
    try:
        allowed_types = ['csv', 'json', 'parquet']
        data_type = key.split('.')[-1]
        if data_type not in allowed_types:
            raise UnsupportedData(f'Function supports only {", ".join(allowed_types)} types.')
        return data_type
    except UnsupportedData:
        # Confirm expected required behaviour
        raise

def get_data(client, bucket, key):
    """
    Retrieve data from s3

    :param: client s3 boto client 
    :param: bucket (string) s3 bucket name
    :param: key (string) s3 data key 
    :return: bytestream representation of a data
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
        if error.response['Error']['Code'] == 'NoSuchBucket':
            # Confirm expected required behaviour
            pass
        if error.response['Error']['Code'] == 'InvalidObjectState':
            #Object is archived and inaccessible until restored.
            # Confirm expected required behaviour
            pass
        raise

def obfuscate_csv(data:str, pii_fields:list):
    """
    Pure function that mask pii_fields in data
    Behaviour: 
        :Will return empty str if data is empty
        :If pii_fields contain fields different than data headers
            function Will update data header to represent this

    :param: data (string) representation of csv data
    :param: pii_fields (list) of the names of the fields that to be obfuscated
    :return: string representation of csv file with pii masked
    """
    dict_reader = csv.DictReader(StringIO(data))
    masked = []
    for row in dict_reader:
        for field in pii_fields:
            row[field] = '***'
        masked.append(row)
    try:
        headers = list(masked[0].keys())
    except IndexError:
        headers = []
    
    masked_bufer = StringIO()
    writer = csv.DictWriter(masked_bufer, headers)
    writer.writeheader()
    writer.writerows(masked)
    return masked_bufer.getvalue()