from urllib.parse import urlparse
import csv
import boto3

def gdpr_obfuscator(file_to_obfuscate:str, pii_fields:list):
    """
    some dot str here
    """
    o = urlparse(file_to_obfuscate)
    bucket, key = 's3://' + o.netloc, o.path
    new_string = bucket + key
    print(new_string == file_to_obfuscate)
    print(new_string)

def get_bucket_and_key(s3_file_path:str):
    """
    Extract S3 bucket name and key of the object from s3 file path
    expect file path like 's3://my_backet/some_folder/file.txt
    
    :param: s3_file_path (string) 
    :retunr: bucket, key 
    """
    
    o = urlparse(s3_file_path, allow_fragments=False)
    return o.netloc, o.path.lstrip('/')



