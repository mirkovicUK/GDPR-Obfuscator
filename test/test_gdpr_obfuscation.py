import botocore.client
from src.gdpr_obfuscation import get_bucket_and_key,\
        get_data_type, UnsupportedData, gdpr_obfuscator,\
        get_data, obfuscate_csv

from io import StringIO
from botocore.exceptions import ClientError
from moto import mock_aws
import pytest, boto3, botocore, csv, json

@pytest.mark.describe('get_bucket_and_key()')
@pytest.mark.it('Extract correct bucket and key from S3 data location')
def test_extract_correct_bucket_and_key_from_S3_location():
    s3_file = 's3://my_bucket/some_folder/file.txt'
    bucket, key = get_bucket_and_key(s3_file)
    assert bucket == 'my_bucket'
    assert key == 'some_folder/file.txt'

@pytest.mark.describe('get_data_type()')
@pytest.mark.it('Extract correct data type')
def test_extract_correct_data_type():
    s3_file = 's3://my_bucket/some_folder/file.csv'
    _, key = get_bucket_and_key(s3_file)
    data_type = get_data_type(key)
    assert data_type == 'csv'

@pytest.mark.describe('get_data_type()')
@pytest.mark.it('Raise UnsupportedData exeption')
def test_raise_UnsuporetedData():
    s3_file = 's3://my_bucket/some_folder/file.txt'
    _, key = get_bucket_and_key(s3_file)
    with pytest.raises(UnsupportedData) as excinfo:
        get_data_type(key)
    assert "Function supports only csv, json, parquet types." in str(excinfo.value)


@pytest.mark.describe('get_data()')
@pytest.mark.it('Return correct data')
@mock_aws
def test_get_data_return_corect_data():
    s3_file = 's3://TESTbucket/some_folder/file.csv'
    bucket, key  = get_bucket_and_key(s3_file)
    
    csv_buffer = StringIO()
    headers = ['name', 'surname', 'country']
    data = [['test_name1', 'test_surname1', 'test_country1'],
            ['test_name2', 'test_surname2', 'test_country2']]
    writer = csv.writer(csv_buffer)
    writer.writerow(headers)
    writer.writerows(data)
    csv_data = csv_buffer.getvalue()
    
    client = boto3.client('s3')
    client.create_bucket(Bucket=bucket)
    client.put_object(
        Body = csv_data,
        Bucket = bucket,
        Key = 'some_folder/file.csv')
    
    s3_data = get_data(client, bucket, key).decode()
    reader = csv.reader(StringIO(s3_data))
    l = [row for row in reader]
    
    assert l[0] == ['name', 'surname', 'country']
    assert data == l[1:]
    assert s3_data == csv_buffer.getvalue()

@pytest.mark.describe('get_data()')
@pytest.mark.it('Raise NoSuchKey with wrong key')
@mock_aws
def test_raise_NoSuchKey_with_wrong_key():
    s3_file = 's3://TESTbucket/some_folder/file.csv'
    bucket, key  = get_bucket_and_key(s3_file)
    
    csv_buffer = StringIO()
    headers = ['name', 'surname', 'country']
    data = [['test_name1', 'test_surname1', 'test_country1'],
            ['test_name2', 'test_surname2', 'test_country2']]
    writer = csv.writer(csv_buffer)
    writer.writerow(headers)
    writer.writerows(data)
    csv_data = csv_buffer.getvalue()
    
    client = boto3.client('s3')
    client.create_bucket(Bucket=bucket)
    client.put_object(
        Body = csv_data,
        Bucket = bucket,
        Key = key)
    
    with pytest.raises(ClientError) as excinfo:
        get_data(client, bucket, 'wrong_key')
    assert 'NoSuchKey' in str(excinfo.value)

@pytest.mark.describe('get_data()')
@pytest.mark.it('Raise NoSuchBacket with wrong buket')
@mock_aws
def test_raise_NoSuchBucket_with_wrong_bucket():
    s3_file = 's3://TESTbucket/some_folder/file.csv'
    bucket, key  = get_bucket_and_key(s3_file)
    
    csv_buffer = StringIO()
    headers = ['name', 'surname', 'country']
    data = [['test_name1', 'test_surname1', 'test_country1'],
            ['test_name2', 'test_surname2', 'test_country2']]
    writer = csv.writer(csv_buffer)
    writer.writerow(headers)
    writer.writerows(data)
    csv_data = csv_buffer.getvalue()
    
    client = boto3.client('s3')
    client.create_bucket(Bucket=bucket)
    client.put_object(
        Body = csv_data,
        Bucket = bucket,
        Key = key)
    
    with pytest.raises(ClientError) as excinfo:
        get_data(client, 'WRONG_BUCKET', key)
    assert 'NoSuchBucket' in str(excinfo.value)

@pytest.mark.describe('obfuscate_csv()')
@pytest.mark.it('Function mask correct fields')
@mock_aws
def test_Mask_correct_fields_and_return_str():
    s3_file = 's3://TESTbucket/some_folder/file.csv'
    bucket, key  = get_bucket_and_key(s3_file)
    
    csv_buffer = StringIO()
    headers = ['id','name', 'surname', 'country']
    data = [['1','test_name1', 'test_surname1', 'test_country1'],
            ['2','test_name2', 'test_surname2', 'test_country2']]
    writer = csv.writer(csv_buffer)
    writer.writerow(headers)
    writer.writerows(data)
    csv_data = csv_buffer.getvalue()
    
    client = boto3.client('s3')
    client.create_bucket(Bucket=bucket)
    client.put_object(
        Body = csv_data,
        Bucket = bucket,
        Key = key)
    
    s3_data = get_data(client, bucket, key).decode()
    pii_fields = ['name', 'country']
    masked_csv = obfuscate_csv(s3_data, pii_fields)
    expected_output_headers = ['id','name', 'surname', 'country']
    expected_output_data = [['1','***', 'test_surname1', '***'],
            ['2','***', 'test_surname2', '***']]
    reader = csv.reader(StringIO(masked_csv))
    l = [row for row in reader]
    
    assert l[0] == expected_output_headers
    assert l[1:] == expected_output_data

@pytest.mark.describe('obfuscate_csv()')
@pytest.mark.it('Is Pure function')
@mock_aws
def test_Is_Pure_function():
    s3_file = 's3://TESTbucket/some_folder/file.csv'
    bucket, key  = get_bucket_and_key(s3_file)
    
    csv_buffer = StringIO()
    headers = ['id','name', 'surname', 'country']
    data = [['1','test_name1', 'test_surname1', 'test_country1'],
            ['2','test_name2', 'test_surname2', 'test_country2']]
    writer = csv.writer(csv_buffer)
    writer.writerow(headers)
    writer.writerows(data)
    csv_data = csv_buffer.getvalue()
    
    client = boto3.client('s3')
    client.create_bucket(Bucket=bucket)
    client.put_object(
        Body = csv_data,
        Bucket = bucket,
        Key = key)
    
    s3_data = get_data(client, bucket, key).decode()
    pii_fields = ['name', 'country']
    obfuscate_csv(s3_data, pii_fields)

    assert pii_fields == ['name', 'country']


@pytest.mark.describe('gdpr_obfuscator()')
@pytest.mark.it('Return data with correct pii_fields masked')
@mock_aws
def test_Return_data_with_correct_pii_fields_masked():
    csv_buffer = StringIO()
    headers = ['id','name', 'surname', 'country']
    data = [['1','test_name1', 'test_surname1', 'test_country1'],
            ['2','test_name2', 'test_surname2', 'test_country2']]
    writer = csv.writer(csv_buffer)
    writer.writerow(headers)
    writer.writerows(data)
    csv_data = csv_buffer.getvalue()

    client = boto3.client('s3')
    client.create_bucket(Bucket='TESTbucket')
    client.put_object(
        Body = csv_data,
        Bucket = 'TESTbucket',
        Key = 'some_folder/file.csv')
    
    s3_file = 's3://TESTbucket/some_folder/file.csv'
    pii_fields = ['name', 'country']
    masked_csv = gdpr_obfuscator(s3_file, pii_fields).decode()
    expected_output_headers = ['id','name', 'surname', 'country']
    expected_output_data = [['1','***', 'test_surname1', '***'],
            ['2','***', 'test_surname2', '***']]
    reader = csv.reader(StringIO(masked_csv))
    l = [row for row in reader]
    
    assert l[0] == expected_output_headers
    assert l[1:] == expected_output_data

@pytest.mark.describe('gdpr_obfuscator()')
@pytest.mark.it('Return bytestream representation of data')
@mock_aws
def test_Return_bytestream_representation_of_data():
    csv_buffer = StringIO()
    headers = ['id','name', 'surname', 'country']
    data = [['1','test_name1', 'test_surname1', 'test_country1'],
            ['2','test_name2', 'test_surname2', 'test_country2']]
    writer = csv.writer(csv_buffer)
    writer.writerow(headers)
    writer.writerows(data)
    csv_data = csv_buffer.getvalue()

    client = boto3.client('s3')
    client.create_bucket(Bucket='TESTbucket')
    client.put_object(
        Body = csv_data,
        Bucket = 'TESTbucket',
        Key = 'some_folder/file.csv')
    
    s3_file = 's3://TESTbucket/some_folder/file.csv'
    pii_fields = ['name', 'country']
    masked_csv = gdpr_obfuscator(s3_file, pii_fields)
    assert isinstance(masked_csv, bytes)