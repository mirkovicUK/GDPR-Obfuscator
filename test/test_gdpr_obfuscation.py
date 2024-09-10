from src.gdpr_obfuscation import get_bucket_and_key,\
        get_data_type, UnsupportedData, gdpr_obfuscator,\
        get_data, obfuscate_csv, obfuscate_json, obfuscate_parquet

from io import StringIO, BytesIO
from botocore.exceptions import ClientError
from moto import mock_aws
import pytest, boto3, csv, json, sys, time
import pyarrow as pa
import numpy as np
import pyarrow.parquet as pq

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
    s3_file = 's3://test_bucket/some_folder/file.csv'
    bucket, key  = get_bucket_and_key(s3_file)
    
    csv_buffer = StringIO()
    headers = ['name', 'surname', 'country']
    data = [['test_name1', 'test_surname1', 'test_country1'],
            ['test_name2', 'test_surname2', 'test_country2']]
    writer = csv.writer(csv_buffer)
    writer.writerow(headers)
    writer.writerows(data)
    csv_data = csv_buffer.getvalue()
    
    client = boto3.client('s3',region_name="us-east-1")
    client.create_bucket(Bucket=bucket)
    client.put_object(
        Body = csv_data,
        Bucket = bucket,
        Key = key)
    
    s3_data = get_data(client, 'test_bucket', 'some_folder/file.csv').decode()
    reader = csv.reader(StringIO(s3_data))
    l = [row for row in reader]
    
    assert l[0] == ['name', 'surname', 'country']
    assert data == l[1:]
    assert s3_data == csv_data

@pytest.mark.describe('get_data()')
@pytest.mark.it('Raise NoSuchKey with wrong key')
@mock_aws
def test_raise_NoSuchKey_with_wrong_key():
    s3_file = 's3://test_bucket/some_folder/file.csv'
    bucket, key  = get_bucket_and_key(s3_file)
    
    csv_buffer = StringIO()
    headers = ['name', 'surname', 'country']
    data = [['test_name1', 'test_surname1', 'test_country1'],
            ['test_name2', 'test_surname2', 'test_country2']]
    writer = csv.writer(csv_buffer)
    writer.writerow(headers)
    writer.writerows(data)
    csv_data = csv_buffer.getvalue()
    
    client = boto3.client('s3', region_name="us-east-1")
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
    s3_file = 's3://test_bucket/some_folder/file.csv'
    bucket, key  = get_bucket_and_key(s3_file)
    
    csv_buffer = StringIO()
    headers = ['name', 'surname', 'country']
    data = [['test_name1', 'test_surname1', 'test_country1'],
            ['test_name2', 'test_surname2', 'test_country2']]
    writer = csv.writer(csv_buffer)
    writer.writerow(headers)
    writer.writerows(data)
    csv_data = csv_buffer.getvalue()
    
    client = boto3.client('s3', region_name="us-east-1")
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
def test_Function_mask_correct_fields_csv():
    s3_file = 's3://test_bucket/some_folder/file.csv'
    bucket, key  = get_bucket_and_key(s3_file)
    
    csv_buffer = StringIO()
    headers = ['id','name', 'surname', 'country']
    data = [['1','test_name1', 'test_surname1', 'test_country1'],
            ['2','test_name2', 'test_surname2', 'test_country2']]
    writer = csv.writer(csv_buffer)
    writer.writerow(headers)
    writer.writerows(data)
    csv_data = csv_buffer.getvalue()
    
    client = boto3.client('s3', region_name="us-east-1")
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
def test_Is_Pure_function():    
    csv_buffer = StringIO()
    headers = ['id','name', 'surname', 'country']
    data = [['1','test_name1', 'test_surname1', 'test_country1'],
            ['2','test_name2', 'test_surname2', 'test_country2']]
    writer = csv.writer(csv_buffer)
    writer.writerow(headers)
    writer.writerows(data)
    csv_data = csv_buffer.getvalue()
    pii_fields = ['name', 'country']
    obfuscate_csv(csv_data, pii_fields)

    assert pii_fields == ['name', 'country']

@pytest.mark.describe('obfuscate_json()')
@pytest.mark.it('Function mask correct fields')
@mock_aws
def test_Function_mask_correct_fields_json():
    s3_file = 's3://test_bucket/some_folder/file.json'
    bucket, key  = get_bucket_and_key(s3_file)
    
    json_data = []
    for i in range(2):
        json_data.append({
            'id' : i,
            'name' : 'test_name' + str(i),
            'surname' : 'test_surname' + str(i),
            'country' : 'test_country' + str(i)
        })
    expected_output = json.dumps([
        {"id": 0, "name": "***", "surname": "test_surname0", "country": "***"},
        {"id": 1, "name": "***", "surname": "test_surname1", "country": "***"}
    ])

    client = boto3.client('s3', region_name="us-east-1")
    client.create_bucket(Bucket=bucket)
    client.put_object(
        Body = json.dumps(json_data),
        Bucket = bucket,
        Key = key)  
    s3_data = get_data(client, bucket, key)
    pii_fields = ['name', 'country']
    masked_data = obfuscate_json(s3_data, pii_fields)
    assert expected_output == masked_data
    
@pytest.mark.describe('obfuscate_json()')
@pytest.mark.it('Return empty serilized list when data oject is empty')
def test_Return_empty_serilized_list_when_encount_empty_data_obj():  
    pii_fields = ['name', 'country']
    masked_data = obfuscate_json(b'', pii_fields)
    assert json.dumps([]) == masked_data

@pytest.mark.describe('obfuscate_json()')
@pytest.mark.it('Is pure function')
def test_Is_pure_function():  
    pii_fields = ['name', 'country']
    obfuscate_json(b'', pii_fields)
    assert pii_fields == ['name', 'country']

@pytest.mark.describe('gdpr_obfuscator()')
@pytest.mark.it('Return data with correct pii_fields masked csv data')
@mock_aws
def test_Return_data_with_correct_pii_fields_masked_csv_data():
    csv_buffer = StringIO()
    headers = ['id','name', 'surname', 'country']
    data = [['1','test_name1', 'test_surname1', 'test_country1'],
            ['2','test_name2', 'test_surname2', 'test_country2']]
    writer = csv.writer(csv_buffer)
    writer.writerow(headers)
    writer.writerows(data)
    csv_data = csv_buffer.getvalue()

    client = boto3.client('s3', region_name="us-east-1")
    client.create_bucket(Bucket='test_bucket')
    client.put_object(
        Body = csv_data,
        Bucket = 'test_bucket',
        Key = 'some_folder/file.csv')
    
    s3_file = 's3://test_bucket/some_folder/file.csv'
    pii_fields = ['name', 'country']
    d = {}
    d['file_to_obfuscate'], d['pii_fields'] = s3_file, pii_fields
    json_str = json.dumps(d)
    masked_csv = gdpr_obfuscator(json_str).decode()
    expected_output_headers = ['id','name', 'surname', 'country']
    expected_output_data = [['1','***', 'test_surname1', '***'],
            ['2','***', 'test_surname2', '***']]
    reader = csv.reader(StringIO(masked_csv))
    l = [row for row in reader]
    
    assert l[0] == expected_output_headers
    assert l[1:] == expected_output_data

@pytest.mark.describe('gdpr_obfuscator()')
@pytest.mark.it('Return data with correct pii_fields masked json data')
@mock_aws
def test_Return_data_with_correct_pii_fields_masked_json_data():
    json_data = []
    for i in range(2):
        json_data.append({
            'id' : i,
            'name' : 'test_name' + str(i),
            'surname' : 'test_surname' + str(i),
            'country' : 'test_country' + str(i)
        })
    expected_output = json.dumps([
        {"id": 0, "name": "***", "surname": "test_surname0", "country": "***"},
        {"id": 1, "name": "***", "surname": "test_surname1", "country": "***"}
    ]).encode()

    client = boto3.client('s3', region_name="us-east-1")
    client.create_bucket(Bucket='test_bucket')
    client.put_object(
        Body = json.dumps(json_data),
        Bucket = 'test_bucket',
        Key = 'some_folder/file.json')
    
    s3_file = 's3://test_bucket/some_folder/file.json'
    pii_fields = ['name', 'country']
    json_str = json.dumps({
        'file_to_obfuscate' : s3_file,
        'pii_fields' : pii_fields
    })
    masked_json = gdpr_obfuscator(json_str)
    assert masked_json == expected_output

@pytest.mark.describe('gdpr_obfuscator()')
@pytest.mark.it('Return bytestream representation of data csv data')
@mock_aws
def test_Return_bytestream_representation_of_data_csv_data():
    csv_buffer = StringIO()
    headers = ['id','name', 'surname', 'country']
    data = [['1','test_name1', 'test_surname1', 'test_country1'],
            ['2','test_name2', 'test_surname2', 'test_country2']]
    writer = csv.writer(csv_buffer)
    writer.writerow(headers)
    writer.writerows(data)
    csv_data = csv_buffer.getvalue()

    client = boto3.client('s3', region_name="us-east-1")
    client.create_bucket(Bucket='test_bucket')
    client.put_object(
        Body = csv_data,
        Bucket = 'test_bucket',
        Key = 'some_folder/file.csv')
    
    s3_file = 's3://test_bucket/some_folder/file.csv'
    pii_fields = ['name', 'country']
    d = {}
    d['file_to_obfuscate'], d['pii_fields'] = s3_file, pii_fields
    json_str = json.dumps(d)
    masked_csv = gdpr_obfuscator(json_str)
    assert isinstance(masked_csv, bytes)

@pytest.mark.describe('gdpr_obfuscator()')
@pytest.mark.it('Return bytestream representation of data json data')
@mock_aws
def test_Return_bytestream_representation_of_data_json_data():
    json_data = []
    for i in range(2):
        json_data.append({
            'id' : i,
            'name' : 'test_name' + str(i),
            'surname' : 'test_surname' + str(i),
            'country' : 'test_country' + str(i)
        })

    client = boto3.client('s3', region_name="us-east-1")
    client.create_bucket(Bucket='test_bucket')
    client.put_object(
        Body = json.dumps(json_data),
        Bucket = 'test_bucket',
        Key = 'some_folder/file.json')
    
    s3_file = 's3://test_bucket/some_folder/file.json'
    pii_fields = ['name', 'country']
    d = {}
    d['file_to_obfuscate'], d['pii_fields'] = s3_file, pii_fields
    json_str = json.dumps(d)
    masked_csv = gdpr_obfuscator(json_str)
    assert isinstance(masked_csv, bytes)

@pytest.mark.describe('gdpr_obfuscator()')
@pytest.mark.it('Function output is compatible with the boto3 S3 Put Object')
@mock_aws
def test_Function_outputis_compatible_with_the_boto3_S3_Put_Object():
    csv_buffer = StringIO()
    headers = ['id','name', 'surname', 'country']
    data = [['1','test_name1', 'test_surname1', 'test_country1'],
            ['2','test_name2', 'test_surname2', 'test_country2']]
    writer = csv.writer(csv_buffer)
    writer.writerow(headers)
    writer.writerows(data)
    csv_data = csv_buffer.getvalue()

    client = boto3.client('s3', region_name="us-east-1")
    client.create_bucket(Bucket='test_bucket')
    client.put_object(
        Body = csv_data,
        Bucket = 'test_bucket',
        Key = 'some_folder/file.csv')
    
    s3_file = 's3://test_bucket/some_folder/file.csv'
    pii_fields = ['name', 'country']
    d = {}
    d['file_to_obfuscate'], d['pii_fields'] = s3_file, pii_fields
    response = client.put_object(
        Body = gdpr_obfuscator(json.dumps(d)),
        Bucket = 'test_bucket',
        Key = 'Masked_TEST_data'
    )
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

@pytest.mark.describe('gdpr_obfuscator()')
@pytest.mark.it('Function process 1MB data in less than 1min')
@mock_aws
def test_Function_process_1MB_data_in_less_than_1min():
    headers = ['id','name', 'surname', 'country']
    data = []
    counter = 1
    while sys.getsizeof(data) <= 1000000:
        data.append([str_counter:=str(counter), 'test_name' + str_counter, 
                     'test_surname' + str_counter, 'test_country' + str_counter])
        counter += 1
    csv_buffer = StringIO()
    writer = csv.writer(csv_buffer)
    writer.writerow(headers)
    writer.writerows(data)
    csv_data = csv_buffer.getvalue()

    client = boto3.client('s3', region_name="us-east-1")
    client.create_bucket(Bucket='test_bucket')
    client.put_object(
        Body = csv_data,
        Bucket = 'test_bucket',
        Key = 'some_folder/file.csv')
    
    s3_file = 's3://test_bucket/some_folder/file.csv'
    pii_fields = headers
    d = {}
    d['file_to_obfuscate'], d['pii_fields'] = s3_file, pii_fields
    start_time = time.time()
    gdpr_obfuscator(json.dumps(d))
    assert time.time() - start_time < 60

@pytest.mark.describe('obfuscate_parquet()')
@pytest.mark.it('Function mask correct fields')
@mock_aws
def test_Function_mask_correct_fields_parquet():
    size = 100
    pydict = {
        'id' : pa.array(np.arange(size)),
        'name' : pa.array(['test_name' + str(i) for i in range(size)]),
        'surname' : pa.array(['test_surname' + str(i) for i in range(size)]),
        'country' : pa.array(['test_country' + str(i) for i in range(size)])
    }
    table = pa.Table.from_pydict(pydict)
    pq.write_table(table, parquet_bufer:=BytesIO())
    
    client = boto3.client('s3', region_name="us-east-1")
    client.create_bucket(Bucket='test_bucket')
    client.put_object(
        Body = parquet_bufer.getvalue(),
        Bucket = 'test_bucket',
        Key = 'some_folder/file.parquet')
    
    s3_data = get_data(client, 'test_bucket', 'some_folder/file.parquet')
    pii_fields = ['name', 'country']
    masked_parquet = obfuscate_parquet(s3_data, pii_fields)
    masked_table = pq.ParquetFile(BytesIO(masked_parquet)).read()

    expected_pydict = {
        'id' : pa.array(np.arange(size)),
        'name' : pa.array(['***' for _ in range(size)]),
        'surname' : pa.array(['test_surname' + str(i) for i in range(size)]),
        'country' : pa.array(['***' for _ in range(size)])
    }
    expected_table = pa.Table.from_pydict(expected_pydict)
    

    assert expected_table == masked_table