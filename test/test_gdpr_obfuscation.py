from src.gdpr_obfuscation import get_bucket_and_key,\
        get_data_type, UnsupportedData, gdpr_obfuscator,\
        get_data, obfuscate_csv, obfuscate_json,\
        obfuscate_parquet, setup_logger

from io import StringIO, BytesIO
from botocore.exceptions import ClientError
from moto import mock_aws
import pytest, boto3, csv, json, sys, time, logging
import pyarrow as pa
import numpy as np
import pyarrow.parquet as pq

####################################################################
#                    *FIXTURE*
####################################################################
@pytest.fixture
def csv_data():
    """
    :returns: (tuple) csv data structured for boto3 put_object(),
        and expected csv data for assertion
    """
    csv_buffer = StringIO()
    headers = ['id','name', 'surname', 'country']
    data = [['1','test_name1', 'test_surname1', 'test_country1'],
            ['2','test_name2', 'test_surname2', 'test_country2']]
    writer = csv.writer(csv_buffer)
    writer.writerow(headers)
    writer.writerows(data)

    expected_output_headers = ['id','name', 'surname', 'country']
    expected_output_data = [['1','***', 'test_surname1', '***'],
            ['2','***', 'test_surname2', '***']]
    expected_csv_buffer = StringIO()
    writer = csv.writer(expected_csv_buffer)
    writer.writerow(expected_output_headers)
    writer.writerows(expected_output_data)

    return csv_buffer.getvalue(), expected_csv_buffer.getvalue()


@pytest.fixture
def json_data():
    """
    returns: (tuple) json data structured for boto3 put_object(),
        and expected json data for assertion after masking pii fields
    """
    json_data = []
    for i in range(2):
        json_data.append({
            'id' : i,
            'name' : 'test_name' + str(i),
            'surname' : 'test_surname' + str(i),
            'country' : 'test_country' + str(i)
        })

    expected_json_data = json.dumps([
        {"id": 0, "name": "***", "surname": "test_surname0", "country": "***"},
        {"id": 1, "name": "***", "surname": "test_surname1", "country": "***"}
    ]).encode()

    return json.dumps(json_data), expected_json_data

@pytest.fixture
def parquet_data():
    """
    :return: (tuple) parquet_data structured for boto3 put_object(),
        and expected_parquet_data for assertion after masking pii fields
    """
    size = 100000
    pydict = {
        'id' : pa.array(np.arange(size)),
        'name' : pa.array(['test_name' + str(i) for i in range(size)]),
        'surname' : pa.array(['test_surname' + str(i) for i in range(size)]),
        'country' : pa.array(['test_country' + str(i) for i in range(size)]),
        'address' : pa.array(['test_address' + str(i) for i in range(size)]),
        'post_code' : pa.array(['post_code' + str(i) for i in range(size)]),
        'some_column' : pa.array(['some_column' + str(i) for i in range(size)]),
    }
    table = pa.Table.from_pydict(pydict)
    pq.write_table(table, parquet_buffer:=BytesIO())

    expected_pydict = {
        'id' : pa.array(['***' for _ in range(size)]),
        'name' : pa.array(['***' for _ in range(size)]),
        'surname' : pa.array(['test_surname' + str(i) for i in range(size)]),
        'country' : pa.array(['test_country' + str(i) for i in range(size)]),
        'address' : pa.array(['test_address' + str(i) for i in range(size)]),
        'post_code' : pa.array(['***' for _ in range(size)]),
        'some_column' : pa.array(['***' for _ in range(size)]),
    }
    expected_table = pa.Table.from_pydict(expected_pydict)
    pq.write_table(expected_table, expected_parquet_buffer:=BytesIO())

    return parquet_buffer.getvalue(), expected_parquet_buffer.getvalue()

# Arrange
@pytest.fixture
def csv_json_parquet(csv_data, json_data, parquet_data):
    """
    Fixture to arrange data
    :return: {
                'csv' : csv_data, 
                'json' : json_data,
                'parquet' : parquet_data    
    """
    d = {
        'csv' : csv_data,
        'json' : json_data,
        'parquet' : parquet_data
    }

    return d
    
######################################################################################
#                 *FIXTURE END*
######################################################################################

# @pytest.mark.describe('get_bucket_and_key()')
# @pytest.mark.it('Extract correct bucket and key from S3 data location')
# def test_extract_correct_bucket_and_key_from_S3_location():
#     s3_file = 's3://my_bucket/some_folder/file.txt'
#     bucket, key = get_bucket_and_key(s3_file)
#     assert bucket == 'my_bucket'
#     assert key == 'some_folder/file.txt'


# @pytest.mark.describe('get_data_type()')
# @pytest.mark.it('Extract correct data type')
# def test_extract_correct_data_type():
#     s3_file = 's3://my_bucket/some_folder/file.csv'
#     _, key = get_bucket_and_key(s3_file)
#     data_type = get_data_type(key)
#     assert data_type == 'csv'


# @pytest.mark.describe('get_data_type()')
# @pytest.mark.it('Raise UnsupportedData exeption')
# def test_raise_UnsuporetedData():
#     s3_file = 's3://my_bucket/some_folder/file.txt'
#     _, key = get_bucket_and_key(s3_file)
#     with pytest.raises(UnsupportedData) as excinfo:
#         get_data_type(key)
#     assert "Function supports only csv, json, parquet types." in str(excinfo.value)


# @pytest.mark.describe('get_data()')
# @pytest.mark.it('Return correct data')
# @mock_aws
# def test_get_data_return_corect_data(csv_data):
#     csv_data_s3, _ = csv_data
#     s3_file = 's3://test_bucket/some_folder/file.csv'
#     bucket, key  = get_bucket_and_key(s3_file)
    
#     client = boto3.client('s3',region_name="us-east-1")
#     client.create_bucket(Bucket=bucket)
#     client.put_object(
#         Body = csv_data_s3,
#         Bucket = bucket,
#         Key = key)
    
#     s3_data = get_data(client, 'test_bucket', 'some_folder/file.csv').decode()
#     reader = csv.reader(StringIO(s3_data))
#     l = [row for row in reader]
    
#     assert l[0] == ['id','name', 'surname', 'country']
#     assert l[1:] == [['1','test_name1', 'test_surname1', 'test_country1'],
#                     ['2','test_name2', 'test_surname2', 'test_country2']]
#     assert s3_data == csv_data_s3


# @pytest.mark.describe('get_data()')
# @pytest.mark.it('Raise NoSuchKey with wrong key')
# @mock_aws
# def test_raise_NoSuchKey_with_wrong_key(csv_data):
#     csv_data_s3, _ = csv_data
#     s3_file = 's3://test_bucket/some_folder/file.csv'
#     bucket, key  = get_bucket_and_key(s3_file)
    
#     client = boto3.client('s3', region_name="us-east-1")
#     client.create_bucket(Bucket=bucket)
#     client.put_object(
#         Body = csv_data_s3,
#         Bucket = bucket,
#         Key = key
#     )
    
#     with pytest.raises(ClientError) as excinfo:
#         get_data(client, bucket, 'wrong_key')
#     assert 'NoSuchKey' in str(excinfo.value)


# @pytest.mark.describe('get_data()')
# @pytest.mark.it('Raise NoSuchBacket with wrong buket')
# @mock_aws
# def test_raise_NoSuchBucket_with_wrong_bucket(csv_data):
#     csv_data_s3, _ = csv_data
#     s3_file = 's3://test_bucket/some_folder/file.csv'
#     bucket, key  = get_bucket_and_key(s3_file)
    
#     client = boto3.client('s3', region_name="us-east-1")
#     client.create_bucket(Bucket=bucket)
#     client.put_object(
#         Body = csv_data_s3,
#         Bucket = bucket,
#         Key = key)
    
#     with pytest.raises(ClientError) as excinfo:
#         get_data(client, 'WRONG_BUCKET', key)
#     assert 'NoSuchBucket' in str(excinfo.value)


# @pytest.mark.describe('obfuscate_csv()')
# @pytest.mark.it('Function mask correct fields')
# @mock_aws
# def test_Function_mask_correct_fields_csv(csv_data):
#     csv_data_s3, expected_csv_data = csv_data

#     s3_file = 's3://test_bucket/some_folder/file.csv'
#     bucket, key  = get_bucket_and_key(s3_file)
#     client = boto3.client('s3', region_name="us-east-1")
#     client.create_bucket(Bucket=bucket)
#     client.put_object(
#         Body = csv_data_s3,
#         Bucket = bucket,
#         Key = key)
    
#     s3_data = get_data(client, bucket, key).decode()
#     pii_fields = ['name', 'country']
#     masked_csv = obfuscate_csv(s3_data, pii_fields)
    
#     assert masked_csv == expected_csv_data


# @pytest.mark.describe('obfuscate_csv()')
# @pytest.mark.it('Is Pure function')
# def test_Is_Pure_function(csv_data):
#     csv_data, _ = csv_data  
#     pii_fields = ['name', 'country']
#     obfuscate_csv(csv_data, pii_fields)

#     assert pii_fields == ['name', 'country']


# @pytest.mark.describe('obfuscate_json()')
# @pytest.mark.it('Function mask correct fields')
# @mock_aws
# def test_Function_mask_correct_fields_json(json_data):
#     json_data_s3, expected_json_data = json_data

#     s3_file = 's3://test_bucket/some_folder/file.json'
#     bucket, key  = get_bucket_and_key(s3_file)
#     client = boto3.client('s3', region_name="us-east-1")
#     client.create_bucket(Bucket=bucket)
#     client.put_object(
#         Body = json_data_s3,
#         Bucket = bucket,
#         Key = key)
      
#     s3_data = get_data(client, bucket, key)
#     pii_fields = ['name', 'country']
#     masked_data = obfuscate_json(s3_data, pii_fields).encode()
#     assert expected_json_data == masked_data
    

# @pytest.mark.describe('obfuscate_json()')
# @pytest.mark.it('Return empty serilized list when data oject is empty')
# def test_Return_empty_serilized_list_when_encount_empty_data_obj():  
#     pii_fields = ['name', 'country']
#     masked_data = obfuscate_json(b'', pii_fields)
#     assert json.dumps([]) == masked_data


# @pytest.mark.describe('obfuscate_json()')
# @pytest.mark.it('Is pure function')
# def test_Is_pure_function():  
#     pii_fields = ['name', 'country']
#     obfuscate_json(b'', pii_fields)
#     assert pii_fields == ['name', 'country']


# ########################################################################
# # gdpr_obsfucator() tests
# #######################################################################
# @pytest.mark.describe('gdpr_obfuscator()')
# @pytest.mark.it('Return data with correct pii_fields masked parquet data')
# @mock_aws
# def test_Return_data_with_correct_pii_fields_masked_parquet_data(parquet_data):
#     parquet_data, expected_parquet_data = parquet_data

#     client = boto3.client('s3', region_name="us-east-1")
#     client.create_bucket(Bucket='test_bucket')
#     client.put_object(
#         Body = parquet_data,
#         Bucket = 'test_bucket',
#         Key = 'some_folder/file.parquet')

#     pii_fields = ['id', 'name', 'post_code', 'some_column']
#     s3_file = 's3://test_bucket/some_folder/file.parquet'
#     masked_parquet = gdpr_obfuscator(json.dumps(
#         {
#             'file_to_obfuscate' : s3_file,
#             'pii_fields' : pii_fields
#         }
#     ))
#     assert masked_parquet == expected_parquet_data


# @pytest.mark.describe('gdpr_obfuscator()')
# @pytest.mark.it('Return data with correct pii_fields masked csv data')
# @mock_aws
# def test_Return_data_with_correct_pii_fields_masked_csv_data(csv_data):
#     csv_data_s3, expected_csv_data = csv_data

#     client = boto3.client('s3', region_name="us-east-1")
#     client.create_bucket(Bucket='test_bucket')
#     client.put_object(
#         Body = csv_data_s3,
#         Bucket = 'test_bucket',
#         Key = 'some_folder/file.csv')
    
#     s3_file = 's3://test_bucket/some_folder/file.csv'
#     pii_fields = ['name', 'country']
#     d = {}
#     d['file_to_obfuscate'], d['pii_fields'] = s3_file, pii_fields
#     json_str = json.dumps(d)
#     masked_csv = gdpr_obfuscator(json_str).decode()

#     assert expected_csv_data == masked_csv


# @pytest.mark.describe('gdpr_obfuscator()')
# @pytest.mark.it('Return data with correct pii_fields masked json data')
# @mock_aws
# def test_Return_data_with_correct_pii_fields_masked_json_data(json_data):
#     json_data_s3, expected_output = json_data

#     client = boto3.client('s3', region_name="us-east-1")
#     client.create_bucket(Bucket='test_bucket')
#     client.put_object(
#         Body = json_data_s3,
#         Bucket = 'test_bucket',
#         Key = 'some_folder/file.json')
    
#     s3_file = 's3://test_bucket/some_folder/file.json'
#     pii_fields = ['name', 'country']
#     json_str = json.dumps({
#         'file_to_obfuscate' : s3_file,
#         'pii_fields' : pii_fields
#     })
#     masked_json = gdpr_obfuscator(json_str)
    
#     assert masked_json == expected_output


# @pytest.mark.describe('gdpr_obfuscator()')
# @pytest.mark.it('Return bytestream representation of data parquet data')
# @mock_aws
# def test_Return_bytestream_representation_of_data_parquet_data(parquet_data):
#     parquet_data, _ = parquet_data

#     client = boto3.client('s3', region_name="us-east-1")
#     client.create_bucket(Bucket='test_bucket')
#     client.put_object(
#         Body = parquet_data,
#         Bucket = 'test_bucket',
#         Key = 'some_folder/file.parquet')
#     pii_fields = ['id', 'name', 'post_code', 'some_column']
#     s3_file = 's3://test_bucket/some_folder/file.parquet'
#     masked_parquet = gdpr_obfuscator(json.dumps(
#         {
#             'file_to_obfuscate' : s3_file,
#             'pii_fields' : pii_fields
#         }
#     ))
#     assert isinstance(masked_parquet, bytes)


# @pytest.mark.describe('gdpr_obfuscator()')
# @pytest.mark.it('Return bytestream representation of data csv data')
# @mock_aws
# def test_Return_bytestream_representation_of_data_csv_data(csv_data):
#     csv_data_s3, _ = csv_data

#     client = boto3.client('s3', region_name="us-east-1")
#     client.create_bucket(Bucket='test_bucket')
#     client.put_object(
#         Body = csv_data_s3,
#         Bucket = 'test_bucket',
#         Key = 'some_folder/file.csv')
    
#     s3_file = 's3://test_bucket/some_folder/file.csv'
#     pii_fields = ['name', 'country']
#     d = {}
#     d['file_to_obfuscate'], d['pii_fields'] = s3_file, pii_fields
#     json_str = json.dumps(d)
#     masked_csv = gdpr_obfuscator(json_str)

#     assert isinstance(masked_csv, bytes)


# @pytest.mark.describe('gdpr_obfuscator()')
# @pytest.mark.it('Return bytestream representation of data json data')
# @mock_aws
# def test_Return_bytestream_representation_of_data_json_data(json_data):
#     json_data_s3, _ = json_data

#     client = boto3.client('s3', region_name="us-east-1")
#     client.create_bucket(Bucket='test_bucket')
#     client.put_object(
#         Body = json_data_s3,
#         Bucket = 'test_bucket',
#         Key = 'some_folder/file.json')
    
#     s3_file = 's3://test_bucket/some_folder/file.json'
#     pii_fields = ['name', 'country']
#     d = {}
#     d['file_to_obfuscate'], d['pii_fields'] = s3_file, pii_fields
#     masked_csv = gdpr_obfuscator(json.dumps(d))
    
#     assert isinstance(masked_csv, bytes)


# @pytest.mark.describe('gdpr_obfuscator()')
# @pytest.mark.it('Function output is compatible with the boto3 S3 Put Object')
# @mock_aws
# def test_Function_outputis_compatible_with_the_boto3_S3_Put_Object(csv_json_parquet):
#     client = boto3.client('s3', region_name="us-east-1")
#     client.create_bucket(Bucket='test_bucket')
#     for key in csv_json_parquet.keys():
#         client.put_object(
#             Body = csv_json_parquet[key][0],
#             Bucket = 'test_bucket',
#             Key = f'some_folder/file.{key}')

#     responses = []
#     pii_field_pydict = {
#         'csv' : ['name', 'country'],
#         'json' : ['name', 'country'],
#         'parquet' : ['id', 'name', 'post_code', 'some_column']
#     }
#     for key in csv_json_parquet.keys():
#         s3_file = f's3://test_bucket/some_folder/file.{key}'
#         pii_fields = pii_field_pydict[key]
#         d = {}
#         d['file_to_obfuscate'], d['pii_fields'] = s3_file, pii_fields
#         response = client.put_object(
#         Body = gdpr_obfuscator(json.dumps(d)),
#         Bucket = 'test_bucket',
#         Key = 'Masked_TEST_data.csv')
#         responses.append(response['ResponseMetadata']['HTTPStatusCode'] == 200)
    
#     assert len(responses) > 0 and all(responses)


# @pytest.mark.describe('gdpr_obfuscator()')
# @pytest.mark.it('Function process 1MB data in less than 1min')
# @mock_aws
# def test_Function_process_1MB_data_in_less_than_1min():
#     headers = ['id','name', 'surname', 'country']
#     data = []
#     counter = 1
#     while sys.getsizeof(data) <= 1000000:
#         data.append([str_counter:=str(counter), 'test_name' + str_counter, 
#                      'test_surname' + str_counter, 'test_country' + str_counter])
#         counter += 1
#     csv_buffer = StringIO()
#     writer = csv.writer(csv_buffer)
#     writer.writerow(headers)
#     writer.writerows(data)
#     csv_data = csv_buffer.getvalue()

#     client = boto3.client('s3', region_name="us-east-1")
#     client.create_bucket(Bucket='test_bucket')
#     client.put_object(
#         Body = csv_data,
#         Bucket = 'test_bucket',
#         Key = 'some_folder/file.csv')
    
#     s3_file = 's3://test_bucket/some_folder/file.csv'
#     pii_fields = headers
#     d = {}
#     d['file_to_obfuscate'], d['pii_fields'] = s3_file, pii_fields
#     start_time = time.time()
#     gdpr_obfuscator(json.dumps(d))
#     assert time.time() - start_time < 60


# ####################################################################################
# # obsfuscate_parquet() tests
# ###################################################################################

# @pytest.mark.describe('obfuscate_parquet()')
# @pytest.mark.it('Function mask correct fields')
# def test_Function_mask_correct_fields_parquet(parquet_data):
#     parquet_data, expected_parquet_data = parquet_data
#     pii_fields = ['id', 'name', 'post_code', 'some_column']
#     masked_parquet = obfuscate_parquet(parquet_data, pii_fields)

#     assert expected_parquet_data == masked_parquet


# @pytest.mark.describe('obfuscate_parquet()')
# @pytest.mark.it('Function keep column order')
# @pytest.mark.parametrize('pii_fields',[
#     ['some_column', 'post_code', 'address', 'country'],
#     ['id', 'name', 'surname', 'country'],
#     ['id', 'name', 'some_column', 'post_code'],
#     ['id', 'name', 'some_column', 'post_code', 'country']
# ])
# def test_Function_keep_columns_order(pii_fields, parquet_data):
#     parquet_data, _ = parquet_data
#     expected_columns = [
#         'id', 'name', 'surname', 'country', 'address', 'post_code', 'some_column']
#     masked_pqfile = obfuscate_parquet(
#         parquet_data,
#         pii_fields)
    
#     masked_table = pq.ParquetFile(BytesIO(masked_pqfile)).read()
#     masked_columns = masked_table.column_names
#     assert masked_columns == expected_columns


# @pytest.mark.describe('obfuscate_parquet()')
# @pytest.mark.it('Function handles pii_field that is not in table')
# @pytest.mark.parametrize('wrong_column_name',[
#     ['this', 'can', 'be', 'spelling', 'error']])
# def test_Function_handles_pii_field_that_is_not_in_table(wrong_column_name, parquet_data):
#     parquet_data, expected_parquet_data = parquet_data
#     masked_pqfile = obfuscate_parquet(
#         parquet_data,
#         wrong_column_name
#     )
#     masked_table = pq.ParquetFile(BytesIO(masked_pqfile)).read()
#     masked_columns = masked_table.column_names

#     expected_columns = [
#         'id', 'name', 'surname', 'country', 'address', 'post_code', 'some_column']
#     assert masked_columns == expected_columns
#     assert masked_table == pq.ParquetFile(BytesIO(parquet_data)).read()

# @pytest.mark.describe('obfuscate_parquet()')
# @pytest.mark.it('Is Pure function')
# @pytest.mark.parametrize('pii_fields, expected',[(
#     ['id', 'name', 'some_column', 'address'],
#     ['id', 'name', 'some_column', 'address']
# )])
# def test_Is_Pure_Function(pii_fields, expected, parquet_data):
#     parquet_data, _ = parquet_data
#     obfuscate_parquet(parquet_data, pii_fields)
#     assert pii_fields == expected

# @pytest.mark.describe('obfuscate_parquet()')
# @pytest.mark.it('Function applies correct compression algorithms')
# def test_Function_applies_correct_compression_algorithms(parquet_data):
#     parquet_data, _ = parquet_data
#     masked_pqfile = obfuscate_parquet(
#         parquet_data,
#         ['name'],
#         compression='GZIP'
#     )
#     metadata = pq.read_metadata(BytesIO(masked_pqfile))
#     metadata_dict = metadata.to_dict()
#     compression = metadata_dict['row_groups'][0]['columns'][0]['compression']
#     assert compression == 'GZIP'
# #####################################################################################
# #   *END*  obsfuscate_parquet() tests
# ####################################################################################


# @pytest.mark.describe('setup_logger()')
# @pytest.mark.it('Function setup correct_loggin_handlers')
# def test_setup_logger_sets_correct_loggin_handlers():
#     logging.getLogger().handlers.clear()
#     setup_logger()
#     handlers = logging.getLogger().handlers
#     stream_handler = handlers[1]
#     file_handler = handlers[0]
#     assert len(handlers) == 2
#     assert issubclass(logging.FileHandler, logging.StreamHandler)
#     assert isinstance(stream_handler, logging.StreamHandler)
#     assert not isinstance(stream_handler, logging.FileHandler)
#     assert isinstance(
#         file_handler,
#         (logging.FileHandler,logging.StreamHandler)
#     )


@pytest.mark.describe('gdpr_obfuscator')
@pytest.mark.it('Function passing correct kwargs to boto3.client')
@mock_aws
def test_Function_passing_correct_kwargs_to_boto3_client(csv_data):
    csv_data_s3, _ = csv_data

    # client = boto3.client('s3', region_name="us-east-1")
    # client.create_bucket(Bucket='test_bucket')
    # client.put_object(
    #     Body = csv_data_s3,
    #     Bucket = 'test_bucket',
    #     Key = 'some_folder/file.csv')
    
    s3_file = 's3://test_bucket/some_folder/file.csv'
    pii_fields = ['name', 'country']
    d = {}
    d['file_to_obfuscate'], d['pii_fields'] = s3_file, pii_fields
    json_str = json.dumps(d)
    gdpr_obfuscator(json_str, use_ssl=False, compression='GZIP')
   
