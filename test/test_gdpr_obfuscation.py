from src.gdpr_obfuscation import get_bucket_and_key,\
    get_data_type, UnsupportedData
import pytest

@pytest.mark.describe('get_bucket_and_key()')
@pytest.mark.it('Pure function')
def test_is_pure_function():
    s3_file = 's3://my_bucket/some_folder/file.txt'
    get_bucket_and_key(s3_file)
    assert s3_file == 's3://my_bucket/some_folder/file.txt'

@pytest.mark.describe('get_bucket_and_key()')
@pytest.mark.it('Extract correct bucket and key from S3 data location')
def test_extract_correct_bucket_and_key_from_S3_location():
    s3_file = 's3://my_bucket/some_folder/file.txt'
    bucket, key = get_bucket_and_key(s3_file)
    assert bucket == 'my_bucket'
    assert key == 'some_folder/file.txt'


@pytest.mark.describe('get_data_type()')
@pytest.mark.it('Pure function')
def test_pure_function():
    s3_file = 's3://my_bucket/some_folder/file.json'
    _, key = get_bucket_and_key(s3_file)
    data_type = get_data_type(key)
    assert key == 'some_folder/file.json'

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
        data_type = get_data_type(key)
    assert "Function supports only ['csv', 'json', 'parquet']" in str(excinfo.value)
    
