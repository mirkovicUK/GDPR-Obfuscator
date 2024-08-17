from src.gdpr_obfuscation import get_bucket_and_key
import pytest

@pytest.mark.describe('get_bucket_and_key()')
@pytest.mark.it('fuction extract correct Bucket and key from S3 location')
def test_extract_correct_bucket_and_key_from_S3_location():
    s3_file = 's3://my_bucket/some_folder/file.txt'
    bucket, key = get_bucket_and_key(s3_file)
    assert bucket == 'my_bucket'
    assert key == 'some_folder/file.txt'

@pytest.mark.describe('get_bucket_and_key()')
@pytest.mark.it('fuction doesnot mutate data')
def test_func_doesnot_mutate_data():
    s3_file = 's3://my_bucket/some_folder/file.txt'
    get_bucket_and_key(s3_file)
    assert s3_file == 's3://my_bucket/some_folder/file.txt'