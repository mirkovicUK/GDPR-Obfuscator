from urllib.parse import urlparse
from botocore.exceptions import ClientError
from io import StringIO, BytesIO
import boto3, csv, json,botocore, sys
import pyarrow as pa
import pyarrow.parquet as pq
import logging
try:
    import colorlog
except ModuleNotFoundError:
    logging.getLogger().info('pip install colorlog for log formatred with colors')
    pass


def gdpr_obfuscator(JSON:str) -> bytes:
    """
    Retrieve data ingested to AWS S3 and
    intercept personally identifiable information (PII).
    Return a byte stream object containing an exact 
    copy of the input file but with the sensitive
    data replaced with obfuscated strings.

    Exepts csv, json or parquet data file format
        JSON data format = [{data1}, {data2}...]
    
    :param: JSON (string) containing:
    "file_to_obfuscate" key:
        the S3 location of the required file for obfuscation
    "pii_fields" key:
        the list with names of the fields that are required to be obfuscated
    
    example:
    {
        "file_to_obfuscate": "s3://my_ingestion_bucket/new_data/file1.csv",
        "pii_fields": ["name", "email_address"]
    }

    :return: bytestream representation of a file with obfuscated data fields
    """
    setup_logger() if not logging.getLogger().hasHandlers() else None
    pydict = json.loads(JSON)
    bucket, key = get_bucket_and_key(pydict['file_to_obfuscate'])
    data_type = get_data_type(key)
    s3 = boto3.client('s3')
    data:bytes = get_data(s3, bucket, key)
    if data_type == 'csv':
        masked = obfuscate_csv(data.decode(), pydict['pii_fields']).encode()
    elif data_type == 'json':
        masked = obfuscate_json(data, pydict['pii_fields']).encode()
    return masked

def get_bucket_and_key(s3_file_path:str) -> tuple[str, str]:
    """
    Extract S3 bucket name and key of the object from s3 file path
    expect file path like:
        's3://backet_name/folder1/../file.txt'
    
    :param: s3_file_path (str) path to data on AWS s3
    :return: Tuple[str, str] Bucket, Key 
    """
    o = urlparse(s3_file_path, allow_fragments=False)
    return o.netloc, o.path.lstrip('/')

class UnsupportedData(Exception):
        """Traps error where data is not supported"""
        pass

def get_data_type(key:str) -> str:
    """
    Extract data type from s3 object key
    Valid data type: csv, json, parquet
    
    :param: key (string) s3 object key
    :raise: UnsupportedData Exeption 
        when data not csv, json or parquet
    :return: (string) indicating data type
    """
    allowed_types = ['csv', 'json', 'parquet']
    if (data_type := key.split('.')[-1]) not in allowed_types:
        raise UnsupportedData(f'Function supports only {", ".join(allowed_types)} types.')
    return data_type

def get_data(client:botocore.client, bucket:str, key:str) -> bytes:
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
            # log to cloud watch?
            pass
        elif error.response['Error']['Code'] == 'NoSuchBucket':
            # Confirm expected required behaviour
            # log to cloud watch?
            pass
        raise

def obfuscate_csv(data:str, pii_fields:list) -> str:
    """
    Pure function that mask pii_fields in data
    Behaviour: 
        :Will return empty str if data is empty
        :If pii_fields contain fields different than data headers
            function will update data header to represent this

    :param: data (string) csv data
    :param: pii_fields (list) of the names of the fields that to be obfuscated
    :return: csv file with pii masked
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

def obfuscate_json(data:bytes, pii_fields:list) -> str:
    """
    Pure function that mask pii_fields in data
    Behaviour: 
        :Will return empty serilized list if data is empty or wrong format
            expect bytes data in format [{data1}, {data2} ...]
        :If pii_fields contain fields different than dict key
            function will update data vith new key:value to represent this

    :param: data (bytes) representation of json data
    :param: pii_fields (list) of the names of the fields that to be obfuscated
    :return: parsed object with pii masked
    """
    try:
        data_list = json.loads(data)
    except json.decoder.JSONDecodeError:
        data_list = []
    for x in data_list:
        for y in pii_fields:
            x[y] = '***'
    return json.dumps(data_list)

def obfuscate_parquet(data:bytes, pii_fields:list, **kwargs) -> bytes:
    """
    Pure function that mask pii_fields in parquet data,
    function “delete” columns from a Parquet file by 
    reading the data into memory, filter out the PII columns,
    and create a new Parquet file, while keeping column order.

    Default Behaviour:
        :PyArrow Parquete engine default params as per
        pyarrow.parquet.write_table function.
        Use kwargs to modify default behaviour.

        :When detect pii_fild that is not present in table function
        will log to cloud wach on warning level,and disregard that pii_fild.

    :param: data (bytes) parquet data
    :param: pii_fields (list) of the names of the fields to be obfuscated
    :return: parquet data with pii masked
    """
    table = pq.read_table(pa.BufferReader(data))
    num_rows = table.num_rows
    column_names = table.column_names
    for pii_field in pii_fields:
        try:
            table = table.drop_columns(pii_field)
            table = table.add_column(
                column_names.index(pii_field),
                pii_field,
                [['***' for _ in range(num_rows)]]
            )
        except KeyError:
            #write loger to cloud watch here
            pass
    pq.write_table(table,parquet_bufer:=BytesIO(), **kwargs)
    return parquet_bufer.getvalue()
    
def setup_logger():
    """
    Function create logger that output to stdout and gdpr_obfuscator.log file
    logging level WARNING
    """
    file_handler = logging.FileHandler(filename='gdpr_obfuscator.log')
    formatter = logging.Formatter(
        '[%(asctime)s] %(levelname)s [%(filename)s.%(funcName)s:%(lineno)d] %(message)s'
        ,datefmt='%a, %d %b %Y %H:%M:%S')
    file_handler.setFormatter(formatter)

    stdout_handler = logging.StreamHandler(stream=sys.stdout)
    try:
        stdout_handler.setFormatter(colorlog.ColoredFormatter(
            '%(log_color)s [%(asctime)s] %(levelname)s [%(filename)s.%(funcName)s: %(lineno)d] %(message)s',
            datefmt='%a, %d %b %Y %H:%M:%S'))
    except NameError:
        formatter = logging.Formatter(
        '[%(asctime)s] %(levelname)s [%(filename)s.%(funcName)s: %(lineno)d] %(message)s',
        datefmt='%a, %d %b %Y %H:%M:%S')
        stdout_handler.setFormatter(formatter)

    handlers = [file_handler, stdout_handler]
    logging.basicConfig(
        level=logging.WARNING,
        handlers=handlers
    )



if __name__ == "__main__":
    gdpr_obfuscator(json.dumps({'file_to_obfuscate':'s', 'l':[]}))