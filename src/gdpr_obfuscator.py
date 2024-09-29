from urllib.parse import urlparse
from botocore.exceptions import ClientError
from io import StringIO, BytesIO
import boto3
import csv
import json
import botocore
import sys
import pyarrow as pa
import pyarrow.parquet as pq
import logging


def gdpr_obfuscator(JSON: str) -> bytes:
    """
    Retrieve data ingested to AWS S3 and
    intercept personally identifiable information (PII).
    Return a byte stream object containing an exact
    copy of the input file but with the sensitive
    data replaced with obfuscated strings.

    Exept csv, json or parquet data file format
        JSON data format = [{data1}, {data2}...]

    Behaviour:
        csv data:
            :Will return empty str if receives empty data string
            :When detect pii_fild that is not present in csv fieldnames
            function will log with warning level,and disregard that pii_fild.

        json data:
            :Will return empty serilized list if data is empty or wrong format
            :When detect pii_fild that is not present in fieldnames
            function will log with warning level,and disregard that pii_fild.

        parquet data:
            :PyArrow Parquete engine write parquet file with default params.
            as per pyarrow.parquet.write_table function.Use kwargs to modify
            default behaviour.
            :When detect pii_fild that is not present in table function
            will log with warning level,and disregard that pii_fild.

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
    data: bytes = get_data(s3, bucket, key)

    if data_type == 'csv':
        masked = obfuscate_csv(data.decode(), pydict['pii_fields']).encode()
    elif data_type == 'json':
        masked = obfuscate_json(data, pydict['pii_fields']).encode()
    elif data_type == 'parquet':
        masked = obfuscate_parquet(data, pydict['pii_fields'])

    return masked


def get_bucket_and_key(s3_file_path: str) -> tuple[str, str]:
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


def get_data_type(key: str) -> str:
    """
    Extract data type from s3 object key
    Valid data type: csv, json, parquet

    :param: key (string) s3 object key
    :raise: UnsupportedData Exeption
        when data not csv, json or parquet
    :return: (string) indicating data type
    """
    allowed_types = ['csv', 'json', 'parquet']
    data_type = key.split('.')[-1]
    if data_type not in allowed_types:
        raise UnsupportedData(
            f'Function supports only {", ".join(allowed_types)} types.')
    return data_type


def get_data(client: botocore.client, bucket: str, key: str) -> bytes:
    """
    Retrieve data from s3

    :param: client s3 boto client
    :param: bucket (string) s3 bucket name
    :param: key (string) s3 data key
    :return: bytestream representation of a data
    """
    logger = logging.getLogger(__name__)
    logger.setLevel('CRITICAL')
    try:
        response = client.get_object(
            Bucket=bucket,
            Key=key)
        return response['Body'].read()
    except ClientError as error:
        if error.response['Error']['Code'] == 'NoSuchKey':
            logger.critical('NoSuchKey')
            pass
        elif error.response['Error']['Code'] == 'NoSuchBucket':
            logger.critical('NoSuchBucket')
            pass
        raise


def obfuscate_csv(data: str, pii_fields: list) -> str:
    """
    Pure function that mask pii_fields in data
    Behaviour:
        :Will return empty str if receives empty data string
        :When detect pii_fild that is not present in csv fieldnames
            function will log with warning level,and disregard that pii_fild.

    :param: data (string) csv data
    :param: pii_fields (list) of the names of the fields that to be obfuscated
    :return: (str) csv file with pii masked
    """
    logger = logging.getLogger(__name__)
    logger.setLevel('WARNING')
    dict_reader = csv.DictReader(StringIO(data))
    if dict_reader.fieldnames is None:
        return str()

    masked = []
    for row in dict_reader:
        for field in pii_fields:
            if field in dict_reader.fieldnames:
                row[field] = '***'
            else:
                logger.warning(
                    f'WARNING pii_field:\'{field}\' not in data...skipping...'
                )
        masked.append(row)

    masked_bufer = StringIO()
    writer = csv.DictWriter(masked_bufer, dict_reader.fieldnames)
    writer.writeheader()
    writer.writerows(masked)
    return masked_bufer.getvalue()


def obfuscate_json(data: bytes, pii_fields: list) -> str:
    """
    Pure function that mask pii_fields in data
    Behaviour:
        :Will return empty serilized list if data is empty or wrong format
            expect bytes data in format [{data1}, {data2} ...]
        :When detect pii_fild that is not present in fieldnames
            function will log with warning level,and disregard that pii_fild.

    :param: data (bytes) representation of json data
    :param: pii_fields (list) of the names of the fields that to be obfuscated
    :return: parsed object with pii masked
    """
    logger = logging.getLogger(__name__)
    logger.setLevel('WARNING')
    try:
        data_list = json.loads(data)
        keys = list(data_list[0].keys())
    except json.decoder.JSONDecodeError:
        data_list = []
    for x in data_list:
        for y in pii_fields:
            if y in keys:
                x[y] = '***'
            else:
                logger.warning(
                    f'WARNING pii_field:\'{y}\' not in data...skipping...'
                )
    return json.dumps(data_list)


def obfuscate_parquet(data: bytes, pii_fields: list, **kwargs) -> bytes:
    """
    Pure function that mask pii_fields in parquet data,
    function “delete” columns from a Parquet file by
    reading the data into memory, filter out the PII columns,
    and create a new Parquet file, while keeping column order.

    Default Behaviour:
        :PyArrow Parquete engine write parquet file with default params.
        as per pyarrow.parquet.write_table function.
        Use kwargs to modify default behaviour.

        :When detect pii_fild that is not present in table function
        will log with warning level,and disregard that pii_fild.

    :param: data (bytes) parquet data
    :param: pii_fields (list) of the names of the fields to be obfuscated
    :return: parquet data with pii masked
    """
    logger = logging.getLogger(__name__)
    logger.setLevel('WARNING')
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
            logger.warning(
                f'WARNING pii_field:\'{pii_field}\' not in data...skipping...'
            )
            pass
    pq.write_table(table, parquet_bufer := BytesIO(), **kwargs)
    return parquet_bufer.getvalue()


def setup_logger():
    """
    Function to setup FileHandler and StreamHandler logger
    log file: gdpr_obfuscator.log file
    logging level: WARNING
    """
    file_handler = logging.FileHandler(filename='gdpr_obfuscator.log')
    formatter = logging.Formatter(
        '[%(asctime)s] %(levelname)s [%(filename)s.%(funcName)s:%(lineno)d] %(message)s',
        datefmt='%a, %d %b %Y %H:%M:%S')
    file_handler.setFormatter(formatter)

    stdout_handler = logging.StreamHandler(stream=sys.stdout)
    formatter = logging.Formatter(
        '[%(asctime)s] %(levelname)s [%(filename)s.%(funcName)s:%(lineno)d] %(message)s',
        datefmt='%a, %d %b %Y %H:%M:%S')
    stdout_handler.setFormatter(formatter)

    handlers = [file_handler, stdout_handler]
    logging.basicConfig(
        level=logging.WARNING,
        handlers=handlers
    )
