#Follow usage from read me to install module localy#


from gdpr.obfuscator import gdpr_obfuscator,setup_logger
from dotenv import load_dotenv, dotenv_values
from datetime import date, timedelta
from random import randint, choice
from botocore.exceptions import ClientError
import boto3
import csv
import io
import json
import sys
import logging

credentials = dotenv_values() if load_dotenv() else {}
setup_logger() if not logging.getLogger().hasHandlers() else None

# create some student csv data
headers = ['student_id', 'name', 'course', 'graduation_date', 'email_address']
course = ['Data Eng', 'ML Eng', 'Data Science']
people = [['Oliver', 'William', 'Jill', 'James', 'Charles', 'Albert'],
          ['Twist', 'Shakespeare', 'Smyth', 'Cook', 'Darwin', 'Einstein']]
graduation_date = [
    (date.today() - timedelta(randint(0, 100)))
    .strftime('%d-%m-%Y') for _ in range(3)
]
data = []
for _ in range(10):
    student = [
        str(randint(0, 1000)),
        name := choice(people[0]) + ' ' + choice(people[1]),
        choice(course),
        choice(graduation_date),
        f'{name[0].lower()}.{name.split(" ")[1].lower()}@emial.com'
    ]
    data.append(student)

csv_writer = csv.writer(csv_buffer := io.StringIO())
csv_writer.writerow(headers)
csv_writer.writerows(data)
# print data
print(csv_buffer.getvalue())

# create s3 bucket and upload data
try:
    s3 = boto3.client(
        's3',
        aws_access_key_id=credentials['aws_access_key_id'],
        aws_secret_access_key=credentials['aws_secret_access_key'],
        region_name=credentials['region_name']
        )
except KeyError:
    s3 = boto3.client('s3')
    
try:
    response = s3.create_bucket(
        Bucket=credentials['bucket'],
        CreateBucketConfiguration={
            'LocationConstraint': credentials['region_name']
        }
    )
except ClientError as error:
    if error.response['Error']['Code'] == 'BucketAlreadyOwnedByYou':
        pass
    else:
        raise
except KeyError:
    logger = logging.getLogger()
    logger.critical('Set bucket and region name with pyhon .env')
    raise

# upload data to bucket
print(f'uploading {sys.getsizeof(csv_buffer.getvalue())} bytes in s3')
s3.put_object(
    Body=csv_buffer.getvalue(),
    Bucket=credentials['bucket'],
    Key='some_data.csv'
)
# bucket is created and above created data is uploaded
file_to_obfuscate = f's3://{credentials["bucket"]}/some_data.csv'
pii_fields = ['name', 'email_address']

# read data from s3 and mask Pii fields
masked_data = gdpr_obfuscator(
    json.dumps(
        {
            'file_to_obfuscate': file_to_obfuscate,
            'pii_fields': pii_fields
        }
    )
)
# print out result
print(masked_data.decode())

# cleaning resources
s3.delete_object(
    Bucket=credentials['bucket'],
    Key='some_data.csv'
)
response = s3.delete_bucket(
    Bucket=credentials['bucket']
)
if response['ResponseMetadata']['HTTPStatusCode'] == 204:
    print('All S3 resources successfully deleted')
