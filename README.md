# GDPR-Obfuscator
GDPR Obfuscation tool that can be integrated as a library module into a Python codebase.

## Table of Contents
- [About](#about)
- [Requirements](#requirements)
- [Tests_and_Coverage](#Tests_and_Coverage)
- [PEP8_and_security](#PEP8_and_security)
- [Assumptions_and_Prerequisites](#Assumptions_and_Prerequisites)
- [Usage](#Usage)
- [Example](#Example)

## About

This is a general-purpose Python tool to process data being ingested to AWS and intercept 
personally identifiable information (PII). All information stored by companies data
projects should be for bulk data analysis only. Consequently, there is a requirement
under [GDPR](https://ico.org.uk/media/for-organisations/guide-to-data-protection/guide-to-the-general-data-protection-regulation-gdpr-1-1.pdf/).
to ensure that all data containing information that can be used to identify an individual
should be anonymised.

**This obfuscation tool can be integrated as a library module into a Python codebase.**

**It is expected that the tool will be deployed within the _AWS_ account.**

**It is expected that the code will use the AWS SDK for Python (boto3).**

**It is expected that the code will use the PyArrow when handling parquet data.**

**The library is suitable for deployment on a platform within the AWS ecosystem, such as EC2, ECS, or Lambda.**

[Back to top](#top)

## Requirements

Ensure you have installed latest python version.

Local run
```
pip install -r ./requirements.txt
```
or clone repo and run
```
make requirements
```


[Back to top](#top)

## Tests_and_Coverage

Code is tested with [Pytest](https://docs.pytest.org/en/stable/), 
With test [coverage](https://coverage.readthedocs.io/en/7.6.1/) of %100<br>
See [tests](https://github.com/mirkovicUK/GDPR-Obfuscator/blob/main/test/test_gdpr_obfuscator.py) for more details.

[Back to top](#top)


## PEP8_and_security
Code is written in Python, <br>
PEP8 compliant, tested with [flake8](https://flake8.pycqa.org/en/latest/)<br>
As well as tested for security vulnerabilities:<br>
dependency vulnerability [safety](https://pypi.org/project/safety/),
security issues [bandit](https://bandit.readthedocs.io/en/latest/).

[Back to top](#top)

## Assumptions_and_Prerequisites

1. Data is stored in CSV, JSON, or parquet format in S3.<br>
This tool uses External Python libralies: <br>
    &emsp;:[Boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html) for managing AWS resources <br>
    &emsp;:[Botocore](https://botocore.amazonaws.com/v1/documentation/api/latest/index.html) for Error handling available witin AWS enviroment<br>
    &emsp;:[PyArrow](https://arrow.apache.org/docs/python/index.html) for parquet data handling
 

2. Fields containing GDPR-sensitive data are known and will
      be supplied in advance, see [Usage](#Usage)
    
3. Data records will be supplied with a primary key.

[Back to top](#top)

## Usage
pip install
```
pip install "git+https://github.com/mirkovicUK/GDPR-Obfuscator.git@pip"
```
Imports
```
from gdpr.obfuscator import gdpr_obfuscator
```
The tool should be invoked by sending a JSON string containing:<br>
    the S3 location of the required CSV,JSON or Parquet file for obfuscation<br> 
    and the names of the fields that are required to be obfuscated
<br><br>
JSON string format:<br>
{<br>
    "file_to_obfuscate": "s3://my_ingestion_bucket/new_data/file.csv",<br>
    "pii_fields": ["name", "surname", "email_address"]<br>
}
<br>

masked_data = gdpr_obfuscator(JSON: str)
<br><br>

## Example:<br>
Following example will create resources:[S3](https://aws.amazon.com/s3/),<br> and upload some data for testing, 
example is designed to clean all resources after execution , and to work with AWS Free Tier.

Example will expect AWS credentials in python .env file as this.

bucket='unique bucket name' : mandatory<br>
aws_access_key_id='Your account access key' :optional<br>
aws_secret_access_key= 'Your account secret access key' :optional<br>
region_name = 'region_name' :mandatory<br>

```
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

```


[Back to top](#top)
