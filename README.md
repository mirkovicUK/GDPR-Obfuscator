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
pip install from pip branch
```
pip install "git+https://github.com/mirkovicUK/GDPR-Obfuscator.git@pip"
```
Imports
```
from gdpr.obfuscator import gdpr_obfuscator
```
---------------------------------------------------------------------------------
Alternatively clone the repo:
```
git clone https://github.com/mirkovicUK/GDPR-Obfuscator.git
```
Import:
```
from src.gdpr_obfuscator import gdpr_obfuscator
```
---------------------------------------------------------------------------------
<br>

The tool should be invoked by sending a JSON string containing:<br>
    the S3 location of the required CSV,JSON or Parquet file for obfuscation<br> 
    and the names of the fields that are required to be obfuscated
<br><br>
JSON string format:<br>
{<br>
    "file_to_obfuscate": "s3://bucket_name/path_to_data/file.csv",<br>
    "pii_fields": ["name", "surname", "other_filelds_to_mask"]<br>
}
<br>

masked_data = gdpr_obfuscator(JSON: str)
<br><br>

## Example:<br>
Following [example]() will create resources:[S3](https://aws.amazon.com/s3/),<br> and upload some data for testing, 
example is designed to clean all resources after execution , and to work with AWS Free Tier.

Example will expect AWS credentials in python .env file as this.

bucket='unique bucket name' : mandatory<br>
aws_access_key_id='Your account access key' :optional<br>
aws_secret_access_key= 'Your account secret access key' :optional<br>
region_name = 'region_name' :mandatory<br>



[Back to top](#top)
