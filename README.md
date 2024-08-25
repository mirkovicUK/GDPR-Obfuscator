# GDPR-Obfuscator
GDPR Obfuscation tool that can be integrated as a library module into a Python codebase

## Table of Contents
- [About](#about)
- [Requirements](#requirements)
- [Tests](#tests)
- [Assumptions_And_Prerequisites](#Assumptions_And_Prerequisites)
- [Usage](#Usage)

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

**The library is suitable for deployment on a platform within the AWS ecosystem, such as EC2, ECS, or Lambda.**

[Back to top](#top)

## Requirements

Ensure you have installed latest python version.

Check your python version
```
python --version
```

Local run
```
pip install -r .\requiremnets.txt
```

[Back to top](#top)

## Tests

Code is tested with [Pytest](https://docs.pytest.org/en/stable/)
```
pip install pytest
```
And decorated with [pytest-testdox](https://pypi.org/project/pytest-testdox/) for better readability.
```
pip install pytest-testdox
```
Run tests
```
pytest --disable-warnings --testdox -vvrP
```

[Back to top](#top)

## Assumptions_And_Prerequisites

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
pip instal
```
pip install github branch <-------->
```
Imports
```
from gdpr_obfuscation import gdpr_obfuscator
```
The tool will be invoked by sending a JSON string containing:<br>
    the S3 location of the required CSV file for obfuscation<br>
    the names of the fields that are required to be obfuscated
```
{
    "file_to_obfuscate": "s3://my_ingestion_bucket/new_data/file.csv","pii_fields": ["name", "surname", "email_address"]
}
```
```
gdpr_obfuscator(JSON:str)

:param: JSON (string) containing:
        the S3 location of the required CSV file for obfuscation
        the names of the fields that are required to be obfuscated

:return: bytestream representation of a file with obfuscated data fields
```

text in Aqua


[Back to top](#top)
