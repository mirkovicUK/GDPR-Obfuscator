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

## Tests

Code is tested with [Pytest](https://docs.pytest.org/en/stable/)
```
pip install pytest
```
And decorated with [pytest-testdox](https://pypi.org/project/pytest-testdox/) for better readability.
```
pip install pytest-testdox
```

## Assumptions_And_Prerequisites

1. Data is stored in CSV, JSON, or parquet format in S3.

    CSV & JSON data masking rely on external Python libralies 
        :[Boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html) for managing AWS resources 
        :[Botocore](https://botocore.amazonaws.com/v1/documentation/api/latest/index.html) for Error handling available witin AWS enviroment
    Parquer data masking rely on external Python libralies
    :TBC 

2. Fields containing GDPR-sensitive data are known and will
      be supplied in advance, see [Usage](#Usage)
    
3. Data records will be supplied with a primary key.




[Back to top](#top)
