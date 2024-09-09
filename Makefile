PROJECT_NAME = GDPR-OBFUSCATOR
REGION = eu-west-2
PYTHON_INTERPRETER = python
WD=$(shell pwd)
PYTHONPATH=${WD}
SHELL := /bin/bash
PROFILE = default
PIP:=pip

# Define utility variable to help calling Python from the virtual environment
ACTIVATE_ENV := source venv/bin/activate

# Execute python related functionalities from within the project's environment
define execute_in_env
	$(ACTIVATE_ENV) && $1
endef

## Build the environment requirements
requirements:
	$(call execute_in_env, $(PIP) install -r ./requirements.txt)

## Run the all unit tests
unit-test:
	$(call execute_in_env, PYTHONPATH=${PYTHONPATH} pytest -v)

## Run a single test
tests:
	$(call execute_in_env, PYTHONPATH=${PYTHONPATH} pytest --testdox -vvrP )