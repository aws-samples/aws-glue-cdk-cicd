#!/usr/bin/env bash

# (c) 2022 Amazon Web Services, Inc. or its affiliates. All Rights Reserved.
# This AWS Content is provided subject to the terms of the AWS Customer Agreement
# available at http://aws.amazon.com/agreement or other written agreement between
# Customer and Amazon Web Services, Inc.

OUTPUT_FILE="logs/lint-output.txt"
[[ ! -d logs ]] && mkdir "logs"
echo "Output File: "${OUTPUT_FILE}""
echo -n "" > "${OUTPUT_FILE}"

echo "#---------------------------------------------------#" | tee -a "${OUTPUT_FILE}"
echo "#               Linting Python Files                 " | tee -a "${OUTPUT_FILE}"
echo "#---------------------------------------------------#" | tee -a "${OUTPUT_FILE}"
find "." -name "*.py" | \
  grep -Ev ".venv|.pytest_cach|.tox|botocore|boto3|.aws" | \
  xargs pylint --rcfile .pylintrc | tee -a "${OUTPUT_FILE}"


echo "#---------------------------------------------------#" | tee -a "${OUTPUT_FILE}"
echo "#               Linting Cfn Files                    " | tee -a "${OUTPUT_FILE}"
echo "#---------------------------------------------------#" | tee -a "${OUTPUT_FILE}"
# https://github.com/aws-cloudformation/cfn-python-lint/issues/1265
IGNORED_FILES=()

ALL_CFN_TEMPLATES=$(grep -r '^AWSTemplateFormatVersion' . | cut -d: -f1)

for TEMPLATE in ${ALL_CFN_TEMPLATES}; do
    if [[ "${TEMPLATE}" == "${IGNORED_FILES[0]}" ]] || [[ "${TEMPLATE}" == "${IGNORED_FILES[1]}" ]]; then
        echo "Template Ignored: $TEMPLATE" | tee -a "${OUTPUT_FILE}"
        continue
    fi
    echo "Linting CloudFormation Template - ${TEMPLATE}" | tee -a "${OUTPUT_FILE}"
    rm -f /tmp/cfn-lint-output.txt
    cfn-lint -t "${TEMPLATE}" | tee -a "${OUTPUT_FILE}"
done
