#!/usr/bin/env bash

# (c) 2022 Amazon Web Services, Inc. or its affiliates. All Rights Reserved.
# This AWS Content is provided subject to the terms of the AWS Customer Agreement
# available at http://aws.amazon.com/agreement or other written agreement between
# Customer and Amazon Web Services, Inc.

OUTPUT_FILE="logs/scan-output.txt"
[[ ! -d logs ]] && mkdir "logs"
echo "Output File: "${OUTPUT_FILE}""
echo -n "" > "${OUTPUT_FILE}"

EXCLUDED_DIR="./.venv,./cdk.out"

echo "#-----------------------------------------------#" | tee -a "${OUTPUT_FILE}"
echo "#   Scanning Python code for Vulnerabilities     " | tee -a "${OUTPUT_FILE}"
echo "#-----------------------------------------------#" | tee -a "${OUTPUT_FILE}"
echo "Output File: "${OUTPUT_FILE}""
bandit -x "${EXCLUDED_DIR}" -r . | tee -a "${OUTPUT_FILE}"

echo "#-----------------------------------------------#" | tee -a "${OUTPUT_FILE}"
echo "#   Scanning Python depend for Vulnerabilities   " | tee -a "${OUTPUT_FILE}"
echo "#-----------------------------------------------#" | tee -a "${OUTPUT_FILE}"
safety check | tee -a "${OUTPUT_FILE}"
