# Copyright 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import sys
import json
import boto3
from awsglue.utils import getResolvedOptions

# Read params from commandline
args = getResolvedOptions(sys.argv, ['WORKFLOW_NAME', 'WORKFLOW_RUN_ID'])

workflow_name = args['WORKFLOW_NAME']
workflow_run_id = args['WORKFLOW_RUN_ID']

# Initiate Events client
events = boto3.client('events')

detail = json.dumps({'workflowName': workflow_name, 'runId': workflow_run_id, 'state': 'ENDED'})

# Submit event to PutEvents API
response = events.put_events(
    Entries=[
        {
            'Detail': detail,
            'DetailType': 'Glue Workflow State Change',
            'Source': 'Covid19 workflow'
        }
    ]
)
response_string = json.dumps(response)
