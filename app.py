#!/usr/bin/env python3

# Copyright 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import yaml
from aws_cdk import core
from pipeline import PipelineCDKStack

app = core.App()

# Get target stage from cdk context
config_file = app.node.try_get_context('config')

# Load stage config and set cdk environment
if config_file:
    configFilePath = config_file
else:
    configFilePath = "./default-config.yaml"

with open(configFilePath, 'r', encoding="utf-8") as f:
    config = yaml.load(f, Loader=yaml.SafeLoader)

env = core.Environment(region=config["awsAccount"]["awsRegion"])

# Initiating the CodePipeline stack
PipelineCDKStack(
    app,
    "PipelineCDKStack",
    description='This stack creates a CICD pipeline using  AWS CodeCommit, AWS CodeBuild and AWS CodePipeline.',
    config=config,
    env=env,
    stack_name=config["codepipeline"]["pipelineStackName"]
)

app.synth()
