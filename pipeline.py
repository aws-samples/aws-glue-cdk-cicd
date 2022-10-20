# Copyright 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from typing import Dict
from constructs import Construct
from aws_cdk import Stack
from aws_cdk import (
    aws_codecommit as codecommit,
    aws_iam as iam
)
from aws_cdk.pipelines import (
    CodePipeline,
    CodePipelineSource,
    CodeBuildStep
)
from etl.infrastructure import CdkGlueBlogStage
from helper import create_archive


class PipelineCDKStack(Stack):

    def create_pipeline(self, config):
        # Archive need to be created since there's an issue with creating assets for codecommit
        # https://github.com/aws/aws-cdk/issues/19012
        archive_file = create_archive()

        codecommit_repo = codecommit.Repository(
            self, "Repository",
            repository_name=config['codepipeline']['repoName'],
            code=codecommit.Code.from_zip_file(
                file_path=archive_file,
                branch=config['codepipeline']['repoBranch']
            )
        )

        i_codecommit_repo = codecommit.Repository.from_repository_name(
            self, "ImportedBlogCode",
            codecommit_repo.repository_name
        )

        # https://aws.amazon.com/blogs/big-data/develop-and-test-aws-glue-version-3-0-jobs-locally-using-a-docker-container/
        pipeline = CodePipeline(
            self, "Pipeline",
            pipeline_name=config['codepipeline']['applicationName'],
            docker_enabled_for_synth=True,
            synth=CodeBuildStep(
                "PyTest_CdkSynth",
                input=CodePipelineSource.code_commit(
                    repository=i_codecommit_repo,
                    branch=config['codepipeline']['repoBranch']
                ),
                install_commands=[
                    "pip install -r requirements.txt",
                    "npm install -g aws-cdk",
                ],
                commands=[
                    "curl -qLk -o jq https://stedolan.github.io/jq/download/linux64/jq && chmod +x ./jq",
                    "curl -qL -o aws_credentials.json http://169.254.170.2/$AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
                    "AWS_ACCESS_KEY_ID=$(cat aws_credentials.json | jq -r '.AccessKeyId')",
                    "AWS_SECRET_ACCESS_KEY=$(cat aws_credentials.json | jq -r '.SecretAccessKey')",
                    "AWS_SESSION_TOKEN=$(cat aws_credentials.json | jq -r '.Token')",
                    "AWS_REGION='us-east-1'",
                    "aws configure --profile covid19blogpost set output \"json\"",
                    "aws configure --profile covid19blogpost set region \"us-east-1\"",
                    "aws configure --profile covid19blogpost set aws_access_key_id ${AWS_ACCESS_KEY_ID}",
                    "aws configure --profile covid19blogpost set aws_secret_access_key ${AWS_SECRET_ACCESS_KEY}",
                    "aws configure --profile covid19blogpost set aws_session_token ${AWS_SESSION_TOKEN}",
                    "sed -i 's/profile //g' ~/.aws/config",
                    "AWS_PROFILE='covid19blogpost'",
                    "WORKSPACE_LOCATION=$(pwd)",
                    "TABLE_COUNT=$(aws glue get-tables --database-name covid19db --profile covid19blogpost 2>/dev/null | jq -r '.TableList[].Name' | wc -l)",
                    "echo TABLE_COUNT : $TABLE_COUNT",
                    "docker pull amazon/aws-glue-libs:glue_libs_3.0.0_image_01",
                    "if [ $TABLE_COUNT -ge 4 ]; then docker run -v ~/.aws:/home/glue_user/.aws -v $WORKSPACE_LOCATION:/home/glue_user/workspace/"
                    " -e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY"
                    " -e AWS_SESSION_TOKEN=$AWS_SESSION_TOKEN -e AWS_REGION=$AWS_REGION -e AWS_PROFILE=$AWS_PROFILE"  
                    " -e DISABLE_SSL=true --rm -p 4040:4040 -p 18080:18080"
                    " --name glue_pytest amazon/aws-glue-libs:glue_libs_3.0.0_image_01 -c \"python3 -m pytest\"  ; fi",
                    "cdk synth -c stage=${stage}"
                ],
                env={
                    "stage": "default",
                    "SCRIPT_FILE_NAME": "j_neherlab_denorm_etl.py",
                    "UNIT_TEST_FILE_NAME": "test_j_neherlab_denorm_etl.py",
                },
                role_policy_statements=[
                    iam.PolicyStatement(
                        actions=[
                            "glue:GetPartitions",
                            "glue:GetTable",
                            "glue:GetTables",
                            "s3:ListBucket",
                            "s3:GetObject"
                        ],
                        resources=["*"],
                    )
                ]
            )
        )
        pipeline.add_stage(CdkGlueBlogStage(self, 'CdkGlueStage', config=config))
        pipeline.node.add_dependency(codecommit_repo)

    def __init__(self, scope: Construct, construct_id: str, config: Dict, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.create_pipeline(
            config=config,
        )
