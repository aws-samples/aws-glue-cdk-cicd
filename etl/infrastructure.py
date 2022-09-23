# Copyright 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import datetime
from typing import Dict
from aws_cdk import (
    core,
    aws_glue as glue,
    aws_iam as iam,
    aws_s3 as s3,
    aws_s3_deployment as s3_deployment
)


class CdkGlueBlogStack(core.Stack):
    def __init__(self, scope: core.Construct, construct_id: str, config: Dict, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        glue_security_configuration = self.create_glue_securityconf(config)
        roles = self.create_glue_iamrole(config, glue_security_configuration)
        self.create_script_bucket(config, glue_security_configuration, roles['lambda_role'])
        raw_database = self.create_glue_database(config)

        # Creating Crawler for Neherlab Case Counts
        neherlab_case_cnt_s3_trg_path = config["dataSourcePath"]["crawlerSourceS3PathNeherlabCaseCnt"]
        neherlab_case_cnt_crawler_name = config["glueJobConfig"]["neherlabCaseCntCrawlerName"]
        neherlab_case_cnt_crawler = self.create_glue_crawlers(
            glue_role=roles['glue_role'],
            glue_security_configuration=glue_security_configuration,
            raw_database=raw_database,
            s3_target_path=neherlab_case_cnt_s3_trg_path,
            crawler_name=neherlab_case_cnt_crawler_name
        )

        # Creating Crawler for Neherlab Country Codes
        neherlab_cntry_cd_s3_trg_path = config["dataSourcePath"]["crawlerSourceS3PathNeherlabCntryCd"]
        neherlab_cntry_cd_crawler_name = config["glueJobConfig"]["neherlabCntryCdCrawlerName"]
        neherlab_cntry_cd_crawler = self.create_glue_crawlers(
            glue_role=roles['glue_role'],
            glue_security_configuration=glue_security_configuration,
            raw_database=raw_database,
            s3_target_path=neherlab_cntry_cd_s3_trg_path,
            crawler_name=neherlab_cntry_cd_crawler_name
        )

        # Creating Crawler for Neherlab ICU Capacity
        neherlab_icu_cap_s3_trg_path = config["dataSourcePath"]["crawlerSourceS3PathNeherlabICUCap"]
        neherlab_icu_cap_crawler_name = config["glueJobConfig"]["neherlabICUCapCrawlerName"]
        neherlab_icu_cap_crawler = self.create_glue_crawlers(
            glue_role=roles['glue_role'],
            glue_security_configuration=glue_security_configuration,
            raw_database=raw_database,
            s3_target_path=neherlab_icu_cap_s3_trg_path,
            crawler_name=neherlab_icu_cap_crawler_name
        )

        # Creating Crawler for Neherlab Population
        neherlab_population_s3_trg_path = config["dataSourcePath"]["crawlerSourceS3PathNeherlabPopulation"]
        neherlab_population_crawler_name = config["glueJobConfig"]["neherlabPopulationCrawlerName"]
        neherlab_population_crawler = self.create_glue_crawlers(
            glue_role=roles['glue_role'],
            glue_security_configuration=glue_security_configuration,
            raw_database=raw_database,
            s3_target_path=neherlab_population_s3_trg_path,
            crawler_name=neherlab_population_crawler_name
        )

        # Glue Temp dir S3 path
        glue_script_base_path = "s3://" + config["glueJobConfig"]["s3BucketNameScript"] + "-" + str(
            self.account) + "-" + config["awsAccount"]["awsRegion"] + "/" + config["glueJobConfig"]["scriptKeyPrefix"]
        glue_temp_s3_path = "s3://" + config["glueJobConfig"]["s3BucketNameScript"] + "-" + str(
            self.account) + "-" + config["awsAccount"]["awsRegion"] + "/" + config["dataSourcePath"]["s3PathGlueTemp"]

        # Creating Glue Python job for workflow Start event
        emit_start_glue_job_name = config["glueJobConfig"]["emitStartGlueJobName"]
        emit_start_script_name = config["glueJobConfig"]["emitStartScriptName"]
        emit_start_script_path = glue_script_base_path + "/" + emit_start_script_name
        emit_start_arguments = {
            '--job-bookmark-option': 'job-bookmark-disable',
            '--job-language': "python",
            '--TempDir': glue_temp_s3_path
        }
        emit_start_job_type = config["glueJobConfig"]["jobTypePythonShell"]
        emit_start_job_capacity = config["glueJobConfig"]["emitStartGlueJobCapacity"]
        emit_start_glue_job = self.create_glue_jobs(
            glue_role=roles['glue_role'],
            glue_security_configuration=glue_security_configuration,
            glue_job_name=emit_start_glue_job_name,
            script_path=emit_start_script_path,
            arguments=emit_start_arguments,
            job_type=emit_start_job_type,
            capcacity=emit_start_job_capacity
        )

        # S3 path for the Glue output
        glue_output_path = "s3://" + config["dataSourcePath"]["s3BucketNameNeherlabDenorm"] + "-" + str(self.account)\
                           + "-" + config["awsAccount"]["awsRegion"]

        # Creating Glue ETL job for Neherlab data denomalization
        neherlab_denorm_glue_job_name = config["glueJobConfig"]["neherlabDenormGlueJobName"]
        neherlab_denorm_script_name = config["glueJobConfig"]["neherlabDenormGlueELTScriptName"]
        neherlab_denorm_script_path = glue_script_base_path + "/" + neherlab_denorm_script_name
        neherlab_denorm_arguments = {
            '--DATABASENAME': config["glueJobConfig"]["databaseName"],
            '--NHRLAB_CNTRY_CD_TBL': config["glueJobConfig"]["neherlabCntryCdTableName"],
            '--NHRLAB_CS_CNT_TBL': config["glueJobConfig"]["neherlabCaseCntTableName"],
            '--NHRLAB_ICU_CAP_TBL': config["glueJobConfig"]["neherlabICUCapTableName"],
            '--NHRLAB_PPL_TBL': config["glueJobConfig"]["neherlabPopulationTableName"],
            '--OUTPUT_TBL' : config["glueJobConfig"]["neherlabDenormTableName"],
            '--S3_OUTPUT_PATH' : glue_output_path,
            '--job-language' : 'python',
            '--job-bookmark-option' : 'job-bookmark-disable',
            '--enable-continuous-cloudwatch-log': 'true',
            '--enable-glue-datacatalog': 'true',
            '--TempDir': glue_temp_s3_path
        }
        neherlab_denorm_job_type = config["glueJobConfig"]["jobTypeSpark"]
        neherlab_denorm_job_capacity = config["glueJobConfig"]["neherlabDenormGlueJobCapacity"]
        neherlab_denorm_glue_job = self.create_glue_jobs(
            glue_role=roles['glue_role'],
            glue_security_configuration=glue_security_configuration,
            glue_job_name=neherlab_denorm_glue_job_name,
            script_path=neherlab_denorm_script_path,
            arguments=neherlab_denorm_arguments,
            job_type=neherlab_denorm_job_type,
            capcacity=neherlab_denorm_job_capacity
        )

        # Creating Glue Python job for End of the workflow event
        emit_end_glue_job_name = config["glueJobConfig"]["emitEndGlueJobName"]
        emit_end_script_name = config["glueJobConfig"]["emitStartScriptName"]
        emit_end_script_path = glue_script_base_path + "/" + emit_end_script_name
        emit_end_arguments = {
            '--job-bookmark-option': 'job-bookmark-disable',
            '--job-language': "python",
            '--TempDir': glue_temp_s3_path
        }
        emit_end_job_type = config["glueJobConfig"]["jobTypePythonShell"]
        emit_end_job_capacity = config["glueJobConfig"]["emitEndGlueJobCapacity"]
        emit_end_glue_job = self.create_glue_jobs(
            glue_role=roles['glue_role'],
            glue_security_configuration=glue_security_configuration,
            glue_job_name=emit_end_glue_job_name,
            script_path=emit_end_script_path,
            arguments=emit_end_arguments,
            job_type=emit_end_job_type,
            capcacity=emit_end_job_capacity
        )

        # Creating the workflow
        self.create_glue_workflow(
            config,
            start_job=emit_start_glue_job,
            neherlab_case_cnt_crawler=neherlab_case_cnt_crawler,
            neherlab_cntry_cd_crawler=neherlab_cntry_cd_crawler,
            neherlab_icu_cap_crawler=neherlab_icu_cap_crawler,
            neherlab_population_crawler=neherlab_population_crawler,
            neherlab_denorm_glue_job=neherlab_denorm_glue_job,
            emit_end_glue_job=emit_end_glue_job
        )

    # Adding Glue Workflow
    def create_glue_workflow(self, config, **kwargs):
        """
        Create AWS Glue Workflow
        """
        covid19_workflow = glue.CfnWorkflow(
            self,
            config["glueJobConfig"]["glueWorkflowName"],
            name=config["glueJobConfig"]["glueWorkflowName"]
        )

        # Adding the first Trigger to start the workflow
        start_trigger = glue.CfnTrigger(
            self,
            config["glueJobConfig"]["startJobTriggerName"],
            name=config["glueJobConfig"]["startJobTriggerName"],
            type='SCHEDULED',
            schedule='cron(0 10 1 * ? *)',
            start_on_creation=True,
            workflow_name=covid19_workflow.name,
            actions=[
                glue.CfnTrigger.ActionProperty(job_name=kwargs["start_job"].name)
            ]
        )

        start_trigger.add_depends_on(kwargs["start_job"])

        # Adding the Trigger to start the crawler for Neherlab Case Count
        neherlab_case_cnt_crawler_trigger = glue.CfnTrigger(
            self,
            config["glueJobConfig"]["neherlabCaseCntCrawlerTriggerName"],
            name=config["glueJobConfig"]["neherlabCaseCntCrawlerTriggerName"],
            type='CONDITIONAL',
            start_on_creation=True,
            workflow_name=covid19_workflow.name,
            actions=[
                glue.CfnTrigger.ActionProperty(
                    crawler_name=kwargs["neherlab_case_cnt_crawler"].name)
            ],
            predicate=glue.CfnTrigger.PredicateProperty(
                logical='ANY',
                conditions=[glue.CfnTrigger.ConditionProperty(
                    job_name=kwargs["start_job"].name,
                    logical_operator='EQUALS',
                    state='SUCCEEDED'
                )]
            )
        )

        neherlab_case_cnt_crawler_trigger.add_depends_on(start_trigger)

        # Adding the Trigger to start the crawler for Neherlab Country Codes
        neherlab_cntry_cd_crawler_trigger = glue.CfnTrigger(
            self,
            config["glueJobConfig"]["neherlabCntryCdCrawlerTriggerName"],
            name=config["glueJobConfig"]["neherlabCntryCdCrawlerTriggerName"],
            type='CONDITIONAL',
            start_on_creation=True,
            workflow_name=covid19_workflow.name,
            actions=[
                glue.CfnTrigger.ActionProperty(
                    crawler_name=kwargs["neherlab_cntry_cd_crawler"].name)
            ],
            predicate=glue.CfnTrigger.PredicateProperty(
                logical='ANY',
                conditions=[glue.CfnTrigger.ConditionProperty(
                    job_name=kwargs["start_job"].name,
                    logical_operator='EQUALS',
                    state='SUCCEEDED'
                )]
            )
        )

        neherlab_cntry_cd_crawler_trigger.add_depends_on(start_trigger)

        # Adding the Trigger to start the crawler for Neherlab ICU Capacity
        neherlab_icu_cap_crawler_trigger = glue.CfnTrigger(
            self,
            config["glueJobConfig"]["neherlabICUCapCrawlerTriggerName"],
            name=config["glueJobConfig"]["neherlabICUCapCrawlerTriggerName"],
            type='CONDITIONAL',
            start_on_creation = True,
            workflow_name=covid19_workflow.name,
            actions=[
                glue.CfnTrigger.ActionProperty(
                    crawler_name=kwargs["neherlab_icu_cap_crawler"].name)
            ],
            predicate=glue.CfnTrigger.PredicateProperty(
                logical='ANY',
                conditions=[glue.CfnTrigger.ConditionProperty(
                    job_name=kwargs["start_job"].name,
                    logical_operator='EQUALS',
                    state='SUCCEEDED'
                )]
            )
        )

        neherlab_icu_cap_crawler_trigger.add_depends_on(start_trigger)

        # Adding the Trigger to start the crawler for Neherlab Population
        neherlab_population_crawler_trigger = glue.CfnTrigger(
            self,
            config["glueJobConfig"]["neherlabPopulationCrawlerTriggerName"],
            name=config["glueJobConfig"]["neherlabPopulationCrawlerTriggerName"],
            type='CONDITIONAL',
            start_on_creation = True,
            workflow_name=covid19_workflow.name,
            actions=[
                glue.CfnTrigger.ActionProperty(
                    crawler_name=kwargs["neherlab_population_crawler"].name)
            ],
            predicate=glue.CfnTrigger.PredicateProperty(
                logical='ANY',
                conditions=[glue.CfnTrigger.ConditionProperty(
                    job_name=kwargs["start_job"].name,
                    logical_operator='EQUALS',
                    state='SUCCEEDED'
                )]
            )
        )

        neherlab_population_crawler_trigger.add_depends_on(start_trigger)

        # Adding the Trigger to start the Job for Neherlab data denomalization
        neherlab_denorm_glue_job_trigger = glue.CfnTrigger(
            self,
            config["glueJobConfig"]["neherlabDenormGlueJobTriggerName"],
            name=config["glueJobConfig"]["neherlabDenormGlueJobTriggerName"],
            type='CONDITIONAL',
            start_on_creation = True,
            workflow_name=covid19_workflow.name,
            actions=[
                glue.CfnTrigger.ActionProperty(
                    job_name=kwargs["neherlab_denorm_glue_job"].name)
            ],
            predicate=glue.CfnTrigger.PredicateProperty(
                logical='AND',
                conditions=[
                    glue.CfnTrigger.ConditionProperty(
                        crawler_name=kwargs["neherlab_case_cnt_crawler"].name,
                        crawl_state='SUCCEEDED',
                        logical_operator='EQUALS'
                    ),
                    glue.CfnTrigger.ConditionProperty(
                        crawler_name=kwargs["neherlab_cntry_cd_crawler"].name,
                        crawl_state='SUCCEEDED',
                        logical_operator='EQUALS'
                    ),
                    glue.CfnTrigger.ConditionProperty(
                        crawler_name=kwargs["neherlab_icu_cap_crawler"].name,
                        crawl_state='SUCCEEDED',
                        logical_operator='EQUALS'
                    ),
                    glue.CfnTrigger.ConditionProperty(
                        crawler_name=kwargs["neherlab_population_crawler"].name,
                        crawl_state='SUCCEEDED',
                        logical_operator='EQUALS'
                    )
                ]
            )
        )

        neherlab_denorm_glue_job_trigger.add_depends_on(neherlab_case_cnt_crawler_trigger)
        neherlab_denorm_glue_job_trigger.add_depends_on(neherlab_cntry_cd_crawler_trigger)
        neherlab_denorm_glue_job_trigger.add_depends_on(neherlab_icu_cap_crawler_trigger)
        neherlab_denorm_glue_job_trigger.add_depends_on(neherlab_population_crawler_trigger)

        # Adding the Trigger to start the Job for Workflow End Emit Job
        end_job_trigger = glue.CfnTrigger(
            self,
            config["glueJobConfig"]["endJobTriggerName"],
            name=config["glueJobConfig"]["endJobTriggerName"],
            type='CONDITIONAL',
            start_on_creation=True,
            workflow_name=covid19_workflow.name,
            actions=[
                glue.CfnTrigger.ActionProperty(job_name=kwargs["emit_end_glue_job"].name)
            ],
            predicate=glue.CfnTrigger.PredicateProperty(
                conditions=[glue.CfnTrigger.ConditionProperty(
                    job_name=kwargs["neherlab_denorm_glue_job"].name,
                    logical_operator='EQUALS',
                    state='SUCCEEDED'
                )]
            )
        )

        end_job_trigger.add_depends_on(neherlab_denorm_glue_job_trigger)

    # Method to create the Glue Security Config
    def create_glue_securityconf(self, config):
        """
        Create AWS Glue Security Config
        """
        # Security Configuration
        glue_security_configuration = glue.SecurityConfiguration(
            self,
            "MySecurityConfiguration",
            security_configuration_name=config["glueJobConfig"]["securityConfigurationName"],
            s3_encryption={"mode": glue.S3EncryptionMode.KMS}
        )

        glue_security_configuration.s3_encryption_key.add_to_resource_policy(
            statement=iam.PolicyStatement(
                sid="Allow access for key Administration",
                actions=['kms:*'],
                resources=['*'],
                principals=[iam.AccountPrincipal(account_id=self.account)]
            )
        )

        today = datetime.datetime.now()
        date_time = today.strftime("%m%d%Y%H%M%S")
        kms_key_alias = f'{config["glueJobConfig"]["glueKSMKeyName"]}-{date_time}'
        glue_security_configuration.s3_encryption_key.add_alias(kms_key_alias)
        return glue_security_configuration

    # define the IAM role for Glue Crawler and Jobs
    def create_glue_iamrole(self, config, glue_security_configuration):
        """
        Creates AWS Cloud IAM Role/Policy
        """
        roles = {}
        roles['glue_role'] = iam.Role(
            self,
            config["iam"]["glueRoleName"],
            assumed_by=iam.ServicePrincipal('glue.amazonaws.com'),
            role_name=config["iam"]["glueRoleName"],
            managed_policies=[
                iam.ManagedPolicy.from_managed_policy_arn(
                    self,
                    'glue-glue-policy',
                    'arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole'
                )
            ]
        )
        # Adding inline policy to GLue role to access the KMS key
        roles['glue_role'].add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                resources=[
                    glue_security_configuration.s3_encryption_key.key_arn
                ],
                actions=[
                    'kms:DescribeKey',
                    'kms:Encrypt',
                    'kms:Decrypt',
                    'kms:GenerateDataKey'
                ]
            )
        )

        # Adding inline policy to GLue role to access the S3 Bucket
        roles['glue_role'].add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                resources=[
                    'arn:aws:s3:::'+config["glueJobConfig"]["s3BucketNameScript"] + "-" + str(self.account) + "-" + config["awsAccount"]["awsRegion"]+'/*',
                    'arn:aws:s3:::'+config["dataSourcePath"]["s3BucketNameNeherlabDenorm"] + "-" + str(self.account) + "-" + config["awsAccount"]["awsRegion"]+'/*',
                    'arn:aws:s3:::'+config["dataSourcePath"]["neherlabSourceS3Bucket"] + '/*'
                    ],
                actions=[
                    's3:ListBucket',
                    's3:GetObject',
                    's3:PutObject',
                    's3:DeleteObject'
                ]
            )
        )

        # Adding inline policy to GLue role to access the Event Bridge
        roles['glue_role'].add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                resources=[
                    'arn:aws:events:'+config["awsAccount"]["awsRegion"]+':'+str(self.account)+':event-bus/default'
                    ],
                actions=[
                    'events:PutEvents'
                ]
            )
        )

        # Adding inline policy to GLue role to access the KMS key
        roles['glue_role'].add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                resources=['*'],
                actions=[
                    'sts:AssumeRole'
                ]
            )
        )

        # Adding Name tag to the resource
        core.Tags.of(roles['glue_role']).add('Name', config["iam"]["glueRoleName"])

        roles['lambda_role'] = iam.Role(
            self,
            config["iam"]["lambdaRoleName"],
            assumed_by=iam.ServicePrincipal('lambda.amazonaws.com'),
            role_name=config["iam"]["lambdaRoleName"],
            managed_policies=[
                    iam.ManagedPolicy.from_managed_policy_arn(
                        self,
                        'lambda-Basic-Execution-policy',
                        'arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole')
            ],
        )
        return roles

    # Creating the S3 bucket to hold the ETL scripts
    def create_script_bucket(self, config, glue_security_configuration, bucket_role):
        """
        Create S3 bucket for ETL Scripts
        """
        # Creating the S3 bucket for Script
        source_bucket = s3.Bucket(
            self,
            "source-script-bucket",
            encryption=s3.BucketEncryption.KMS,
            removal_policy=core.RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption_key=glue_security_configuration.s3_encryption_key,
            bucket_name=config["glueJobConfig"]["s3BucketNameScript"] + "-" + str(self.account) + "-" + config["awsAccount"]["awsRegion"]
        )

        # Creating the S3 bucket for Output Data
        s3.Bucket(
            self,
            "output-data-bucket",
            encryption=s3.BucketEncryption.KMS,
            removal_policy=core.RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption_key=glue_security_configuration.s3_encryption_key,
            bucket_name=config["dataSourcePath"]["s3BucketNameNeherlabDenorm"] + "-" + str(self.account) + "-" + config["awsAccount"]["awsRegion"]
        )

        # Moving file from local code base to S3 bucket
        s3_deployment.BucketDeployment(
            self,
            "deploy-etl-script",
            destination_bucket=source_bucket,
            role=bucket_role,
            destination_key_prefix=config["glueJobConfig"]["scriptKeyPrefix"],
            sources=[s3_deployment.Source.asset('./src')]
        )

    # Creating the Base Databse
    def create_glue_database(self, config):
        """
        Creates AWS Glue Database
        """
        raw_database = glue.Database(
            self,
            config["glueJobConfig"]["databaseName"],
            database_name=config["glueJobConfig"]["databaseName"],
        )
        return raw_database

    def create_glue_crawlers(self, glue_role, glue_security_configuration, raw_database, s3_target_path,
                             crawler_name):
        """
        Creates AWS Glue Crawlers
        """
        raw_crawler_configuration = {
            'Version': 1.0,
            'CrawlerOutput': {
                'Partitions': {'AddOrUpdateBehavior': 'InheritFromTable'}
            },
            'Grouping': {
                'TableGroupingPolicy': 'CombineCompatibleSchemas'}
        }
        # Adding S3 location for Crawler
        s3_target = glue.CfnCrawler.S3TargetProperty(path=s3_target_path)
        crawler_obj = glue.CfnCrawler(
            self,
            crawler_name,
            role=glue_role.role_arn,
            targets=glue.CfnCrawler.TargetsProperty(s3_targets=[s3_target]),
            database_name=raw_database.database_name,
            name=crawler_name,
            crawler_security_configuration=glue_security_configuration.security_configuration_name,
            configuration=json.dumps(raw_crawler_configuration)
        )
        # Adding Name tag to the resource
        core.Tags.of(crawler_obj).add('Name', crawler_name)

        return crawler_obj

    def create_glue_jobs(self, glue_role, glue_security_configuration, glue_job_name, script_path, arguments,
                         job_type, capcacity):
        """
        Creates AWS Glue Jobs
        """
        if job_type == 'glueetl':
            glue_version = '3.0'

            # Adding Spark Glue Job
            glue_job = glue.CfnJob(
                self,
                glue_job_name,
                name=glue_job_name,
                role=glue_role.role_arn,
                number_of_workers=capcacity,
                worker_type='G.1X',
                max_retries=0,
                glue_version=glue_version,
                security_configuration=glue_security_configuration.security_configuration_name,
                default_arguments=arguments,
                command=glue.CfnJob.JobCommandProperty(
                    name=job_type,
                    script_location=script_path
                )
            )

        else:
            glue_version = '2.0'
            # Adding Python Glue Job
            glue_job = glue.CfnJob(
                self,
                glue_job_name,
                name=glue_job_name,
                role=glue_role.role_arn,
                max_capacity=capcacity,
                max_retries=0,
                glue_version=glue_version,
                security_configuration=glue_security_configuration.security_configuration_name,
                default_arguments=arguments,
                command=glue.CfnJob.JobCommandProperty(
                    name=job_type,
                    script_location=script_path,
                    python_version='3.9'
                )
            )

        return glue_job


class CdkGlueBlogStage(core.Stage):
    def __init__(self, scope: core.Construct, construct_id: str, *, config: Dict, env=None, outdir=None):
        super().__init__(scope, construct_id, env=env, outdir=outdir)

        CdkGlueBlogStack(
            self,
            "CdkGlueBlogStack",
            description='This stack create a AWS Glue based data pipeline.',
            config=config,
            stack_name=config["codepipeline"]["glueStackName"]
        )
