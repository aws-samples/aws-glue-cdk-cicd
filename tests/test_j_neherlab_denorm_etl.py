# Copyright 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import sys
import pytest
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from src import j_neherlab_denorm_etl


@pytest.fixture(scope="module", autouse=True)
def glue_context():
    sys.argv.append('--JOB_NAME')
    sys.argv.append('j_neherlab_denorm_etl')

    sys.argv.append('--S3_OUTPUT_PATH')
    sys.argv.append('s3://neherlab-denormalized-dataset-858543917701-us-east-1')

    sys.argv.append('--DATABASENAME')
    sys.argv.append('covid19db')

    sys.argv.append('--NHRLAB_CS_CNT_TBL')
    sys.argv.append('neherlab_case_counts')

    sys.argv.append('--NHRLAB_ICU_CAP_TBL')
    sys.argv.append('neherlab_icu_capacity')

    sys.argv.append('--NHRLAB_CNTRY_CD_TBL')
    sys.argv.append('neherlab_country_codes')

    sys.argv.append('--NHRLAB_PPL_TBL')
    sys.argv.append('neherlab_population')

    sys.argv.append('--OUTPUT_TBL')
    sys.argv.append('neherlab_denormalized')

    args = getResolvedOptions(
        sys.argv, ['JOB_NAME', 'S3_OUTPUT_PATH', 'DATABASENAME', 'NHRLAB_CS_CNT_TBL',
                   'NHRLAB_ICU_CAP_TBL', 'NHRLAB_CNTRY_CD_TBL', 'NHRLAB_PPL_TBL', 'OUTPUT_TBL']
    )
    context = GlueContext(SparkContext.getOrCreate())
    job = Job(context)
    job.init(args['JOB_NAME'], args)

    yield context
    job.commit()


def test_neherlab_case_count(glue_context):
    # Tests
    neherlab_case_counts = j_neherlab_denorm_etl.neherlab_case_counts(
        glue_context, "covid19db", "neherlab_case_counts"
    )
    assert neherlab_case_counts.count() == 7904

    neherlab_icu_capacity = j_neherlab_denorm_etl.neherlab_icu_capacity(
        glue_context, "covid19db", "neherlab_icu_capacity"
    )
    assert neherlab_icu_capacity.count() == 64

    neherlab_country_codes = j_neherlab_denorm_etl.neherlab_country_codes(
        glue_context, "covid19db", "neherlab_country_codes"
    )
    assert neherlab_country_codes.count() == 500

    neherlab_population = j_neherlab_denorm_etl.neherlab_population(
        glue_context, "covid19db", "neherlab_population"
    )
    assert neherlab_population.count() == 474

    denormalized_df = j_neherlab_denorm_etl.denormalized_view(
        neherlab_case_counts, neherlab_icu_capacity, neherlab_country_codes, neherlab_population
    )
    assert denormalized_df.count() == 1911
