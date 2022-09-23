# Copyright 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import sys
from pyspark.context import SparkContext
from pyspark.sql.functions import (col, split)
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job


class j_neherlab_denorm_etl:

    def __init__(self):
        # Declare Array
        params = []

        # Job Name 
        if '--JOB_NAME' in sys.argv:
            params.append('JOB_NAME')

        # Mandatory fields
        params.append('S3_OUTPUT_PATH')
        params.append('DATABASENAME')
        params.append('NHRLAB_CS_CNT_TBL')
        params.append('NHRLAB_ICU_CAP_TBL')
        params.append('NHRLAB_CNTRY_CD_TBL')
        params.append('NHRLAB_PPL_TBL')
        params.append('OUTPUT_TBL')

        # Glue Context
        self.args = getResolvedOptions(sys.argv, params)
        self.context = GlueContext(SparkContext.getOrCreate())
        self.job = Job(self.context)

    def run(self):
        # Create DF's
        df_case_counts = neherlab_case_counts(self.context, self.args['DATABASENAME'], self.args['NHRLAB_CS_CNT_TBL'])
        df_icu_capacity = neherlab_icu_capacity(self.context, self.args['DATABASENAME'], self.args['NHRLAB_ICU_CAP_TBL'])
        df_country_codes = neherlab_country_codes(self.context, self.args['DATABASENAME'], self.args['NHRLAB_CNTRY_CD_TBL'])
        df_population = neherlab_population(self.context, self.args['DATABASENAME'], self.args['NHRLAB_PPL_TBL'])

        # Create Denorm DF
        df_denormalized = denormalized_view(df_case_counts, df_icu_capacity, df_country_codes, df_population)

        # Write to S3
        write_to_sink(self.context,df_denormalized,self.args['S3_OUTPUT_PATH'],self.args['DATABASENAME'],self.args['OUTPUT_TBL'])

        # Job Commit
        self.job.commit()


# Creating the DF for the neherlab_case_counts
def neherlab_case_counts(glueContext, databasename, nhrlab_cs_cnt_tbl):
    dyf = glueContext.create_dynamic_frame.from_catalog(
        database=databasename,
        table_name=nhrlab_cs_cnt_tbl
    )
    df = dyf.toDF().\
        select("location", "date", "datetime", "cases", "deaths", "hospitalized", "icu", "recovered")
    df = df.dropDuplicates()

    # Splitting the location field and getting the Country and City information
    split_col = split(df['location'], '/')
    df = df.withColumn('ecdc-countries', split_col.getItem(0))
    df = df.withColumn('country', split_col.getItem(1))
    df = df.filter(col("ecdc-countries") == "ecdc").alias("neherlab_case_counts")
    return df


# Creating the DF for the neherlab_icu_capacity
def neherlab_icu_capacity(glueContext, databasename, nhrlab_icu_cap_tbl):
    dyf = glueContext.create_dynamic_frame.from_catalog(
        database=databasename,
        table_name=nhrlab_icu_cap_tbl
    )
    df = dyf.toDF().\
        select("country", "acute_care", "acute_care_per_100k", "imcu", "icu",
               "critical_care", "critical_care_per_100k", "percent_of_total", "gdp")
    df.dropDuplicates().alias("neherlab_icu_capacity")
    return df


# Creating the DF for the neherlab_country_codes
def neherlab_country_codes(glueContext, databasename, nhrlab_cntry_cd_tbl):
    dyf = glueContext.create_dynamic_frame.from_catalog(
        database=databasename,
        table_name=nhrlab_cntry_cd_tbl
    )
    df = dyf.toDF().\
        select("name", "alpha_2", "alpha_3", "country_code", "iso_3166_2", "region",
               "sub_region", "intermediate_region", "region_code", "sub_region_code", "intermediate_region_code")
    df.dropDuplicates().alias("neherlab_country_codes")
    return df


# Creating the DF for the neherlab_population
def neherlab_population(glueContext, databasename, nhrlab_ppl_tbl):
    dyf = glueContext.create_dynamic_frame.from_catalog(
        database=databasename,
        table_name=nhrlab_ppl_tbl
    )
    df = dyf.toDF().\
        select("name", "population", "country", "hospital_beds", "icu_beds",
               "suspected_cases_mar_1st", "imports_per_day", "hemisphere")
    df.dropDuplicates().alias("neherlab_population")
    return df


# Creating the denormalised view by joining all above data sources together
def denormalized_view(df_case_counts, df_icu_capacity, df_country_codes, df_population):
    df_join1 = df_case_counts.alias("neherlab_case_counts").\
        join(df_icu_capacity.alias("neherlab_icu_capacity"), "country").\
        drop(df_icu_capacity.country)
    df_join2 = df_join1.alias("joinDF1").\
        join(df_country_codes.alias("neherlab_country_codes"),
             df_join1.country==df_country_codes.name).\
        drop(df_country_codes.name)
    df_join3 = df_join2.alias("joinDF2").\
        join(df_population.alias("neherlab_population"),
             df_join2.country==df_population.name).\
        drop(df_population.name)

    # Dropping duplicate rows
    df_join3 = df_join3.dropDuplicates()
    return df_join3


# Write to Sink
def write_to_sink(glueContext, df_denormalized, s3_output_path, databasename, output_tbl):
    # Converting the DF back to Dynamic Frame
    Transform0 = DynamicFrame.fromDF(df_denormalized, glueContext, "Transform0")

    # Sink the Transform Dynamic Frame to S3
    DataSink0 = glueContext.getSink(
        path=s3_output_path,
        connection_type="s3",
        updateBehavior="UPDATE_IN_DATABASE",
        partitionKeys=[],
        compression="snappy",
        enableUpdateCatalog=True,
        transformation_ctx="DataSink0"
    )
    DataSink0.setCatalogInfo(catalogDatabase=databasename, catalogTableName=output_tbl)
    DataSink0.setFormat("glueparquet")
    DataSink0.writeFrame(Transform0)
    return


# Main
if __name__ == '__main__':
    j_neherlab_denorm_etl().run()
