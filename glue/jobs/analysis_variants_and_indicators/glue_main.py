import sys

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark import SparkContext, SparkConf
from pyspark.sql.window import Window
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

import pandas as pd
import ehr


########### UTILITY FUNCTIONS ###########

# Calculating Schulz ranking
def calculate_combined_outbreak_rankings(location_variant_outbreak_df):
    # Convert to Pandas
    location_variant_outbreak_pandas_df = location_variant_outbreak_df.toPandas()

    per_continent_schulze_outbreak_rank = location_variant_outbreak_pandas_df.groupby('continent').apply(
        lambda x: ehr.calculate_combined_rank(
            x.pivot(index=['variant'], columns=['location'], values='outbreak_rank_global').drop('non_who')
        )
    )
    location_outbreak_df = per_continent_schulze_outbreak_rank.reset_index()

    location_outbreak_df.columns = ['continent', 'location', 'schulze_onset_rank_continent']

    # Return the output as spark dataframe
    return spark.createDataFrame(location_outbreak_df)


def predictor_locations(location_variant_outbreak_df, top_five_per_continent):
    # Convert to Pandas
    location_variant_outbreak_pandas_df = location_variant_outbreak_df.toPandas()
    top_five_per_continent_pandas = top_five_per_continent.toPandas()
    global_rank_matrix_df = location_variant_outbreak_pandas_df.pivot(index=['continent', 'location'],
                                                                      columns=['variant'],
                                                                      values='outbreak_rank_global').drop('non_who',
                                                                                                          axis=1)

    results = []
    for index, row in top_five_per_continent_pandas.iterrows():
        predictors_df = ehr.calculate_predictors(global_rank_matrix_df, row['location'], row['continent'])
        predictors_df['continent'] = row['continent']
        predictors_df['location'] = row['location']
        results.append(predictors_df)

    results_df = pd.concat(results, axis=0)

    # Return the output as spark dataframe
    return spark.createDataFrame(results_df)


# Calculating Schulz ranking
def calculate_schulz_rank(combined_df):
    # Convert to Pandas
    combined_pandas_df = combined_df.toPandas()

    # First When does each country get each variant
    first_appearance_df = combined_pandas_df.loc[
        combined_pandas_df['num_sequences'] > 0,
        ['continent', 'location', 'variant', 'date']
    ].groupby(
        ['continent', 'location', 'variant']
    ).min()

    ranked_df = first_appearance_df.groupby(['continent', 'variant']).rank('min')
    ranked_df = ranked_df.reset_index().pivot(index=['continent', 'location'], columns=['variant'], values=['date'])
    ranked_df.columns = ranked_df.columns.get_level_values(1)

    # Calculate the combined ranking using Schulz method
    early_hit_ranked_df = ranked_df.drop(
        ['others', 'non_who'], axis=1
    ).groupby('continent', group_keys=False).apply(lambda x: ehr.calculate_combined_rank(x.transpose()))

    early_hit_ranked_df = early_hit_ranked_df.reset_index()
    early_hit_ranked_df.columns = ['continent', 'location', 'early_hit_ranking']

    # Return the output as spark dataframe
    return spark.createDataFrame(early_hit_ranked_df)


## SPARK JOB ##

sparkConf = SparkConf().setAppName("CleanToCuratedVariantsIndicators") \
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .set("spark.sql.hive.convertMetastoreParquet", "false")

glueContext = GlueContext(SparkContext.getOrCreate(sparkConf))

spark = glueContext.spark_session
logger = glueContext.get_logger()

# Defining all arguments list
default_args = ["JOB_NAME", "BUCKET_NAME"]

# Resolving all arguments
args = getResolvedOptions(
    sys.argv, default_args
)

# Creating job context
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
job_run_id = args["JOB_RUN_ID"]

# Extracting data
bucket_name = args["BUCKET_NAME"]

curated_path_continent_country_variants = f"s3://{bucket_name}/curated/country_variant_indicators/"
analysis_path_outbreak_rank = f"s3://{bucket_name}/analysis/location_outbreak_rank/"
analysis_path_variant_predictors = f"s3://{bucket_name}/analysis/location_variant_predictors/"

# Load and prepare data
format_options = {"format": "parquet"}

combined_df = spark.read \
    .options(**format_options) \
    .load(curated_path_continent_country_variants)

# calculate the first outbreak dates
variant_outbreaks_df = combined_df.filter(F.col("num_sequences") > 0) \
    .select('continent', 'location', 'variant', 'date') \
    .groupBy('continent', 'location', 'variant') \
    .agg(F.min("date").alias("outbreak_date"))

# Create the LocationVariantOnset table
windowSpec = Window.partitionBy("continent", "variant").orderBy("outbreak_date")

# Create the LocationVariantOnset table
location_variant_outbreak_df = variant_outbreaks_df

location_variant_outbreak_df = location_variant_outbreak_df.withColumn("rank", F.rank().over(windowSpec)) \
    .groupBy("continent", "location", "variant", "outbreak_date") \
    .agg(F.min("rank").alias("outbreak_rank_continent"))

windowSpec = Window.partitionBy("variant").orderBy("outbreak_date")

location_variant_outbreak_df = location_variant_outbreak_df.withColumn("rank", F.rank().over(windowSpec)) \
    .groupBy("location", "variant", "outbreak_date", "continent", "outbreak_rank_continent") \
    .agg(F.min("rank").alias("outbreak_rank_global"))

# Calculating the 5 highest ranked
location_outbreak_df = calculate_combined_outbreak_rankings(location_variant_outbreak_df)

location_outbreak_df = location_outbreak_df.withColumn("earliest_5_in_continent",
                                                       F.col("schulze_onset_rank_continent") <= 5)

# Identify the top five earliest outbreak locations within and off each continent
top_five_per_continent = location_outbreak_df.filter(F.col("earliest_5_in_continent")).select('continent', 'location')

predictor_locations_df = predictor_locations(location_variant_outbreak_df, top_five_per_continent)

# Writing the output corresponding to Q1
writer = (
    location_outbreak_df.write.mode("overwrite")
        .partitionBy("continent")
        .format("parquet")
        .option("path", analysis_path_outbreak_rank)
)

table_name = f"{bucket_name}.analysis_location_outbreak_rank"
logger.info(f"SAVING TO TABLE: {table_name}")
writer.saveAsTable(table_name)

# Writing the output corresponding to Q2
writer = (
    predictor_locations_df.write.mode("overwrite")
        .partitionBy("continent")
        .format("parquet")
        .option("path", analysis_path_variant_predictors)
)

table_name = f"{bucket_name}.analysis_location_variant_predictors"
logger.info(f"SAVING TO TABLE: {table_name}")
writer.saveAsTable(table_name)

job.commit()
