import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark import SparkContext, SparkConf

# Spark imports
from pyspark.sql.functions import col, to_date
from pyspark.sql.types import DoubleType, IntegerType, StringType
from pyspark.sql.functions import udf, col

import pycountry
import difflib

import pydeequ
from pydeequ.checks import *
from pydeequ.verification import *
from pydeequ.analyzers import *

########### UTILITY FUNCTIONS ###########

# Country Mapping to handle awkward names
country_mapping = {
    'Bonaire Sint Eustatius and Saba': 'Bonaire, Sint Eustatius and Saba',
    'Cape Verde': 'Cabo Verde',
    'Democratic Republic of Congo': 'Congo, The Democratic Republic of',
    'Faeroe Islands': 'Faroe Islands',
    'Iran': 'Iran, Islamic Republic of',
    'Laos': "Lao People's Democratic Republic",
    'Micronesia (country)': 'Micronesia, Federated States of',
    'United States Virgin Islands': 'Virgin Islands, U.S.'
}


def country_iso3166(location: str) -> str:
    # apply mapping in case it's an awkward country
    fixed_location = country_mapping.get(location, location)

    # Try the simple lookup as it's quickest
    country = pycountry.countries.get(name=fixed_location)

    # Then try the common name
    if not country:
        country = pycountry.countries.get(common_name=fixed_location)

    if not country:
        # try the fuzzy lookup, which is slower
        try:
            candidates = pycountry.countries.search_fuzzy(fixed_location)

            if len(candidates) > 1:
                # We use a secondary difflib check as sometimes things get weird
                # eg: Curacao will return both Curacao and Netherlands
                top_candidates = difflib.get_close_matches(fixed_location, [c.name for c in candidates], n=1)

                if top_candidates:
                    # if we have at least one match, go with the best match
                    country = pycountry.countries.get(name=top_candidates[0])
            else:
                country = candidates[0]

        except:
            country = None

    if not country:
        print("Could not find country: %s" % location)

    return country.alpha_3 if country else None


@udf(returnType=StringType())
def udf_country_iso3166(location: str) -> str:
    return country_iso3166(location)

########### MAIN GLUE JOB ###########

# Using optimized serializer and using dynamic overwriting partitioning
sparkConf = SparkConf().setAppName("RawToCleanIndicators") \
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .set("spark.sql.hive.convertMetastoreParquet", "false")

glueContext = GlueContext(SparkContext.getOrCreate(sparkConf))

spark = glueContext.spark_session
logger = glueContext.get_logger()

# Defining all arguments list
default_args = ["JOB_NAME", "BUCKET_NAME", "ingestion_date"]

# Resolving all arguments
args = getResolvedOptions(
    sys.argv, default_args
)

# Creating job context
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
job_run_id = args["JOB_RUN_ID"]

########### DATA EXTRACTION ###########
ingestion_date = args["ingestion_date"]
bucket_name = args["BUCKET_NAME"]
raw_path = f"s3://{bucket_name}/raw/data2_country_indicators/ingest_date={ingestion_date}/"
clean_path = f"s3://{bucket_name}/clean/data2_country_indicators/"

logger.info(f"To load data from: {raw_path}")
df = spark.read \
    .format("excel") \
    .option("header", "true") \
    .load(raw_path)

# Applying cleaning transformations
for col_name in df.columns:
    if col_name == "date":
        df = df.withColumn(col_name, to_date(col(col_name), "yyyy-MM-dd"))
    elif col_name in ("hosp_patients", "total_tests", "new_tests", "new_tests_smoothed", "total_vaccinations",
                      "people_vaccinated", "people_fully_vaccinated", "total_boosters", "new_vaccinations",
                      "new_vaccinations_smoothed", "new_vaccinations_smoothed_per_million",
                      "new_people_vaccinated_smoothed", "population"):
        df = df.withColumn(col_name, df[col_name].cast(IntegerType()))
    elif col_name not in ("location", "continent", "tests_units"):
        df = df.withColumn(col_name, df[col_name].cast(DoubleType()))

########### CLEANING TRANSFORMATIONS ###########
# df_country_iso3166 = df.dropDuplicates(["location"])\
#    .select("location")\
#    .withColumn("country_iso", udf_country_iso3166(col("location")))
# df = df.join(df_country_iso3166, on='location', how='left')

########### DATA CHECKS ###########
analysis_result = AnalysisRunner(spark) \
    .onData(df) \
    .addAnalyzer(PatternMatch("location", "^[A-Z][A-Za-z\(\)\'\-\s]+$")) \
    .addAnalyzer(PatternMatch("continent", "^[A-Za-z,\s]+$")) \
    .run()

analysis_result = AnalyzerContext.successMetricsAsDataFrame(spark, analysis_result)

if analysis_result.filter(~(col("value") == 0)).count() > 0:
    raise Exception("The locations or variant are not following the patterns")


########### MATERIALIZING RESULT ###########
format_options = {
    "format": "parquet",
    "path": f"{clean_path}"
}
writer = (
    df.write.mode("overwrite")
        .options(**format_options)
)

table_name = f"{bucket_name}.cleaned_country_indicators"
logger.info(f"SAVING TO TABLE: {table_name}")
writer.saveAsTable(table_name)

job.commit()
logger.info("Glue job committed")
