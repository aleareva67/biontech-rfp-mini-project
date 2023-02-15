import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext

from pyspark import SparkContext, SparkConf
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

import pydeequ
from pydeequ.checks import *
from pydeequ.verification import *
from pydeequ.analyzers import *

########### MAIN GLUE JOB ###########

# Using optimized serializer and using dynamic overwriting partitioning
sparkConf = SparkConf().setAppName("RawToCleanVariants") \
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

raw_path = f"s3://{bucket_name}/raw/data1_country_variants/ingest_date={ingestion_date}/"
clean_path = f"s3://{bucket_name}/clean/data1_country_variants/"

# READ CSV file covid variants evolution
format_options = {
    "format": "csv",
    "quoteChar": '"',
    "sep": ";",
    "header": True
}

df = spark.read.load(raw_path, **format_options)

########### CLEANING TRANSFORMATIONS ###########
# Rename the first column which is all columns to all_columns
df = df.withColumnRenamed(df.columns[0], "all_columns")

# Split the rows by comma
split_col = F.split(df['all_columns'], ',')
df = df.withColumn('location', split_col.getItem(0)) \
    .withColumn('date', split_col.getItem(1)) \
    .withColumn('variant', split_col.getItem(2)) \
    .withColumn('num_sequences', split_col.getItem(3)) \
    .withColumn('perc_sequences', split_col.getItem(4)) \
    .withColumn('num_sequences_total', split_col.getItem(5)) \
    .drop("all_columns")

# Remove quotes from the records
for col_name in df.columns:
    df = df.withColumn(col_name, F.regexp_replace(col_name, '"', ''))

# Parse the dates
date_format = "dd.MM.yyyy"
df = df.withColumn("date", F.to_date(F.col("date"), date_format))

# Convert num_sequences to integer
df = df.withColumn("num_sequences", F.col("num_sequences").cast(IntegerType())) \
    .withColumn("num_sequences_total", F.col("num_sequences_total").cast(IntegerType()))

# Recalculate the Percentages
df = df.withColumn("perc_sequences", 100 * F.col("num_sequences") / F.col("num_sequences_total"))

########### DATA CHECKS ###########
check = Check(spark, CheckLevel.Warning, "Review Check")

check_result = VerificationSuite(spark)\
    .onData(df)\
    .addCheck(
        check.isGreaterThanOrEqualTo("num_sequences_total", "num_sequences")\
        .isNonNegative("num_sequences_total")\
        .isNonNegative("num_sequences")\
        .isNonNegative("perc_sequences")\
        .areComplete(["num_sequences_total", "num_sequences", "perc_sequences"]))\
    .run()

check_result = VerificationResult.checkResultsAsDataFrame(spark, check_result)
check_result.show(10,False)

analysis_result = AnalysisRunner(spark) \
                    .onData(df) \
                    .addAnalyzer(PatternMatch("location", "^[A-Z][A-Za-z\(\)\'\-\s]+$")) \
                    .addAnalyzer(PatternMatch("variant", "^[A-Za-z\.:_][A-Za-z0-9\.:_]+$")) \
                    .run()
                    
analysis_result = AnalyzerContext.successMetricsAsDataFrame(spark, analysis_result)

if analysis_result.filter(~(F.col("value") == 0)).count() > 0:
    raise Exception("The locations or variant are not following the patterns")

# If we exclude the non_who row, the sum of num_sequences for a given location/date pair will
# add up to the num_sequences_total for that location/date pair
sequence_sums = df.filter(~(F.col("variant") == "non_who")).select('location', 'date', 'num_sequences').groupBy(
    "location", "date").agg(F.sum("num_sequences").alias("num_sequences"))

sequence_totals = df.select('location', 'date', 'num_sequences_total').groupBy("location", "date").agg(
    F.first("num_sequences_total").alias("num_sequences_total"))

sequence_sums = sequence_sums.orderBy("location", "date").withColumn("id", F.monotonically_increasing_id())
sequence_totals = sequence_totals.orderBy("location", "date").withColumn("id", F.monotonically_increasing_id())
combined_totals = sequence_sums.join(sequence_totals, sequence_sums.id == sequence_totals.id, how='inner')

if combined_totals.filter(~(F.col("num_sequences") == F.col("num_sequences_total"))).count() > 0:
    combined_totals.filter(~(F.col("num_sequences") == F.col("num_sequences_total"))).show(10, False)
    raise Exception("There is a mismatch")

########### MATERIALIZING RESULT ###########
format_options = {
    "format": "parquet",
    "path": f"{clean_path}"
}
writer = (
    df.write.mode("overwrite")
        .options(**format_options)
)

table_name = f"{bucket_name}.cleaned_country_variants"
logger.info(f"SAVING TO TABLE: {table_name}")
writer.saveAsTable(table_name)

job.commit()
logger.info("Glue job committed")
