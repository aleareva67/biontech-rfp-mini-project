import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark import SparkContext, SparkConf
from pyspark.sql.window import Window
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

########### MAIN GLUE JOB ###########

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

########### DATA LOADING ###########
bucket_name = args["BUCKET_NAME"]
clean_path_country_indicators = f"s3://{bucket_name}/clean/data2_country_indicators/"
clean_path_country_variants = f"s3://{bucket_name}/clean/data1_country_variants/"
curated_path_continent_country_variants = f"s3://{bucket_name}/curated/country_variant_indicators/"


# Load and prepare data
format_options = {"format": "parquet"}

df_variants = spark.read\
    .options(**format_options)\
    .load(clean_path_country_variants)

df_continent = spark.read\
    .options(**format_options)\
    .load(clean_path_country_indicators)
df_continent = df_continent.withColumnRenamed("date", "date_indicators")


########### CURATION TRANSFORMATIONS ###########
w = Window.partitionBy('location')
df_continent = df_continent.withColumn('maxDate', F.max('date_indicators').over(w))\
    .where(F.col('date_indicators') == F.col('maxDate'))\
    .drop('maxDate')
    
# Joining to get continent Information
combined_df = df_variants.join(df_continent, on='location', how='left')


########### MATERIALIZING RESULT ###########
format_options = {
    "format": "parquet",
    "path": curated_path_continent_country_variants
}
writer = (
    combined_df.write.mode("overwrite")
        .partitionBy("continent")
        .options(**format_options)
)

table_name = f"{bucket_name}.curated_country_variant_indicators"
logger.info(f"SAVING TO TABLE: {table_name}")
writer.saveAsTable(table_name)


job.commit()
logger.info("Glue job committed")