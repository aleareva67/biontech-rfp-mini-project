import logging
import os
from datetime import date

import boto3

# Initializing logger
logging.basicConfig()
logging.root.setLevel(logging.INFO)
logger = logging.getLogger("MoveToRaw-fx")


def get_parameter_value(ssm, param):
    """
        Utility method that retrieves a parameter value from the Parameter Store service.

        Parameters
        ----------
        ssm - System manager service client from AWS CDK.
        param - Param key

        Returns
        -------
        Parameter decrypted value
    """
    return ssm.get_parameter(Name=param, WithDecryption=True)['Parameter']['Value']


def handler(event, context):
    """
        Main method of lambda function.
    :param event: event triggering the lambda
    :param context: context of execution of the lambda.
    :return:
    """
    ssm = boto3.client('ssm')
    s3_source = boto3.resource('s3',
                               aws_access_key_id=get_parameter_value(ssm, f"/biontech/etl-demo/source/access_key"),
                               aws_secret_access_key=get_parameter_value(ssm, f"/biontech/etl-demo/source/secret_key"),
                               region_name=get_parameter_value(ssm, f"/biontech/etl-demo/source/region")
                               )

    s3_dest = boto3.resource('s3')

    # Getting ingestion date from input event or calculating it.
    fmt = "%Y%m%d"
    ingestion_date = event["ingestion_date"] if "ingestion_date" in event else date.today().strftime(fmt)

    logger.info(f"Move to Raw - ingestion_date: {ingestion_date}")

    bucket_src_name = get_parameter_value(ssm, f"/biontech/etl-demo/source/s3/name")
    bucket_dest_name = get_parameter_value(ssm, f"/biontech/etl-demo/destination/s3/name")

    source_path = "data/"
    raw_path = f"raw/"

    bucket_src = s3_source.Bucket(bucket_src_name)
    bucket_dest = s3_dest.Bucket(bucket_dest_name)

    logger.info(f"To move from {bucket_src_name}/{source_path} to {bucket_dest_name}/{raw_path}")

    try:
        
        # Iterating landing files
        for obj in bucket_src.objects.filter(Prefix=f"{source_path}"):
            file_key: str = obj.key
            file_name: str = file_key.split("/")[-1]

            # Filtering by supported files
            if (file_name.startswith("Data1") or file_name.startswith("Data2")) and \
                    (file_name.endswith(".csv") or file_name.endswith(".xlsx")):

                # Downloading files
                logger.info(f"Copying file: {file_key} locally.")
                bucket_src.Object(file_key).download_file(
                    f'/tmp/{file_name}'
                )

                # Uploading files
                raw_dataset = "data1_country_variants/" if file_name.startswith("Data1") else "data2_country_indicators/"
                logger.info(f"Uploading file: {file_key}")
                with open(f'/tmp/{file_name}', 'rb') as data:
                    bucket_dest.put_object(
                        Key=f'{raw_path}{raw_dataset}ingest_date={ingestion_date}/{file_name}',
                        Body=data
                    )

        logger.info(f"FINISHED SUCCESSFULLY")
    finally:
        do_cleanup()

    return {
        "ingestion_date": ingestion_date
    }


def do_cleanup():
    # Deleting temporary files to free-up lambda space.
    try:
        logger.info("Performing clean-up")
        os.system("rm -rf /tmp/*.csv")
    except:
        logger.warning("Clean-up was unsuccessful")
