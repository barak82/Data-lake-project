#!/usr/bin/env python3
import os
import sys
import logging
import boto3
from ec2_metadata import ec2_metadata
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, expr,from_json, size,col,array_max,array_min


os.environ['AWS_DEFAULT_REGION'] = ec2_metadata.region
ssm_client = boto3.client('ssm')
s3 = boto3.resource('s3')
logging.basicConfig(format='[%(asctime)s] %(levelname)s - %(message)s', level=logging.INFO)

def process_data_validation(spark,day_select,params,vehicle_num_axles):
    """validate the data staged 

    Args:
        spark (object): spark application 
        day_select (str): day to be processed
        params (json): parameters for AWS 
        vehicle_num_axles (integer): number of axles that needs to be filtered
    """

    # load partitioned data 
    staging_data=spark.read.parquet(f"s3a://{params['processed_data']}/partitioned-acc-data/{day_select}")

    # extract to sql executable
    staging_data.createOrReplaceTempView("staging_data_table")
    # sql statement 
    sql_statement='''
                    SELECT *
                    FROM staging_data_table
                    WHERE def_neg < -0.02 AND num_axles={};
                        '''.format(vehicle_num_axles)
    # filter by actions for acc sn data
    valid_data = spark.sql(sql_statement)
    logging.info("DAY {} valid_data rows= {} and columns={}".format(day_select,valid_data.count(),len(valid_data.columns)))
    # write valid data table to parquet files partitioned by num of axles and serial number
    valid_data.write.mode('append').partitionBy("num_axles", "serialNumber").parquet(f"s3a://{params['processed_data']}/valid-acc-data/{day_select}", mode="append")


def write_parquet(df_spark, out_put_data,params):
    """write a file to parquet

    Args:
        df_spark (object):spark application 
        out_put_data (spark dataframe): data output
        params (json): parameters defined in AWS SSM
    """
    df_spark.write \
        .format("parquet") \
        .save(f"s3a://{params['processed_data']}/{out_put_data}/", mode="append")

def get_parameters():
    """Load parameter values from AWS Systems Manager (SSM)"""

    params = {
        'acc_data': ssm_client.get_parameter(Name='/data-lake-pro/acc-data')['Parameter']['Value'],
        'bvr_data': ssm_client.get_parameter(Name='/data-lake-pro/bvr-data')['Parameter']['Value'],
        'schema_data': ssm_client.get_parameter(Name='/data-lake-pro/schema-data')['Parameter']['Value'],
        'processed_data': ssm_client.get_parameter(Name='/data-lake-pro/processed-data')['Parameter']['Value'],
        'analyzed_data': ssm_client.get_parameter(Name='/data-lake-pro/analyzed-data')['Parameter']['Value'],

    }

    return params
def main():
    params = get_parameters()

    spark = SparkSession \
        .builder \
        .appName("process-sensor-data") \
        .getOrCreate()

    day_select=sys.argv[1]
    vehicle_num_axles=sys.argv[2]
    process_data_validation(spark, day_select,params,vehicle_num_axles)


if __name__ == "__main__":
  main()

