#!/usr/bin/env python3
import os
import sys
import logging
import boto3
from ec2_metadata import ec2_metadata
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, expr, col,from_json, size,col,array_max,array_min


os.environ['AWS_DEFAULT_REGION'] = ec2_metadata.region
ssm_client = boto3.client('ssm')
s3 = boto3.resource('s3')
logging.basicConfig(format='[%(asctime)s] %(levelname)s - %(message)s', level=logging.INFO)


def analyze_sensor_data(spark,day_select,params):
    """analyze data from the sensor and the vehicle based on bvrId (unique ID)

    Args:
        spark (object): spark application
        day_select (str): date to b processed
        params (json): parameters defined in AWS
    """

    # load partitioned data 
    processed_data_acc=spark.read.parquet(f"s3a://{params['processed_data']}/valid-acc-data/{day_select}")
    processed_data_bvr=spark.read.parquet(f"s3a://{params['processed_data']}/valid-bvr-data/{day_select}")

    # extract spark data frame to sql table
    processed_data_acc.createOrReplaceTempView("processed_data_acc_table")
    processed_data_bvr.createOrReplaceTempView("processed_data_bvr_table")

    # merge data by bvr ID
    analyze_data_bvrId = spark.sql('''
                            SELECT 
                            bvr.bvrId,
                            bvr.startTime,
                            bvr.axleCount,
                            bvr.speed,
                            bvr.stripesTemperature,
                            bvr.weight,
                            acc.def_pos,
                            acc.def_neg
                            FROM processed_data_acc_table acc
                            INNER JOIN processed_data_bvr_table bvr ON bvr.bvrId = acc.bvrId ;
                            ''')
    analyze_data_bvrId.createOrReplaceTempView("analyze_data_bvrId_table")
    logging.info("DAY {} analyze_data rows= {} and columns={}".format(day_select,analyze_data_bvrId.count(),len(analyze_data_bvrId.columns)))

    # analyze the data to store final table 
    Final_analyze_data_bvrId = spark.sql('''
                            SELECT FIRST_VALUE(bvrId) as bvrId,
                            FIRST_VALUE(startTime) as record_time,
                            FIRST_VALUE(axleCount) as axleCount,
                            ROUND(AVG(speed)*3.6,1) as average_speed,
                            ROUND(AVG(weight),2) as average_wheel_weight,
                            AVG(def_neg)  as deflection,
                            FIRST_VALUE(stripesTemperature) as average_stripes_temperature
                            FROM analyze_data_bvrId_table
                            WHERE def_neg IS NOT NULL
                            AND def_pos IS NOT NULL
                            AND stripesTemperature IS NOT NULL
                            AND weight IS NOT NULL
                            AND speed IS NOT NULL
                            GROUP BY bvrId;
                                ''')

    Final_analyze_data_bvrId.createOrReplaceTempView("Final_analyze_data_bvrId_table")

    logging.info("DAY {} analyze_data rows= {} and columns={}".format(day_select,analyze_data_bvrId.count(),len(analyze_data_bvrId.columns)))
    # write valid data table to parquet files partitioned by year and month
    Final_analyze_data_bvrId.write.mode('append').parquet(f"s3a://{params['analyzed_data']}/analyzed-sensor-data/{day_select}", mode="overwrite")

def get_parameters():
    """Load parameter values from AWS Systems Manager (SSM) that are stored in AWS"""

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
    analyze_sensor_data(spark, day_select,params)


if __name__ == "__main__":
  main()

