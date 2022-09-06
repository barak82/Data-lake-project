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
  

def process_data_validation(spark,day_select,params,vehicle_num_axles):
    """validate the data staged 

    Args:
        spark (object): spark application 
        day_select (str): day to be processed
        params (json): parameters for AWS 
        vehicle_num_axles (integer): number of axles that needs to be filtered
    """


     # load partitioned data 
    staging_data=spark.read.parquet(f"s3a://{params['processed_data']}/partitioned-bvr-data/{day_select}")

    # extract columns to create songs table
    staging_data.createOrReplaceTempView("staging_sensor_data")

    # check the sensor data
    sql_statement_1='''
                      SELECT DISTINCT(bvrId) As bvrId,
                      axleCount     As num_of_axles
                      FROM staging_sensor_data
                      WHERE staging_sensor_data.axleCount={}
                                                  '''.format(vehicle_num_axles)
    check_num_axles_per_vehicle = spark.sql(sql_statement_1)
    
    sql_statement_2='''
                    SELECT bvrId,
                    COUNT(*) AS num_sensor_data_vehicle
                    FROM staging_sensor_data
                    WHERE staging_sensor_data.axleCount={}
                    GROUP BY bvrId;
                                                    '''.format(vehicle_num_axles)
    check_valid_data_per_vehicle = spark.sql(sql_statement_2)
    check_num_axles_per_vehicle.createOrReplaceTempView("check_num_axles_per_vehicle_table")
    check_valid_data_per_vehicle.createOrReplaceTempView("check_valid_data_per_vehicle_table")

   # filter and join the vehicle and sensor data
    merge_vehicle_data_validity = spark.sql('''
                        SELECT
                        num_ax.bvrId    As bvrId,
                        num_ax.num_of_axles     As num_of_axles,
                        num_ax.num_of_axles * 4  AS expected_num_records_per_vehicle,
                        data_per_veh.num_sensor_data_vehicle   As num_records_per_vehicle
                        FROM check_num_axles_per_vehicle_table num_ax 
                        INNER JOIN check_valid_data_per_vehicle_table data_per_veh ON data_per_veh.bvrId=num_ax.bvrId;
                                ''')
    merge_vehicle_data_validity.createOrReplaceTempView("merge_vehicle_data_validity_table")
    
    # select valid record 
    check_vehicle_data_validity=spark.sql('''
                        SELECT
                          CASE
                            WHEN
                              merge_vehicle_data_validity_table.expected_num_records_per_vehicle = merge_vehicle_data_validity_table.num_records_per_vehicle
                                THEN
                                  'True'
                            ELSE
                              'False'
                          END AS valid_vehicle_record,
                          merge_vehicle_data_validity_table.*
                        FROM
                          merge_vehicle_data_validity_table
                          ''')

    check_vehicle_data_validity.createOrReplaceTempView("check_vehicle_data_validity_table")

    # aggregate valid record 
    valid_vehicle_data_record_check = spark.sql('''
                            SELECT *
                            FROM check_vehicle_data_validity_table veh_data
                            WHERE veh_data.valid_vehicle_record='True';
                                ''')
    valid_vehicle_data_record_check.createOrReplaceTempView("valid_vehicle_data_record_check_table")
    
    # merge valid records
    valid_vehicle_data_record = spark.sql('''
                                SELECT
                                st.*
                            FROM staging_sensor_data st 
                            INNER JOIN valid_vehicle_data_record_check_table s  ON s.bvrId=st.bvrId;
                                    ''')
    logging.info("DAY {} vehicle Num axles {} valid_vehicle_data_record rows= {} and columns={}".format(day_select,vehicle_num_axles,valid_vehicle_data_record.count(),len(valid_vehicle_data_record.columns)))
    # write valid data table to parquet files partitioned by year and month
    valid_vehicle_data_record.write.mode('append').partitionBy("axleCount", "axle_index").parquet(f"s3a://{params['processed_data']}/valid-bvr-data/{day_select}", mode="append")

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
    vehicle_num_axles=sys.argv[2]
    process_data_validation(spark, day_select,params,vehicle_num_axles)


if __name__ == "__main__":
  main()

