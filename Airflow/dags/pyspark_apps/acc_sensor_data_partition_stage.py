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

def extract_sensor_data_json_batch_file(spark,data_file,schema_json,schema=""):
  """_summary_

  Args:
      spark (object): spark application
      data_file (str): data file name
      schema_json (str: schema json
      schema (str, optional): schema type. Defaults to "".

  Returns:
      spark dataframe: output data mapped using schema
  """
  
  txt = spark.read.option("multiline",True).option("lineSep", "\n").text(data_file)
    
  df = spark.read.option("multiline",True).option("inferSchema", "true").json(schema_json)

  dfJSON = txt.withColumn("jsonData",from_json(col("value"),df.schema)).select("jsonData.*")

   
  if schema=="acc_schema":
    df1 = (dfJSON
      .withColumn('buffer', explode(col('vehicleAxles')))
      .withColumn('num_axles', size(col('buffer.wheels')))
      .withColumn('buffer_w', explode("buffer.wheels"))
      .withColumn('num_patch', explode("buffer_w.patchClusters"))
      .withColumn('num_patch_clusters', size("buffer_w.patchClusters"))
      .withColumn('buffer_defResults', explode("num_patch.deflectionResult.cummulativeDeflection"))
      .withColumn('serialNumber', expr("buffer_defResults.serialNr"))
      .withColumn('channelNumber', expr("buffer_defResults.channelNr"))
      .withColumn('def_pos', array_max(col("buffer_defResults.data")))
      .withColumn('def_neg', array_min(col("buffer_defResults.data")))
      .withColumn('defData', expr("buffer_defResults.data").cast("string"))
      .withColumn('notice_buffer', explode("notifications"))
      .withColumn('notificationType', expr("notice_buffer.notificationType"))
      .withColumn('title', expr("notice_buffer.title"))
      .withColumn('message', expr("notice_buffer.message"))
      .drop(*['metrologicalId','id', 'hullClusters','notifications' ,'buffer','buffer_w','num_patch','num_F','buffer_defResults','notice_buffer','vehicleAxles','startTime'])
    )
    return df1
  if schema=="acc_v2":
    df2 = (dfJSON
      .withColumn('buffer', explode("vehicleAxles.wheels.patchClusters"))
      .withColumn('serialNumber', expr("buffer.deflectionResult.cummulativeDeflection.serialNr"))
      .withColumn('channelNumber', expr("buffer.deflectionResult.cummulativeDeflection.channelNr"))
      .withColumn('def_pos', array_max(col("buffer.deflectionResult.cummulativeDeflection.data")))
      .withColumn('def_neg', array_min(col("buffer.deflectionResult.cummulativeDeflection.data")))
      .withColumn('defData', expr("buffer.deflectionResult.cummulativeDeflection.data").cast("string"))
      .drop(*['metrologicalId','id', 'hullClusters','notifications' ,'buffer','buffer_w','num_patch','num_F','buffer_defResults','notice_buffer','vehicleAxles','startTime'])
    )
    
    return df2

  if schema=="bvr_schema_actual":
    df1 = (dfJSON
      .withColumn('bvrId', col('id'))
      .withColumn('buffer', explode(col('vehicleAxles')))
      .withColumn('buffer_w', explode("buffer.wheels"))
      .withColumn('num_patch', explode("buffer_w.patchClusters"))
      .withColumn('num_F', explode("num_patch.forceRecords"))
      .withColumn('area', expr("num_F.area"))
      .withColumn('serialNumber_bvr', expr("num_F.serialNumber"))
      .drop(*['metrologicalId','id','hullClusters','num_F' ,'buffer','buffer_w','num_patch','num_d','vehicleAxles','laneNumber','drivingDirection','startTime','archived',])
    )

  if schema=="cvr_schema":
    df1 = (dfJSON
      .withColumn('bvrId', expr('bvrId'))
      .withColumn('vdrId', expr('vdrId'))
      .withColumn('startTime', expr('startTime'))
      .withColumn('axleCount', expr('axleCount'))
      .withColumn('weight', expr('weight.value'))
      .withColumn('speed', expr('speed.value'))
      .withColumn('temperatureRecordIds_buffer', explode('temperatureRecordIds'))
      .withColumn('temperatureRecordIds_sn', expr('temperatureRecordIds_buffer.serialNumber'))
      .withColumn('stripesTemperature', expr('temperatureRecordIds_buffer.stripesTemperature'))
      .withColumn('buffer', explode('axles'))
      .withColumn('axle_index', expr('buffer.index'))
      .withColumn('buffer_w', explode("buffer.wheels"))
      .withColumn('wheelPosition', expr("buffer_w.wheelPosition"))
      .withColumn('num_patch', explode("buffer_w.patches"))
      .withColumn('patch_weight', expr("num_patch.weight.value"))
      .withColumn('patch_length', expr("num_patch.length.value"))
      .withColumn('patch_width', expr("num_patch.width.value"))
      .drop(*['metrologicalId','id','hullClusters','num_F' ,'axles','temperatureRecordIds_buffer','buffer','temperatureRecordIds','buffer_w','num_patch','num_d','vehicleAxles','laneNumber','drivingDirection','archived',])
    )
    return df1
 
 
def aggregate_s3_bucket(spark,params,day_select,record_name,key_content_data,schema_type):
    """aggregate data from s3 bucket

    Args:
        spark (object): spark API
        params (dict): parameters to be
        day_select (str): day selected for filtering the data
        record_name (str): type of record 
        key_content_data (str): key for s3 bucket
        schema_type (str): schema for mapping the nested json to spark dataframe / SQL 

    Returns:
        _type_: _description_
    """
    key_sel=[key for key in key_content_data if day_select in key]
    print("number of files selected {}".format(len(key_sel)))
    schema_json=f"s3a://{params['schema_data']}/{schema_type}.json"
    for i,key in enumerate(key_sel):
        data_file=f"s3a://{params[record_name]}/{key}"
        print(data_file)
        df_acc_=extract_sensor_data_json_batch_file(spark,data_file,schema_json,schema_type)
        
        if i==0:
            d1=df_acc_
        else:
            d1=d1.union(df_acc_)
    print("all s3 bucket key\n length= {} and width={} of df\n".format(d1.count(),len(d1.columns)))
    df_acc_=d1
    df_acc_.printSchema()
    return df_acc_    

def get_s3_bucket(bucket):
    """list se buckets contents

    Args:
        bucket (str): bucket name

    Returns:
        list:s3 bucket content
    """
    s3 = boto3.client('s3')
    resp = s3.list_objects_v2(Bucket=bucket)
    key_content=[]
    for c in resp.get('Contents'):
        print(c.get('Key'))
        key_content.append(c.get('Key'))
    return key_content

def process_sensor_data(spark,sensor_data, day_select, params):
    """process the sensor raw data

    Args:
        spark (object): spark application
        sensor_data (spark data frame): sensor data input
        day_select (str): day to be selected
        params (json): parameters defined to run the spark 
    """
      
    sensor_data.createOrReplaceTempView("staging_sensor_data")
    sensor_data_table =spark.sql('''
                            SELECT *
                            FROM staging_sensor_data 
                            ORDER BY bvrId
                            ''')
    sensor_data_table.write.mode('append').partitionBy("num_axles", "serialNumber").parquet(f"s3a://{params['processed_data']}/partitioned-acc-data/{day_select}", mode="append")
    logging.info("DAY {} sensor_data_table rows= {} and columns={}".format(day_select,sensor_data_table.count(),len(sensor_data_table.columns)))
  

def get_parameters():
    """Load parameter values from AWS Systems Manager (SSM) """

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
        .appName("process-log-song-data") \
        .getOrCreate()

    day_select=sys.argv[1]
    record_name=sys.argv[2]
    schema_type=sys.argv[3]

    key_content_data=get_s3_bucket(params["acc_data"])
    sensor_data=aggregate_s3_bucket(spark,params,day_select,record_name,key_content_data,schema_type)
    sensor_data_=sensor_data.dropDuplicates((["bvrId","num_axles","serialNumber","defData"])).select(sensor_data.columns)
    logging.info("DAY {} NO DUPLICATE DATA rows= {} and columns={}".format(day_select,sensor_data_.count(),len(sensor_data_.columns)))
    process_sensor_data(spark,sensor_data_, day_select,params)

if __name__ == "__main__":
  main()

