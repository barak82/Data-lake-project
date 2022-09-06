#!/usr/bin/env python3
from logging import raiseExceptions
from airflow.operators.dummy_operator import DummyOperator
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow import DAG
import sys
import os 
from airflow.operators.python import get_current_context
dir_path = os.path.realpath(os.path.join(os.path.dirname(__file__), '..','..','..'))
dir_path_main = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(f'{dir_path_main}/project-template/')
sys.path.append(f'{dir_path}/dags/')
sys.path.append(f'{dir_path}/dags/data-lake-project-final/scripts/')
sys.path.append(f'{dir_path}/plugins/operators/')
sys.path.append(f'{dir_path}/plugins/')

aws_config_path=Variable.get('aws_config_path')

with DAG(
    dag_id="data_lake_project_EMR_plugin",
    schedule_interval="0 7 * * *",
    start_date=datetime(2022, 7, 21),
    catchup=False,
    tags=["Final_project"]
    ) as dag:

    start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)
    def connect_to_aws_resources(KEY,SECRET,REGION):
        import boto3
        emr_client = boto3.client('emr', aws_access_key_id=KEY, aws_secret_access_key=SECRET, region_name=REGION)
        ssm_client = boto3.client('ssm',aws_access_key_id=KEY, aws_secret_access_key=SECRET, region_name=REGION)
        return ssm_client, emr_client
    
    def dag_task():

        context = get_current_context()
        return context

    def bvr_stage():
        """executes the data processing by running the job flow 
        """
        import logging
        import os
        import sys
        from airflow.utils import timezone
        from time import sleep 
        import configparser
        from EMR_cluster_state import EMRClusterState
        config = configparser.ConfigParser()

        #connect to AWS resources
        dir_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
        logging.info("path parameters",aws_config_path)
        sys.path.append(aws_config_path)
        from parameters import parameters

        config.read(f'{dir_path}/scripts/aws.cfg')
        KEY,SECRET,REGION=config['AWS'].values()
        ssm_client, emr_client=connect_to_aws_resources(*config['AWS'].values())

        logging.basicConfig(format='[%(asctime)s] %(levelname)s - %(message)s', level=logging.INFO)
        logging.info("check this path-->"+aws_config_path[:-1]+'aws.cfg')
        logging.info("check this path-->{} {} {}".format(KEY,SECRET,REGION))
        args = parse_args()
        params = parameters.get_parameters(ssm_client)
        arg_app=[["2022-08-03","bvr_data","cvr_schema"]]
        steps = get_steps(params, args["bvr_stage"],arg_app)
        # new_steps_job_flow=steps.format_job()
        cluster_name='bvr-stage'
        run_job_flow(params, steps,emr_client,cluster_name)


        state_cluster_Id=EMRClusterState(
            cluster_name=cluster_name,
            emr_client=emr_client
            )
        cluster_state=state_cluster_Id.get_status()
        cluster_id=cluster_state["cluster_id"]
        state_cluster=cluster_state["state_cluster"]

        context=dag_task()
        ds=context["ds"]
        logging.info(f"return check-->{ds}")
        sleep(10)
        while state_cluster!="TERMINATED" and state_cluster!="TERMINATED_WITH_ERRORS":
            # check the cluster state
            
            sleep(5)
            state_cluster = emr_client.describe_cluster(ClusterId=cluster_id)["Cluster"]["Status"]["State"]
            start_date = timezone.utcnow()
            logging.info("cluster ID= {} time = {}, state -->{}".format(cluster_id,start_date,state_cluster))
            if state_cluster=="TERMINATED_WITH_ERRORS":
                raise ValueError('A task failed with errors')
        start_date = timezone.utcnow()
        logging.info("cluster ID= {} time = {}, state -->{}".format(cluster_id,start_date,state_cluster))

    def acc_stage():
        """executes the data processing by running the job flow 
        """
        import logging
        import os
        import sys
        import configparser
        from time import sleep 
        from airflow.utils import timezone
        from EMR_cluster_state import EMRClusterState
        config = configparser.ConfigParser()

        #connect to AWS resources
        dir_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
        logging.info("path parameters",f'{dir_path}/scripts/')
        sys.path.append(f'{dir_path}/scripts/')
        from parameters import parameters

        config.read(f'{dir_path}/scripts/aws.cfg')
        ssm_client, emr_client=connect_to_aws_resources(*config['AWS'].values())

        logging.basicConfig(format='[%(asctime)s] %(levelname)s - %(message)s', level=logging.INFO)
        args = parse_args()
        params = parameters.get_parameters(ssm_client)
        
        arg_app=[["2022-08-03","acc_data","acc_schema"]]
        steps = get_steps(params, args["acc_stage"],arg_app)
        cluster_name='acc-stage'
        run_job_flow(params, steps,emr_client,cluster_name)
        
        state_cluster_Id=EMRClusterState(
            cluster_name=cluster_name,
            emr_client=emr_client
            )
        cluster_state=state_cluster_Id.get_status()
        cluster_id=cluster_state["cluster_id"]
        state_cluster=cluster_state["state_cluster"]
        sleep(10)
        while state_cluster!="TERMINATED" and state_cluster!="TERMINATED_WITH_ERRORS":
            # check the cluster state
            
            sleep(5)
            state_cluster = emr_client.describe_cluster(ClusterId=cluster_id)["Cluster"]["Status"]["State"]
            start_date = timezone.utcnow()
            logging.info("cluster ID= {} time = {}, state -->{}".format(cluster_id,start_date,state_cluster))
            if state_cluster=="TERMINATED_WITH_ERRORS":
                raise ValueError('A task failed with errors')
        start_date = timezone.utcnow()
        logging.info("cluster ID= {} time = {}, state -->{}".format(cluster_id,start_date,state_cluster))

    def bvr_validate_Ax_134():
        """executes the data processing by running the job flow 
        """
        import logging
        import os
        import sys
        from time import sleep
        from airflow.utils import timezone
        import configparser
        from EMR_cluster_state import EMRClusterState
        config = configparser.ConfigParser()

        #connect to AWS resources
        dir_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
        logging.info("path parameters",f'{dir_path}/scripts/')
        sys.path.append(f'{dir_path}/scripts/')
        from parameters import parameters

        config.read(f'{dir_path}/scripts/aws.cfg')
        ssm_client, emr_client=connect_to_aws_resources(*config['AWS'].values())

        logging.basicConfig(format='[%(asctime)s] %(levelname)s - %(message)s', level=logging.INFO)
        args = parse_args()
        params = parameters.get_parameters(ssm_client)
        arg_app=[["2022-08-03","1"],["2022-08-03","3"],["2022-08-03","4"]]
        steps = get_steps(params, args["bvr_validate_Ax_134"],arg_app)
        cluster_name='bvr-validate-Ax-1-3-4'
        run_job_flow(params, steps,emr_client,cluster_name)
        
        state_cluster_Id=EMRClusterState(
            cluster_name=cluster_name,
            emr_client=emr_client
            )
        cluster_state=state_cluster_Id.get_status()
        cluster_id=cluster_state["cluster_id"]
        state_cluster=cluster_state["state_cluster"]
        sleep(10)
        while state_cluster!="TERMINATED" and state_cluster!="TERMINATED_WITH_ERRORS":
            # check the cluster state
            
            sleep(5)
            state_cluster = emr_client.describe_cluster(ClusterId=cluster_id)["Cluster"]["Status"]["State"]
            start_date = timezone.utcnow()
            logging.info("cluster ID= {} time = {}, state -->{}".format(cluster_id,start_date,state_cluster))
            if state_cluster=="TERMINATED_WITH_ERRORS":
                raise ValueError('A task failed with errors')
        start_date = timezone.utcnow()
        logging.info("cluster ID= {} time = {}, state -->{}".format(cluster_id,start_date,state_cluster))
    
    def bvr_validate_Ax2():
        """executes the data processing by running the job flow 
        """
        import logging
        import os
        import sys
        from airflow.utils import timezone
        from time import sleep
        import configparser
        from EMR_cluster_state import EMRClusterState
        config = configparser.ConfigParser()

        #connect to AWS resources
        dir_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
        logging.info("path parameters",f'{dir_path}/scripts/')
        sys.path.append(f'{dir_path}/scripts/')
        from parameters import parameters

        config.read(f'{dir_path}/scripts/aws.cfg')
        ssm_client, emr_client=connect_to_aws_resources(*config['AWS'].values())

        logging.basicConfig(format='[%(asctime)s] %(levelname)s - %(message)s', level=logging.INFO)
        args = parse_args()
        params = parameters.get_parameters(ssm_client)
        arg_app=[["2022-08-03","2"]]
        steps = get_steps(params, args["bvr_validate_Ax_2"],arg_app)
        cluster_name='bvr-validate-Ax-2'
        
        run_job_flow(params, steps,emr_client,cluster_name)
        
        state_cluster_Id=EMRClusterState(
            cluster_name=cluster_name,
            emr_client=emr_client
            )
        cluster_state=state_cluster_Id.get_status()
        cluster_id=cluster_state["cluster_id"]
        state_cluster=cluster_state["state_cluster"]
        sleep(10)
        while state_cluster!="TERMINATED" and state_cluster!="TERMINATED_WITH_ERRORS":
            # check the cluster state
            
            sleep(5)
            state_cluster = emr_client.describe_cluster(ClusterId=cluster_id)["Cluster"]["Status"]["State"]
            start_date = timezone.utcnow()
            logging.info("cluster ID= {} time = {}, state -->{}".format(cluster_id,start_date,state_cluster))
            if state_cluster=="TERMINATED_WITH_ERRORS":
                raise ValueError('A task failed with errors')
        start_date = timezone.utcnow()
        logging.info("cluster ID= {} time = {}, state -->{}".format(cluster_id,start_date,state_cluster))
    
    def bvr_validate_Ax_56():
        """executes the data processing by running the job flow 
        """
        import logging
        import os
        import sys
        import configparser
        from airflow.utils import timezone
        from time import sleep
        from EMR_cluster_state import EMRClusterState
        config = configparser.ConfigParser()

        #connect to AWS resources
        dir_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
        logging.info("path parameters",f'{dir_path}/scripts/')
        sys.path.append(f'{dir_path}/scripts/')
        from parameters import parameters

        config.read(f'{dir_path}/scripts/aws.cfg')
        ssm_client, emr_client=connect_to_aws_resources(*config['AWS'].values())

        logging.basicConfig(format='[%(asctime)s] %(levelname)s - %(message)s', level=logging.INFO)
        args = parse_args()
        params = parameters.get_parameters(ssm_client)
        arg_app=[["2022-08-03","5"],["2022-08-03","6"]]
        steps = get_steps(params, args["bvr_validate_Ax_56"],arg_app)
        cluster_name='bvr-validate-Ax-5-6'
        run_job_flow(params, steps,emr_client,cluster_name)
        
        state_cluster_Id=EMRClusterState(
            cluster_name=cluster_name,
            emr_client=emr_client
            )
        cluster_state=state_cluster_Id.get_status()
        cluster_id=cluster_state["cluster_id"]
        state_cluster=cluster_state["state_cluster"]
        sleep(10)
        while state_cluster!="TERMINATED" and state_cluster!="TERMINATED_WITH_ERRORS":
            # check the cluster state
            
            sleep(5)
            state_cluster = emr_client.describe_cluster(ClusterId=cluster_id)["Cluster"]["Status"]["State"]
            start_date = timezone.utcnow()
            logging.info("cluster ID= {} time = {}, state -->{}".format(cluster_id,start_date,state_cluster))
            if state_cluster=="TERMINATED_WITH_ERRORS":
                raise ValueError('A task failed with errors')
        start_date = timezone.utcnow()
        logging.info("cluster ID= {} time = {}, state -->{}".format(cluster_id,start_date,state_cluster))



    def acc_validate():
        """executes the data processing by running the job flow 
        """
        import logging
        import os
        import sys
        import configparser
        from time import sleep 
        from airflow.utils import timezone
        from EMR_cluster_state import EMRClusterState
        config = configparser.ConfigParser()

        #connect to AWS resources
        dir_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
        logging.info("path parameters",f'{dir_path}/scripts/')
        sys.path.append(f'{dir_path}/scripts/')
        from parameters import parameters

        config.read(f'{dir_path}/scripts/aws.cfg')
        ssm_client, emr_client=connect_to_aws_resources(*config['AWS'].values())

        logging.basicConfig(format='[%(asctime)s] %(levelname)s - %(message)s', level=logging.INFO)
        args = parse_args()
        params = parameters.get_parameters(ssm_client)
        arg_app=[["2022-08-03","1"],["2022-08-03","2"]]
        steps = get_steps(params, args["acc_validate"],arg_app)
        cluster_name='acc-validate'
        run_job_flow(params, steps,emr_client,cluster_name)
        
        state_cluster_Id=EMRClusterState(
            cluster_name=cluster_name,
            emr_client=emr_client
            )
        cluster_state=state_cluster_Id.get_status()
        cluster_id=cluster_state["cluster_id"]
        state_cluster=cluster_state["state_cluster"]
        sleep(10)
        while state_cluster!="TERMINATED" and state_cluster!="TERMINATED_WITH_ERRORS":
            # check the cluster state
            
            sleep(5)
            state_cluster = emr_client.describe_cluster(ClusterId=cluster_id)["Cluster"]["Status"]["State"]
            start_date = timezone.utcnow()
            logging.info("cluster ID= {} time = {}, state -->{}".format(cluster_id,start_date,state_cluster))
            if state_cluster=="TERMINATED_WITH_ERRORS":
                raise ValueError('A task failed with errors')
        start_date = timezone.utcnow()
        logging.info("cluster ID= {} time = {}, state -->{}".format(cluster_id,start_date,state_cluster))

    def analyze_sensor_data():
        """executes the data processing by running the job flow 
        """
        import logging
        import os
        import sys
        import configparser
        from time import sleep 
        from airflow.utils import timezone
        from EMR_cluster_state import EMRClusterState
        config = configparser.ConfigParser()

        #connect to AWS resources
        dir_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
        logging.info("path parameters",f'{dir_path}/scripts/')
        sys.path.append(f'{dir_path}/scripts/')
        from parameters import parameters

        config.read(f'{dir_path}/scripts/aws.cfg')
        ssm_client, emr_client=connect_to_aws_resources(*config['AWS'].values())

        logging.basicConfig(format='[%(asctime)s] %(levelname)s - %(message)s', level=logging.INFO)
        args = parse_args()
        params = parameters.get_parameters(ssm_client)
        arg_app=[["2022-08-03"]]
        steps = get_steps(params, args["sensor_final_data"],arg_app)
        cluster_name='sensor-dataAnalyze'
        run_job_flow(params, steps,emr_client,cluster_name)
        
        state_cluster_Id=EMRClusterState(
            cluster_name=cluster_name,
            emr_client=emr_client
            )
        cluster_state=state_cluster_Id.get_status()
        cluster_id=cluster_state["cluster_id"]
        state_cluster=cluster_state["state_cluster"]
        sleep(10)
        while state_cluster!="TERMINATED" and state_cluster!="TERMINATED_WITH_ERRORS":
            # check the cluster state
            
            sleep(5)
            state_cluster = emr_client.describe_cluster(ClusterId=cluster_id)["Cluster"]["Status"]["State"]
            start_date = timezone.utcnow()
            logging.info("cluster ID= {} time = {}, state -->{}".format(cluster_id,start_date,state_cluster))
            if state_cluster=="TERMINATED_WITH_ERRORS":
                raise ValueError('A task failed with errors')
        start_date = timezone.utcnow()
        logging.info("cluster ID= {} time = {}, state -->{}".format(cluster_id,start_date,state_cluster))

    def run_job_flow(params, steps,emr_client,cluster_name):

        import logging
        from botocore.exceptions import ClientError
        """
        The function allows to start and stop automatically when a job is completed
        Create EMR cluster, followed by run Steps, and then finally terminate cluster
        """
        
        try:
            response = emr_client.run_job_flow(
                Name=cluster_name,
                LogUri=f's3n://{params["logs_bucket"]}',
                ReleaseLabel='emr-6.2.0',
                Instances={
                    'InstanceFleets': [
                        {
                            'Name': 'MASTER',
                            'InstanceFleetType': 'MASTER',
                            'TargetOnDemandCapacity': 1,
                            'InstanceTypeConfigs': [
                                {
                                    'InstanceType': 'c4.2xlarge',
                                },
                            ]
                        },
                        {
                            'Name': 'CORE',
                            'InstanceFleetType': 'CORE',
                            'TargetOnDemandCapacity': 2,
                            'InstanceTypeConfigs': [
                                {
                                    'InstanceType': 'c4.2xlarge',
                                },
                            ],
                        },
                    ],
                    'Ec2KeyName': params['ec2_key_name'],
                    'KeepJobFlowAliveWhenNoSteps': False,
                    'TerminationProtected': False,
                    'Ec2SubnetId': params['ec2_subnet_id'],
                },
                Steps=steps,
                BootstrapActions=[
                    {
                        'Name': 'string',
                        'ScriptBootstrapAction': {
                            'Path': f's3://{params["bootstrap_bucket"]}/bootstrap_actions.sh',
                        }
                    },
                ],
                Applications=[
                    {
                        'Name': 'Spark'
                    },
                ],
                Configurations=[
                    {
                        "Classification": "hive-site",
                        "Properties": {
                        "hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
                        "hive.metastore.schema.verification": "false"
                        }
                    }
                ],
                VisibleToAllUsers=True,
                JobFlowRole='EMR_EC2_DefaultRole',
                ServiceRole=params['emr_role'],
                
                Tags=[
                    {
                        'Key': 'Environment',
                        'Value': 'Development'
                    },
                    {
                        'Key': 'Name',
                        'Value': 'project data lake'
                    },
                    {
                        'Key': 'Owner',
                        'Value': 'Biruk Hailesilassie'
                    },
                ],
                EbsRootVolumeSize=32,
                StepConcurrencyLevel=5
            )

            print(f'Response: {response}')
        except ClientError as e:
            logging.error(e)
            return False
        return True


    def get_steps(params, job_type,arg_app):

        """
        Load EMR Steps from a given JSON-format file and 
        extracts as well as substitutes tags for SSM parameter values
        
        """
        import os
        import json


        dir_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
        file = open(f'{dir_path}/job_flow_steps/job_flow_steps_{job_type}.json', 'r')

        steps_job_flow = json.load(file)
        new_steps_job_flow = []

        for k,step in enumerate(steps_job_flow):
            step['HadoopJarStep']['Args'] = list(
                map(lambda st: str.replace(st, '{{ work_bucket }}', params['work_bucket']), step['HadoopJarStep']['Args']))+arg_app[k]
            new_steps_job_flow.append(step)

        return new_steps_job_flow
    

    def parse_args():
        """Parse argument values from input file or manually specified parameters"""
        args={"acc_stage":'process_acc_stage',
            "bvr_stage":'process_bvr_stage',
            "acc_validate":'process_acc_validate',
            "sensor_final_data":'analyze_sensor_data',
            "bvr_validate_Ax_56":'process_bvr_validate_Ax_56',
            "bvr_validate_Ax_134":'process_bvr_validate_Ax_134',
            "bvr_validate_Ax_2":'process_bvr_validate_Ax_2',
            "ec2-key-name": 'spark-cluster'}
        
        return args

    
    process_acc_stage = PythonOperator(
        task_id="StageAccSensorData",
        python_callable=acc_stage,
        dag=dag
    )

    process_bvr_stage = PythonOperator(
        task_id="StageVehicleData",
        python_callable=bvr_stage,
        dag=dag
    )

    process_bvr_validate_ax_group_1_3_4 = PythonOperator(
        task_id="ValidateVehicleGroup_1_3_4",
        python_callable=bvr_validate_Ax_134,
        dag=dag
    )
    process_bvr_validate_ax_group_2 = PythonOperator(
        task_id="ValidateVehicleGroup_2",
        python_callable=bvr_validate_Ax2,
        
        dag=dag
    )
    process_bvr_validate_ax_group_5_6 = PythonOperator(
        task_id="ValidateVehicleGroup_5_6",
        python_callable=bvr_validate_Ax_56,
        dag=dag
    )

    process_acc_validate = PythonOperator(
        task_id="ValidateAccSensorData",
        python_callable=acc_validate,
        dag=dag
    )

    analyze_sensor_data_ = PythonOperator(
        task_id="AnalyzeSensorVehicleData",
        python_callable=analyze_sensor_data,
        dag=dag
    )
    end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

    start_operator >> process_acc_stage >>process_acc_validate
    start_operator >> process_bvr_stage >>process_bvr_validate_ax_group_2
    start_operator >> process_bvr_stage >>process_bvr_validate_ax_group_1_3_4
    start_operator >> process_bvr_stage >>process_bvr_validate_ax_group_5_6
    process_acc_validate >> analyze_sensor_data_>> end_operator 
    process_bvr_validate_ax_group_2 >> analyze_sensor_data_>> end_operator  
    process_bvr_validate_ax_group_1_3_4 >> analyze_sensor_data_>> end_operator  
    process_bvr_validate_ax_group_5_6 >> analyze_sensor_data_>> end_operator
    




  
