#!/usr/bin/env python3


def get_parameters(ssm_client):
    """Load parameter values from AWS Systems Manager (SSM) Parameter Store"""

    params = {
        'airflow_bucket': ssm_client.get_parameter(Name='/data-lake-pro/airflow-bucket')['Parameter']['Value'],
        'bootstrap_bucket': ssm_client.get_parameter(Name='/data-lake-pro/bootstrap_bucket')['Parameter']['Value'],
        'acc_data': ssm_client.get_parameter(Name='/data-lake-pro/acc-data')['Parameter']['Value'],
        'bvr_data': ssm_client.get_parameter(Name='/data-lake-pro/bvr-data')['Parameter']['Value'],
        'work_bucket': ssm_client.get_parameter(Name='/data-lake-pro/work_bucket')['Parameter']['Value'],
        'logs_bucket': ssm_client.get_parameter(Name='/data-lake-pro/logs-bucket')['Parameter']['Value'],
        'processed_data': ssm_client.get_parameter(Name='/data-lake-pro/processed-data')['Parameter']['Value'],
        'schema_data': ssm_client.get_parameter(Name='/data-lake-pro/schema-data')['Parameter']['Value'],
        'ec2_key_name': ssm_client.get_parameter(Name='/data-lake-pro/ec2_key_name')['Parameter']['Value'],
        'ec2_subnet_id': ssm_client.get_parameter(Name='/data-lake-pro/ec2_subnet_id')['Parameter']['Value'],
        'emr_ec2_role': ssm_client.get_parameter(Name='/data-lake-pro/emr_ec2_role')['Parameter']['Value'],
        'emr_role': ssm_client.get_parameter(Name='/data-lake-pro/emr_role')['Parameter']['Value'],
        'analyzed_data': ssm_client.get_parameter(Name='/data-lake-pro/analyzed-data')['Parameter']['Value'],
        'sm_log_group_arn': ssm_client.get_parameter(Name='/data-lake-pro/sm_log_group_arn')['Parameter']['Value'],
        'sm_role_arn': ssm_client.get_parameter(Name='/data-lake-pro/sm_role_arn')['Parameter']['Value'],
        'vpc_id': ssm_client.get_parameter(Name='/data-lake-pro/vpc_id')['Parameter']['Value'],
    }

    return params
