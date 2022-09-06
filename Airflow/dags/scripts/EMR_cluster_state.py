import logging
import os
import json
import boto3
#connect to AWS resources
import os
import time
import logging
import sys
import configparser


class EMRClusterState():

    def __init__(self,
                 cluster_name="",
                 emr_client="",
                 ):

        self.emr_client = emr_client
        self.cluster_name = cluster_name

    def connect_to_aws_resources(self,KEY,SECRET,REGION):
        emr_client = boto3.client('emr', aws_access_key_id=KEY, aws_secret_access_key=SECRET, region_name=REGION)
        ssm_client = boto3.client('ssm',aws_access_key_id=KEY, aws_secret_access_key=SECRET, region_name=REGION)
        return ssm_client, emr_client
    
    def get_status(self):
        config = configparser.ConfigParser()
        dir_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
        config.read(f'{dir_path}/scripts/aws.cfg')
        
        ssm_client, emr_client=self.connect_to_aws_resources(*config['AWS'].values())
        # logging.info("path parameters",self.path_config_aws)
        response = emr_client.list_clusters(ClusterStates=['RUNNING', 'WAITING','STARTING','BOOTSTRAPPING','TERMINATING'])
        matching_clusters = list(
            filter(lambda cluster: cluster['Name'] == self.cluster_name, response['Clusters'])
        )

        if len(matching_clusters) == 1:
            cluster_id = matching_clusters[0]['Id']
            logging.info("cluster found {}".format(cluster_id))
        else:
            cluster_id=None
            logging.info("cluster ID provided unknown")
        if cluster_id:
            state_cluster = emr_client.describe_cluster(ClusterId=cluster_id)["Cluster"]["Status"]["State"]
        return {"state_cluster":state_cluster,"cluster_id":cluster_id}