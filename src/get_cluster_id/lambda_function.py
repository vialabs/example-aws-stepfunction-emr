import json, os

import boto3

def lambda_handler(event, context):

    cluster_id_param = os.environ['CLUTER_ID_PARAM']

    ssm_param = boto3.client('ssm').get_parameter(
        Name=cluster_id_param
    )

    cluster_id = ssm_param['Parameter']['Value']

    return {
        'clusterId': cluster_id
    }
