import os
import boto3

def lambda_handler(event, context):

    input_1 = event['input1']
    input_2 = event['input2']

    try:

        cluster_id   = ''
        keep_cluster = False

        # Não finalizar cluster após a execução
        if 'KeepCluster' in event:
            keep_cluster = event['KeepCluster']

        # Utilizar cluster ou criar um novo
        if 'ClusterId' in event:

            cluster_id = event['ClusterId']

            # Padrão de cluster existe é não finalizar
            if 'KeepCluster' not in event:
                keep_cluster = True

        else:

            print('Getting Cluster Id...')

            cluster_name = os.environ['CLUSTER_NAME']

            print(f'Cluster Name = {cluster_name}')

            cluster_status = ['STARTING','BOOTSTRAPPING','RUNNING','WAITING']
            marker = ''
            while True:
                
                if marker == '':
                    clusters = boto3.client('emr').list_clusters(
                        ClusterStates=cluster_status
                    )
                else:
                    clusters = boto3.client('emr').list_clusters(
                        ClusterStates=cluster_status,
                        Marker=marker
                    )
                    
                for cluster in clusters['Clusters']:
                    if cluster['Name'] == cluster_name:
                        cluster_id = cluster['Id']
                        break
                    
                if 'Marker' in clusters:
                    marker = clusters['Marker']
                else:
                    marker = ''
                    break

        print(f'Cluster Id = {cluster_id}')
        print(f'Keep Cluster = {keep_cluster}')

        rValues = event
        rValues['Cluster'] = {'ClusterId':cluster_id}
        rValues['KeepCluster'] = keep_cluster

    except Exception as e:
        print('ERROR')
        print(str(e))
        try:
            boto3.client('sns').publish(
                TopicArn=os.environ['SNS_ERROR'],
                Subject=f'ERROR {input_1}.{input_2} GetParamFunctions '+os.environ['STACK_NAME'],
                Message=str(e)
            )
        except:
            print('SNS Publish Error!')
        rValues = {
            'Error': str(e)
        }

    return rValues
