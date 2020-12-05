import os
import boto3

def lambda_handler(event, context):

    print('***************')
    print(event)
    print('***************')

    try:

        if 'ClusterId' in event:
            # Get Cluster Active Step Count

            print('Getting Cluster Active Step Count...')

            cluster_id = event['ClusterId']

            print(f'Cluster Id = {cluster_id}')

            step_states = ['PENDING','RUNNING']
            step_count = 0
            marker = ''
            while True:
                
                if marker == '':
                    steps = boto3.client('emr').list_steps(
                        ClusterId=cluster_id,
                        StepStates=step_states
                    )
                else:
                    steps = boto3.client('emr').list_steps(
                        ClusterId=cluster_id,
                        StepStates=step_states,
                        Marker=marker
                    )
                    
                step_count += len(steps['Steps'])
                    
                if 'Marker' in steps:
                    marker = steps['Marker']
                else:
                    marker = ''
                    break

            print(f'Active Step Count = {step_count}')

            rValues = {
                'StepCount': step_count,
            }

        else:
            # Get Cluster Id

            print('Getting Cluster Id...')

            cluster_name = os.environ['CLUSTER_NAME']

            print(f'Cluster Name = {cluster_name}')

            cluster_status = ['STARTING','BOOTSTRAPPING','RUNNING','WAITING']
            cluster_id = ''
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

            rValues = {
                'ClusterId': cluster_id
            }

    except Exception e:
        print('ERROR')
        print(str(e))
        try:
            boto3.client('sns').publish(
                TopicArn=os.environ['SNS_ERROR'],
                Subject='ERROR Lambda '+os.environ['STACK_NAME'],
                Message=str(e)
            )
        except:
            print('SNS Publish Error!')
        rValues = {
            'Error': str(e)
        }

    return rValues
