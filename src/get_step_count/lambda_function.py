import os
import boto3

def lambda_handler(event, context):

    input_1 = event['input1']
    input_2 = event['input2']

    try:

        print('Getting Cluster Active Step Count...')

        cluster_id = event['Cluster']['ClusterId']

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

    except Exception as e:
        print('ERROR')
        print(str(e))
        try:
            boto3.client('sns').publish(
                TopicArn=os.environ['SNS_ERROR'],
                Subject=f'ERROR {input_1}.{input_2} GetStepCount '+os.environ['STACK_NAME'],
                Message=str(e)
            )
        except:
            print('SNS Publish Error!')
        rValues = {
            'Error': str(e)
        }

    return rValues
