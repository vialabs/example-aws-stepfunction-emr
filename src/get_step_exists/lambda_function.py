import os
import boto3

def lambda_handler(event, context):

    input1 = event['input1']
    input2 = event['input2']
    step_id = ''

    try:

        cluster_id = event['Cluster']['ClusterId']
        print(f'Cluster Id = {cluster_id}')

        step_states = ['PENDING','RUNNING']
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

            for step in steps['Steps']:
                if step['Name'] == f'{input1}.{input2}':
                    step_id = step['Id']
                    break    
            if step_id != '':
                break

            if 'Marker' in steps:
                marker = steps['Marker']
            else:
                marker = ''
                break

        print(f'StepId = {step_id}')

        rValues = {
            'StepId': step_id,
        }

    except Exception as e:
        print('ERROR')
        print(str(e))
        try:
            boto3.client('sns').publish(
                TopicArn=os.environ['SNS_ERROR'],
                Subject=f'ERROR {input1}.{input2} GetStepExists '+os.environ['STACK_NAME'],
                Message=str(e)
            )
        except:
            print('SNS Publish Error!')
        rValues = {
            'Error': str(e)
        }

    return rValues
