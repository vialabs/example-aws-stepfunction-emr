import sys, time

import boto3

print('Hello World - START')

stack_name = sys.argv[1]
sns_topic  = sys.argv[2]
input1     = sys.argv[3]
input2     = sys.argv[4]

try:

    print(f'input1 = {input1}')
    print(f'input2 = {input2}')

    print('Sleeping...')
    time.sleep(15)
    print('Wake!')

    error = True
    # error = False

    if error:
        raise Exception("Erro teste de dentro do Script executado no Spark.")

    print('Execução com sucesso')

except Exception as e:

    boto3.client('sns').publish(
        TopicArn=sns_topic,
        Subject=f'ERROR {input1}.{input2} Script {stack_name}',
        Message=str(e)
    )

    print('Finalizado com erro')

print('Hello World - END')
