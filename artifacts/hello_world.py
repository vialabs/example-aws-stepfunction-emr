import sys

import boto3

print('Hello World - START')

sns_topic = sys.argv[1]

error = True

try:

    if error:
        raise Exception("Erro teste de dentro do Script executado no Spark.")

    print('Execução com sucesso')

except Exception as e:

    boto3.client('sns').publish(
        TopicArn=sns_topic,
        Subject='ERROR Script Spark',
        Message=str(e)
    )

    print('Finalizado com erro')

print('Hello World - END')
