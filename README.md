# example-aws-stepfunction-emr
Example for AWS Step Function, Lambda SSM Parameter Store, EMR and SNS

![Diagrama Modelo Exemplo](modelo-exemplo.png)

Upload the files to SCRIPT_BUCKET:
- `hello_world.py`
- `install_libs.sh`

LINUX:
```bash
export AWS_PROFILE=<value>
```

```bash
ENV=dev
BUCKET=<value>
SCRIPT_BUCKET=<value>
EMAIL_ERROR=<value>
```

```bash
aws cloudformation package --template-file template.yaml --output-template-file template_output_${ENV}.tmp --s3-bucket ${BUCKET}
```

```bash
aws cloudformation deploy --template-file template_output_${ENV}.tmp --stack-name cfstack-${ENV}-test --capabilities CAPABILITY_IAM --parameter-overrides ScriptBucket=${SCRIPT_BUCKET} EmailError=${EMAIL_ERROR}
```

WINDOWS:
```bash
set AWS_PROFILE=<value>
```

```bash
set ENV=dev
set BUCKET=<value>
set SCRIPT_BUCKET=<value>
set EMAIL_ERROR=<value>
```

```bash
aws cloudformation package --template-file template.yaml --output-template-file template_output_%ENV%.tmp --s3-bucket %BUCKET%
```

```bash
aws cloudformation deploy --template-file template_output_%ENV%.tmp --stack-name cfstack-%ENV%-test --capabilities CAPABILITY_IAM --parameter-overrides ScriptBucket=%SCRIPT_BUCKET% EmailError=%EMAIL_ERROR%
```
