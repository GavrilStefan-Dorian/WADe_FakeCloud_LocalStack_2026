"""Deploy ONLY the separate graduate processing Lambda and its dedicated S3 buckets.

Never contacts AWS: both endpoints are hardcoded to 127.0.0.1.
Does not modify wade-hello, wade-events, wade-results or existing benchmarks.
Requires boto3 from the project's existing Python .venv.
"""
import io
import json
import time
import zipfile
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

HERE = Path(__file__).resolve().parent
NAME = 'wade-graduates-v1'
INPUT_BUCKET = 'wade-graduates-in'
OUTPUT_BUCKET = 'wade-graduates-out'
PORTS = {'FakeCloud': 4566, 'LocalStack': 4567}
ROLE = 'wade-graduates-lambda'


def client(service, port):
    return boto3.client(
        service,
        endpoint_url=f'http://127.0.0.1:{port}',
        region_name='us-east-1',
        aws_access_key_id='test',
        aws_secret_access_key='test',
        config=Config(retries={'max_attempts': 1}),
    )


def exists_error(error):
    return error.response.get('Error', {}).get('Code') in {
        'ResourceNotFoundException', '404', 'NoSuchEntity'
    }


def wait_for_lambda(lam):
    for _ in range(60):
        details = lam.get_function_configuration(FunctionName=NAME)
        if details.get('State') == 'Active' and details.get('LastUpdateStatus', 'Successful') == 'Successful':
            return details
        if details.get('State') == 'Failed' or details.get('LastUpdateStatus') == 'Failed':
            raise RuntimeError(f'Lambda setup failed: {details}')
        time.sleep(1)
    raise TimeoutError(f'{NAME} never became Active')


def ensure_role(iam):
    """Create a dedicated role in the local emulator, without any older demo."""
    try:
        role = iam.get_role(RoleName=ROLE)['Role']
    except ClientError as error:
        if not exists_error(error):
            raise
        role = iam.create_role(
            RoleName=ROLE,
            AssumeRolePolicyDocument=json.dumps({
                'Version': '2012-10-17',
                'Statement': [{'Effect': 'Allow',
                               'Principal': {'Service': 'lambda.amazonaws.com'},
                               'Action': 'sts:AssumeRole'}],
            }),
        )['Role']
    iam.put_role_policy(
        RoleName=ROLE,
        PolicyName='GraduateDemoStorageAndLogs',
        PolicyDocument=json.dumps({
            'Version': '2012-10-17',
            'Statement': [
                {'Effect': 'Allow', 'Action': ['s3:GetObject'],
                 'Resource': [f'arn:aws:s3:::{INPUT_BUCKET}/*',
                              'arn:aws:s3:::wade-queued-graduates-in/*']},
                {'Effect': 'Allow', 'Action': ['s3:PutObject'],
                 'Resource': f'arn:aws:s3:::{OUTPUT_BUCKET}/*'},
                {'Effect': 'Allow',
                 'Action': ['logs:CreateLogGroup', 'logs:CreateLogStream', 'logs:PutLogEvents'],
                 'Resource': 'arn:aws:logs:us-east-1:*:*'},
            ],
        }),
    )
    return role['Arn']


def main():
    archive = io.BytesIO()
    with zipfile.ZipFile(archive, 'w', zipfile.ZIP_DEFLATED) as out:
        out.write(HERE / 'index.js', 'index.js')
        out.write(HERE / 'processor.js', 'processor.js')
    package = archive.getvalue()

    for name, port in PORTS.items():
        print(f'\n== {name} localhost:{port} ==', flush=True)
        s3 = client('s3', port)
        lam = client('lambda', port)
        for bucket in (INPUT_BUCKET, OUTPUT_BUCKET):
            try:
                s3.head_bucket(Bucket=bucket)
                print('Existing bucket:', bucket)
            except ClientError as error:
                if error.response.get('ResponseMetadata', {}).get('HTTPStatusCode') != 404:
                    raise
                s3.create_bucket(Bucket=bucket)
                print('Created bucket:', bucket)

        try:
            lam.get_function_configuration(FunctionName=NAME)
        except ClientError as error:
            if not exists_error(error):
                raise
            role = ensure_role(client('iam', port))
            lam.create_function(
                FunctionName=NAME,
                Runtime='nodejs20.x',
                Role=role,
                Handler='index.handler',
                Code={'ZipFile': package},
                Timeout=30,
                MemorySize=128,
                Publish=False,
            )
            print('Created new Lambda:', NAME)
        else:
            lam.update_function_code(FunctionName=NAME, ZipFile=package)
            print('Updated own Lambda:', NAME)

        arn = wait_for_lambda(lam)['FunctionArn']
        print('Actual function ARN:', arn)
        try:
            lam.add_permission(
                FunctionName=NAME,
                StatementId='WadeGraduateInputS3Event',
                Action='lambda:InvokeFunction',
                Principal='s3.amazonaws.com',
                SourceArn=f'arn:aws:s3:::{INPUT_BUCKET}',
            )
        except ClientError as error:
            if error.response.get('Error', {}).get('Code') not in {
                'ResourceConflictException', 'ResourceAlreadyExistsException'
            }:
                raise
            print('Lambda notification permission already exists')

        # Only changes notifications on the new, isolated graduate input bucket.
        s3.put_bucket_notification_configuration(
            Bucket=INPUT_BUCKET,
            NotificationConfiguration={
                'LambdaFunctionConfigurations': [{
                    'Id': 'GraduateCsvUpload',
                    'LambdaFunctionArn': arn,
                    'Events': ['s3:ObjectCreated:*'],
                    'Filter': {'Key': {'FilterRules': [
                        {'Name': 'prefix', 'Value': 'incoming/'},
                        {'Name': 'suffix', 'Value': '.csv'}
                    ]}}
                }]
            },
        )
        print('S3 notification configured; no old Lambda or buckets changed')

    print('\nDeployment configured on both local emulators.')


if __name__ == '__main__':
    main()
