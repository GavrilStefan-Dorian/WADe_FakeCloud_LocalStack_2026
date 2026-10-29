"""Run identical CSV uploads against both LOCAL emulators; verify actual S3 reports.

Invoke AFTER deploy_local.py succeeded. No real AWS credentials or endpoints.
"""
import json
import time
import uuid
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

HERE = Path(__file__).resolve().parent
INPUT_BUCKET = 'wade-graduates-in'
OUTPUT_BUCKET = 'wade-graduates-out'
PORTS = {'FakeCloud': 4566, 'LocalStack': 4567}


def client(port):
    return boto3.client(
        's3', endpoint_url=f'http://127.0.0.1:{port}',
        region_name='us-east-1',
        aws_access_key_id='test', aws_secret_access_key='test',
        config=Config(retries={'max_attempts': 1}),
    )


def retrieve(client_, key, timeout=30):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            response = client_.get_object(Bucket=OUTPUT_BUCKET, Key=key)
            return json.loads(response['Body'].read())
        except ClientError as error:
            if error.response.get('Error', {}).get('Code') not in {
                'NoSuchKey', '404', 'NotFound'
            }:
                raise
            time.sleep(0.5)
    raise TimeoutError(f'No report appeared in {OUTPUT_BUCKET}/{key}')


def main():
    evidence = HERE / 'evidence'
    evidence.mkdir(exist_ok=True)
    for filename, expected in [
        ('graduates-valid.csv', ('ok', 12, 0)),
        ('graduates-problems.csv', ('partial', 4, 4)),
    ]:
        # Identical object key across the two isolated emulators.
        key = f'incoming/{filename[:-4]}-{uuid.uuid4().hex[:8]}.csv'
        report_key = 'reports/' + key[len('incoming/'):].replace('.csv', '.json')
        data = (HERE / 'samples' / filename).read_bytes()
        reports = {}
        print(f'\n== {filename} ==', flush=True)
        for emulator, port in PORTS.items():
            s3 = client(port)
            print(emulator, 'uploading', key, flush=True)
            s3.put_object(Bucket=INPUT_BUCKET, Key=key, Body=data, ContentType='text/csv')
            report = retrieve(s3, report_key)
            result = (report['status'], report['accepted'], report['rejected'])
            if result != expected:
                raise AssertionError(f'{emulator}: got {result}, expected {expected}')
            reports[emulator] = report
            (evidence / f'{emulator.lower()}-{filename[:-4]}.json').write_text(
                json.dumps(report, indent=2), encoding='utf-8'
            )
            print(emulator, result, flush=True)
        if reports['FakeCloud'] != reports['LocalStack']:
            raise AssertionError(f'Reports disagree for {filename}: inspect evidence/*.json')
        print('MATCH: byte-equivalent report data across both emulators', flush=True)
    print('\nPASS: both datasets processed and both emulator reports agree')


if __name__ == '__main__':
    main()
