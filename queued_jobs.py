"""S3 + SQS + Lambda + DynamoDB graduate job experiment.

Separate mode: no edits to the original S3-notification pipeline.
All clients are fixed to the two local emulator endpoints; no AWS account is used.
The browser bridge acts as the polling worker. It uses real SQS receive/delete
and invokes the existing Lambda with a genuine S3-shaped event payload.
"""
from __future__ import annotations
import json
import secrets
import time
from datetime import datetime, timezone
from urllib.parse import urlsplit, urlunsplit

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

PORTS = {'FakeCloud': 4566, 'LocalStack': 4567}
BUCKET = 'wade-queued-graduates-in'
REPORT_BUCKET = 'wade-graduates-out'
QUEUE = 'wade-graduate-jobs-v2'
TABLE = 'wade-graduate-jobs-v2'
FUNCTION = 'wade-graduates-v1'
VISIBILITY_SECONDS = 8
REGION = 'us-east-1'


def clients(name):
    if name not in PORTS:
        raise ValueError('Select FakeCloud or LocalStack')
    opts = dict(endpoint_url=f'http://127.0.0.1:{PORTS[name]}',
                region_name=REGION, aws_access_key_id='test', aws_secret_access_key='test',
                config=Config(retries={'max_attempts': 1}, connect_timeout=2, read_timeout=8))
    s3opts = {**opts, 'config': Config(s3={'addressing_style': 'path'}, retries={'max_attempts': 1}, connect_timeout=2, read_timeout=8)}
    return {kind: boto3.client(kind, **(s3opts if kind == 's3' else opts))
            for kind in ('s3', 'sqs', 'dynamodb', 'lambda')}


def queue_url(sqs, name):
    """Keep an emulator-provided account/path but send it to our local port."""
    original = sqs.get_queue_url(QueueName=QUEUE)['QueueUrl']
    parts = urlsplit(original)
    if parts.scheme not in ('http', 'https'):
        raise ValueError('Unexpected queue URL')
    return urlunsplit(('http', f'127.0.0.1:{PORTS[name]}', parts.path, parts.query, ''))


def ensure(name, c=None):
    c = c or clients(name)
    try:
        c['s3'].head_bucket(Bucket=BUCKET)
    except ClientError as exc:
        if exc.response['Error']['Code'] not in ('404', 'NoSuchBucket', 'NotFound'):
            raise
        c['s3'].create_bucket(Bucket=BUCKET)
    # A queue is explicitly application-managed; no event-mapping needs installing.
    c['sqs'].create_queue(QueueName=QUEUE, Attributes={'VisibilityTimeout': str(VISIBILITY_SECONDS)})
    try:
        c['dynamodb'].describe_table(TableName=TABLE)
    except ClientError as exc:
        if exc.response['Error']['Code'] != 'ResourceNotFoundException':
            raise
        c['dynamodb'].create_table(
            TableName=TABLE,
            AttributeDefinitions=[{'AttributeName':'jobId','AttributeType':'S'}],
            KeySchema=[{'AttributeName':'jobId','KeyType':'HASH'}],
            BillingMode='PAY_PER_REQUEST')
    return c


def _item(d, jobid):
    raw = d.get_item(TableName=TABLE, Key={'jobId': {'S': jobid}}).get('Item')
    if not raw:
        return None
    return {k: next(iter(v.values())) for k, v in raw.items()}


def _put_status(d, jobid, status, note=''):
    d.update_item(
        TableName=TABLE, Key={'jobId': {'S': jobid}},
        UpdateExpression='SET #st = :st, #note = :note, updatedAt = :at',
        ExpressionAttributeNames={'#st':'status', '#note':'note'},
        ExpressionAttributeValues={':st': {'S':status}, ':note': {'S':note},
                                   ':at': {'S':datetime.now(timezone.utc).isoformat(timespec='seconds')}})


def submit(csv: str, targets: list[str]):
    if not isinstance(csv, str) or not csv.strip() or len(csv.encode('utf-8')) > 110000:
        raise ValueError('Provide CSV text, no more than 110 KB')
    if not isinstance(targets, list) or not targets or len(set(targets)) != len(targets) or any(t not in PORTS for t in targets):
        raise ValueError('Select one or both local emulators')
    jobid = secrets.token_hex(6)
    key = f'incoming/queued-{jobid}.csv'
    now = datetime.now(timezone.utc).isoformat(timespec='seconds')
    out = {}
    for name in targets:
        c = ensure(name)
        # Save actual source file, then index job, then publish queue message.
        c['s3'].put_object(Bucket=BUCKET, Key=key, Body=csv.encode('utf-8'), ContentType='text/csv')
        c['dynamodb'].put_item(TableName=TABLE, Item={
            'jobId': {'S':jobid}, 'inputKey': {'S':key}, 'status': {'S':'queued'},
            'createdAt': {'S':now}, 'updatedAt': {'S':now},
            'note': {'S':'Awaiting worker'}})
        message = c['sqs'].send_message(QueueUrl=queue_url(c['sqs'], name),
                MessageBody=json.dumps({'jobId':jobid, 'key':key}, separators=(',', ':')))
        out[name] = {'jobId':jobid, 'inputKey':key, 'sqsMessageId':message['MessageId'], 'status':'queued'}
    return {'jobs':out, 'servicePath':['S3', 'SQS', 'Lambda', 'S3', 'DynamoDB']}


def pickup(name, *, interrupt=False):
    c = ensure(name)
    url = queue_url(c['sqs'], name)
    response = c['sqs'].receive_message(QueueUrl=url, MaxNumberOfMessages=1,
              VisibilityTimeout=VISIBILITY_SECONDS, WaitTimeSeconds=0,
              AttributeNames=['ApproximateReceiveCount'])
    msgs = response.get('Messages', [])
    if not msgs:
        return {'state':'empty-or-invisible', 'explanation':f'No visible job. A picked-up message becomes visible again after {VISIBILITY_SECONDS} seconds if not deleted.'}
    message = msgs[0]
    payload = json.loads(message['Body'])
    jobid, key = payload['jobId'], payload['key']
    if not jobid or not key.startswith('incoming/queued-'):
        raise ValueError('Unexpected queue message')
    count = int(message.get('Attributes', {}).get('ApproximateReceiveCount', '1'))
    if interrupt:
        _put_status(c['dynamodb'], jobid, 'interrupted', 'Worker picked up the message but stopped before processing; message was not deleted')
        return {'state':'interrupted', 'jobId':jobid, 'receiveCount':count,
                'explanation':f'Worker stopped. The same job is invisible for about {VISIBILITY_SECONDS} seconds, then can be retried.'}
    try:
        event = {'Records':[{'eventSource':'aws:s3', 'awsRegion':REGION,
                  's3':{'bucket':{'name':BUCKET}, 'object':{'key':key}}}]}
        invocation = c['lambda'].invoke(FunctionName=FUNCTION, InvocationType='RequestResponse',
                                         Payload=json.dumps(event).encode('utf-8'))
        body = invocation['Payload'].read()
        if invocation.get('FunctionError') or invocation.get('StatusCode') != 200:
            raise RuntimeError(f'Lambda execution failed: {body[:180]!r}')
        parsed = json.loads(body)
        if not parsed.get('processed'):
            raise RuntimeError(f'Lambda returned no processed records: {parsed}')
        reportkey = 'reports/' + key[len('incoming/'):].replace('.csv', '.json')
        file = c['s3'].get_object(Bucket=REPORT_BUCKET, Key=reportkey)['Body']
        try:
            report = json.loads(file.read())
        finally:
            file.close()
        if not isinstance(report.get('accepted'), int):
            raise RuntimeError('Generated report is missing counts')
        # Commit the outcome in DynamoDB, then acknowledge SQS message.
        _put_status(c['dynamodb'], jobid, 'completed',
                    f"{report['accepted']} accepted, {report['rejected']} rejected")
        c['sqs'].delete_message(QueueUrl=url, ReceiptHandle=message['ReceiptHandle'])
        return {'state':'completed', 'jobId':jobid, 'receiveCount':count,
                'accepted':report['accepted'], 'rejected':report['rejected'],
                'employmentRatePct':report.get('employmentRatePct'), 'reportKey':reportkey,
                'report':report, 'messageDeleted':True}
    except Exception as exc:
        _put_status(c['dynamodb'], jobid, 'failed', str(exc)[:200])
        return {'state':'failed', 'jobId':jobid, 'receiveCount':count,
                'messageDeleted':False, 'error':f'{type(exc).__name__}: {str(exc)[:240]}'}


def state(name):
    c = ensure(name)
    response = c['dynamodb'].scan(TableName=TABLE, Limit=100)
    items = []
    for raw in response.get('Items', []):
        items.append({k:next(iter(v.values())) for k,v in raw.items()})
    items.sort(key=lambda x:x.get('createdAt',''), reverse=True)
    attrs = c['sqs'].get_queue_attributes(QueueUrl=queue_url(c['sqs'],name),
                    AttributeNames=['ApproximateNumberOfMessages','ApproximateNumberOfMessagesNotVisible'])['Attributes']
    return {'emulator':name, 'jobs':items[:10], 'queue':{
        'visibleApprox':int(attrs.get('ApproximateNumberOfMessages',0)),
        'inFlightApprox':int(attrs.get('ApproximateNumberOfMessagesNotVisible',0)),
        'visibilitySeconds':VISIBILITY_SECONDS}}
