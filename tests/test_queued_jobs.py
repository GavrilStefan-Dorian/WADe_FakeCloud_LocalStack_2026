"""Offline test of the actual job lifecycle; no Docker or real AWS involved."""
import io, json, sys, unittest
from pathlib import Path
from unittest.mock import patch
from botocore.exceptions import ClientError
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import queued_jobs as q

class ServiceError(ClientError):
    def __init__(self,code):super().__init__({'Error':{'Code':code,'Message':code}},'operation')

class S3:
    def __init__(self):self.buckets=set();self.objects={}
    def head_bucket(self, Bucket):
        if Bucket not in self.buckets:raise ServiceError('404')
    def create_bucket(self,Bucket):self.buckets.add(Bucket)
    def put_object(self,Bucket,Key,Body,**kwargs):self.objects[(Bucket,Key)]=Body
    def get_object(self,Bucket,Key):
        if (Bucket,Key) not in self.objects:raise ServiceError('NoSuchKey')
        return {'Body':io.BytesIO(self.objects[(Bucket,Key)])}

class Queue:
    def __init__(self):self.items=[];self.clock=0
    def create_queue(self,QueueName,Attributes):return {'QueueUrl':'http://somewhere/123/'+QueueName}
    def get_queue_url(self,QueueName):return {'QueueUrl':'http://unreachable:4566/123/'+QueueName}
    def send_message(self,QueueUrl,MessageBody):
        self.items.append({'Body':MessageBody,'ReceiptHandle':str(len(self.items)),'visible':self.clock,'count':0});return {'MessageId':'message-123'}
    def receive_message(self,QueueUrl,VisibilityTimeout,**kwargs):
        for x in self.items:
            if x['visible']<=self.clock:
                x['visible']=self.clock+VisibilityTimeout;x['count']+=1
                return {'Messages':[{'Body':x['Body'],'ReceiptHandle':x['ReceiptHandle'],'Attributes':{'ApproximateReceiveCount':str(x['count'])}}]}
        return {}
    def delete_message(self,QueueUrl,ReceiptHandle):self.items=[x for x in self.items if x['ReceiptHandle'] != ReceiptHandle]
    def get_queue_attributes(self,QueueUrl,AttributeNames):return {'Attributes':{'ApproximateNumberOfMessages':str(sum(x['visible']<=self.clock for x in self.items)),'ApproximateNumberOfMessagesNotVisible':str(sum(x['visible']>self.clock for x in self.items))}}

class DDB:
    def __init__(self):self.table=False;self.items={}
    def describe_table(self,TableName):
        if not self.table:raise ServiceError('ResourceNotFoundException')
        return {'Table':{'TableName':TableName}}
    def create_table(self,**kwargs):self.table=True
    def put_item(self,TableName,Item):self.items[Item['jobId']['S']]=Item
    def update_item(self,TableName,Key,ExpressionAttributeValues,**kwargs):
        item=self.items[Key['jobId']['S']]
        item.update(status=ExpressionAttributeValues[':st'],note=ExpressionAttributeValues[':note'],updatedAt=ExpressionAttributeValues[':at'])
    def scan(self,TableName,Limit):return {'Items':list(self.items.values())}
    def get_item(self,TableName,Key):return {'Item':self.items.get(Key['jobId']['S'])}

class Lambda:
    def __init__(self,s3):self.s3=s3;self.calls=[]
    def invoke(self,FunctionName,InvocationType,Payload):
        event=json.loads(Payload);self.calls.append(event)
        key=event['Records'][0]['s3']['object']['key']
        reportkey='reports/'+key.split('incoming/',1)[1].replace('.csv','.json')
        self.s3.put_object(Bucket=q.REPORT_BUCKET,Key=reportkey,Body=json.dumps({'accepted':12,'rejected':0,'employmentRatePct':58.3}).encode())
        return {'StatusCode':200,'Payload':io.BytesIO(b'{"processed":[{"status":"ok"}]}')}

class Tests(unittest.TestCase):
    def setUp(self):
        self.s3=S3();self.s3.buckets.add(q.REPORT_BUCKET)
        self.sqs=Queue();self.ddb=DDB();self.lam=Lambda(self.s3)
        self.c={'s3':self.s3,'sqs':self.sqs,'dynamodb':self.ddb,'lambda':self.lam}
        self.patch=patch.object(q,'clients',return_value=self.c);self.patch.start()
    def tearDown(self):self.patch.stop()
    def test_full_failure_retry(self):
        sent=q.submit('graduate_id,graduation_year,program,industry,city,status\nG001,2022,CS,IT,Iasi,employed',['FakeCloud'])
        id=sent['jobs']['FakeCloud']['jobId']
        self.assertEqual(q.state('FakeCloud')['jobs'][0]['status'],'queued')
        crash=q.pickup('FakeCloud',interrupt=True)
        self.assertEqual(crash['state'],'interrupted')
        self.assertEqual(q.pickup('FakeCloud')['state'],'empty-or-invisible')
        self.sqs.clock+=9
        done=q.pickup('FakeCloud')
        self.assertEqual(done['state'],'completed')
        self.assertEqual(done['receiveCount'],2)
        self.assertTrue(done['messageDeleted'])
        self.assertEqual(q.state('FakeCloud')['jobs'][0]['status'],'completed')
        self.assertEqual(q.state('FakeCloud')['queue']['visibleApprox'],0)
        self.assertEqual(len(self.lam.calls),1)
        self.assertEqual(id,done['jobId'])
    def test_separate_emulators_have_separate_message_state(self):
        other={'s3':S3(),'sqs':Queue(),'dynamodb':DDB(),'lambda':Lambda(S3())}
        other['s3'].buckets.add(q.REPORT_BUCKET);other['lambda'].s3=other['s3']
        with patch.object(q,'clients',side_effect=lambda name:self.c if name=='FakeCloud' else other):
            q.submit('a,b,c',['FakeCloud','LocalStack'])
            q.pickup('FakeCloud', interrupt=True)
            self.assertEqual(q.state('FakeCloud')['queue']['visibleApprox'],0)
            self.assertEqual(q.state('LocalStack')['queue']['visibleApprox'],1)
    def test_validate_targets(self):
        with self.assertRaises(ValueError):q.submit('csv', ['FakeCloud','unknown'])

if __name__=='__main__':unittest.main()
