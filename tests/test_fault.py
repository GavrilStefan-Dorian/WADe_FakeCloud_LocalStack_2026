import io
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import ui_server as app
from botocore.exceptions import ClientError

class DummyBody:
    def __init__(self, value): self.value=value
    def read(self): return self.value
    def close(self): pass

class FakeS3:
    def __init__(self, arn):
        self.arn=arn
        self.config={'LambdaFunctionConfigurations':[{'Id':'WadeObjectCreated', 'LambdaFunctionArn':arn, 'Events':['s3:ObjectCreated:*']}]}
        self.objects={}
        self.fail_upload=False
        self.writes=0
    def get_bucket_notification_configuration(self,**kwargs):
        return {**self.config,'ResponseMetadata':{'HTTPStatusCode':200}}
    def put_bucket_notification_configuration(self,NotificationConfiguration,**kwargs):
        self.writes+=1
        self.config=NotificationConfiguration.copy()
    def put_object(self,Bucket,Key,Body,**kwargs):
        if self.fail_upload: raise RuntimeError('simulated S3 failure')
        self.objects[(Bucket,Key)] = Body
        if self.config.get('LambdaFunctionConfigurations'):
            filename=Key.split('/')[-1]
            import json
            self.objects[(app.OUTPUT_BUCKET,'reports/'+filename[:-4]+'.json')] = json.dumps(
                {'accepted':12,'rejected':0,'status':'complete'}).encode()
        return {'ETag':'abc'}
    def head_object(self,Bucket,Key):
        if (Bucket,Key) not in self.objects: raise KeyError(Key)
        return {'ContentLength': len(self.objects[(Bucket,Key)])}
    def get_object(self,Bucket,Key):
        if (Bucket,Key) not in self.objects:
            raise ClientError({'Error':{'Code':'NoSuchKey','Message':'missing'}},'GetObject')
        return {'Body':DummyBody(self.objects[(Bucket,Key)])}

class FakeLambda:
    def __init__(self,arn):self.arn=arn
    def get_function_configuration(self,**kwargs):return {'FunctionArn':self.arn}

class TestOutage(unittest.TestCase):
    def setUp(self):
        self.a='arn:aws:lambda:us-east-1:123456789012:function:wade-graduates-v1'
        self.b='arn:aws:lambda:us-east-1:000000000000:function:wade-graduates-v1'
        self.services={'FakeCloud':FakeS3(self.a),'LocalStack':FakeS3(self.b)}
        self.dir=tempfile.TemporaryDirectory()
        self.old=app.EVIDENCE
        app.EVIDENCE=Path(self.dir.name)
        self.s3patch=patch.object(app,'s3_client',side_effect=lambda name:self.services[name]);self.s3patch.start()
        real_boto3=app.boto3.client
        self.b3patch=patch.object(app.boto3,'client',side_effect=lambda typ,**kwargs: FakeLambda(self.a if '4566' in kwargs.get('endpoint_url','') else self.b) if typ=='lambda' else real_boto3(typ,**kwargs))
        self.b3patch.start()
    def tearDown(self):
        self.s3patch.stop();self.b3patch.stop();app.EVIDENCE=self.old;self.dir.cleanup()
    def test_disconnect_and_restore_both(self):
        with patch.object(app.time,'sleep',return_value=None):
            result=app.notification_outage_test('graduate_id,...', ['FakeCloud','LocalStack'])
        self.assertTrue(result['complete'],result)
        for item in result['emulators']:
            self.assertTrue(item['failedReportStillAbsent'])
            self.assertTrue(item['notificationRestored'])
            self.assertEqual(item['accepted'],12)
            self.assertEqual(self.services[item['emulator']].writes,2)
            self.assertTrue((app.EVIDENCE / ('notification-backup-'+item['emulator'].lower()+'.json')).exists())
        self.assertTrue((app.EVIDENCE/'latest-notification-outage.json').exists())
    def test_restore_even_if_upload_fails(self):
        self.services['FakeCloud'].fail_upload=True
        with patch.object(app.time,'sleep',return_value=None):
            result=app.notification_outage_test('valid csv', ['FakeCloud'])
        row=result['emulators'][0]
        self.assertFalse(result['complete'])
        self.assertTrue(row['notificationRestored'])
        self.assertIsNotNone(row['error'])
        self.assertEqual(self.services['FakeCloud'].writes,2)
        self.assertEqual(len(self.services['FakeCloud'].config['LambdaFunctionConfigurations']),1)
    def test_bad_input_does_not_touch_config(self):
        with self.assertRaises(ValueError):app.notification_outage_test('', ['FakeCloud'])
        self.assertEqual(self.services['FakeCloud'].writes,0)

if __name__=='__main__':unittest.main()
