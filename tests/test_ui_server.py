"""Fast isolated API tests. No Docker or emulators required; mock S3 only."""
import io
import json
import sys
import threading
import unittest
from http.server import ThreadingHTTPServer
from pathlib import Path
from urllib.request import Request, urlopen
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import ui_server


class MockS3:
    def __init__(self, outputs):
        self.outputs = outputs
    def head_object(self, Bucket, Key):
        if Key not in self.outputs:
            return {'ContentLength': 629, 'ContentType': 'text/csv'}
        return {'ContentLength': len(self.outputs[Key]), 'ContentType': 'application/json'}
    def get_bucket_notification_configuration(self, **kwargs):
        return {'LambdaFunctionConfigurations': [{'LambdaFunctionArn': 'arn:aws:lambda:us-east-1:123456789012:function:wade-graduates-v1', 'Events': ['s3:ObjectCreated:*']}]}
    def head_bucket(self, **kwargs):
        return {'ResponseMetadata': {'HTTPStatusCode': 200}}
    def put_object(self, Bucket, Key, Body, ContentType):
        output_key = 'reports/' + Key[len('incoming/'):].replace('.csv','.json')
        self.outputs[output_key] = json.dumps({
            'source': Key,
            'status': 'ok', 'accepted': 12, 'rejected': 0,
            'employmentRatePct': 58.3,
            'byStatus': {'employed': 7, 'seeking': 3, 'studying': 2},
            'byProgram': {'Computer Science': 5},
            'byIndustry': {'Software': 3},
            'errors': [], 'acceptedRecords': []
        }).encode()
        return {'ETag': '"test-etag"'}
    def get_object(self, Bucket, Key):
        return {'Body': io.BytesIO(self.outputs[Key])}


class Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.outputs = {'FakeCloud': {}, 'LocalStack': {}}
        cls.patch = patch.object(ui_server, 's3_client', side_effect=lambda name: MockS3(cls.outputs[name]))
        cls.patch.start()
        cls.server = ThreadingHTTPServer(('127.0.0.1',0), ui_server.Handler)
        cls.url = f'http://127.0.0.1:{cls.server.server_port}'
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.patch.stop()

    def request(self, path, payload=None):
        data = None if payload is None else json.dumps(payload).encode()
        req = Request(self.url+path,data=data,headers={'Content-Type':'application/json'})
        with urlopen(req) as response:
            return response.status,json.loads(response.read())

    def test_status_and_sample(self):
        status,response = self.request('/api/status')
        self.assertEqual(status,200)
        self.assertTrue(response['emulators']['FakeCloud']['reachable'])
        _,sample=self.request('/api/samples/valid')
        self.assertIn('graduate_id',sample['csv'])

    def test_run_both_end_to_end_http(self):
        _,sample=self.request('/api/samples/valid')
        status,result=self.request('/api/runs',{'csv':sample['csv'],'targets':['FakeCloud','LocalStack']})
        self.assertEqual(status,201)
        self.assertEqual(result['reportsMatch'],True)
        self.assertEqual([x['kind'] for x in result['targets']['FakeCloud']['events']],
                         ['upload_ack','input_present','output_present','report_checked'])
        self.assertEqual(result['targets']['FakeCloud']['input']['sizeBytes'],629)
        self.assertEqual(result['targets']['FakeCloud']['report']['accepted'],12)
        self.assertEqual(result['targets']['LocalStack']['state'],'complete')
        _,again=self.request('/api/runs/'+result['id'])
        self.assertEqual(again['reportsMatch'],True)

    def test_live_inspection_uses_actual_api_fields(self):
        _, sample = self.request('/api/samples/valid')
        _, run = self.request('/api/runs', {'csv': sample['csv'], 'targets':['LocalStack']})
        class MockLambda:
            def get_function_configuration(self, **kwargs):
                return {'FunctionArn': 'arn:aws:lambda:us-east-1:000000000000:function:wade-graduates-v1',
                        'State': 'Active', 'Runtime': 'nodejs20.x', 'LastUpdateStatus': 'Successful'}
        class MockLogs:
            def filter_log_events(self, **kwargs):
                return {'events':[{'message':'WADE_GRADUATES_REPORT ' + run['inputKey'] + ' ok 12 0'}]}
        with patch.object(ui_server.boto3, 'client', side_effect=lambda service, **kwargs: MockLambda() if service == 'lambda' else MockLogs()):
            _, inspected = self.request('/api/inspect/' + run['id'] + '/LocalStack')
        self.assertEqual(inspected['function']['Runtime'], 'nodejs20.x')
        self.assertEqual(inspected['input']['bucket'], 'wade-graduates-in')
        self.assertIn(run['inputKey'], inspected['logs'][0])
        self.assertEqual(inspected['logSource'], 'CloudWatch Lambda logs (matched by input key)')

    def test_reject_invalid_targets(self):
        with self.assertRaises(Exception):
            self.request('/api/runs',{'csv':'hello','targets':['AWS']})

    def test_serve_frontend(self):
        for path in ('/','/styles.css','/app.js'):
            with urlopen(self.url+path) as response:
                self.assertEqual(response.status,200)
                self.assertTrue(response.read())

if __name__ == '__main__':
    unittest.main(verbosity=2)
