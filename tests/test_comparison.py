import importlib.util
import json
import tempfile
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
spec = importlib.util.spec_from_file_location("lab", ROOT / "ui_server.py")
lab = importlib.util.module_from_spec(spec)
spec.loader.exec_module(lab)

class Body:
    def __init__(self, blob): self.blob = blob
    def read(self): return self.blob
    def close(self): pass

class FakeClient:
    def __init__(self): self.objects = {}; self.puts = []
    def put_object(self, **kwargs):
        key=kwargs['Key']
        self.objects[key]=kwargs['Body']
        self.puts.append((kwargs['Bucket'],key,kwargs['Body']))
        out='reports/'+key.split('/')[-1].replace('.csv','.json')
        self.objects[out]=json.dumps({'source':key,'status':'ok','accepted':12,'rejected':0}).encode()
        return {'ETag':'abc'}
    def get_object(self, **kwargs): return {'Body': Body(self.objects[kwargs['Key']])}

class CompareTests(unittest.TestCase):
    def test_preserved_raw_results(self):
        groups=lab.recorded_benchmarks()['measurements']
        self.assertEqual(len(groups),10)
        self.assertTrue(all(row['runs']==30 for row in groups))
        names={x['emulator'] for x in groups}
        self.assertEqual(names, {'FakeCloud','LocalStack'})
    def test_repeated_pipeline_checks_identical_inputs_and_reports(self):
        fc,ls=FakeClient(),FakeClient()
        clients={'FakeCloud':fc,'LocalStack':ls}
        with tempfile.TemporaryDirectory() as td:
            with patch.object(lab,'EVIDENCE',Path(td)),patch.object(lab,'s3_client',side_effect=lambda name:clients[name]):
                response=lab.pipeline_comparison('graduate_id,graduation_year\nG001,2022')
                self.assertEqual(len(response['samples']),8)
                self.assertTrue(response['allReportsAgree'])
                self.assertEqual(response['summary']['FakeCloud']['completed'],4)
                self.assertEqual(response['summary']['LocalStack']['completed'],4)
                self.assertTrue((Path(td)/'latest-pipeline-comparison.json').exists())
        self.assertEqual(fc.puts,ls.puts)
    def test_reject_empty_input(self):
        with self.assertRaises(ValueError):lab.pipeline_comparison('')

if __name__=='__main__': unittest.main()
