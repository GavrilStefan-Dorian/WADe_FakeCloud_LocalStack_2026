"""Safety tests for the local-only demo reset; no emulator calls."""
import sys
import unittest
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import prepare_fresh as fresh


class FakeS3:
    def __init__(self):
        self.objects = {
            ('wade-graduates-in', 'incoming/browser-abc.csv'),
            ('wade-graduates-in', 'incoming/manual-important.csv'),
            ('wade-graduates-out', 'reports/browser-abc.json'),
            ('wade-graduates-out', 'reports/queued-abc.json'),
            ('wade-graduates-out', 'reports/manual-important.json'),
            ('wade-queued-graduates-in', 'incoming/queued-abc.csv'),
        }
        self.deleted = []
    def list_objects_v2(self, Bucket, Prefix, MaxKeys, **kwargs):
        return {'Contents': [{'Key': key} for bucket, key in self.objects if bucket == Bucket and key.startswith(Prefix)]}
    def delete_object(self, Bucket, Key):
        self.deleted.append((Bucket, Key))
        self.objects.remove((Bucket, Key))

class FakeDDB:
    def __init__(self): self.deleted = []
    def describe_table(self, TableName): return {}
    def scan(self, **kwargs): return {'Items': [{'jobId': {'S': 'job1'}}]}
    def delete_item(self, TableName, Key): self.deleted.append(Key['jobId']['S'])

class FakeSQS:
    def __init__(self): self.purged = False
    def purge_queue(self, QueueUrl): self.purged = True

class ResetTests(unittest.TestCase):
    def test_preview_reads_only(self):
        s3, ddb, sqs = FakeS3(), FakeDDB(), FakeSQS()
        with patch.object(fresh.trigger_lab, 'state', return_value={'connected': True, 'backupSaved': False}), \
             patch.object(fresh.queued_jobs, 'clients', return_value={'s3': s3,'dynamodb':ddb,'sqs':sqs}), \
             patch.object(fresh.queued_jobs, 'queue_url', return_value='http://127.0.0.1:4566/q'), \
             patch.object(fresh.trigger_lab, 'reset') as reset:
            fresh.reset_emulator('FakeCloud', False)
            self.assertEqual(s3.deleted, [])
            self.assertEqual(ddb.deleted, [])
            self.assertFalse(sqs.purged)
            reset.assert_not_called()

    def test_apply_preserves_unrelated_files_and_restores_first(self):
        s3, ddb, sqs = FakeS3(), FakeDDB(), FakeSQS()
        steps = []
        def restored(_): steps.append('restore'); return {'removed': 0}
        orig_delete = s3.delete_object
        def guard_delete(**args):
            self.assertEqual(steps, ['restore'])
            return orig_delete(**args)
        s3.delete_object = guard_delete
        with patch.object(fresh.trigger_lab, 'state', return_value={'connected': True, 'backupSaved': True}), \
             patch.object(fresh.queued_jobs, 'clients', return_value={'s3': s3,'dynamodb':ddb,'sqs':sqs}), \
             patch.object(fresh.queued_jobs, 'queue_url', return_value='http://127.0.0.1:4566/q'), \
             patch.object(fresh.trigger_lab, 'reset', side_effect=restored):
            fresh.reset_emulator('FakeCloud', True)
        self.assertEqual(steps, ['restore'])
        self.assertTrue(sqs.purged)
        self.assertEqual(ddb.deleted, ['job1'])
        self.assertEqual(s3.objects, {
            ('wade-graduates-in', 'incoming/manual-important.csv'),
            ('wade-graduates-out', 'reports/manual-important.json'),
        })

    def test_disconnected_without_backup_refuses_any_cleanup(self):
        with patch.object(fresh.trigger_lab, 'state', return_value={'connected': False, 'backupSaved': False}), \
             patch.object(fresh.queued_jobs, 'clients') as clients:
            with self.assertRaises(RuntimeError): fresh.reset_emulator('LocalStack', True)
            clients.assert_not_called()

if __name__ == '__main__': unittest.main()
