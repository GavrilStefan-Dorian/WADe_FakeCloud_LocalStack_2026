import io, json, sys, tempfile, threading, unittest, urllib.request
from pathlib import Path
from unittest.mock import patch
from botocore.exceptions import ClientError
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import trigger_lab as tl

ARN = "arn:aws:lambda:us-east-1:123456789012:function:wade-graduates-v1"

class Body:
    def __init__(self, b): self.b = b
    def read(self): return self.b
    def close(self): pass

class Emu:
    """Stands in for one emulator: S3 sends an event (and a report appears) only while a notification exists."""
    def __init__(self):
        self.cfg = {"LambdaFunctionConfigurations": [{"Id": "x", "LambdaFunctionArn": ARN, "Events": ["s3:ObjectCreated:*"]}]}
        self.obj = {}
    def get_function_configuration(self, **k): return {"FunctionArn": ARN}
    def get_bucket_notification_configuration(self, **k): return {"ResponseMetadata": {}, **json.loads(json.dumps(self.cfg))}
    def put_bucket_notification_configuration(self, Bucket, NotificationConfiguration): self.cfg = json.loads(json.dumps(NotificationConfiguration))
    def put_object(self, Bucket, Key, Body, **k):
        self.obj[(Bucket, Key)] = Body
        if Bucket == tl.INPUT_BUCKET and self.cfg.get("LambdaFunctionConfigurations"):
            self.obj[(tl.OUTPUT_BUCKET, tl._report_key(Key))] = json.dumps({"accepted": 12, "rejected": 0, "status": "complete"}).encode()
    def head_object(self, Bucket, Key):
        if (Bucket, Key) not in self.obj: raise ClientError({"Error": {"Code": "404"}}, "Head")
    def get_object(self, Bucket, Key):
        if (Bucket, Key) not in self.obj: raise ClientError({"Error": {"Code": "NoSuchKey"}}, "Get")
        return {"Body": Body(self.obj[(Bucket, Key)])}
    def list_objects_v2(self, Bucket, Prefix):
        return {"Contents": [{"Key": k, "Size": len(v)} for (b, k), v in self.obj.items() if b == Bucket and k.startswith(Prefix)]}
    def delete_object(self, Bucket, Key): self.obj.pop((Bucket, Key), None)

class Lab(unittest.TestCase):
    def setUp(self):
        self.emu = Emu(); self.tmp = tempfile.TemporaryDirectory()
        self.ps = [patch.object(tl, "_clients", lambda name: (self.emu, self.emu)), patch.object(tl, "EVIDENCE", Path(self.tmp.name))]
        for p in self.ps: p.start()
    def tearDown(self):
        for p in self.ps: p.stop()
        self.tmp.cleanup()

    def test_full_journey_reads_real_state(self):
        self.assertTrue(tl.state("FakeCloud")["connected"])
        self.assertTrue(tl.disconnect("FakeCloud")["changed"])
        s = tl.state("FakeCloud"); self.assertFalse(s["connected"]); self.assertTrue(s["backupSaved"])
        key = tl.upload("FakeCloud", "name,x\n1,2\n")["key"]
        s = tl.state("FakeCloud"); self.assertEqual([f["key"] for f in s["files"]], [key]); self.assertIsNone(s["files"][0]["report"])
        with self.assertRaises(RuntimeError): tl.replay("FakeCloud", key)          # no replay while disconnected
        self.assertTrue(tl.reconnect("FakeCloud")["changed"])
        s = tl.state("FakeCloud"); self.assertTrue(s["connected"]); self.assertIsNone(s["files"][0]["report"])   # not replayed
        tl.replay("FakeCloud", key)
        self.assertEqual(tl.state("FakeCloud")["files"][0]["report"]["accepted"], 12)

    def test_configuration_restored_exactly(self):
        before = tl._payload(self.emu.get_bucket_notification_configuration())
        tl.disconnect("FakeCloud")
        self.assertEqual(tl.state("FakeCloud")["phase"], "disconnected")
        tl.upload("FakeCloud", "header\nvalue\n")
        tl.reconnect("FakeCloud")
        self.assertEqual(tl._payload(self.emu.get_bucket_notification_configuration()), before)
        self.assertEqual(tl.state("FakeCloud")["phase"], "reconnected")

    def test_out_of_order_requests_are_rejected(self):
        with self.assertRaises(RuntimeError): tl.upload("FakeCloud", "a,b\n1,2\n")
        with self.assertRaises(RuntimeError): tl.reconnect("FakeCloud")
        with self.assertRaises(RuntimeError): tl.replay("FakeCloud", "incoming/trigger-lab-x.csv")
        tl.disconnect("FakeCloud")
        with self.assertRaises(RuntimeError): tl.disconnect("FakeCloud")
        with self.assertRaises(RuntimeError): tl.reconnect("FakeCloud")
        tl.upload("FakeCloud", "a,b\n1,2\n")
        with self.assertRaises(RuntimeError): tl.upload("FakeCloud", "a,b\n1,2\n")
        tl.reconnect("FakeCloud")
        with self.assertRaises(RuntimeError): tl.disconnect("FakeCloud")

    def test_recovery_without_backup_cannot_delete_files(self):
        tl.disconnect("FakeCloud")
        key = tl.upload("FakeCloud", "a,b\n1,2\n")["key"]
        tl._backup("FakeCloud").unlink()
        with self.assertRaises(RuntimeError): tl.reset("FakeCloud")
        self.assertFalse(tl.state("FakeCloud")["connected"])
        self.assertIn((tl.INPUT_BUCKET, key), self.emu.obj)

    def test_validation(self):
        with self.assertRaises(ValueError): tl.upload("FakeCloud", "  ")
        tl.disconnect("FakeCloud")
        key = tl.upload("FakeCloud", "a,b\n1,2\n")["key"]
        tl.reconnect("FakeCloud")
        with self.assertRaises(ValueError): tl.replay("FakeCloud", "incoming/other.csv")
        with self.assertRaises(ValueError): tl.replay("FakeCloud", "incoming/trigger-lab-1/../x.csv")
        tl.replay("FakeCloud", key)
        self.assertEqual(tl.state("FakeCloud")["phase"], "complete")

    def test_persisted_state_after_web_refresh(self):
        tl.disconnect("FakeCloud")
        self.assertEqual(tl.state("FakeCloud")["phase"], "disconnected")
        key = tl.upload("FakeCloud", "a,b\n1,2\n")["key"]
        self.assertEqual(tl._session("FakeCloud")["key"], key)
        self.assertEqual(tl.state("FakeCloud")["phase"], "uploaded")
        # No client-side state is needed; a fresh GET reads emulator + saved session.

    def test_reset_reconnects_and_cleans(self):
        tl.disconnect("FakeCloud"); tl.upload("FakeCloud", "a,b\n1,2\n")
        tl.reset("FakeCloud"); s = tl.state("FakeCloud")
        self.assertTrue(s["connected"]); self.assertEqual(s["files"], [])

class Http(unittest.TestCase):
    def test_routes(self):
        import ui_server
        emu = Emu(); tmp = tempfile.TemporaryDirectory()
        with patch.object(tl, "_clients", lambda n: (emu, emu)), patch.object(tl, "EVIDENCE", Path(tmp.name)):
            srv = ui_server.ThreadingHTTPServer(("127.0.0.1", 0), ui_server.Handler)
            threading.Thread(target=srv.serve_forever, daemon=True).start()
            base = "http://127.0.0.1:%d" % srv.server_port
            def call(path, body=None):
                req = urllib.request.Request(base + path, data=None if body is None else json.dumps(body).encode(), headers={"Content-Type": "application/json"})
                try:
                    with urllib.request.urlopen(req) as r: return r.status, json.loads(r.read())
                except urllib.error.HTTPError as e: return e.code, json.loads(e.read())
            self.assertTrue(call("/api/trigger/state?emulator=FakeCloud")[1]["connected"])
            self.assertEqual(call("/api/trigger/disconnect", {"emulator": "FakeCloud"})[0], 200)
            self.assertFalse(call("/api/trigger/state?emulator=FakeCloud")[1]["connected"])
            self.assertEqual(call("/api/trigger/replay", {"emulator": "FakeCloud", "key": "incoming/trigger-lab-1.csv"})[0], 409)
            self.assertEqual(call("/api/trigger/state?emulator=Nope")[0], 400)
            self.assertEqual(call("/api/trigger/bogus", {"emulator": "FakeCloud"})[0], 404)
            for p in ("/", "/trigger.js", "/trigger.css"):
                with urllib.request.urlopen(base + p) as r: self.assertEqual(r.status, 200)
            srv.shutdown()
        tmp.cleanup()

if __name__ == "__main__": unittest.main()
