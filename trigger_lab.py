"""Four-step, presenter-controlled S3 notification experiment.

Uses actual FakeCloud/LocalStack APIs. Per-emulator progression is kept on disk
so refreshing the browser does not skip the experiment's safety checks.
All notification changes are backed up before application and can be recovered.
"""
from __future__ import annotations

import json
import os
import secrets
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

EMULATORS = {"FakeCloud": 4566, "LocalStack": 4567}
INPUT_BUCKET = "wade-graduates-in"
OUTPUT_BUCKET = "wade-graduates-out"
FUNCTION = "wade-graduates-v1"
PREFIX = "incoming/trigger-lab-"
MAX_CSV_CHARS = 120_000
EVIDENCE = Path(__file__).resolve().parent / "evidence"
_KEYS = ("TopicConfigurations", "QueueConfigurations", "LambdaFunctionConfigurations", "EventBridgeConfiguration")
_lock = threading.RLock()


def _kw(name):
    if name not in EMULATORS:
        raise ValueError("Choose FakeCloud or LocalStack")
    return dict(endpoint_url=f"http://127.0.0.1:{EMULATORS[name]}", region_name="us-east-1",
                aws_access_key_id="test", aws_secret_access_key="test",
                config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 1},
                              connect_timeout=2, read_timeout=6))


def _check(name):
    if name not in EMULATORS:
        raise ValueError("Choose FakeCloud or LocalStack")


def _clients(name):
    kw = _kw(name)
    return boto3.client("s3", **kw), boto3.client("lambda", **kw)


def _payload(response):
    return {k: response[k] for k in _KEYS if k in response and response[k]}


def _arn(lam):
    return lam.get_function_configuration(FunctionName=FUNCTION)["FunctionArn"]


def _ours(config, arn):
    return [r for r in config.get("LambdaFunctionConfigurations", []) if r.get("LambdaFunctionArn") == arn]


def _backup(name):
    return EVIDENCE / f"trigger-lab-backup-{name.lower()}.json"


def _session_path(name):
    return EVIDENCE / f"trigger-lab-session-{name.lower()}.json"


def _session(name):
    path = _session_path(name)
    if not path.is_file():
        return {"phase": "ready", "key": None}
    data = json.loads(path.read_text(encoding="utf-8"))
    if data.get("phase") not in {"ready", "disconnected", "uploaded", "reconnected", "replayed"}:
        raise RuntimeError("Unknown experiment session; use Reset the experiment")
    return data


def _write_session(name, phase, key=None):
    EVIDENCE.mkdir(exist_ok=True)
    path = _session_path(name)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps({"phase": phase, "key": key}), encoding="utf-8")
    os.replace(tmp, path)


def _report_key(key):
    return "reports/" + key[len("incoming/"):].rsplit(".", 1)[0] + ".json"


def _missing(error):
    return error.response.get("Error", {}).get("Code") in {"NoSuchKey", "NotFound", "404"}


def _report(s3, key):
    try:
        resp = s3.get_object(Bucket=OUTPUT_BUCKET, Key=key)
        try:
            data = json.loads(resp["Body"].read())
        finally:
            resp["Body"].close()
    except ClientError as error:
        if _missing(error):
            return None
        raise
    return {"accepted": data.get("accepted"), "rejected": data.get("rejected"), "status": data.get("status")}


def _configuration(s3, lam):
    arn = _arn(lam)
    config = _payload(s3.get_bucket_notification_configuration(Bucket=INPUT_BUCKET))
    return config, arn, bool(_ours(config, arn))


def _phase(session, connected, report):
    phase = session["phase"]
    if phase in ("disconnected", "uploaded") and connected:
        return "unsafe"
    if phase in ("reconnected", "replayed") and not connected:
        return "unsafe"
    if phase == "ready" and not connected:
        return "unsafe"
    if phase in ("disconnected", "uploaded") and report:
        return "unsafe"  # A report unexpectedly exists while the trigger should be off.
    if phase == "replayed" and report is not None:
        return "complete"
    return phase


def state(name):
    _check(name)
    s3, lam = _clients(name)
    _, arn, connected = _configuration(s3, lam)
    session = _session(name)
    key = session.get("key")
    listing = s3.list_objects_v2(Bucket=INPUT_BUCKET, Prefix=PREFIX).get("Contents", [])
    files = []
    for entry in sorted(listing, key=lambda o: o["Key"]):
        if key and entry["Key"] != key:
            continue
        # Before a new experiment, older lab files should not distract the audience.
        if not key:
            continue
        files.append({"key": entry["Key"], "size": entry["Size"],
                      "report": _report(s3, _report_key(entry["Key"]))})
    report = files[0]["report"] if files else None
    phase = _phase(session, connected, report)
    if key and not files and phase not in {"ready", "disconnected"}:
        phase = "unsafe"  # File disappeared outside this experiment.
    return {"emulator": name, "connected": connected, "lambdaArn": arn,
            "files": files, "phase": phase, "key": key,
            "backupSaved": _backup(name).exists()}


def _require(name, phase):
    snapshot = state(name)
    if snapshot["phase"] != phase:
        if snapshot["phase"] == "unsafe":
            raise RuntimeError("The notification or experiment state changed outside the expected sequence. Restore/reset before proceeding")
        raise RuntimeError(f"This is step {phase}; current step is {snapshot['phase']}. Follow the highlighted action or reset")
    return snapshot


def disconnect(name):
    _check(name)
    with _lock:
        _require(name, "ready")
        s3, lam = _clients(name)
        original, arn, connected = _configuration(s3, lam)
        matching = _ours(original, arn)
        if not connected or len(matching) != 1:
            raise RuntimeError("Expected exactly one graduate S3-to-Lambda notification; nothing changed")
        EVIDENCE.mkdir(exist_ok=True)
        # Keep the exact backup: it is never recreated by Upload/Replay.
        _backup(name).write_text(json.dumps(original, indent=2), encoding="utf-8")
        without = dict(original)
        without["LambdaFunctionConfigurations"] = [r for r in original["LambdaFunctionConfigurations"]
                                                   if r.get("LambdaFunctionArn") != arn]
        without = {k: v for k, v in without.items() if v}
        s3.put_bucket_notification_configuration(Bucket=INPUT_BUCKET, NotificationConfiguration=without)
        if _configuration(s3, lam)[2]:
            raise RuntimeError("Emulator still reports the notification after removal. Use Reset or the recovery script")
        _write_session(name, "disconnected")
        return {"changed": True}


def _restore_if_needed(name, s3, lam):
    original_path = _backup(name)
    actual, _, connected = _configuration(s3, lam)
    if connected:
        return False
    if not original_path.is_file():
        raise RuntimeError("S3 notification is disconnected but no backup exists. Do not delete files; restore manually")
    original = json.loads(original_path.read_text(encoding="utf-8"))
    s3.put_bucket_notification_configuration(Bucket=INPUT_BUCKET, NotificationConfiguration=original)
    restored = _payload(s3.get_bucket_notification_configuration(Bucket=INPUT_BUCKET))
    if restored != original:
        raise RuntimeError("Notification restoration did not match the saved configuration. Run restore_notification.py")
    return True


def reconnect(name):
    _check(name)
    with _lock:
        _require(name, "uploaded")
        s3, lam = _clients(name)
        changed = _restore_if_needed(name, s3, lam)
        _write_session(name, "reconnected", _session(name)["key"])
        return {"changed": changed}


def upload(name, csv_text):
    _check(name)
    if not isinstance(csv_text, str) or not csv_text.strip() or len(csv_text) > MAX_CSV_CHARS:
        raise ValueError("Choose a CSV first")
    with _lock:
        _require(name, "disconnected")
        s3, _ = _clients(name)
        key = f"{PREFIX}{time.strftime('%H%M%S')}-{secrets.token_hex(3)}.csv"
        s3.put_object(Bucket=INPUT_BUCKET, Key=key, Body=csv_text.encode("utf-8"),
                      ContentType="text/csv; charset=utf-8")
        s3.head_object(Bucket=INPUT_BUCKET, Key=key)
        if _report(s3, _report_key(key)) is not None:
            raise RuntimeError("Unexpected report while notification was disabled. Investigate rather than claiming a successful outage")
        _write_session(name, "uploaded", key)
        return {"key": key}


def replay(name, key):
    _check(name)
    with _lock:
        snapshot = state(name)
        if snapshot["phase"] not in {"reconnected", "replayed"}:
            raise RuntimeError("Replay is only available after reconnecting the trigger")
        if not (isinstance(key, str) and key == snapshot["key"] and key.startswith(PREFIX)
                and key.endswith(".csv") and key.count("/") == 1):
            raise ValueError("Replay only the file from this experiment")
        s3, lam = _clients(name)
        if not _configuration(s3, lam)[2]:
            raise RuntimeError("Reconnect the trigger first")
        if _report(s3, _report_key(key)) is not None:
            raise RuntimeError("A report already exists. No replay is needed")
        resp = s3.get_object(Bucket=INPUT_BUCKET, Key=key)
        try:
            body = resp["Body"].read()
        finally:
            resp["Body"].close()
        # This is a NEW ObjectCreated event for the SAME key, not recovery of the lost event.
        s3.put_object(Bucket=INPUT_BUCKET, Key=key, Body=body, ContentType="text/csv; charset=utf-8")
        _write_session(name, "replayed", key)
        return {"key": key}


def reset(name):
    _check(name)
    with _lock:
        s3, lam = _clients(name)
        _restore_if_needed(name, s3, lam)  # Do this FIRST; never clean objects if recovery failed.
        removed = 0
        for bucket, prefix in ((INPUT_BUCKET, PREFIX), (OUTPUT_BUCKET, "reports/trigger-lab-")):
            for item in s3.list_objects_v2(Bucket=bucket, Prefix=prefix).get("Contents", []):
                s3.delete_object(Bucket=bucket, Key=item["Key"])
                removed += 1
        _write_session(name, "ready")
        return {"removed": removed}


def evidence(name):
    """Inspect the selected emulator; never imply unrelated logs prove this upload ran."""
    _check(name)
    key = _session(name).get("key")
    if not key:
        return {"source": f"{name}: no experiment file selected", "text": "Upload a CSV during this experiment first."}
    if name == "FakeCloud":
        url = f"http://127.0.0.1:{EMULATORS[name]}/_fakecloud/lambda/invocations"
        try:
            with urllib.request.urlopen(url, timeout=4) as response:
                data = response.read().decode("utf-8", "replace")
        except urllib.error.HTTPError as error:
            raise RuntimeError(f"FakeCloud invocation endpoint returned HTTP {error.code}; introspection not confirmed in this installation") from error
        try:
            data = json.dumps(json.loads(data), indent=2)
        except ValueError:
            pass
        location = data.find(key)
        if location < 0:
            return {"source": "FakeCloud: invocation inspection (not matched)",
                    "text": f"The endpoint responded, but these records did not contain {key}. This does not establish whether Lambda ran."}
        # Do not dump nested, escaped JSON at projector size: it obscures the
        # evidence and makes an old invocation look like a new notification.
        # The record is actually matched by exact object key in the API result.
        report_key = _report_key(key)
        report = _report(_clients(name)[0], report_key)
        lines = ["A Lambda invocation record contains the exact S3 object key from this experiment:",
                 key,
                 "",
                 "Source: /_fakecloud/lambda/invocations (the emulator's invocation history)",
                 "This is the NEW upload event after reconnecting the notification, not the missed event."]
        if report:
            lines.append(f"Separately verified S3 report: {report['accepted']} accepted, {report['rejected']} rejected.")
        return {"source": "FakeCloud: matching Lambda invocation found for the replayed CSV",
                "text": "\n".join(lines)}
    logs = boto3.client("logs", **_kw(name))
    group = f"/aws/lambda/{FUNCTION}"
    try:
        streams = logs.describe_log_streams(logGroupName=group, orderBy="LastEventTime", descending=True, limit=6)["logStreams"]
    except ClientError as error:
        if error.response.get("Error", {}).get("Code") == "ResourceNotFoundException":
            return {"source": f"LocalStack: CloudWatch Logs {group}", "text": "No Lambda application log group found."}
        raise
    for stream in streams:
        events = logs.get_log_events(logGroupName=group, logStreamName=stream["logStreamName"], limit=50)["events"]
        matching = [event["message"] for event in events if key in event.get("message", "")]
        if matching:
            return {"source": f"LocalStack: CloudWatch Logs {group}; matched {key}",
                    "text": "\n".join(matching)[-2500:]}
    return {"source": f"LocalStack: CloudWatch Logs {group} (no matching entry)",
            "text": f"No Lambda application log mentioning {key} was found in the latest six log streams. This is not proof the function did not execute."}
