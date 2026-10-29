"""Local-only browser bridge for the already-deployed WADe graduate CSV pipeline.

Uses only the existing FakeCloud/LocalStack endpoints, dedicated graduate buckets,
Python stdlib HTTP server + the project's existing boto3 installation.
Never contacts real AWS or modifies Lambda configuration.
"""
from __future__ import annotations

import json
import csv as csvlib
import io
import statistics
import mimetypes
import os
import secrets
import subprocess
from datetime import datetime, timezone
import threading
import time
import traceback
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse, unquote

import boto3
import queued_jobs
import trigger_lab
from botocore.config import Config
from botocore.exceptions import ClientError, EndpointConnectionError

ROOT = Path(__file__).resolve().parent
WEB = ROOT / "web"
SAMPLES = ROOT / "samples"
EVIDENCE = ROOT / "evidence"
PIPELINE_REPEATS = 4
BIND_HOST = "127.0.0.1"  # Deliberately not accessible from the LAN.
BIND_PORT = int(os.environ.get("WADE_WEB_PORT", "8000"))
INPUT_BUCKET = "wade-graduates-in"
OUTPUT_BUCKET = "wade-graduates-out"
EMULATORS = {"FakeCloud": 4566, "LocalStack": 4567}
MAX_POST_BYTES = 256 * 1024
MAX_CSV_CHARS = 120_000
RUN_TIMEOUT_SECONDS = 35

_runs: dict[str, dict] = {}
_runs_lock = threading.RLock()

_queue_sessions: dict[str, dict] = {}
_queue_lock = threading.RLock()


def s3_client(name: str):
    if name not in EMULATORS:
        raise ValueError("Unknown emulator")
    return boto3.client(
        "s3",
        endpoint_url=f"http://127.0.0.1:{EMULATORS[name]}",
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
        config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 1},
                      connect_timeout=2, read_timeout=4),
    )


def _event(entry: dict, kind: str, label: str, detail: str = ""):
    """Record an observed operation; never fabricate a Lambda progress step."""
    if kind in entry["seen"]:
        return
    entry["seen"].add(kind)
    entry["events"].append({
        "kind": kind,
        "label": label,
        "detail": detail,
        "at": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
    })


def _metadata(value: dict) -> dict:
    return {
        "sizeBytes": value.get("ContentLength"),
        "contentType": value.get("ContentType"),
        "etag": str(value.get("ETag", "")).strip('"'),
    }


def _is_missing(error: ClientError) -> bool:
    code = error.response.get("Error", {}).get("Code")
    return code in {"NoSuchKey", "NotFound", "404", "NoSuchBucket"}


def snapshot(run_id: str) -> dict | None:
    """Refresh report state with real S3 GETs, without inferring internal stage progress."""
    with _runs_lock:
        run = _runs.get(run_id)
        if run is None:
            return None
        targets = list(run["targets"])

    for name in targets:
        with _runs_lock:
            entry = run["targets"][name]
            if entry["state"] != "waiting":
                continue
        try:
            client = s3_client(name)
            # HEAD confirms what S3 actually stored; it is not a simulated stage.
            with _runs_lock:
                needs_input = "input_present" not in entry["seen"]
            if needs_input:
                try:
                    head = client.head_object(Bucket=INPUT_BUCKET, Key=run["inputKey"])
                    with _runs_lock:
                        entry["input"] = _metadata(head)
                        _event(entry, "input_present", "S3 input confirmed", f'{head.get("ContentLength", "?")} bytes · {INPUT_BUCKET}')
                except ClientError:
                    pass  # The upload outcome was already recorded; keep polling.
            result = client.get_object(Bucket=OUTPUT_BUCKET, Key=run["outputKey"])
            try:
                report = json.loads(result["Body"].read())
            finally:
                result["Body"].close()
            if not isinstance(report, dict) or "accepted" not in report or "rejected" not in report:
                raise ValueError("The output object was not a valid graduate report")
            with _runs_lock:
                entry["state"] = "complete"
                entry["report"] = report
                entry["output"] = _metadata(result)
                _event(entry, "output_present", "S3 report found", f'{OUTPUT_BUCKET} · {run["outputKey"]}')
                _event(entry, "report_checked", "Report read and checked", f'{report["accepted"]} accepted · {report["rejected"]} rejected')
                entry["elapsedMs"] = round((time.monotonic() - run["created"]) * 1000)
        except ClientError as exc:
            if not _is_missing(exc):
                with _runs_lock:
                    entry["state"] = "error"
                    entry["error"] = f"S3 could not read the report: {exc.response.get('Error', {}).get('Code', 'unknown')}"
                    _event(entry, "read_error", "Report read failed", entry["error"])
        except Exception as exc:
            with _runs_lock:
                entry["state"] = "error"
                entry["error"] = f"Could not retrieve the report: {type(exc).__name__}: {str(exc)[:180]}"
                _event(entry, "read_error", "Report read failed", entry["error"])

    with _runs_lock:
        if time.monotonic() - run["created"] > RUN_TIMEOUT_SECONDS:
            for entry in run["targets"].values():
                if entry["state"] == "waiting":
                    entry["state"] = "timeout"
                    entry["error"] = "No report appeared within 35 seconds. Check the Lambda or S3 notification."
                    _event(entry, "timeout", "No report observed", "35-second deadline reached")
        reports = {name: entry["report"] for name, entry in run["targets"].items() if entry["state"] == "complete"}
        match = None
        if len(run["targets"]) == 2 and len(reports) == 2:
            names = list(run["targets"])
            match = reports[names[0]] == reports[names[1]]
        return {
            "id": run_id,
            "inputKey": run["inputKey"],
            "outputKey": run["outputKey"],
            "targets": {
                name: {key: value for key, value in entry.items() if key != "seen"}
                for name, entry in run["targets"].items()
            },
            "reportsMatch": match,
        }


def start_run(payload: dict) -> dict:
    if _fault_lock.locked():
        raise ValueError("A notification outage test is in progress; retry the normal upload after restoration")
    if not isinstance(payload, dict):
        raise ValueError("Expected a JSON object")
    csv = payload.get("csv")
    targets = payload.get("targets")
    if not isinstance(csv, str) or not csv.strip() or len(csv) > MAX_CSV_CHARS:
        raise ValueError("Provide nonempty CSV text, up to 120,000 characters")
    if not isinstance(targets, list) or not targets or len(set(map(str, targets))) != len(targets):
        raise ValueError("Choose one or both emulators")
    if not all(isinstance(name, str) and name in EMULATORS for name in targets):
        raise ValueError("Only FakeCloud and LocalStack are supported")

    # The same key and exact same CSV bytes are used in both isolated emulators.
    run_id = secrets.token_hex(8)
    input_key = f"incoming/browser-{run_id}.csv"
    output_key = f"reports/browser-{run_id}.json"
    data = csv.encode("utf-8")
    run = {
        "created": time.monotonic(),
        "createdEpoch": time.time(),
        "inputKey": input_key,
        "outputKey": output_key,
        "targets": {name: {
            "state": "waiting", "report": None, "error": None,
            "input": None, "output": None, "events": [], "seen": set(), "elapsedMs": None,
        } for name in targets},
    }
    with _runs_lock:
        _runs[run_id] = run
        # Bound in-memory history; this does not delete actual S3 objects.
        while len(_runs) > 64:
            _runs.pop(next(iter(_runs)))

    for name in targets:
        try:
            response = s3_client(name).put_object(
                Bucket=INPUT_BUCKET, Key=input_key, Body=data,
                ContentType="text/csv; charset=utf-8",
            )
            with _runs_lock:
                _event(run["targets"][name], "upload_ack", "S3 accepted the upload",
                       f'{len(data)} bytes · ETag {str(response.get("ETag", "unknown")).strip(chr(34))}')
        except Exception as exc:
            with _runs_lock:
                run["targets"][name]["state"] = "error"
                run["targets"][name]["error"] = f"S3 upload failed: {type(exc).__name__}: {str(exc)[:180]}"
                _event(run["targets"][name], "upload_error", "S3 upload failed", run["targets"][name]["error"])
    return snapshot(run_id)



def inspect_run(run_id: str, name: str) -> dict | None:
    """Fetch actual resource details on demand; no changes to emulator state."""
    if name not in EMULATORS:
        return None
    with _runs_lock:
        run = _runs.get(run_id)
        if not run or name not in run["targets"]:
            return None
        input_key, output_key, started = run["inputKey"], run["outputKey"], run["createdEpoch"]

    endpoint = f"http://127.0.0.1:{EMULATORS[name]}"
    common = dict(endpoint_url=endpoint, region_name="us-east-1", aws_access_key_id="test",
                  aws_secret_access_key="test", config=Config(retries={"max_attempts": 1},
                  connect_timeout=2, read_timeout=4))
    result = {"emulator": name, "endpoint": endpoint, "input": None, "output": None,
              "function": None, "notification": None, "logs": [], "logSource": None, "notes": []}
    s3 = s3_client(name)
    for bucket, key, field in ((INPUT_BUCKET, input_key, "input"), (OUTPUT_BUCKET, output_key, "output")):
        try:
            result[field] = {"bucket": bucket, "key": key, **_metadata(s3.head_object(Bucket=bucket, Key=key))}
        except ClientError as exc:
            result["notes"].append(f"{field} object: {exc.response.get('Error', {}).get('Code', 'unavailable')}")
        except Exception as exc:
            result["notes"].append(f"{field} object: {type(exc).__name__}")
    try:
        config = boto3.client("lambda", **common).get_function_configuration(FunctionName="wade-graduates-v1")
        result["function"] = {key: config.get(key) for key in
                              ("FunctionArn", "Runtime", "State", "LastUpdateStatus")}
    except Exception as exc:
        result["notes"].append(f"Lambda configuration: {type(exc).__name__}")
    try:
        notice = s3.get_bucket_notification_configuration(Bucket=INPUT_BUCKET)
        result["notification"] = [{"arn": row.get("LambdaFunctionArn"), "events": row.get("Events", [])}
                                  for row in notice.get("LambdaFunctionConfigurations", [])]
    except Exception as exc:
        result["notes"].append(f"S3 notification: {type(exc).__name__}")
    try:
        logs = boto3.client("logs", **common).filter_log_events(
            logGroupName="/aws/lambda/wade-graduates-v1", startTime=int((started - 5) * 1000), limit=100)
        matched = [row for row in logs.get("events", []) if input_key in row.get("message", "")]
        result["logs"] = [row["message"].strip()[:650] for row in matched[-6:]]
        result["logSource"] = "CloudWatch Lambda logs (matched by input key)"
        if not matched:
            result["notes"].append("No Lambda application log with this input key was returned.")
    except Exception:
        # FakeCloud 0.51.0 was observed without an /aws/lambda log group.
        result["notes"].append("CloudWatch Lambda application logs unavailable from this endpoint.")
        if name == "FakeCloud":
            try:
                # A container log line cannot be tied to an object key. Label accordingly.
                proc = subprocess.run(
                    ["docker", "logs", "--since", str(max(0, int(time.time()-started+10))) + "s",
                     "--tail", "250", "wade-fakecloud"],
                    stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                    timeout=3, check=False, text=True, encoding="utf-8", errors="replace")
                hits = [line.strip()[:400] for line in proc.stdout.splitlines()
                        if "S3->Lambda invocation succeeded" in line and "wade-graduates-v1" in line]
                if hits:
                    result["logs"] = hits[-5:]
                    result["logSource"] = "Recent FakeCloud service log (not uniquely linked to this CSV)"
            except (OSError, subprocess.TimeoutExpired):
                result["notes"].append("Docker CLI logs could not be read from the web server.")
    return result


# Actual user-measured benchmark CSVs, preserved from the Codex handoff.
def recorded_benchmarks() -> dict:
    """Read original CSV data; never substitute published/vendor benchmark claims."""
    groups = {}
    for kind, filename in (("S3", "s3-benchmark.csv"), ("Lambda", "lambda-boto3-benchmark.csv")):
        file = EVIDENCE / filename
        if not file.exists():
            continue
        with file.open(encoding="utf-8-sig", newline="") as stream:
            for row in csvlib.DictReader(stream):
                label = (kind, row.get("Size", ""), row.get("Operation", "Invoke"), row.get("Emulator"))
                try:
                    value = float(row["Milliseconds"])
                except (KeyError, ValueError, TypeError):
                    continue
                groups.setdefault(label, []).append(value)
    result = []
    for (kind, size, operation, emulator), values in sorted(groups.items()):
        sorted_values = sorted(values)
        index = max(0, (95*len(sorted_values)+99)//100 - 1)  # Nearest-rank p95.
        result.append({"kind":kind, "size":size,"operation":operation,"emulator":emulator,
                       "runs":len(values),"meanMs":round(statistics.mean(values),2),
                       "medianMs":round(statistics.median(values),2),
                       "p95Ms":round(sorted_values[index],2)})
    return {"source":"Original local benchmark CSV files", "measurements": result,
            "limitations":"Single Windows laptop; warm SDK clients; S3 GET follows PUT; no real AWS reference."}


def container_memory_snapshot() -> dict:
    """Read-only Docker stats. A snapshot is not a whole-system memory benchmark."""
    try:
        proc = subprocess.run(["docker", "stats", "--no-stream", "--format", "{{json .}}"],
                              capture_output=True, text=True, timeout=9, check=False)
        if proc.returncode != 0:
            raise RuntimeError(proc.stderr.strip()[:200] or "docker stats failed")
        rows = []
        for line in proc.stdout.splitlines():
            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                continue
            name = entry.get("Name", "")
            if not (name in {"wade-fakecloud", "wade-localstack"} or
                    name.startswith("wade-localstack-lambda-") or
                    name.startswith("wade-fakecloud-lambda-")):
                continue
            rows.append({"container":name,"cpu":entry.get("CPUPerc"),
                         "memory":entry.get("MemUsage"),"pids":entry.get("PIDs")})
        return {"containers":rows,"limitations":"One instantaneous Docker stats sample. Lambda runtime containers may start/stop independently; it excludes Docker Desktop's own overhead."}
    except (OSError,subprocess.TimeoutExpired,RuntimeError) as exc:
        return {"containers":[],"error":f"Docker stats unavailable: {type(exc).__name__}: {str(exc)[:130]}"}


def pipeline_comparison(csv_text: str, repeats: int = PIPELINE_REPEATS) -> dict:
    """Time genuine S3 upload -> notification -> Lambda -> report retrieval on BOTH emulators.

    The source and report keys are identical in both separate emulators for each round.
    This is an end-to-end wall-clock measurement, including polling, NOT Lambda handler duration.
    """
    if not isinstance(csv_text, str) or not csv_text.strip() or len(csv_text) > MAX_CSV_CHARS:
        raise ValueError("Provide a valid CSV input")
    if repeats != PIPELINE_REPEATS:
        raise ValueError("This lab uses exactly four repetitions")
    payload = csv_text.encode("utf-8")
    clients = {name: s3_client(name) for name in EMULATORS}
    batch_id = secrets.token_hex(7)
    rows = []
    for round_number in range(1, repeats+1):
        key = f"incoming/compare-{batch_id}-{round_number}.csv"
        output_key = f"reports/compare-{batch_id}-{round_number}.json"
        order = list(EMULATORS)
        if round_number % 2 == 0:
            order.reverse()
        t0 = {}
        outcomes = {}
        # Alternate order so the same emulator doesn't always get first access to the CPU.
        for name in order:
            t0[name] = time.perf_counter()
            try:
                clients[name].put_object(Bucket=INPUT_BUCKET, Key=key,
                    Body=payload, ContentType="text/csv; charset=utf-8")
            except Exception as exc:
                outcomes[name] = {"ok":False, "error":f"Upload failed: {type(exc).__name__}: {str(exc)[:120]}"}
        deadline = time.monotonic() + 12
        while time.monotonic() < deadline and len(outcomes) < 2:
            for name in order:
                if name in outcomes:
                    continue
                try:
                    response = clients[name].get_object(Bucket=OUTPUT_BUCKET, Key=output_key)
                    try:
                        content = response["Body"].read()
                        report = json.loads(content)
                    finally:
                        response["Body"].close()
                    if not isinstance(report,dict) or not all(k in report for k in ("accepted","rejected","status")):
                        outcomes[name] = {"ok":False, "error":"Unexpected report schema"}
                    else:
                        outcomes[name] = {"ok":True,
                             "elapsedMs":round((time.perf_counter()-t0[name])*1000,2),
                             "accepted":report["accepted"],"rejected":report["rejected"],
                             "report":report}
                except ClientError as exc:
                    if not _is_missing(exc):
                        outcomes[name] = {"ok":False,"error":f"Report read failed: {exc.response.get('Error',{}).get('Code','unknown')}"}
                except Exception as exc:
                    outcomes[name] = {"ok":False,"error":f"Unexpected: {type(exc).__name__}"}
            if len(outcomes) < 2:
                time.sleep(.06)
        for name in EMULATORS:
            if name not in outcomes:
                outcomes[name] = {"ok":False,"error":"No report within 12 seconds"}
        # Input keys match, so identical report JSONs are expected for a correct implementation.
        equal = (outcomes["FakeCloud"].get("ok") and outcomes["LocalStack"].get("ok")
                 and outcomes["FakeCloud"]["report"] == outcomes["LocalStack"]["report"])
        for name in EMULATORS:
            entry = outcomes[name]
            rows.append({"round":round_number,"emulator":name,
                         "elapsedMs":entry.get("elapsedMs"),"ok":entry["ok"],
                         "error":entry.get("error"),"accepted":entry.get("accepted"),
                         "rejected":entry.get("rejected"),"reportsAgree":bool(equal)})
    stats = {}
    for name in EMULATORS:
        vals = [row["elapsedMs"] for row in rows if row["emulator"]==name and row["ok"]]
        stats[name] = {"completed":len(vals),"total":repeats,
                       "medianMs":round(statistics.median(vals),2) if vals else None,
                       "minMs":min(vals) if vals else None,"maxMs":max(vals) if vals else None}
    result = {"type":"graduate-pipeline", "runs":repeats, "samples":rows,"summary":stats,
              "allReportsAgree":all(row["reportsAgree"] for row in rows),
              "inputBytes":len(payload),
              "method":"S3 PUT to first observed JSON report from S3; 60 ms polling; warm emulators; alternating submission order.",
              "limitations":"One machine and four repetitions, including SDK overhead and polling. Not a Lambda-only latency or universal speed ranking."}
    EVIDENCE.mkdir(exist_ok=True)
    (EVIDENCE/"latest-pipeline-comparison.json").write_text(json.dumps(result,indent=2),encoding="utf-8")
    return result


# Safe, reversible fault experiment.  No real AWS endpoints or production resources.
# This is deliberately NOT an emulator performance benchmark.
_fault_lock = threading.Lock()
_NOTIFICATION_KEYS = (
    'TopicConfigurations', 'QueueConfigurations',
    'LambdaFunctionConfigurations', 'EventBridgeConfiguration',
)


def _notification_payload(response):
    """Remove read-only response metadata before applying configuration via S3."""
    return {key: response[key] for key in _NOTIFICATION_KEYS if key in response and response[key]}


def _notification_targets(configuration, arn):
    return [row for row in configuration.get('LambdaFunctionConfigurations', [])
            if row.get('LambdaFunctionArn') == arn]


def _output_for(client, key):
    try:
        response = client.get_object(Bucket=OUTPUT_BUCKET, Key=key)
        try:
            payload = json.loads(response['Body'].read())
        finally:
            response['Body'].close()
        if not isinstance(payload, dict) or not all(field in payload for field in ('accepted', 'rejected', 'status')):
            raise ValueError('An S3 output was found, but it is not a graduate report')
        return payload
    except ClientError as error:
        if _is_missing(error):
            return None
        raise


def _fault_one(name, csv_text):
    """Temporarily disconnect S3 notifications, observe lost event, restore and retry.

    Always attempts to restore the exact original notification configuration.
    A backup is also written on disk before any change, for crash recovery.
    """
    client = s3_client(name)
    token = secrets.token_hex(6)
    prefix = 'incoming/outage-' + token
    failed_key = prefix + '-no-notification.csv'
    recovered_key = prefix + '-retry.csv'
    missing_report_key = 'reports/' + failed_key.split('/')[-1].replace('.csv', '.json')
    recovered_report_key = 'reports/' + recovered_key.split('/')[-1].replace('.csv', '.json')
    row = {'emulator': name, 'endpoint': '127.0.0.1:' + str(EMULATORS[name]),
           'notificationDisabled': False, 'failedObjectStored': False,
           'failedReportAbsent': False, 'notificationRestored': False,
           'retrySucceeded': False, 'failedReportStillAbsent': False,
           'failedKey': failed_key, 'retryKey': recovered_key,
           'steps': [], 'error': None, 'restorationError': None}

    original = None
    changed = False
    try:
        config_response = client.get_bucket_notification_configuration(Bucket=INPUT_BUCKET)
        original = _notification_payload(config_response)
        # Resolve the ARN dynamically. FakeCloud and LocalStack use different account IDs.
        port = EMULATORS[name]
        lambda_client = boto3.client(
            'lambda', endpoint_url='http://127.0.0.1:' + str(port),
            region_name='us-east-1', aws_access_key_id='test', aws_secret_access_key='test',
            config=Config(retries={'max_attempts': 1}, connect_timeout=2, read_timeout=4))
        arn = lambda_client.get_function_configuration(FunctionName='wade-graduates-v1')['FunctionArn']
        matching = _notification_targets(original, arn)
        if len(matching) != 1:
            raise RuntimeError('Expected exactly one S3 notification to our graduate Lambda; made no changes')
        EVIDENCE.mkdir(exist_ok=True)
        # Preserve on-disk recovery information BEFORE touching S3 configuration.
        backup = EVIDENCE / ('notification-backup-' + name.lower() + '.json')
        backup.write_text(json.dumps(original, indent=2), encoding='utf-8')
        row['backupFile'] = str(backup.name)
        without_ours = dict(original)
        without_ours['LambdaFunctionConfigurations'] = [
            value for value in original.get('LambdaFunctionConfigurations', [])
            if value.get('LambdaFunctionArn') != arn]
        without_ours = {key: value for key, value in without_ours.items() if value}
        changed = True  # Treat even an ambiguous API failure as possibly modified.
        client.put_bucket_notification_configuration(
            Bucket=INPUT_BUCKET, NotificationConfiguration=without_ours)
        actual = _notification_payload(client.get_bucket_notification_configuration(Bucket=INPUT_BUCKET))
        if _notification_targets(actual, arn):
            raise RuntimeError('Notification was not disabled; stopping before upload')
        row['notificationDisabled'] = True
        row['steps'].append('The S3 → Lambda notification was removed from the test bucket.')

        client.put_object(Bucket=INPUT_BUCKET, Key=failed_key, Body=csv_text.encode('utf-8'),
                          ContentType='text/csv; charset=utf-8')
        client.head_object(Bucket=INPUT_BUCKET, Key=failed_key)
        row['failedObjectStored'] = True
        row['steps'].append('CSV uploaded and confirmed in input S3, with no Lambda notification configured.')
        time.sleep(.35)
        row['failedReportAbsent'] = (_output_for(client, missing_report_key) is None)
        row['steps'].append('No output report was found for the disconnected upload.'
                            if row['failedReportAbsent'] else
                            'Unexpected output found despite the disconnected notification.')
    except Exception as error:
        row['error'] = f'{type(error).__name__}: {str(error)[:230]}'
    finally:
        if changed and original is not None:
            try:
                client.put_bucket_notification_configuration(
                    Bucket=INPUT_BUCKET, NotificationConfiguration=original)
                actual = _notification_payload(client.get_bucket_notification_configuration(Bucket=INPUT_BUCKET))
                # Different order of dictionary keys doesn't affect equality.
                if actual != original:
                    raise RuntimeError('Restored S3 configuration differs from saved configuration')
                row['notificationRestored'] = True
                row['steps'].append('Original S3 notification was restored and re-read from the emulator.')
            except Exception as restore_error:
                row['restorationError'] = f'{type(restore_error).__name__}: {str(restore_error)[:230]}'
        elif not changed:
            row['notificationRestored'] = True  # Never modified it.

    if changed and not row['notificationRestored']:
        row['error'] = (row['error'] or '') + ' RESTORE FAILED. See backupFile and RESTORE.md.'
        return row  # Do not upload more objects to a misconfigured bucket.

    if not (row['notificationDisabled'] and row['failedObjectStored'] and row['failedReportAbsent']):
        return row
    try:
        # Restoring notification does NOT replay the old upload. An application must retry.
        client.put_object(Bucket=INPUT_BUCKET, Key=recovered_key,
                          Body=csv_text.encode('utf-8'), ContentType='text/csv; charset=utf-8')
        start = time.monotonic()
        while time.monotonic() - start < 10:
            report = _output_for(client, recovered_report_key)
            if report is not None:
                row['retrySucceeded'] = True
                row['accepted'] = report['accepted']
                row['rejected'] = report['rejected']
                row['retryElapsedMs'] = round((time.monotonic() - start) * 1000)
                break
            time.sleep(.12)
        row['steps'].append('A new upload after restoring the notification produced a report.'
                            if row['retrySucceeded'] else 'Retry did not produce a report within 10 seconds.')
        row['failedReportStillAbsent'] = (_output_for(client, missing_report_key) is None)
        row['steps'].append('The original upload was not automatically replayed.'
                            if row['failedReportStillAbsent'] else 'The original upload also has a report; investigate event delivery.')
    except Exception as error:
        row['error'] = f'{type(error).__name__}: {str(error)[:230]}'
    return row


def notification_outage_test(csv_text, targets):
    if not isinstance(csv_text, str) or not csv_text.strip() or len(csv_text) > MAX_CSV_CHARS:
        raise ValueError('Choose or enter a CSV before testing the notification')
    if not isinstance(targets, list) or not targets or len(set(targets)) != len(targets) or any(
        name not in EMULATORS for name in targets):
        raise ValueError('Choose FakeCloud, LocalStack, or both')
    if not _fault_lock.acquire(blocking=False):
        raise RuntimeError('A notification test is already in progress')
    try:
        rows = [_fault_one(name, csv_text) for name in targets]
        result = {'type': 'notification-outage', 'emulators': rows,
                  'complete': all(row['notificationDisabled'] and row['failedObjectStored'] and
                                  row['failedReportAbsent'] and row['notificationRestored'] and
                                  row['retrySucceeded'] and row['failedReportStillAbsent'] for row in rows),
                  'explanation': 'Restoring an S3 notification does not retroactively emit an ObjectCreated event for earlier uploads.',
                  'safety': 'Only the dedicated wade-graduates-in bucket was touched. Original notification settings were backed up and restoration attempted in finally.'}
        EVIDENCE.mkdir(exist_ok=True)
        (EVIDENCE / 'latest-notification-outage.json').write_text(
            json.dumps(result, indent=2), encoding='utf-8')
        return result
    finally:
        _fault_lock.release()

def _trigger_reply(handler, fn, *args):
    """Run one trigger-lab step and answer with the emulator's real result."""
    try:
        return handler.send_json(200, fn(*args))
    except ValueError as error:
        return handler.send_json(400, {"error": str(error)})
    except RuntimeError as error:
        return handler.send_json(409, {"error": str(error)})
    except (ClientError, EndpointConnectionError, OSError) as error:
        return handler.send_json(502, {"error": f"{type(error).__name__}: {str(error)[:200]}"})
    except Exception as error:
        traceback.print_exc()
        return handler.send_json(500, {"error": f"{type(error).__name__}: {str(error)[:180]}"})


class Handler(BaseHTTPRequestHandler):
    server_version = "WadeGraduateLab/2.0"

    def log_message(self, fmt, *args):
        print("[web]", self.address_string(), fmt % args, flush=True)

    def send_json(self, status: int, obj: object):
        data = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        path = urlparse(self.path).path
        if path == '/api/jobs/status':
            from urllib.parse import parse_qs
            name = parse_qs(urlparse(self.path).query).get('emulator',[''])[0]
            try:
                return self.send_json(200, queued_jobs.state(name))
            except ValueError as error:
                return self.send_json(400, {'error': str(error)})
            except Exception as error:
                traceback.print_exc()
                return self.send_json(500, {'error':f'{type(error).__name__}: {str(error)[:180]}'})
        if path in ("/api/trigger/state", "/api/trigger/evidence"):
            from urllib.parse import parse_qs
            name = parse_qs(urlparse(self.path).query).get("emulator", [""])[0]
            fn = trigger_lab.state if path.endswith("state") else trigger_lab.evidence
            return _trigger_reply(self, fn, name)
        if path == "/api/docker-stats":
            return self.send_json(200, container_memory_snapshot())
        if path == "/api/recorded-benchmarks":
            return self.send_json(200, recorded_benchmarks())
        if path == "/api/status":
            status = {}
            for name, port in EMULATORS.items():
                try:
                    s3_client(name).head_bucket(Bucket=INPUT_BUCKET)
                    status[name] = {"reachable": True, "endpoint": f"127.0.0.1:{port}"}
                except Exception:
                    status[name] = {"reachable": False, "endpoint": f"127.0.0.1:{port}"}
            return self.send_json(200, {"emulators": status})
        if path in ("/api/samples/valid", "/api/samples/problems"):
            name = "graduates-valid.csv" if path.endswith("valid") else "graduates-problems.csv"
            return self.send_json(200, {"csv": (SAMPLES / name).read_text(encoding="utf-8")})
        if path.startswith("/api/inspect/"):
            parts = path.split("/")
            if len(parts) != 5 or len(parts[3]) != 16 or any(ch not in "0123456789abcdef" for ch in parts[3]):
                return self.send_json(404, {"error": "Unknown run"})
            found = inspect_run(parts[3], unquote(parts[4]))
            return self.send_json(200, found) if found else self.send_json(404, {"error": "Unknown run or emulator"})
        if path.startswith("/api/runs/"):
            run_id = path[len("/api/runs/"):]
            if len(run_id) != 16 or any(ch not in "0123456789abcdef" for ch in run_id):
                return self.send_json(404, {"error": "Unknown run"})
            result = snapshot(run_id)
            return self.send_json(200, result) if result else self.send_json(404, {"error": "Unknown run"})
        if path.startswith("/api/"):
            return self.send_json(404, {"error": "Unknown API route"})
        if path == "/":
            path = "/index.html"
        if path == "/jobs":
            path = "/jobs.html"
        if path not in {"/index.html", "/styles.css", "/app.js", "/jobs.html", "/jobs.js", "/jobs.css", "/trigger.js", "/trigger.css"}:
            return self.send_json(404, {"error": "Not found"})
        file = WEB / path.lstrip("/")
        raw = file.read_bytes()
        kind = mimetypes.guess_type(file.name)[0] or "application/octet-stream"
        self.send_response(200)
        self.send_header("Content-Type", kind + "; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.end_headers()
        self.wfile.write(raw)

    def do_POST(self):
        path = urlparse(self.path).path
        if path.startswith('/api/jobs/'):
            try:
                length = int(self.headers.get('Content-Length','0'))
                if length < 1 or length > MAX_POST_BYTES:
                    return self.send_json(413, {'error':'Request too large'})
                payload = json.loads(self.rfile.read(length))
                if not isinstance(payload, dict):
                    raise ValueError('Expected an object')
                action = path[len('/api/jobs/'):]
                if action == 'submit':
                    result = queued_jobs.submit(payload.get('csv'), payload.get('targets'))
                elif action in ('pickup','interrupt'):
                    result = queued_jobs.pickup(payload.get('emulator'), interrupt=(action=='interrupt'))
                else:
                    return self.send_json(404, {'error':'Unknown job action'})
                return self.send_json(200, result)
            except (ValueError, json.JSONDecodeError) as error:
                return self.send_json(400, {'error':str(error)})
            except Exception as error:
                traceback.print_exc()
                return self.send_json(500, {'error':f'{type(error).__name__}: {str(error)[:180]}'})
        if path.startswith("/api/trigger/"):
            try:
                length = int(self.headers.get("Content-Length", "0"))
                if length < 1 or length > MAX_POST_BYTES:
                    return self.send_json(413, {"error": "Request too large"})
                body = json.loads(self.rfile.read(length))
                if not isinstance(body, dict):
                    raise ValueError("Expected an object")
            except (ValueError, json.JSONDecodeError) as error:
                return self.send_json(400, {"error": str(error)})
            action, name = path[len("/api/trigger/"):], body.get("emulator", "")
            steps = {"disconnect": (trigger_lab.disconnect, name), "reconnect": (trigger_lab.reconnect, name),
                     "reset": (trigger_lab.reset, name), "upload": (trigger_lab.upload, name, body.get("csv")),
                     "replay": (trigger_lab.replay, name, body.get("key"))}
            if action not in steps:
                return self.send_json(404, {"error": "Unknown trigger action"})
            return _trigger_reply(self, *steps[action])
        if path == "/api/notification-outage":
            try:
                length = int(self.headers.get("Content-Length", "0"))
                if length < 1 or length > MAX_POST_BYTES:
                    return self.send_json(413, {"error": "Request too large"})
                body = json.loads(self.rfile.read(length))
                return self.send_json(200, notification_outage_test(body.get("csv"), body.get("targets")))
            except (ValueError, json.JSONDecodeError) as error:
                return self.send_json(400, {"error": str(error)})
            except RuntimeError as error:
                return self.send_json(409, {"error": str(error)})
            except Exception as error:
                traceback.print_exc()
                return self.send_json(500, {"error": f"Experiment failed: {type(error).__name__}"})
        if path == "/api/compare-pipeline":
            try:
                length = int(self.headers.get("Content-Length", "0"))
                if length < 1 or length > MAX_POST_BYTES:
                    return self.send_json(413, {"error":"CSV request too large"})
                body = json.loads(self.rfile.read(length))
                return self.send_json(200, pipeline_comparison(body.get("csv")))
            except (ValueError, json.JSONDecodeError) as exc:
                return self.send_json(400, {"error":str(exc)})
            except Exception as exc:
                traceback.print_exc()
                return self.send_json(500, {"error":f"Comparison failed: {type(exc).__name__}"})
        if path != "/api/runs":
            return self.send_json(404, {"error": "Unknown API route"})
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            return self.send_json(400, {"error": "Invalid request length"})
        if length < 1 or length > MAX_POST_BYTES:
            return self.send_json(413, {"error": "CSV request too large"})
        try:
            payload = json.loads(self.rfile.read(length))
            result = start_run(payload)
            return self.send_json(201, result)
        except (ValueError, json.JSONDecodeError) as exc:
            return self.send_json(400, {"error": str(exc)})
        except Exception as exc:
            traceback.print_exc()
            return self.send_json(500, {"error": f"Server problem: {type(exc).__name__}"})


def main():
    address = (BIND_HOST, BIND_PORT)
    print(f"WADe Graduate Lab: http://{BIND_HOST}:{BIND_PORT}", flush=True)
    print("Only local FakeCloud :4566 and LocalStack :4567; no real AWS.", flush=True)
    print("Press Ctrl+C to stop this web server; Docker containers remain running.", flush=True)
    server = ThreadingHTTPServer(address, Handler)
    try:
        server.serve_forever(poll_interval=0.2)
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        print("Web server stopped.", flush=True)


if __name__ == "__main__":
    main()
