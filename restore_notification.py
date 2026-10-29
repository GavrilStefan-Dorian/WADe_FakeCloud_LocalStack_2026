r"""Emergency restore for the dedicated LOCAL graduate input bucket.

Usage (from the repository directory): python restore_notification.py FakeCloud
Recognizes both the old one-click-outage backup and the new trigger-lab backup.
No AWS production endpoints or user credentials are involved.
"""
import argparse
import json
from pathlib import Path

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parent
PORTS = {"FakeCloud": 4566, "LocalStack": 4567}
BUCKET = "wade-graduates-in"


def find_backup(name):
    evidence = ROOT / "evidence"
    recent = evidence / f"trigger-lab-backup-{name.lower()}.json"
    legacy = evidence / f"notification-backup-{name.lower()}.json"
    for path in (recent, legacy):
        if path.is_file():
            return path
    raise FileNotFoundError(f"No backup found. Checked {recent} and {legacy}; no changes made.")


def main():
    parser = argparse.ArgumentParser(description="Restore saved LOCAL S3 notification configuration")
    parser.add_argument("emulator", choices=PORTS)
    args = parser.parse_args()
    backup = find_backup(args.emulator)
    config = json.loads(backup.read_text(encoding="utf-8"))
    s3 = boto3.client("s3", endpoint_url=f"http://127.0.0.1:{PORTS[args.emulator]}",
                      region_name="us-east-1", aws_access_key_id="test", aws_secret_access_key="test",
                      config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 1}))
    s3.put_bucket_notification_configuration(Bucket=BUCKET, NotificationConfiguration=config)
    current = s3.get_bucket_notification_configuration(Bucket=BUCKET)
    keys = ("TopicConfigurations", "QueueConfigurations", "LambdaFunctionConfigurations", "EventBridgeConfiguration")
    actual = {k: current[k] for k in keys if k in current and current[k]}
    if actual != config:
        raise SystemExit("WARNING: Emulator configuration differs from backup after restoring. Investigate before demo.")
    print(f"{args.emulator}: restored configuration from {backup.name}; verified against emulator.")
    for entry in actual.get("LambdaFunctionConfigurations", []):
        print("  Lambda:", entry.get("LambdaFunctionArn", "unknown"))


if __name__ == "__main__":
    main()
