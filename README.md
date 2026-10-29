# FakeCloud vs. LocalStack

WADe 2026 · Gavril Ștefan-Dorian · Presentation and demo

A local AWS-style application using synthetic graduate data. The same CSV is processed on FakeCloud and LocalStack:

`CSV → S3 → ObjectCreated event → Lambda → S3 JSON report`

The browser shows reports and lets you disconnect the S3 notification, observe the missing output, restore the connection and retry with a new upload. The optional queued mode uses SQS, a Python worker and DynamoDB; the worker invokes Lambda explicitly.

## Presentation

[View the presentation on Canva](https://canva.link/cttoe3s0duqmvom)

[View the slides (PDF)](presentation/FakeCloud-vs-LocalStack.pdf)

[Download the editable PowerPoint](presentation/FakeCloud-vs-LocalStack.pptx)

## Run locally

Requires Docker with Linux containers, Python 3.11+ and a LocalStack account/token. No real AWS account or credentials are needed.

From this project directory, in PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
Copy-Item .env.example .env
```

Put your LocalStack activation token in `.env`, then:

```powershell
docker compose up -d
.\.venv\Scripts\python.exe deploy_local.py
.\.venv\Scripts\python.exe verify_pipeline.py
.\.venv\Scripts\python.exe ui_server.py
```

Wait for the emulators to accept requests before deployment. Open **http://127.0.0.1:8000/**. FakeCloud uses port 4566 and LocalStack 4567. If you already run them on these ports, reuse those containers and skip Compose startup. If `.venv` already exists, reuse it and skip environment creation.

The deployment creates this demo's buckets, Lambda and notification. The Lambda uses Node.js 20 and its runtime AWS SDK; no frontend build or npm install is required. Containers have no persistent storage configured.

## Try the demo

1. Select **Valid sample**, choose the emulator(s), then **Process CSV**.
2. Inspect the generated reports; try **Sample with errors** to see rejected records.
3. Open **Break the S3 notification** and follow **Disconnect → Upload → Reconnect → Replay**.

Replay creates a new upload event; reconnecting does not replay the old event. If interrupted, use **Reset the experiment** to restore the notification. Maintenance helpers are `restore_notification.py` and `prepare_fresh.py`; the latter previews changes unless passed `--apply`. Stop the web server before applying a reset.

## Files and tests

- `index.js`, `processor.js`: Lambda adapter and CSV validation/aggregation.
- `ui_server.py`, `web/`: browser interface and local backend.
- `trigger_lab.py`: notification experiment; `queued_jobs.py`: optional queue mode.
- `samples/`: fictional input data; `tests/`: application tests.
- `evidence/`: historical benchmark CSVs with [methodology limits](evidence/README.md).

Node.js 20+ is needed only for the local JavaScript tests:

```powershell
node tests/processor.test.js
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
```

These tests use fake service clients. `verify_pipeline.py` performs the separate live check against the local emulators. No real alumni data or production hosting is used. Keep the server bound to localhost and never commit `.env`.
