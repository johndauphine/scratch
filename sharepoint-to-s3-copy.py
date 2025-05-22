Analyze the following code:

# sharepoint_to_s3_lambda.py
"""
AWS Lambda that copies SharePoint files to Amazon S3 —or just lists
document‑library folders for debugging—and sends an SNS alert on any failure.

Highlights
----------
* Always‑fresh config: JSON pulled from Secrets Manager every invocation.
* Verbose logging (LOG_LEVEL env var, default DEBUG).
* Connection verification (site + drive) before any work.
* Three modes
  1. Debug listing  – event {"debug": "list_folders"}
  2. Single‑file    – event["file_path"] or secret key SHAREPOINT_FILE_PATH
  3. Folder‑scan    – secret key SHAREPOINT_FOLDER_PATH
* Flexible `S3_PREFIX`
  • ends with “/” → treated as folder; basename appended  
  • otherwise     → treated as complete object key
* Any un‑handled exception → SNS notification (topic ARN from secret key
  SNS_TOPIC_ARN or env var of the same name).

Python 3.11 stdlib + boto3 only.
"""
from __future__ import annotations

import base64
import json
import logging
import os
import posixpath
import time
import urllib.parse
import urllib.request
import traceback
from typing import List, Tuple

import boto3
from botocore.exceptions import ClientError

# ──────────────────────────────────────────
# Logging
# ──────────────────────────────────────────
LOG_LEVEL = os.getenv("LOG_LEVEL", "DEBUG").upper()
logging.basicConfig(format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger()
logger.setLevel(LOG_LEVEL)

# ──────────────────────────────────────────
# Secrets helper
# ──────────────────────────────────────────
def load_config() -> dict:
    secret_name = os.environ["CONFIG_SECRET_NAME"]
    region      = os.getenv("AWS_REGION", "us-east-1")
    sm          = boto3.client("secretsmanager", region_name=region)
    resp        = sm.get_secret_value(SecretId=secret_name)
    raw         = resp.get("SecretString") or base64.b64decode(resp["SecretBinary"]).decode()
    return json.loads(raw)

# ──────────────────────────────────────────
# AWS clients (re‑used across calls)
# ──────────────────────────────────────────
auth_s3  = boto3.client("s3")
auth_sns = boto3.client("sns")

# ──────────────────────────────────────────
# SNS helper
# ──────────────────────────────────────────
def send_sns(subject: str, message: str, arn: str | None = None) -> None:
    arn = arn or os.getenv("SNS_TOPIC_ARN")
    if not arn:
        logger.warning("SNS_TOPIC_ARN not set; skipping alert")
        return
    try:
        auth_sns.publish(TopicArn=arn, Subject=subject[:100], Message=message)
        logger.info("SNS alert sent to %s", arn)
    except ClientError as exc:
        logger.error("Failed to send SNS: %s", exc)

# ──────────────────────────────────────────
# Utility – build final S3 key
# ──────────────────────────────────────────
def build_s3_key(prefix: str | None, filename: str) -> str:
    if not prefix:
        return posixpath.basename(filename)
    if prefix.endswith("/"):
        return f"{prefix}{posixpath.basename(filename)}"
    return prefix.lstrip("/")

# ──────────────────────────────────────────
# Tiny urllib helper
# ──────────────────────────────────────────
def http_request(
    method: str,
    url: str,
    headers: dict | None = None,
    data: bytes | None = None,
    timeout: int = 15,
) -> Tuple[int, dict, bytes]:
    logger.debug("HTTP %s %s", method, url)
    req = urllib.request.Request(url=url, data=data, headers=headers or {}, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, dict(resp.headers), resp.read()
    except urllib.error.HTTPError as err:
        body = err.read() if err.fp else b""
        logger.warning("HTTPError %s on %s %s – %s", err.code, method, url, body[:200])
        return err.code, dict(err.headers or {}), body
    except urllib.error.URLError as err:
        logger.error("URLError on %s %s – %s", method, url, err)
        raise

# ──────────────────────────────────────────
# Microsoft Graph helpers
# ──────────────────────────────────────────
def get_access_token(tenant_id: str, client_id: str, client_secret: str) -> str:
    url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    data = urllib.parse.urlencode({
        "grant_type":    "client_credentials",
        "client_id":     client_id,
        "client_secret": client_secret,
        "scope":         "https://graph.microsoft.com/.default",
    }).encode()
    status, _, body = http_request("POST", url,
                                   headers={"Content-Type": "application/x-www-form-urlencoded"},
                                   data=data)
    if status != 200:
        raise RuntimeError(f"Token request failed {status}: {body.decode()[:200]}")
    return json.loads(body)["access_token"]

def verify_sharepoint_access(token: str, site_id: str, drive_id: str) -> None:
    hdr = {"Authorization": f"Bearer {token}"}
    for label, url in {
        "site":  f"https://graph.microsoft.com/v1.0/sites/{site_id}",
        "drive": f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}",
    }.items():
        status, _, body = http_request("GET", url, headers=hdr)
        if status != 200:
            raise RuntimeError(f"{label.capitalize()} check failed {status}: {body.decode()[:200]}")
    logger.debug("Site and drive verified accessible")

def list_folder_items(token: str, site_id: str, drive_id: str, folder_path: str = "") -> List[dict]:
    if folder_path:
        encoded = urllib.parse.quote(folder_path.lstrip("/"))
        url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/root:/{encoded}:/children"
    else:
        url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/root/children"
    hdr   = {"Authorization": f"Bearer {token}"}
    items: List[dict] = []
    while url:
        status, _, body = http_request("GET", url, headers=hdr)
        if status != 200:
            raise RuntimeError(f"Folder list failed {status}: {body.decode()[:200]}")
        pl = json.loads(body)
        items.extend(pl.get("value", []))
        url = pl.get("@odata.nextLink")
    return items

def list_folders(token: str, site_id: str, drive_id: str, folder_path: str = "") -> List[str]:
    return [i["name"] for i in list_folder_items(token, site_id, drive_id, folder_path) if "folder" in i]

def download_by_path(token: str, site_id: str, drive_id: str, path_in_drive: str) -> bytes:
    encoded = urllib.parse.quote(path_in_drive.lstrip("/"))
    url     = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/root:/{encoded}:/content"
    status, _, body = http_request("GET", url, headers={"Authorization": f"Bearer {token}"}, timeout=60)
    if status != 200:
        raise RuntimeError(f"Download failed {status}: {body.decode()[:200]}")
    return body

def download_by_id(token: str, site_id: str, drive_id: str, item_id: str) -> bytes:
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/items/{item_id}/content"
    status, _, body = http_request("GET", url, headers={"Authorization": f"Bearer {token}"}, timeout=60)
    if status != 200:
        raise RuntimeError(f"Download failed {status}: {body.decode()[:200]}")
    return body

def delete_item(token: str, site_id: str, drive_id: str, item_id: str) -> None:
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/items/{item_id}"
    status, _, _ = http_request("DELETE", url, headers={"Authorization": f"Bearer {token}"})
    if status not in (204, 202):
        raise RuntimeError(f"Delete failed with status {status}")

# ──────────────────────────────────────────
# S3 helper
# ──────────────────────────────────────────
def upload_to_s3(bucket: str, key: str, data: bytes) -> None:
    auth_s3.put_object(Bucket=bucket, Key=key, Body=data)
    logger.info("Uploaded %d bytes to s3://%s/%s", len(data), bucket, key)

# ──────────────────────────────────────────
# Lambda handler
# ──────────────────────────────────────────
def lambda_handler(event: dict | None, context):
    """AWS Lambda entry point (debug / single / folder‑scan modes)."""
    logger.info("Invocation started (requestId=%s)", context.aws_request_id)
    logger.debug("Event payload: %s", json.dumps(event))

    try:
        # 1. Config & connectivity
        cfg      = load_config()
        sns_arn  = cfg.get("SNS_TOPIC_ARN") or os.getenv("SNS_TOPIC_ARN")
        token    = get_access_token(cfg["TENANT_ID"], cfg["CLIENT_ID"], cfg["CLIENT_SECRET"])
        site_id  = cfg["SHAREPOINT_SITE_ID"]
        drive_id = cfg["DRIVE_ID"]
        verify_sharepoint_access(token, site_id, drive_id)

        # 2. Debug folder listing
        if isinstance(event, dict) and event.get("debug") == "list_folders":
            folders = list_folders(token, site_id, drive_id, cfg.get("SHAREPOINT_FOLDER_PATH", ""))
            return {"statusCode": 200, "body": json.dumps({"mode": "debug", "folders": folders})}

        # 3. Runtime parameters
        s3_bucket    = cfg["S3_BUCKET"]
        s3_prefix    = cfg.get("S3_PREFIX", "")
        delete_after = str(cfg.get("DELETE_AFTER_COPY", "false")).lower() == "true"
        file_path    = (event or {}).get("file_path") if isinstance(event, dict) else None or cfg.get("SHAREPOINT_FILE_PATH")
        folder_path  = cfg.get("SHAREPOINT_FOLDER_PATH", "")

        start = time.perf_counter()

        # 4. Single‑file mode
        if file_path:
            data   = download_by_path(token, site_id, drive_id, file_path)
            s3_key = build_s3_key(s3_prefix, file_path)
            upload_to_s3(s3_bucket, s3_key, data)

            elapsed = round((time.perf_counter() - start) * 1000, 1)
            return {"statusCode": 200,
                    "body": json.dumps({"mode": "single",
                                        "file": file_path,
                                        "bytes": len(data),
                                        "elapsed_ms": elapsed})}

        # 5. Folder‑scan mode
        items = list_folder_items(token, site_id, drive_id, folder_path)
        if not items:
            return {"statusCode": 204,
                    "body": json.dumps({"mode": "folder", "files": 0})}

        transferred = 0
        for itm in items:
            if "file" not in itm:
                continue
            name = itm["name"]
            data = download_by_id(token, site_id, drive_id, itm["id"])
            s3_key = build_s3_key(s3_prefix, name)
            upload_to_s3(s3_bucket, s3_key, data)
            transferred += 1
            if delete_after:
                delete_item(token, site_id, drive_id, itm["id"])

        elapsed = round((time.perf_counter() - start) * 1000, 1)
        return {"statusCode": 200,
                "body": json.dumps({"mode": "folder",
                                    "files": transferred,
                                    "elapsed_ms": elapsed})}

    # 6. Global error handler
    except Exception as exc:                   # pylint: disable=broad-except
        tb = traceback.format_exc()
        brief = (json.dumps(event)[:500] + "...") if len(str(event)) > 500 else json.dumps(event)
        msg = f"Request ID: {context.aws_request_id}\\nEvent: {brief}\\n\\nTraceback:\\n{tb}"
        logger.error("Unhandled exception: %s", exc)
        send_sns("SharePoint→S3 Lambda failure", msg, arn=sns_arn)
        raise