#!/usr/bin/env python3
"""
smart-ingest-corpus.py — Ingest daily-memory corpus into mem9 via the import API.

Reads daily markdown files from the corpus directory and uploads each through
the /v1alpha1/mem9s/{tenantID}/imports endpoint as a multipart file upload.

Usage:
    python3 benchmark/daily-memory/smart-ingest-corpus.py \
        --corpus-dir benchmark/results/.../corpus \
        --base-url https://api.mem9.ai \
        --tenant-id <space-id> \
        --agent-id daily-memory-bench
"""

import argparse
import glob
import io
import json
import os
import sys
import time
import urllib.request
import urllib.error


def import_file(base_url: str, tenant_id: str, agent_id: str,
                filepath: str, session_id: str) -> dict:
    """Upload one corpus file through the import endpoint."""
    url = f"{base_url}/v1alpha1/mem9s/{tenant_id}/imports"
    filename = os.path.basename(filepath)

    with open(filepath, "rb") as f:
        file_data = f.read()

    # Build multipart/form-data body
    boundary = "----Mem9BenchmarkBoundary"
    body = io.BytesIO()

    # file field
    body.write(f"--{boundary}\r\n".encode())
    body.write(f'Content-Disposition: form-data; name="file"; filename="{filename}"\r\n'.encode())
    body.write(b"Content-Type: text/markdown\r\n\r\n")
    body.write(file_data)
    body.write(b"\r\n")

    # agent_id field
    body.write(f"--{boundary}\r\n".encode())
    body.write(b'Content-Disposition: form-data; name="agent_id"\r\n\r\n')
    body.write(agent_id.encode())
    body.write(b"\r\n")

    # session_id field
    body.write(f"--{boundary}\r\n".encode())
    body.write(b'Content-Disposition: form-data; name="session_id"\r\n\r\n')
    body.write(session_id.encode())
    body.write(b"\r\n")

    # file_type field
    body.write(f"--{boundary}\r\n".encode())
    body.write(b'Content-Disposition: form-data; name="file_type"\r\n\r\n')
    body.write(b"memory")
    body.write(b"\r\n")

    body.write(f"--{boundary}--\r\n".encode())

    data = body.getvalue()
    req = urllib.request.Request(
        url,
        data=data,
        headers={
            "Content-Type": f"multipart/form-data; boundary={boundary}",
            "X-Mnemo-Agent-Id": agent_id,
        },
        method="POST",
    )

    max_retries = 3
    for attempt in range(max_retries + 1):
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                resp_body = resp.read().decode("utf-8")
                if resp_body.strip():
                    return json.loads(resp_body)
                return {"status": "accepted"}
        except urllib.error.HTTPError as e:
            if e.code >= 500 and attempt < max_retries:
                time.sleep(1 * (attempt + 1))
                continue
            body_text = e.read().decode("utf-8", errors="replace") if e.fp else ""
            return {"status": "failed", "error": f"HTTP {e.code}: {body_text[:200]}"}
        except Exception as e:
            if attempt < max_retries:
                time.sleep(1 * (attempt + 1))
                continue
            return {"status": "failed", "error": str(e)}
    return {"status": "failed", "error": "max retries exceeded"}


def main():
    parser = argparse.ArgumentParser(
        description="Ingest daily-memory corpus into mem9 via import API")
    parser.add_argument("--corpus-dir", required=True,
                        help="Directory containing YYYY-MM-DD.md corpus files")
    parser.add_argument("--base-url", required=True,
                        help="mem9 API base URL")
    parser.add_argument("--tenant-id", required=True,
                        help="mem9 space/tenant ID")
    parser.add_argument("--agent-id", default="daily-memory-bench",
                        help="Agent ID for import requests")
    parser.add_argument("--session-id", default="daily-memory-e2e",
                        help="Session ID for import requests")
    args = parser.parse_args()

    base_url = args.base_url.rstrip("/")
    corpus_dir = args.corpus_dir

    # Find all daily markdown files (YYYY-MM-DD.md)
    pattern = os.path.join(corpus_dir, "*.md")
    files = sorted(glob.glob(pattern))
    if not files:
        print(f"ERROR: No corpus files found matching {pattern}", file=sys.stderr)
        sys.exit(1)

    print(f"    Importing {len(files)} daily files via import API")
    print(f"    Target: {base_url} / tenant={args.tenant_id}")

    succeeded = 0
    failed = 0
    for i, filepath in enumerate(files):
        filename = os.path.basename(filepath)

        result = import_file(
            base_url, args.tenant_id, args.agent_id,
            filepath, args.session_id,
        )

        status = result.get("status", "unknown")
        if status in ("accepted", "pending", "complete", "partial"):
            succeeded += 1
        else:
            failed += 1
            err = result.get("error", "unknown error")
            print(f"    WARN: {filename} failed: {err}", file=sys.stderr)

        if (i + 1) % 10 == 0 or i == len(files) - 1:
            print(f"    Progress: {i + 1}/{len(files)} "
                  f"(ok={succeeded}, fail={failed})")

    print(f"    Import complete: {succeeded} succeeded, {failed} failed")
    if failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
