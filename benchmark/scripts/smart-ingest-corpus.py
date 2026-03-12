#!/usr/bin/env python3
"""
smart-ingest-corpus.py — Ingest daily-memory corpus into mem9 via the smart-ingest API.

Reads daily markdown files from the corpus directory and sends each as a
message-based ingest request (mode: "smart") to the mem9 API, matching the
shape used by the openclaw-plugin smart pipeline.

Usage:
    python3 benchmark/scripts/smart-ingest-corpus.py \
        --corpus-dir benchmark/results/.../corpus \
        --base-url https://api.mem9.ai \
        --tenant-id <space-id> \
        --agent-id daily-memory-bench
"""

import argparse
import glob
import json
import os
import sys
import time
import urllib.request
import urllib.error


def ingest_day(base_url: str, tenant_id: str, agent_id: str,
               date: str, content: str, session_id: str) -> dict:
    """Send one day's content through the smart-ingest pipeline."""
    url = f"{base_url}/v1alpha1/mem9s/{tenant_id}/memories"
    body = {
        "messages": [
            {"role": "user", "content": f"Here is my daily log for {date}."},
            {"role": "assistant", "content": content},
        ],
        "session_id": session_id,
        "agent_id": agent_id,
        "mode": "smart",
    }
    data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={
            "Content-Type": "application/json",
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
        description="Ingest daily-memory corpus into mem9 via smart-ingest")
    parser.add_argument("--corpus-dir", required=True,
                        help="Directory containing day-NNN_YYYY-MM-DD.md files")
    parser.add_argument("--base-url", required=True,
                        help="mem9 API base URL")
    parser.add_argument("--tenant-id", required=True,
                        help="mem9 space/tenant ID")
    parser.add_argument("--agent-id", default="daily-memory-bench",
                        help="Agent ID for ingest requests")
    parser.add_argument("--session-id", default="daily-memory-e2e",
                        help="Session ID for ingest requests")
    args = parser.parse_args()

    base_url = args.base_url.rstrip("/")
    corpus_dir = args.corpus_dir

    # Find all daily markdown files
    pattern = os.path.join(corpus_dir, "day-*_*.md")
    files = sorted(glob.glob(pattern))
    if not files:
        print(f"ERROR: No day files found matching {pattern}", file=sys.stderr)
        sys.exit(1)

    print(f"    Ingesting {len(files)} daily files via smart-ingest")
    print(f"    Target: {base_url} / tenant={args.tenant_id}")

    succeeded = 0
    failed = 0
    for i, filepath in enumerate(files):
        filename = os.path.basename(filepath)
        # Extract date from filename: day-NNN_YYYY-MM-DD.md
        parts = filename.replace(".md", "").split("_", 1)
        date = parts[1] if len(parts) > 1 else filename

        with open(filepath) as f:
            content = f.read()

        result = ingest_day(
            base_url, args.tenant_id, args.agent_id,
            date, content, args.session_id,
        )

        status = result.get("status", "unknown")
        if status in ("accepted", "complete", "partial"):
            succeeded += 1
        else:
            failed += 1
            err = result.get("error", "unknown error")
            print(f"    WARN: {filename} failed: {err}", file=sys.stderr)

        if (i + 1) % 10 == 0 or i == len(files) - 1:
            print(f"    Progress: {i + 1}/{len(files)} "
                  f"(ok={succeeded}, fail={failed})")

    print(f"    Smart-ingest complete: {succeeded} succeeded, {failed} failed")
    if failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
