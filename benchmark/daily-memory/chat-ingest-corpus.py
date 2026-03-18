#!/usr/bin/env python3
"""
chat-ingest-corpus.py — Ingest daily-memory corpus into both profiles via chat.

Sends each day's diary content as a natural chat message through the openclaw
CLI, so both profiles learn through conversation rather than direct file/API
injection.

Usage:
    python3 benchmark/daily-memory/chat-ingest-corpus.py \
        --corpus-dir benchmark/results/.../corpus \
        --profile-a mem9_bench_a \
        --profile-b mem9_bench_b \
        --timeout 120
"""

import argparse
import glob
import os
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

MAX_RETRIES = 10
RETRY_DELAY = 5  # seconds between retries
BATCH_SIZE = 1   # number of daily files merged into one chat message


def send_chat(profile: str, dates: str, content: str, timeout: int,
              session_id: str) -> dict:
    """Send a batch of days' diary content to a profile via openclaw chat."""
    message = f"These are diaries for {dates}:\n{content}"
    cmd = [
        "openclaw",
        "--profile", profile,
        "agent",
        "--agent", "main",
        "--message", message,
        "--session-id", session_id,
    ]

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        if result.returncode != 0:
            stderr = result.stderr.strip()[:200] if result.stderr else "no stderr"
            return {
                "status": "failed",
                "profile": profile,
                "date": dates,
                "error": f"exit {result.returncode}: {stderr}",
            }
        return {"status": "ok", "profile": profile, "date": dates}
    except subprocess.TimeoutExpired:
        return {
            "status": "failed",
            "profile": profile,
            "date": dates,
            "error": f"timeout after {timeout}s",
        }
    except Exception as e:
        return {
            "status": "failed",
            "profile": profile,
            "date": dates,
            "error": str(e),
        }


def send_chat_with_retry(profile: str, dates: str, content: str,
                         timeout: int, label: str,
                         session_id: str) -> dict:
    """Send chat with retries on failure."""
    for attempt in range(1, MAX_RETRIES + 1):
        result = send_chat(profile, dates, content, timeout, session_id)
        if result["status"] == "ok":
            return result
        err = result.get("error", "unknown error")
        if attempt < MAX_RETRIES:
            print(f"    WARN: [{dates}] profile {label} attempt {attempt}/{MAX_RETRIES} "
                  f"failed: {err} — retrying in {RETRY_DELAY}s...",
                  file=sys.stderr)
            time.sleep(RETRY_DELAY)
        else:
            print(f"    ERROR: [{dates}] profile {label} failed after "
                  f"{MAX_RETRIES} attempts: {err}",
                  file=sys.stderr)
    return result


def main():
    parser = argparse.ArgumentParser(
        description="Ingest daily-memory corpus into profiles via chat")
    parser.add_argument("--corpus-dir", required=True,
                        help="Directory containing YYYY-MM-DD.md corpus files")
    parser.add_argument("--profile-a", required=True,
                        help="OpenClaw profile name for baseline (A)")
    parser.add_argument("--profile-b", required=True,
                        help="OpenClaw profile name for treatment (B)")
    parser.add_argument("--timeout", type=int, default=120,
                        help="Timeout in seconds per chat message")
    args = parser.parse_args()

    # Find all daily markdown files (YYYY-MM-DD.md)
    pattern = os.path.join(args.corpus_dir, "*.md")
    files = sorted(glob.glob(pattern))
    if not files:
        print(f"ERROR: No corpus files found matching {pattern}", file=sys.stderr)
        sys.exit(1)

    # Group files into batches of BATCH_SIZE
    batches = []
    for i in range(0, len(files), BATCH_SIZE):
        batches.append(files[i:i + BATCH_SIZE])

    num_batches = len(batches)
    print(f"    Chat-ingesting {len(files)} daily files in {num_batches} batches "
          f"(batch_size={BATCH_SIZE}) into both profiles")
    print(f"    Profile A: {args.profile_a}  Profile B: {args.profile_b}")
    print(f"    Timeout: {args.timeout}s per message  Retries: {MAX_RETRIES}")

    succeeded = 0
    failed = 0

    for bi, batch in enumerate(batches):
        # Merge files in this batch into one message
        parts = []
        dates = []
        for filepath in batch:
            filename = os.path.basename(filepath)
            date = filename.replace(".md", "")
            dates.append(date)
            with open(filepath) as f:
                parts.append(f"## {date}\n\n{f.read()}")
        merged_content = "\n\n".join(parts)
        dates_label = f"{dates[0]} ~ {dates[-1]}"

        # Send to both profiles in parallel (each with its own retry logic)
        session_id = f"diary-ingest-batch-{bi}"
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = {
                executor.submit(send_chat_with_retry, args.profile_a,
                                dates_label, merged_content,
                                args.timeout, "A", session_id): "A",
                executor.submit(send_chat_with_retry, args.profile_b,
                                dates_label, merged_content,
                                args.timeout, "B", session_id): "B",
            }
            for future in as_completed(futures):
                result = future.result()
                if result["status"] == "ok":
                    succeeded += 1
                else:
                    failed += 1

        if (bi + 1) % 2 == 0 or bi == num_batches - 1:
            print(f"    Progress: {bi + 1}/{num_batches} batches "
                  f"(ok={succeeded}, fail={failed})")

    total = num_batches * 2
    print(f"    Chat-ingest complete: {succeeded} succeeded, {failed} failed "
          f"(out of {total} batches x2 profiles)")
    if failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
