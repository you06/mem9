#!/usr/bin/env python3
from __future__ import annotations
"""
drive-daily-memory-e2e.py — E2E benchmark driver for the daily-memory corpus.

Sends benchmark questions to two OpenClaw profiles (A=file-based workspace,
B=mem9 smart-ingest) and scores responses using the same 5-category
rule-based evaluation as the daily-memory retrieval benchmark.

Each question gets a fresh session to avoid answer contamination across turns.

Usage:
    python3 benchmark/daily-memory/drive-daily-memory-e2e.py \
        --manifest-file <path/to/manifest.json> \
        --results-dir <output-dir> \
        --profile-a <baseline-profile> \
        --profile-b <treatment-profile> \
        --timeout 120 \
        --max-questions 20
"""

import argparse
import html as html_lib
import json
import os
import re
import subprocess
import sys
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone


# ---------------------------------------------------------------------------
# Scoring — faithful reimplementation of daily-memory/src/evaluation.ts
# ---------------------------------------------------------------------------

ARTICLES = {"a", "an", "and", "the"}


def normalize(s: str) -> str:
    """Lowercase, strip non-alphanumeric, remove articles."""
    words = re.sub(r"[^a-z0-9\s]", " ", s.lower()).split()
    return " ".join(w for w in words if w and w not in ARTICLES)


def token_f1(prediction: str, gold: str) -> float:
    pred_tokens = [t for t in normalize(prediction).split() if t]
    gold_tokens = [t for t in normalize(gold).split() if t]
    if not pred_tokens and not gold_tokens:
        return 1.0
    if not pred_tokens or not gold_tokens:
        return 0.0
    gold_count: dict[str, int] = Counter(gold_tokens)
    num_same = 0
    for t in pred_tokens:
        if gold_count.get(t, 0) > 0:
            num_same += 1
            gold_count[t] -= 1
    if num_same == 0:
        return 0.0
    precision = num_same / len(pred_tokens)
    recall = num_same / len(gold_tokens)
    return (2 * precision * recall) / (precision + recall)


def score_exact(pred: str, answer: str, aliases: list[str]) -> float:
    norm = normalize(pred)
    if normalize(answer) in norm:
        return 1.0
    for alias in aliases:
        if normalize(alias) in norm:
            return 1.0
    return token_f1(pred, answer)


def score_paraphrase(pred: str, answer: str, aliases: list[str]) -> float:
    return score_exact(pred, answer, aliases)


def score_temporal(pred: str, answer: str, aliases: list[str]) -> float:
    return score_exact(pred, answer, aliases)


def score_multi_hop(pred: str, answer: str, aliases: list[str]) -> float:
    if not aliases:
        return token_f1(pred, answer)
    norm = normalize(pred)
    found = sum(1 for alias in aliases if normalize(alias) in norm)
    return found / len(aliases)


def score_negative(pred: str) -> float:
    neg_phrases = [
        "no information", "not mentioned", "no record", "none",
        "no entries", "not found", "no data", "doesn't mention",
        "does not mention", "no evidence",
    ]
    lower = pred.lower()
    return 1.0 if any(p in lower for p in neg_phrases) else 0.0


def score_answer(pred: str, answer: str, aliases: list[str],
                 category: str) -> float:
    if category == "exact":
        return score_exact(pred, answer, aliases)
    if category == "paraphrase":
        return score_paraphrase(pred, answer, aliases)
    if category == "temporal":
        return score_temporal(pred, answer, aliases)
    if category == "multi-hop":
        return score_multi_hop(pred, answer, aliases)
    if category == "negative":
        return score_negative(pred)
    return 0.0


# ---------------------------------------------------------------------------
# Agent interaction — same pattern as drive-session.py
# ---------------------------------------------------------------------------

def send_prompt(profile: str, message: str, timeout: int,
                session_id: str) -> dict:
    """Send a prompt to an OpenClaw profile and capture the response."""
    start = time.monotonic()
    try:
        cmd = [
            "openclaw",
            "--profile", profile,
            "agent",
            "--agent", "main",
            "--message", message,
            "--json",
            "--session-id", session_id,
        ]
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        elapsed = time.monotonic() - start
        return {
            "profile": profile,
            "returncode": result.returncode,
            "stdout": result.stdout,
            "stderr": result.stderr,
            "elapsed_seconds": round(elapsed, 2),
            "error": None,
        }
    except subprocess.TimeoutExpired:
        elapsed = time.monotonic() - start
        return {
            "profile": profile,
            "returncode": -1,
            "stdout": "",
            "stderr": f"Timeout after {timeout}s",
            "elapsed_seconds": round(elapsed, 2),
            "error": f"Timeout after {timeout}s",
        }
    except Exception as e:
        elapsed = time.monotonic() - start
        return {
            "profile": profile,
            "returncode": -1,
            "stdout": "",
            "stderr": str(e),
            "elapsed_seconds": round(elapsed, 2),
            "error": str(e),
        }


def parse_response(raw: dict) -> str:
    """Extract the assistant's text response from raw OpenClaw output."""
    stdout = raw.get("stdout", "").strip()
    if not stdout:
        return raw.get("stderr", "(no output)")

    try:
        parsed = json.loads(stdout)
        if isinstance(parsed, dict):
            result = parsed.get("result")
            if isinstance(result, dict):
                payloads = result.get("payloads")
                if isinstance(payloads, list):
                    parts = []
                    for p in payloads:
                        if isinstance(p, dict) and "text" in p:
                            parts.append(p["text"])
                    if parts:
                        return "\n".join(parts)
            for key in ("response", "content", "message", "text", "output"):
                if key in parsed:
                    val = parsed[key]
                    if isinstance(val, str):
                        return val
                    if isinstance(val, list):
                        parts = []
                        for block in val:
                            if isinstance(block, dict) and "text" in block:
                                parts.append(block["text"])
                            elif isinstance(block, str):
                                parts.append(block)
                        if parts:
                            return "\n".join(parts)
            return json.dumps(parsed, indent=2, ensure_ascii=False)
        return stdout
    except json.JSONDecodeError:
        return stdout


# ---------------------------------------------------------------------------
# Auto-compaction helpers
# ---------------------------------------------------------------------------

FILLER_MESSAGES = [
    "Write a detailed comparison of five different sorting algorithms, including their time complexity, space complexity, best/worst cases, and when to use each one. Include pseudocode examples.",
    "Explain the complete history of the Internet from ARPANET to modern day, covering every major milestone, protocol development, and societal impact in detail.",
    "Describe the architecture of a modern microservices-based e-commerce platform. Cover service decomposition, inter-service communication, database choices, caching strategies, deployment pipelines, monitoring, and failure handling in depth.",
    "Write a comprehensive guide to memory management in operating systems, covering virtual memory, paging, segmentation, page replacement algorithms, thrashing, and how modern OSes like Linux handle each of these.",
    "Explain the theory of relativity in detail — both special and general — covering the mathematical foundations, key experiments, implications for GPS, black holes, gravitational waves, and time dilation with worked examples.",
    "Describe the complete lifecycle of a web request from typing a URL to rendering a page. Cover DNS resolution, TCP handshake, TLS negotiation, HTTP protocol, server-side processing, response parsing, DOM construction, CSSOM, layout, paint, and compositing.",
    "Write a detailed tutorial on building a distributed key-value store from scratch, covering consistent hashing, replication strategies, conflict resolution, gossip protocols, failure detection, and read/write quorums.",
    "Explain the history and evolution of database systems from hierarchical and network models through relational, object-oriented, NoSQL, and NewSQL, with detailed comparisons of their data models, query languages, and consistency guarantees.",
    "Describe the complete process of how a modern compiler works, from lexical analysis through parsing, semantic analysis, intermediate representation, optimization passes, and code generation, with examples at each stage.",
    "Write a comprehensive overview of cryptographic systems, covering symmetric and asymmetric encryption, hash functions, digital signatures, key exchange protocols, TLS, certificate authorities, and post-quantum cryptography approaches.",
    "Explain the evolution of CPU architecture from simple pipelines through superscalar, out-of-order execution, branch prediction, speculative execution, SIMD, multi-core designs, and modern heterogeneous computing with detailed examples.",
    "Describe the complete landscape of machine learning algorithms, from linear regression through decision trees, SVMs, neural networks, CNNs, RNNs, transformers, and reinforcement learning, with mathematical intuition for each.",
    "Write a detailed guide to container orchestration with Kubernetes, covering pod lifecycle, services, deployments, stateful sets, config maps, secrets, RBAC, network policies, custom resources, and operator patterns.",
    "Explain the mathematical foundations of information theory, covering entropy, mutual information, channel capacity, error-correcting codes, data compression algorithms, and their practical applications in modern communications.",
    "Describe the architecture and internals of the Linux kernel, covering process scheduling, memory management, file systems, device drivers, networking stack, system calls, and kernel modules in comprehensive detail.",
    "Write a thorough analysis of consensus algorithms in distributed systems, covering Paxos, Raft, Byzantine fault tolerance, PBFT, blockchain consensus mechanisms, and the CAP theorem with detailed examples.",
    "Explain the complete field of computer networking, from the physical layer through data link, network, transport, and application layers, covering Ethernet, IP, TCP, UDP, HTTP, DNS, BGP, and MPLS in detail.",
    "Describe the principles and practices of software testing at every level — unit, integration, system, acceptance, performance, security, and chaos testing — with strategies, frameworks, and real-world examples.",
    "Write a comprehensive guide to functional programming concepts including pure functions, immutability, higher-order functions, monads, functors, type classes, algebraic data types, and category theory foundations.",
    "Explain the complete history of programming languages from assembly through Fortran, Lisp, C, Smalltalk, ML, Haskell, Java, Python, Rust, and beyond, covering their design philosophies and lasting contributions.",
]


def get_compaction_count(profile: str, timeout: int,
                         session_id: str) -> int:
    """Send /status and parse compaction count from output."""
    raw = send_prompt(profile, "/status", timeout, session_id)
    text = parse_response(raw)
    match = re.search(r"Compactions:\s*(\d+)", text)
    return int(match.group(1)) if match else 0


def trigger_auto_compaction(profile: str, timeout: int,
                            session_id: str,
                            max_rounds: int = 50) -> int:
    """Send filler chat until auto-compaction fires. Returns rounds sent."""
    initial_count = get_compaction_count(profile, timeout, session_id)

    for i in range(max_rounds):
        msg = FILLER_MESSAGES[i % len(FILLER_MESSAGES)]
        send_prompt(profile, msg, timeout, session_id)

        # Check every 3 rounds to avoid excessive /status calls
        if (i + 1) % 3 == 0:
            current = get_compaction_count(profile, timeout, session_id)
            if current > initial_count:
                print(f"    [{profile}] Auto-compaction detected after "
                      f"{i + 1} filler messages")
                return i + 1

    print(f"    [{profile}] WARNING: max rounds ({max_rounds}) reached "
          f"without compaction")
    return max_rounds


# ---------------------------------------------------------------------------
# Question selection
# ---------------------------------------------------------------------------

CATEGORIES = ["exact", "paraphrase", "temporal", "multi-hop", "negative"]


def select_questions(questions: list[dict], max_total: int,
                     categories: list[str] | None = None) -> list[dict]:
    """Select a balanced subset of questions across categories."""
    cats = categories if categories is not None else CATEGORIES
    if max_total <= 0 or max_total >= len(questions):
        return questions

    per_cat = max(1, max_total // len(cats))
    selected: list[dict] = []
    for cat in cats:
        cat_qs = [q for q in questions if q["category"] == cat]
        selected.extend(cat_qs[:per_cat])

    # Fill remaining slots
    remaining = max_total - len(selected)
    if remaining > 0:
        used_ids = {q["id"] for q in selected}
        extras = [q for q in questions if q["id"] not in used_ids]
        selected.extend(extras[:remaining])

    return selected


# ---------------------------------------------------------------------------
# Prompt construction
# ---------------------------------------------------------------------------

PROMPT_TEMPLATE = (
    "Search your available workspace files, notes, and memory to answer "
    "this question. Be concise and give only the factual answer. "
    'If the information is not available, respond with "No information '
    'available."\n\n'
    "Question: {question}"
)


# ---------------------------------------------------------------------------
# Stats computation
# ---------------------------------------------------------------------------

def compute_stats(results: list[dict], profile_key: str,
                  categories: list[str] | None = None) -> dict:
    """Compute per-category and overall stats for one profile."""
    cats = categories if categories is not None else CATEGORIES
    by_cat: dict[str, list[float]] = {c: [] for c in cats}
    for r in results:
        cat = r["category"]
        if cat in by_cat:
            by_cat[cat].append(r[f"score_{profile_key}"])

    def avg(scores: list[float]) -> float:
        return sum(scores) / len(scores) if scores else 0.0

    all_scores = [r[f"score_{profile_key}"] for r in results]
    return {
        "byCategory": {c: round(avg(by_cat[c]), 4) for c in cats},
        "byCategoryCount": {c: len(by_cat[c]) for c in cats},
        "overall": round(avg(all_scores), 4),
        "total": len(results),
    }


# ---------------------------------------------------------------------------
# Output writers
# ---------------------------------------------------------------------------

def write_results_json(results: list[dict], meta: dict,
                       stats_a: dict, stats_b: dict,
                       results_dir: str) -> str:
    output = {
        "scenario": "daily-memory-e2e",
        "description": "E2E benchmark: file-based workspace (A) vs mem9 smart-ingest (B)",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "meta": meta,
        "stats_a": stats_a,
        "stats_b": stats_b,
        "results": results,
    }
    path = os.path.join(results_dir, "benchmark-results.json")
    with open(path, "w") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    return path


def write_transcript(results: list[dict], results_dir: str) -> str:
    lines = [
        "# Daily-Memory E2E Benchmark Transcript",
        "",
        f"**Date:** {datetime.now(timezone.utc).isoformat()}",
        "",
        "---",
        "",
    ]

    for i, r in enumerate(results, 1):
        score_a = r["score_a"]
        score_b = r["score_b"]
        lines.append(f"## Q{i} [{r['category']}] (id: {r['question_id']})")
        lines.append("")
        lines.append(f"**Question:** {r['question']}")
        lines.append(f"**Expected:** {r['gold_answer']}")
        lines.append("")
        lines.append(f"### Profile A (Baseline) — score: {score_a:.2f}")
        lines.append("")
        lines.append(f"*Elapsed: {r['elapsed_a']}s*")
        lines.append("")
        lines.append(r.get("response_a", "(no response)"))
        lines.append("")
        lines.append(f"### Profile B (mem9) — score: {score_b:.2f}")
        lines.append("")
        lines.append(f"*Elapsed: {r['elapsed_b']}s*")
        lines.append("")
        lines.append(r.get("response_b", "(no response)"))
        lines.append("")
        lines.append("---")
        lines.append("")

    path = os.path.join(results_dir, "transcript.md")
    with open(path, "w") as f:
        f.write("\n".join(lines))
    return path


def write_html_report(results: list[dict], stats_a: dict, stats_b: dict,
                      results_dir: str,
                      categories: list[str] | None = None) -> str:
    """Generate a self-contained HTML report comparing A vs B scores."""
    cats = categories if categories is not None else CATEGORIES
    timestamp = datetime.now(timezone.utc).isoformat()

    # Build category rows
    cat_rows = []
    for cat in cats:
        sa = stats_a["byCategory"].get(cat, 0)
        sb = stats_b["byCategory"].get(cat, 0)
        n = stats_a["byCategoryCount"].get(cat, 0)
        diff = sb - sa
        diff_cls = "pos" if diff > 0 else ("neg" if diff < 0 else "")
        cat_rows.append(
            f'<tr><td>{html_lib.escape(cat)}</td>'
            f'<td>{sa * 100:.1f}%</td>'
            f'<td>{sb * 100:.1f}%</td>'
            f'<td class="{diff_cls}">{diff * 100:+.1f}%</td>'
            f'<td>{n}</td></tr>'
        )
    cat_table = "\n".join(cat_rows)

    # Build question rows
    q_rows = []
    for i, r in enumerate(results, 1):
        sa = r["score_a"]
        sb = r["score_b"]
        winner = "b-wins" if sb > sa else ("a-wins" if sa > sb else "tie")
        resp_a_esc = html_lib.escape(r.get("response_a", "")[:300])
        resp_b_esc = html_lib.escape(r.get("response_b", "")[:300])
        gold_esc = html_lib.escape(r["gold_answer"])
        q_esc = html_lib.escape(r["question"])
        q_rows.append(f"""
    <tr class="{winner}">
      <td>{i}</td>
      <td><span class="cat-badge">{html_lib.escape(r['category'])}</span></td>
      <td class="q-col">
        <details><summary>{q_esc[:80]}{'...' if len(q_esc) > 80 else ''}</summary>
        <p><strong>Full:</strong> {q_esc}</p>
        <p><strong>Expected:</strong> {gold_esc}</p>
        <p><strong>A:</strong> {resp_a_esc}</p>
        <p><strong>B:</strong> {resp_b_esc}</p>
        </details>
      </td>
      <td>{sa * 100:.0f}%</td>
      <td>{sb * 100:.0f}%</td>
      <td>{r['elapsed_a']}s</td>
      <td>{r['elapsed_b']}s</td>
    </tr>""")
    q_table = "\n".join(q_rows)

    overall_a = stats_a["overall"] * 100
    overall_b = stats_b["overall"] * 100
    diff_overall = overall_b - overall_a

    report_html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Daily-Memory E2E Benchmark Report</title>
<style>
  *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{
    background: #0a0a0a; color: #e0e0e0;
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    line-height: 1.6; padding: 2rem 1rem;
  }}
  .container {{ max-width: 1100px; margin: 0 auto; }}
  h1 {{ font-size: 1.4rem; color: #fff; margin-bottom: 0.3rem; }}
  .meta {{ color: #888; font-size: 0.8rem; margin-bottom: 1.5rem; }}

  .summary {{ display: flex; gap: 1rem; flex-wrap: wrap; margin-bottom: 2rem; }}
  .card {{
    background: #111; border: 1px solid #222; border-radius: 8px;
    padding: 1rem 1.2rem; min-width: 160px;
  }}
  .card .label {{ font-size: 0.7rem; color: #888; text-transform: uppercase; letter-spacing: 0.05em; }}
  .card .value {{ font-size: 1.4rem; font-weight: 700; }}
  .card .value.pos {{ color: #22c55e; }}
  .card .value.neg {{ color: #ef4444; }}

  table {{
    width: 100%; border-collapse: collapse; font-size: 0.85rem;
    margin-bottom: 2rem;
  }}
  th {{ text-align: left; padding: 0.5rem 0.75rem; border-bottom: 2px solid #333; color: #aaa; font-size: 0.75rem; text-transform: uppercase; }}
  td {{ padding: 0.5rem 0.75rem; border-bottom: 1px solid #1a1a1a; }}
  tr:hover {{ background: #151515; }}
  .pos {{ color: #22c55e; }}
  .neg {{ color: #ef4444; }}
  .cat-badge {{
    background: #1e293b; color: #93c5fd; padding: 0.1rem 0.4rem;
    border-radius: 3px; font-size: 0.75rem;
  }}
  .q-col {{ max-width: 400px; }}
  details summary {{ cursor: pointer; }}
  details p {{ margin: 0.3rem 0; font-size: 0.8rem; color: #aaa; }}
  .b-wins td:first-child {{ border-left: 3px solid #22c55e; }}
  .a-wins td:first-child {{ border-left: 3px solid #ef4444; }}
  .tie td:first-child {{ border-left: 3px solid #888; }}
  h2 {{ font-size: 1.1rem; color: #fff; margin: 1.5rem 0 0.75rem; }}
</style>
</head>
<body>
<div class="container">
  <h1>Daily-Memory E2E Benchmark</h1>
  <div class="meta">File-based workspace (A) vs mem9 smart-ingest (B) &mdash; {html_lib.escape(timestamp)}</div>

  <div class="summary">
    <div class="card">
      <div class="label">Profile A (Files)</div>
      <div class="value">{overall_a:.1f}%</div>
    </div>
    <div class="card">
      <div class="label">Profile B (mem9)</div>
      <div class="value">{overall_b:.1f}%</div>
    </div>
    <div class="card">
      <div class="label">Delta (B−A)</div>
      <div class="value {'pos' if diff_overall > 0 else 'neg' if diff_overall < 0 else ''}">{diff_overall:+.1f}%</div>
    </div>
    <div class="card">
      <div class="label">Questions</div>
      <div class="value">{len(results)}</div>
    </div>
  </div>

  <h2>Per-Category Breakdown</h2>
  <table>
    <thead><tr><th>Category</th><th>A (Files)</th><th>B (mem9)</th><th>Delta</th><th>n</th></tr></thead>
    <tbody>{cat_table}</tbody>
  </table>

  <h2>Per-Question Results</h2>
  <table>
    <thead><tr><th>#</th><th>Category</th><th>Question</th><th>A</th><th>B</th><th>Time A</th><th>Time B</th></tr></thead>
    <tbody>{q_table}</tbody>
  </table>
</div>
</body>
</html>"""

    path = os.path.join(results_dir, "report.html")
    with open(path, "w") as f:
        f.write(report_html)
    return path


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="E2E daily-memory benchmark driver")
    parser.add_argument("--manifest-file", required=True,
                        help="Path to manifest.json from daily-memory generator")
    parser.add_argument("--results-dir", required=True,
                        help="Directory for output files")
    parser.add_argument("--profile-a", required=True,
                        help="Baseline OpenClaw profile name")
    parser.add_argument("--profile-b", required=True,
                        help="Treatment OpenClaw profile name (mem9)")
    parser.add_argument("--timeout", type=int, default=120,
                        help="Per-question timeout in seconds")
    parser.add_argument("--max-questions", type=int, default=0,
                        help="Max questions (0=all, balanced across categories)")
    parser.add_argument("--skip-negative", default="true",
                        help="Skip negative-category questions (default: true)")
    args = parser.parse_args()

    # Load questions from manifest
    with open(args.manifest_file) as f:
        manifest = json.load(f)
    all_questions = manifest.get("questions", [])

    skip_neg = args.skip_negative.lower() in ("true", "1", "yes")
    if skip_neg:
        all_questions = [q for q in all_questions if q["category"] != "negative"]
        active_categories = [c for c in CATEGORIES if c != "negative"]
    else:
        active_categories = list(CATEGORIES)

    questions = select_questions(all_questions, args.max_questions,
                                active_categories)

    print(f"    Daily-Memory E2E Benchmark")
    print(f"    Questions: {len(questions)} / {len(all_questions)} total")
    print(f"    Profile A: {args.profile_a} (file-based)")
    print(f"    Profile B: {args.profile_b} (mem9)")
    print(f"    Timeout:   {args.timeout}s per question")
    if skip_neg:
        print(f"    Negative:  skipped")
    print()

    os.makedirs(args.results_dir, exist_ok=True)
    results: list[dict] = []

    # Trigger auto-compaction by filling context with unrelated chat
    # print("    Triggering auto-compaction via filler chat...")
    # with ThreadPoolExecutor(max_workers=2) as executor:
    #     fut_a = executor.submit(trigger_auto_compaction, args.profile_a,
    #                             args.timeout, "dm-e2e-compact-a")
    #     fut_b = executor.submit(trigger_auto_compaction, args.profile_b,
    #                             args.timeout, "dm-e2e-compact-b")
    #     rounds_a = fut_a.result()
    #     rounds_b = fut_b.result()
    # print(f"    Compaction done: A={rounds_a} rounds, B={rounds_b} rounds")

    for i, q in enumerate(questions):
        qid = q["id"]
        category = q["category"]
        question_text = q["question"]
        gold_answer = q["answer"]
        aliases = q.get("aliases", [])

        prompt = PROMPT_TEMPLATE.format(question=question_text)

        # Fresh session per question
        session_a = f"dm-e2e-{qid}-a"
        session_b = f"dm-e2e-{qid}-b"

        print(f"  [{i + 1}/{len(questions)}] [{category}] "
              f"{question_text[:70]}{'...' if len(question_text) > 70 else ''}")

        # Send to both profiles in parallel
        with ThreadPoolExecutor(max_workers=2) as executor:
            fut_a = executor.submit(
                send_prompt, args.profile_a, prompt, args.timeout, session_a)
            fut_b = executor.submit(
                send_prompt, args.profile_b, prompt, args.timeout, session_b)
            raw_a = fut_a.result()
            raw_b = fut_b.result()

        resp_a = parse_response(raw_a)
        resp_b = parse_response(raw_b)

        sc_a = score_answer(resp_a, gold_answer, aliases, category)
        sc_b = score_answer(resp_b, gold_answer, aliases, category)

        result = {
            "question_id": qid,
            "category": category,
            "question": question_text,
            "gold_answer": gold_answer,
            "aliases": aliases,
            "response_a": resp_a,
            "response_b": resp_b,
            "score_a": round(sc_a, 4),
            "score_b": round(sc_b, 4),
            "elapsed_a": raw_a["elapsed_seconds"],
            "elapsed_b": raw_b["elapsed_seconds"],
            "error_a": raw_a.get("error"),
            "error_b": raw_b.get("error"),
        }
        results.append(result)

        print(f"    A: {sc_a:.2f} ({raw_a['elapsed_seconds']}s)  "
              f"B: {sc_b:.2f} ({raw_b['elapsed_seconds']}s)")

    # Compute stats
    stats_a = compute_stats(results, "a", active_categories)
    stats_b = compute_stats(results, "b", active_categories)

    # Print summary
    print()
    print("  ── Results ──────────────────────────────────")
    print(f"  Overall   A: {stats_a['overall'] * 100:.1f}%   "
          f"B: {stats_b['overall'] * 100:.1f}%   "
          f"delta: {(stats_b['overall'] - stats_a['overall']) * 100:+.1f}%")
    for cat in active_categories:
        sa = stats_a["byCategory"].get(cat, 0)
        sb = stats_b["byCategory"].get(cat, 0)
        n = stats_a["byCategoryCount"].get(cat, 0)
        if n > 0:
            print(f"  {cat:<16s} A: {sa * 100:.1f}%  B: {sb * 100:.1f}%  "
                  f"(n={n})")
    print("  ──────────────────────────────────────────────")

    # Write outputs
    meta = {
        "seed": manifest.get("seed"),
        "numDays": manifest.get("numDays"),
        "numTopics": manifest.get("numTopics"),
        "wordsPerDay": manifest.get("wordsPerDay"),
        "maxQuestions": args.max_questions,
        "timeout": args.timeout,
        "profileA": args.profile_a,
        "profileB": args.profile_b,
    }

    json_path = write_results_json(results, meta, stats_a, stats_b,
                                   args.results_dir)
    transcript_path = write_transcript(results, args.results_dir)
    html_path = write_html_report(results, stats_a, stats_b, args.results_dir,
                                  active_categories)

    print()
    print(f"    JSON:       {json_path}")
    print(f"    Transcript: {transcript_path}")
    print(f"    HTML:       {html_path}")
    print()
    print(f"    Done. {len(results)} questions evaluated.")


if __name__ == "__main__":
    main()
