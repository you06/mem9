# Benchmark Pipeline

## Overview

Run the top-level A/B benchmark with:

```bash
bash benchmark/scripts/benchmark.sh
```

The harness compares an agent **without** mem9 memory (Profile A / baseline) vs. **with** mem9 memory (Profile B / treatment). Both profiles receive the same prompts within a persistent session, and results are compared side-by-side in an HTML report.

By default the script provisions a fresh mem9 space on the hosted mem9 service at `https://api.mem9.ai` for every run. You can override the backend with `MEM9_BASE_URL`.

## Prerequisites

**Required CLI tools:** `jq`, `curl`, `openclaw`, `python3`

**Python packages:** `pyyaml`

**Required environment variables:**

- `CLAUDE_CODE_TOKEN` — Anthropic API key. The script exits immediately if unset.
- `BENCH_PROMPT_FILE` — Path to the prompt YAML file. The script exits immediately if unset.

## Optional environment variables

| Variable | Default | Description |
|---|---|---|
| `MEM9_BASE_URL` | `https://api.mem9.ai` | Base URL for the mem9 API |
| `BENCH_PROMPT_TIMEOUT` | `600` | Per-prompt timeout in seconds |

## Pipeline phases

The benchmark runs through seven sequential phases:

### Phase 1 — Cleanup

Stops any leftover gateways from previous runs and removes old temporary profile/workspace directories (`~/.openclaw-<profile>`, `~/.openclaw/workspace-<profile>`).

### Phase 2 — Configure mem9 space

1. Normalizes `MEM9_BASE_URL`.
2. Provisions a fresh mem9 space via `POST /v1alpha1/mem9s`.

### Phase 3 — Create profiles

Sets up two OpenClaw profiles:

- **Profile A (baseline)** — vanilla agent, no plugins.
- **Profile B (treatment)** — mem9 plugin installed and configured to point at the mem9 API and space ID.

Both profiles use `anthropic/claude-sonnet-4-6` and are given the same API key.

### Phase 4 — Workspace setup

Copies shared context files (`SOUL.md`, `IDENTITY.md`, `USER.md`) from `benchmark/workspace/` into both profile workspaces so the agents start with identical context.

### Phase 5 — Start gateways

Launches both OpenClaw gateways and waits for their `/health` endpoints to return successfully (up to 60 s each).

### Phase 6 — Run benchmark

1. **`drive-session.py`** — reads the prompt YAML file and sends each prompt to both profiles in parallel. All prompts within a profile share the same session ID, preserving conversation context across turns. Outputs structured JSON and a markdown transcript.
2. **`report.py`** — consumes the JSON results and generates a self-contained HTML report with a side-by-side comparison layout.

### Phase 7 — Summary

Prints result file paths, mem9 connection details, running gateway PIDs, and gateway web UI URLs. Gateways are left running for manual inspection.

## Prompt file format

Prompt files are YAML with the following schema:

```yaml
name: <scenario-name>
description: <description>
prompts:
  - <prompt-1>
  - <prompt-2>
  - <prompt-3>
```

Each entry in `prompts` is a plain-text string sent to both profiles sequentially. All prompts share a single session per profile, so later prompts can reference earlier conversation turns.

## Results output

Each run writes to `benchmark/results/YYYYMMDD-HHMMSS/`:

| File | Description |
|---|---|
| `benchmark-results.json` | Structured JSON with per-turn prompts, responses, timings, and exit codes |
| `transcript.md` | Human-readable markdown showing prompts and responses side-by-side |
| `report.html` | Self-contained HTML report with dark theme, collapsible turns, and summary stats |

## Daily-Memory Retrieval Benchmark

The `benchmark/daily-memory/` harness evaluates mem9 retrieval quality against a synthetic corpus of daily markdown memory files. Unlike the A/B benchmark above, it uses **raw memory writes** and **rule-based scoring** with no LLM dependency.

The harness generates a deterministic corpus (default: 365 days, ~1000 words/day) with 28 recurring topic families, ingests entries into mem9, and evaluates retrieval across five question categories: exact attribute, paraphrase, temporal, multi-hop, and negative.

```bash
cd benchmark/daily-memory
npm install
MEM9_TENANT_ID=your-space-id npm run start
```

See [`benchmark/daily-memory/USAGE.md`](../benchmark/daily-memory/USAGE.md) for full setup and CLI options.

## Daily-Memory E2E Benchmark

The `benchmark/scripts/daily-memory-e2e.sh` harness extends the daily-memory benchmark into a full **end-to-end** comparison using live OpenClaw agent calls. It compares two memory approaches:

- **Profile A (baseline):** Daily markdown files placed in the agent's workspace. The agent uses native file reading and search to answer questions.
- **Profile B (treatment):** The same corpus ingested into mem9 via the **smart-ingest pipeline** (messages-based, `mode: "smart"`). The agent uses the mem9 plugin for recall.

Both profiles receive identical questions and are scored with the same 5-category rule-based evaluation (exact, paraphrase, temporal, multi-hop, negative). Each question runs in a **fresh session** to prevent answer contamination.

### Running

```bash
export CLAUDE_CODE_TOKEN=...
bash benchmark/scripts/daily-memory-e2e.sh
```

### Configuration

| Variable | Default | Description |
|---|---|---|
| `MEM9_BASE_URL` | `https://api.mem9.ai` | mem9 API endpoint |
| `BENCH_DM_SEED` | `42` | Corpus generation seed |
| `BENCH_DM_DAYS` | `30` | Number of daily entries to generate |
| `BENCH_DM_TOPICS` | `10` | Number of topic families |
| `BENCH_DM_WORDS` | `200` | Words per daily entry |
| `BENCH_DM_MAX_QUESTIONS` | `20` | Max questions (balanced across categories) |
| `BENCH_DM_TIMEOUT` | `120` | Per-question timeout in seconds |
| `BENCH_DM_CORPUS_DIR` | (auto) | Reuse an existing generated corpus directory |

### Pipeline

1. **Generate corpus** — reuses the `daily-memory` generator via tsx
2. **Provision mem9 space** — fresh space per run
3. **Create profiles** — Profile A (vanilla) and Profile B (mem9 plugin)
4. **Workspace setup** — daily files copied to A's workspace only; shared context to both
5. **Smart-ingest** — corpus ingested into mem9 for Profile B via `POST /memories` with `mode: "smart"`
6. **Start gateways** — both OpenClaw gateways launched and health-checked
7. **Drive questions** — each question sent to both profiles in parallel with fresh sessions
8. **Score and report** — rule-based scoring, JSON results, markdown transcript, HTML report

### Output

Each run writes to `benchmark/results/dm-e2e-YYYYMMDD-HHMMSS/`:

| File | Description |
|---|---|
| `benchmark-results.json` | Per-question scores for both profiles, aggregate stats |
| `transcript.md` | Human-readable side-by-side comparison |
| `report.html` | Self-contained HTML report with category breakdown and per-question detail |
