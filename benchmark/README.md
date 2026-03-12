# mem9 Benchmark Harnesses

This directory contains benchmark helpers and datasets for comparing OpenClaw's built-in file memory against mem9.

## Running the top-level A/B benchmark

Run the benchmark script directly:

```bash
bash benchmark/scripts/benchmark.sh
```

### Required environment variables

```bash
export CLAUDE_CODE_TOKEN=...
export BENCH_PROMPT_FILE=benchmark/prompts/example.yaml
```

### Optional environment variables

```bash
# Defaults to the hosted mem9 service.
export MEM9_BASE_URL=https://api.mem9.ai

# Optional: per-prompt timeout in seconds.
export BENCH_PROMPT_TIMEOUT=600
```

If `MEM9_BASE_URL` is unset, the harness uses the hosted mem9 API at `https://api.mem9.ai` and provisions a fresh mem9 space for every benchmark run.

## Running the Daily-Memory E2E benchmark

The daily-memory E2E benchmark compares file-based workspace recall (Profile A) against mem9 smart-ingest recall (Profile B) using the synthetic daily-memory corpus and OpenClaw agent calls.

```bash
bash benchmark/scripts/daily-memory-e2e.sh
```

### Required environment variables

```bash
export CLAUDE_CODE_TOKEN=...
```

### Optional environment variables

```bash
export MEM9_BASE_URL=https://api.mem9.ai      # mem9 API endpoint
export BENCH_DM_SEED=42                        # corpus seed
export BENCH_DM_DAYS=30                        # number of daily entries
export BENCH_DM_TOPICS=10                      # topic families
export BENCH_DM_WORDS=200                      # words per day
export BENCH_DM_MAX_QUESTIONS=20               # max questions (balanced)
export BENCH_DM_TIMEOUT=120                    # per-question timeout (s)
export BENCH_DM_CORPUS_DIR=                    # reuse existing corpus
```

Profile A gets the daily markdown files in its workspace. Profile B gets the same corpus ingested via mem9's smart-ingest pipeline (messages-based, `mode: "smart"`). Each question runs in a fresh session to avoid answer contamination.

## Layout

- `scripts/benchmark.sh` — top-level A/B benchmark runner
- `scripts/daily-memory-e2e.sh` — daily-memory E2E benchmark (files vs mem9 smart-ingest)
- `scripts/drive-session.py` — sends the same prompt sequence to baseline and mem9 profiles
- `scripts/drive-daily-memory-e2e.py` — question driver + scorer for the daily-memory E2E benchmark
- `scripts/smart-ingest-corpus.py` — ingests daily corpus into mem9 via smart-ingest API
- `scripts/report.py` — renders the HTML comparison report
- `MR-NIAH/` — dataset bridge for the MR-NIAH benchmark
- `locomo/` — LoCoMo benchmark harness
- `daily-memory/` — synthetic daily-memory retrieval benchmark (raw writes, rule-based scoring)
- `workspace/` — shared workspace context copied into temporary benchmark profiles
- `results/` — benchmark outputs

## Notes

- Profile A uses OpenClaw's native memory files.
- Profile B installs the local `openclaw-plugin`, points it at mem9, and gets a fresh space for each run.
- The benchmark leaves the OpenClaw gateways running after completion for manual inspection.
