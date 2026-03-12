# Daily-Memory Benchmark Usage

This guide shows how to run the `mem9` daily-memory benchmark end-to-end.

## Prerequisites

You need all of the following:

- Node.js 20+ (Node 22+ recommended because the harness uses built-in `fetch`)
- a mem9 **space ID** (the `tenantID` in the API path)
- access to the hosted mem9 API (default) or another mem9-compatible endpoint

No LLM API key is required — answer extraction and scoring are rule-based.

## 1. Install benchmark dependencies

From this directory:

```bash
cd benchmark/daily-memory
npm install
```

## 2. Configure environment variables

Minimal configuration:

```bash
# Optional: defaults to the hosted mem9 API.
export MEM9_BASE_URL=https://api.mem9.ai
export MEM9_TENANT_ID=your-space-id
```

Optional:

```bash
export MEM9_AGENT_ID=daily-memory-bench
export MEM9_RETRIEVAL_LIMIT=10
export MEM9_CLEAR_SESSION_FIRST=0
```

### What these variables mean

- `MEM9_BASE_URL` — base URL of the mem9 API (defaults to `https://api.mem9.ai`)
- `MEM9_TENANT_ID` — mem9 **space ID**
- `MEM9_AGENT_ID` — agent name sent through the `X-Mnemo-Agent-Id` header and stored on writes
- `MEM9_RETRIEVAL_LIMIT` — number of memories pulled per question (default: `10`)
- `MEM9_CLEAR_SESSION_FIRST=1` — delete prior benchmark memories before re-ingesting

## 3. Run the benchmark

### Full run (generate + ingest + evaluate)

```bash
npm run start -- \
  --out-file ./results/daily-memory.json
```

### Customize corpus size

```bash
npm run start -- \
  --num-days 100 \
  --num-topics 16 \
  --words-per-day 500 \
  --seed 123
```

### Skip generation (reuse existing corpus)

```bash
npm run start -- \
  --skip-generate \
  --out-file ./results/rerun.json
```

### Skip ingestion (reuse already-ingested memories)

```bash
npm run start -- \
  --skip-generate \
  --skip-ingest \
  --out-file ./results/eval-only.json
```

### Quick smoke test

```bash
npm run start -- \
  --num-days 7 \
  --num-topics 4 \
  --words-per-day 200 \
  --out-file ./results/smoke.json
```

## 4. What the harness does

1. Generates a deterministic corpus of daily markdown files (one per day)
2. Writes each daily entry as one raw memory via `POST /v1alpha1/mem9s/{tenantID}/memories`
3. For each benchmark question, queries matching memories via `GET /v1alpha1/mem9s/{tenantID}/memories?q=...&session_id=...`
4. Extracts answers deterministically from retrieved context
5. Scores answers with category-specific rubrics
6. Writes a JSON report with per-question results and aggregate statistics

## CLI flags

| Flag | Default | Description |
|---|---|---|
| `--seed` | `42` | PRNG seed for deterministic generation |
| `--num-days` | `365` | Number of daily entries to generate |
| `--num-topics` | `28` | Number of topic families |
| `--words-per-day` | `1000` | Target word count per daily entry |
| `--start-date` | `2024-01-01` | Start date for the corpus |
| `--out-dir, -d` | `./data` | Directory for generated corpus and manifest |
| `--out-file, -o` | `./results/<timestamp>.json` | Output results JSON path |
| `--skip-generate` | `false` | Skip corpus generation, load existing manifest |
| `--skip-ingest` | `false` | Skip mem9 writes, only run retrieval + eval |
| `--concurrency, -c` | `4` | Concurrent retrieval workers |

## Output

Each run writes:

| Path | Description |
|---|---|
| `data/corpus/day-NNN_YYYY-MM-DD.md` | Individual daily markdown files |
| `data/manifest.json` | Full dataset manifest (entries + questions + slots) |
| `data/questions.json` | Questions array for inspection |
| `results/<timestamp>.json` | Benchmark results with scores and statistics |

## Question categories

| Category | Scoring | Description |
|---|---|---|
| `exact` | Exact match / token F1 | Direct attribute lookup from a specific day |
| `paraphrase` | Exact match / token F1 | Same fact, rephrased question wording |
| `temporal` | Date/alias match | First/last occurrence of a topic family |
| `multi-hop` | Fraction of sub-answers found | Requires combining facts from two days |
| `negative` | Binary (contains negation phrase) | Topics that never appear in the corpus |

## Notes and caveats

- The benchmark uses **raw memory writes**, not the smart `messages` ingest pipeline. This is intentional.
- Retrieval quality depends on your server-side embedding / search configuration.
- `mem9` accepts raw memory writes asynchronously (`202 Accepted`), so the harness polls until the expected memories become searchable before evaluation continues.
- The same `--seed` always produces identical corpus and questions, enabling reproducible experiments.
- If you rerun without cleanup, retrieval may see duplicate memories. Set `MEM9_CLEAR_SESSION_FIRST=1` for fresh state.
