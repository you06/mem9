# Daily-Memory Benchmark for mem9

This harness evaluates `mem9` retrieval quality against a synthetic corpus of daily markdown memory files. It uses the hosted mem9 API by default (`https://api.mem9.ai`), but you can point it at another compatible endpoint with `MEM9_BASE_URL`.

The harness works by:

1. generating a deterministic corpus of one markdown file per day with recurring topic families and unique slot-based facts,
2. ingesting each daily entry into a mem9 space through the HTTP API as a raw memory write,
3. retrieving memories per question via `GET /v1alpha1/mem9s/{tenantID}/memories`,
4. extracting answers deterministically from retrieved context (no LLM dependency), and
5. scoring answers with rule-based per-category evaluation.

Unlike the OpenClaw plugin flow, this benchmark intentionally uses **raw memory writes** (`content` + metadata) instead of the smart `messages` ingest pipeline. That keeps the benchmark focused on retrieval quality over the generated daily entries.

## Files

- `src/cli.ts` — benchmark entrypoint
- `src/generator.ts` — deterministic corpus and question generation
- `src/ingest.ts` — writes daily entries into mem9
- `src/retrieve.ts` — queries mem9 search API and builds retrieval context
- `src/evaluation.ts` — rule-based answer extraction and scoring
- `src/stats.ts` — result aggregation and display
- `src/types.ts` — domain types
- `src/mem9.ts` — mem9 HTTP API client
- `data/` — generated corpus, manifest, and questions
- `results/` — benchmark output JSON files
- `USAGE.md` — exact setup and run steps

## Corpus design

The generator builds a configurable number of **topic families** (default: 28) spanning work (standups, code reviews, deployments, incidents), learning (reading, study, conferences), personal (exercise, cooking, health, pets), and more. Each day draws 2–4 topic families and fills their templates with unique slot values from curated pools. Filler sentences pad entries to the target word count.

The generator produces five question categories:

| Category | Description |
|---|---|
| `exact` | Direct attribute lookup: "On 2024-03-15, who reported in standup?" |
| `paraphrase` | Same fact, rephrased question |
| `temporal` | First/last occurrence of a topic |
| `multi-hop` | Compare facts across two different days |
| `negative` | Ask about topics that never appear in the corpus |

## Key design choices

- **Deterministic from seed** — the same `--seed` always produces the same corpus and questions.
- **No LLM dependency** — answer extraction and scoring are purely rule-based.
- **Raw memory writes** — each daily markdown becomes one mem9 memory entry.
- **Session isolation** — all entries share a single `session_id` so the benchmark is self-contained.

For end-to-end commands, see [USAGE.md](./USAGE.md).
