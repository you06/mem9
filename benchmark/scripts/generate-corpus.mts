/**
 * generate-corpus.mts — Standalone corpus generator for the daily-memory E2E benchmark.
 *
 * Wraps the daily-memory generator so it can be invoked directly from the
 * shell script without needing the full CLI (which also runs ingest + eval).
 *
 * Usage:
 *   npx tsx benchmark/scripts/generate-corpus.mts \
 *     --seed 42 --days 30 --topics 10 --words 200 \
 *     --start-date 2024-01-01 --out-dir ./data
 */

import { generate } from "../daily-memory/src/generator.js";
import { parseArgs } from "node:util";

const { values } = parseArgs({
  options: {
    "seed": { type: "string", default: "42" },
    "days": { type: "string", default: "30" },
    "topics": { type: "string", default: "10" },
    "words": { type: "string", default: "200" },
    "start-date": { type: "string", default: "2024-01-01" },
    "out-dir": { type: "string" },
  },
});

const outDir = values["out-dir"];
if (!outDir) {
  console.error("ERROR: --out-dir is required");
  process.exit(1);
}

const manifest = await generate({
  seed: Number.parseInt(values.seed ?? "42", 10),
  numDays: Number.parseInt(values.days ?? "30", 10),
  numTopics: Number.parseInt(values.topics ?? "10", 10),
  wordsPerDay: Number.parseInt(values.words ?? "200", 10),
  startDate: values["start-date"] ?? "2024-01-01",
  outDir,
});

console.log(
  `    Generated ${manifest.entries.length} days, ` +
  `${manifest.questions.length} questions`
);
