import type { BenchmarkStats, EvalResult, QueryCategory } from "./types.js"
import { ALL_CATEGORIES } from "./types.js"

const CATEGORY_LABELS: Record<QueryCategory, string> = {
  "exact": "exact-attribute",
  "paraphrase": "paraphrase",
  "temporal": "temporal",
  "multi-hop": "multi-hop",
  "negative": "negative",
}

const avg = (scores: number[]): number =>
  scores.length > 0 ? scores.reduce((a, b) => a + b, 0) / scores.length : 0

export const computeStats = (results: EvalResult[]): BenchmarkStats => {
  const byCategory = Object.fromEntries(
    ALL_CATEGORIES.map(c => [c, [] as number[]]),
  ) as Record<QueryCategory, number[]>

  for (const r of results) {
    byCategory[r.category].push(r.score)
  }

  return {
    byCategory: Object.fromEntries(
      ALL_CATEGORIES.map(c => [c, avg(byCategory[c])]),
    ) as Record<QueryCategory, number>,
    byCategoryCount: Object.fromEntries(
      ALL_CATEGORIES.map(c => [c, byCategory[c].length]),
    ) as Record<QueryCategory, number>,
    overall: avg(results.map(r => r.score)),
    total: results.length,
  }
}

export const printStats = (stats: BenchmarkStats): void => {
  console.log("\n── Results ──────────────────────────────────")
  console.log(`Overall score:  ${(stats.overall * 100).toFixed(2)}%  (n=${stats.total})`)
  console.log()
  for (const c of ALL_CATEGORIES) {
    const score = stats.byCategory[c]
    const count = stats.byCategoryCount[c]
    if (count > 0) {
      console.log(`  ${CATEGORY_LABELS[c].padEnd(16)}:  ${(score * 100).toFixed(2)}%  (n=${count})`)
    }
  }
  console.log("──────────────────────────────────────────────\n")
}
