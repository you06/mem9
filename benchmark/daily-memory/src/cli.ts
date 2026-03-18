import type { BenchmarkOutput, BenchmarkQuestion, DatasetManifest, EvalResult } from "./types.js"

import { mkdir, readFile, writeFile } from "node:fs/promises"
import { dirname, resolve } from "node:path"
import { exit } from "node:process"
import { fileURLToPath } from "node:url"
import { parseArgs } from "node:util"

import { Spinner } from "picospinner"

import { extractAnswer, scoreAnswer } from "./evaluation.js"
import { generate } from "./generator.js"
import { ingestEntries } from "./ingest.js"
import { getBaseUrl, getRetrievalLimit, getTenantId } from "./mem9.js"
import { getContext } from "./retrieve.js"
import { computeStats, printStats } from "./stats.js"

const __dirname = dirname(fileURLToPath(import.meta.url))

interface Args {
  concurrency: number
  numDays: number
  numTopics: number
  outDir: string
  outFile: string
  seed: number
  skipGenerate: boolean
  skipIngest: boolean
  skipNegative: boolean
  startDate: string
  wordsPerDay: number
}

const parseCliArgs = (): Args => {
  const { values } = parseArgs({
    options: {
      "concurrency": { default: "4", short: "c", type: "string" },
      "num-days": { default: "365", type: "string" },
      "num-topics": { default: "28", type: "string" },
      "out-dir": { short: "d", type: "string" },
      "out-file": { short: "o", type: "string" },
      "seed": { default: "42", type: "string" },
      "skip-generate": { default: false, type: "boolean" },
      "skip-ingest": { default: false, type: "boolean" },
      "skip-negative": { default: true, type: "boolean" },
      "start-date": { default: "2024-01-01", type: "string" },
      "words-per-day": { default: "1000", type: "string" },
    },
  })

  const parseInt = (s: string, fallback: number): number => {
    const n = Number.parseInt(s, 10)
    return Number.isFinite(n) && n > 0 ? n : fallback
  }

  return {
    concurrency: parseInt(values.concurrency, 4),
    numDays: parseInt(values["num-days"], 365),
    numTopics: parseInt(values["num-topics"], 28),
    outDir: values["out-dir"] ?? resolve(__dirname, "../data"),
    outFile: values["out-file"] ?? resolve(__dirname, `../results/${new Date().toISOString().replace(/[:.]/g, "-")}.json`),
    seed: parseInt(values.seed, 42),
    skipGenerate: values["skip-generate"],
    skipIngest: values["skip-ingest"],
    skipNegative: values["skip-negative"],
    startDate: values["start-date"] ?? "2024-01-01",
    wordsPerDay: parseInt(values["words-per-day"], 1000),
  }
}

const runWithConcurrency = async (tasks: Array<() => Promise<void>>, concurrency: number): Promise<void> => {
  if (tasks.length === 0) return
  const limit = Math.max(1, Math.floor(concurrency))
  let nextIndex = 0
  const worker = async (): Promise<void> => {
    while (true) {
      const i = nextIndex
      nextIndex += 1
      if (i >= tasks.length) return
      await tasks[i]()
    }
  }
  await Promise.all(Array.from({ length: Math.min(limit, tasks.length) }, async () => await worker()))
}

const main = async (): Promise<void> => {
  const args = parseCliArgs()

  console.log("Daily-Memory Benchmark for mem9")
  console.log(`  seed:       ${args.seed}`)
  console.log(`  days:       ${args.numDays}`)
  console.log(`  topics:     ${args.numTopics}`)
  console.log(`  words/day:  ${args.wordsPerDay}`)
  console.log(`  startDate:  ${args.startDate}`)
  console.log(`  outDir:     ${args.outDir}`)
  console.log(`  outFile:    ${args.outFile}`)
  console.log(`  baseUrl:    ${getBaseUrl()}`)
  console.log(`  tenant:     ${getTenantId()}`)
  console.log(`  limit:      ${getRetrievalLimit()}`)
  console.log(`  concurrency: ${args.concurrency}`)
  console.log(`  skipNegative: ${args.skipNegative}`)
  console.log()

  // ── Step 1: Generate corpus ──
  let manifest: DatasetManifest

  if (!args.skipGenerate) {
    console.log("── Step 1: Generating corpus ──")
    const genSpinner = new Spinner("Generating daily entries and questions")
    genSpinner.start()

    manifest = await generate({
      seed: args.seed,
      numDays: args.numDays,
      numTopics: args.numTopics,
      wordsPerDay: args.wordsPerDay,
      startDate: args.startDate,
      outDir: args.outDir,
    })

    genSpinner.succeed(`Generated ${manifest.entries.length} days, ${manifest.questions.length} questions`)
  } else {
    console.log("Skipping generation (--skip-generate).")
    const manifestPath = resolve(args.outDir, "manifest.json")
    const raw = await readFile(manifestPath, "utf-8")
    manifest = JSON.parse(raw) as DatasetManifest
    console.log(`Loaded manifest: ${manifest.entries.length} days, ${manifest.questions.length} questions`)
  }

  if (args.skipNegative) {
    const before = manifest.questions.length
    manifest.questions = manifest.questions.filter(q => q.category !== "negative")
    console.log(`Skipped negative questions: ${before} → ${manifest.questions.length}`)
  }

  // ── Step 2: Ingest into mem9 ──
  if (!args.skipIngest) {
    console.log("\n── Step 2: Ingesting into mem9 ──")
    await ingestEntries(manifest.entries)
  } else {
    console.log("\nSkipping ingestion (--skip-ingest).")
  }

  // ── Step 3: Evaluate ──
  console.log("\n── Step 3: Evaluating questions ──")
  const questions: BenchmarkQuestion[] = manifest.questions
  const results: EvalResult[] = []

  // Prefetch contexts
  const prefetchSpinner = new Spinner(`Prefetching ${questions.length} contexts`)
  prefetchSpinner.start()
  const contexts: string[] = Array.from({ length: questions.length }, () => "")
  const contextTasks = questions.map((q, index) => async () => {
    contexts[index] = await getContext(q.question)
  })
  await runWithConcurrency(contextTasks, args.concurrency)
  prefetchSpinner.succeed(`Prefetched ${questions.length} contexts`)

  // Score
  for (let i = 0; i < questions.length; i++) {
    const q = questions[i]
    const context = contexts[i]
    const prediction = extractAnswer(context, q.question, q.answer, q.aliases, q.category)
    const score = scoreAnswer(prediction, q.answer, q.aliases, q.category)

    results.push({
      questionId: q.id,
      category: q.category,
      question: q.question,
      goldAnswer: q.answer,
      prediction,
      contextRetrieved: context,
      score,
    })

    if ((i + 1) % 20 === 0 || i === questions.length - 1) {
      const avgScore = results.reduce((sum, r) => sum + r.score, 0) / results.length
      console.log(`  [${i + 1}/${questions.length}] running avg=${(avgScore * 100).toFixed(2)}%`)
    }
  }

  // ── Step 4: Stats and output ──
  const stats = computeStats(results)
  printStats(stats)

  const output: BenchmarkOutput = {
    meta: {
      baseUrl: getBaseUrl(),
      tenantId: getTenantId(),
      seed: args.seed,
      numDays: args.numDays,
      numTopics: args.numTopics,
      wordsPerDay: args.wordsPerDay,
      retrievalLimit: getRetrievalLimit(),
      timestamp: new Date().toISOString(),
    },
    results,
    stats,
  }

  await mkdir(dirname(args.outFile), { recursive: true })
  await writeFile(args.outFile, JSON.stringify(output, null, 2))
  console.log(`Results written to: ${args.outFile}`)
}

main().catch((err: unknown) => {
  console.error(err)
  exit(1)
})
