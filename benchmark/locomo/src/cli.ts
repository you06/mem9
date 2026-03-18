import type { BenchmarkOutput, LoCoMoSample, QAResult } from './types.js'

import { access, mkdir, readFile, writeFile } from 'node:fs/promises'
import { dirname, resolve } from 'node:path'
import { env, exit } from 'node:process'
import { fileURLToPath } from 'node:url'
import { parseArgs } from 'node:util'

import { llmJudge, generateAnswer } from './llm.js'
import { scoreAnswer } from './evaluation.js'
import { ingestSample } from './ingest.js'
import type { IngestMode } from './ingest.js'
import { getBaseUrl, provisionSpace } from './mem9.js'
import { getContext, computeEvidenceRecall } from './retrieve.js'
import { computeStats, printStats } from './stats.js'

const __dirname = dirname(fileURLToPath(import.meta.url))

interface Args {
  concurrency: number
  dataFile: string
  ingestMode: IngestMode
  judgeModel: string
  outFile: string
  sampleConcurrency: number
  sampleIds: null | string[]
  skipIngest: boolean
  useLlmJudge: boolean
}

const LOCOMO_DATASET_URL = 'https://raw.githubusercontent.com/snap-research/locomo/main/data/locomo10.json'

const parseCliArgs = (): Args => {
  const { values } = parseArgs({
    options: {
      'concurrency': { default: '4', short: 'c', type: 'string' },
      'data-file': { short: 'd', type: 'string' },
      'ingest-mode': { default: 'raw', type: 'string' },
      'judge-model': { type: 'string' },
      'out-file': { short: 'o', type: 'string' },
      'sample-concurrency': { short: 'p', default: '4', type: 'string' },
      'sample-ids': { short: 's', type: 'string' },
      'skip-ingest': { default: false, type: 'boolean' },
      'use-llm-judge': { default: false, type: 'boolean' },
    },
  })

  const concurrency = Number.parseInt(values.concurrency, 10)
  const sampleConcurrency = Number.parseInt(values['sample-concurrency'], 10)
  const sampleIdStr = values['sample-ids'] ?? ''
  const ingestMode = values['ingest-mode'] === 'messages' ? 'messages' : 'raw' as IngestMode

  return {
    concurrency: Number.isFinite(concurrency) && concurrency > 0 ? concurrency : 4,
    dataFile: values['data-file'] ?? resolve(__dirname, '../data/locomo10.json'),
    ingestMode,
    judgeModel: values['judge-model'] ?? '',
    outFile: values['out-file'] ?? resolve(__dirname, `../results/${new Date().toISOString().replace(/[:.]/g, '-')}.json`),
    sampleConcurrency: Number.isFinite(sampleConcurrency) && sampleConcurrency > 0 ? sampleConcurrency : 4,
    sampleIds: sampleIdStr.length > 0 ? sampleIdStr.split(',').map(s => s.trim()) : null,
    skipIngest: values['skip-ingest'],
    useLlmJudge: values['use-llm-judge'],
  }
}

const runWithConcurrency = async <T>(tasks: Array<() => Promise<T>>, concurrency: number): Promise<T[]> => {
  if (tasks.length === 0) return []
  const limit = Math.max(1, Math.floor(concurrency))
  const results: T[] = Array.from({ length: tasks.length }) as T[]
  let nextIndex = 0
  const worker = async (): Promise<void> => {
    while (true) {
      const i = nextIndex
      nextIndex += 1
      if (i >= tasks.length) return
      results[i] = await tasks[i]()
    }
  }
  await Promise.all(Array.from({ length: Math.min(limit, tasks.length) }, async () => await worker()))
  return results
}

const ensureDatasetExists = async (dataFile: string): Promise<void> => {
  try {
    await access(dataFile)
    return
  } catch {
    // Download the official LoCoMo dataset when the file is missing.
  }

  console.log(`Dataset not found at ${dataFile}`)
  console.log(`Downloading official LoCoMo dataset from ${LOCOMO_DATASET_URL} ...`)
  await mkdir(dirname(dataFile), { recursive: true })

  const response = await fetch(LOCOMO_DATASET_URL)
  if (!response.ok) {
    throw new Error(`Failed to download LoCoMo dataset: ${response.status} ${response.statusText}`)
  }

  const text = await response.text()
  await writeFile(dataFile, text, 'utf-8')
  console.log(`Saved dataset to ${dataFile}`)
}

interface SampleResult {
  sampleId: string
  tenantId: string
  results: QAResult[]
}

const runSample = async (sample: LoCoMoSample, tenantId: string, args: Args, model: string): Promise<SampleResult> => {
  const sampleId = sample.sample_id
  const sessionId = sampleId

  if (!args.skipIngest) {
    await ingestSample(tenantId, sample, args.ingestMode)
  }

  console.log(`[${sampleId}] Evaluating ${sample.qa.length} questions`)

  const retrievals: Array<{ context: string, diaIds: string[] }> = Array.from({ length: sample.qa.length }, () => ({ context: '', diaIds: [] }))
  const contextTasks = sample.qa.map((qa, index) => async () => { retrievals[index] = await getContext(tenantId, sessionId, qa.question) })
  await runWithConcurrency(contextTasks, args.concurrency)
  console.log(`[${sampleId}] Prefetched ${sample.qa.length} contexts`)

  const judgeModel = args.judgeModel.length > 0 ? args.judgeModel : (env.OPENAI_JUDGE_MODEL ?? '')

  const results: QAResult[] = []
  const tasks = sample.qa.map((qa, index) => async (): Promise<QAResult | null> => {
    const retrieval = retrievals[index] ?? { context: '', diaIds: [] }
    try {
      const prediction = await generateAnswer(retrieval.context, qa.question, qa.category, model)
      const score = scoreAnswer(prediction, qa.answer, qa.category)
      const effectiveJudgeModel = judgeModel.length > 0 ? judgeModel : model
      let llmScore: number | null = null
      if (args.useLlmJudge && qa.category !== 5) {
        try {
          llmScore = await llmJudge(prediction, qa.answer, qa.question, effectiveJudgeModel)
        } catch (err) {
          console.error(`[${sampleId}] [${index + 1}/${sample.qa.length}] LLM judge failed: ${err instanceof Error ? err.message : String(err)}`)
        }
      }
      const evidenceRecall = computeEvidenceRecall(retrieval.diaIds, qa.evidence)
      console.log(`[${sampleId}] [${index + 1}/${sample.qa.length}] f1=${score.toFixed(2)}${evidenceRecall != null ? ` er=${evidenceRecall.toFixed(2)}` : ''}`)
      return {
        category: qa.category,
        context_retrieved: retrieval.context,
        evidence: qa.evidence,
        evidence_recall: evidenceRecall,
        gold_answer: String(qa.answer),
        llm_judge_score: llmScore,
        prediction,
        question: qa.question,
        retrieved_dia_ids: retrieval.diaIds,
        sample_id: sampleId,
        score,
      }
    } catch (err) {
      console.error(`[${sampleId}] [${index + 1}/${sample.qa.length}] Failed: ${err instanceof Error ? err.message : String(err)}`)
      return null
    }
  })

  const qaResults = await runWithConcurrency(tasks, args.concurrency)
  for (const r of qaResults) { if (r != null) results.push(r) }

  console.log(`[${sampleId}] Done`)
  return { sampleId, tenantId, results }
}

const main = async () => {
  const model = env.OPENAI_CHAT_MODEL ?? 'gpt-4o-mini'
  const args = parseCliArgs()

  await ensureDatasetExists(args.dataFile)

  if ((env.OPENAI_API_KEY ?? '').length === 0) {
    console.error('Error: OPENAI_API_KEY not set.')
    exit(1)
  }

  const judgeModel = args.judgeModel.length > 0 ? args.judgeModel : (env.OPENAI_JUDGE_MODEL ?? '')

  console.log('LoCoMo Benchmark for mem9')
  console.log(`  data:       ${args.dataFile}`)
  console.log(`  out:        ${args.outFile}`)
  console.log(`  model:      ${model}`)
  console.log(`  judgeModel: ${judgeModel.length > 0 ? judgeModel : `(same as model: ${model})`}`)
  console.log(`  ingestMode: ${args.ingestMode}`)
  console.log(`  baseUrl:    ${getBaseUrl()}`)
  console.log(`  sampleConcurrency: ${args.sampleConcurrency}`)
  console.log(`  concurrency: ${args.concurrency}`)
  console.log(`  llmJudge: ${args.useLlmJudge ? 'on' : 'off'}`)
  console.log()

  const raw = await readFile(args.dataFile, 'utf-8')
  const allSamples = JSON.parse(raw) as LoCoMoSample[]
  const samples = args.sampleIds != null ? allSamples.filter(s => args.sampleIds!.includes(s.sample_id)) : allSamples
  console.log(`Loaded ${samples.length} sample(s).`)

  // In --skip-ingest mode, require MEM9_TENANT_ID for backward compat (single tenant for all samples)
  const globalTenantId = (env.MEM9_TENANT_ID ?? '').length > 0 ? env.MEM9_TENANT_ID! : ''
  if (args.skipIngest && globalTenantId.length === 0) {
    console.error('Error: --skip-ingest requires MEM9_TENANT_ID to be set.')
    exit(1)
  }

  const tenantIds: Record<string, string> = {}

  const sampleTasks = samples.map(sample => async () => {
    let tenantId: string
    if (globalTenantId.length > 0) {
      tenantId = globalTenantId
    } else {
      console.log(`[${sample.sample_id}] Provisioning fresh mem9 space...`)
      tenantId = await provisionSpace()
      console.log(`[${sample.sample_id}] Provisioned space: ${tenantId}`)
    }
    tenantIds[sample.sample_id] = tenantId
    return await runSample(sample, tenantId, args, model)
  })

  console.log(`\nRunning ${samples.length} sample(s) with sample-concurrency=${args.sampleConcurrency}`)
  const sampleResults = await runWithConcurrency(sampleTasks, args.sampleConcurrency)

  const results: QAResult[] = []
  for (const sr of sampleResults) results.push(...sr.results)

  const stats = computeStats(results)
  printStats(stats)
  const output: BenchmarkOutput = {
    meta: {
      base_url: getBaseUrl(),
      data_file: args.dataFile,
      model,
      tenant_ids: tenantIds,
      timestamp: new Date().toISOString(),
    },
    results,
    stats,
  }
  await mkdir(dirname(args.outFile), { recursive: true })
  await writeFile(args.outFile, JSON.stringify(output, null, 2))
  console.log(`Results written to: ${args.outFile}`)
}

main().catch((err) => {
  console.error(err)
  exit(1)
})
