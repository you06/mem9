/** Query categories for the daily-memory benchmark. */
export type QueryCategory = "exact" | "paraphrase" | "temporal" | "multi-hop" | "negative"

export const ALL_CATEGORIES: QueryCategory[] = ["exact", "paraphrase", "temporal", "multi-hop", "negative"]

/** A single topic family used to generate recurring daily entries. */
export interface TopicFamily {
  id: number
  name: string
  /** Template with `{{slot}}` placeholders filled per-day. */
  template: string
  /** Keys that appear in the template and are filled with unique values. */
  slotKeys: string[]
}

/** One day's generated markdown content. */
export interface DailyEntry {
  dayIndex: number
  /** ISO date string (YYYY-MM-DD). */
  date: string
  /** Markdown content written to disk. */
  content: string
  /** Which topic families were used on this day. */
  topicIds: number[]
  /** Slot values keyed by `topicId:slotKey`. */
  slots: Record<string, string>
}

/** A single benchmark question with its ground-truth answer. */
export interface BenchmarkQuestion {
  id: string
  category: QueryCategory
  question: string
  /** The expected answer (or aliases for exact-match). */
  answer: string
  aliases: string[]
  /** Day index(es) the answer can be found in. */
  sourceDays: number[]
}

/** Full generated dataset manifest. */
export interface DatasetManifest {
  seed: number
  numDays: number
  numTopics: number
  wordsPerDay: number
  startDate: string
  entries: DailyEntry[]
  questions: BenchmarkQuestion[]
}

/** Result of evaluating one question. */
export interface EvalResult {
  questionId: string
  category: QueryCategory
  question: string
  goldAnswer: string
  prediction: string
  contextRetrieved: string
  score: number
}

/** Aggregated benchmark statistics. */
export interface BenchmarkStats {
  byCategory: Record<QueryCategory, number>
  byCategoryCount: Record<QueryCategory, number>
  overall: number
  total: number
}

/** Full benchmark output written to disk. */
export interface BenchmarkOutput {
  meta: {
    baseUrl: string
    tenantId: string
    seed: number
    numDays: number
    numTopics: number
    wordsPerDay: number
    retrievalLimit: number
    timestamp: string
  }
  results: EvalResult[]
  stats: BenchmarkStats
}

/** Shape of a mem9 memory returned by the API. */
export interface MnemoMemory {
  id: string
  content: string
  score?: number
  session_id?: string
  agent_id?: string
  metadata?: Record<string, unknown> | null
  created_at?: string
  updated_at?: string
}
