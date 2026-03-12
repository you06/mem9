import type { QueryCategory } from "./types.js"

const ARTICLES = new Set(["a", "an", "and", "the"])

const normalize = (s: string): string =>
  s
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, " ")
    .split(/\s+/)
    .filter(w => w.length > 0 && !ARTICLES.has(w))
    .join(" ")

const tokenF1 = (prediction: string, goldAnswer: string): number => {
  const predTokens = normalize(prediction).split(" ").filter(t => t.length > 0)
  const goldTokens = normalize(goldAnswer).split(" ").filter(t => t.length > 0)

  if (predTokens.length === 0 && goldTokens.length === 0) return 1
  if (predTokens.length === 0 || goldTokens.length === 0) return 0

  const goldCount = new Map<string, number>()
  for (const t of goldTokens) goldCount.set(t, (goldCount.get(t) ?? 0) + 1)

  let numSame = 0
  for (const t of predTokens) {
    const cnt = goldCount.get(t) ?? 0
    if (cnt > 0) {
      numSame++
      goldCount.set(t, cnt - 1)
    }
  }

  if (numSame === 0) return 0
  const precision = numSame / predTokens.length
  const recall = numSame / goldTokens.length
  return (2 * precision * recall) / (precision + recall)
}

const scoreExact = (prediction: string, answer: string, aliases: string[]): number => {
  const norm = normalize(prediction)
  // Check if the gold answer or any alias appears in the prediction
  if (norm.includes(normalize(answer))) return 1
  for (const alias of aliases) {
    if (norm.includes(normalize(alias))) return 1
  }
  // Fall back to token F1
  return tokenF1(prediction, answer)
}

const scoreParaphrase = (prediction: string, answer: string, aliases: string[]): number => {
  // Same as exact but we also accept token F1 as the score
  const norm = normalize(prediction)
  if (norm.includes(normalize(answer))) return 1
  for (const alias of aliases) {
    if (norm.includes(normalize(alias))) return 1
  }
  return tokenF1(prediction, answer)
}

const scoreTemporal = (prediction: string, answer: string, aliases: string[]): number => {
  const norm = normalize(prediction)
  // For dates, check if the answer date string appears
  if (norm.includes(normalize(answer))) return 1
  for (const alias of aliases) {
    if (norm.includes(normalize(alias))) return 1
  }
  return tokenF1(prediction, answer)
}

const scoreMultiHop = (prediction: string, answer: string, aliases: string[]): number => {
  // Multi-hop answers are "date1: val1, date2: val2"
  // Check if both values appear; score is average of presence
  if (aliases.length === 0) return tokenF1(prediction, answer)
  const norm = normalize(prediction)
  let found = 0
  for (const alias of aliases) {
    if (norm.includes(normalize(alias))) found++
  }
  return found / aliases.length
}

const scoreNegative = (prediction: string): number => {
  const lower = prediction.toLowerCase()
  const negPhrases = ["no information", "not mentioned", "no record", "none", "no entries", "not found", "no data", "doesn't mention", "does not mention", "no evidence"]
  return negPhrases.some(phrase => lower.includes(phrase)) ? 1 : 0
}

export const scoreAnswer = (
  prediction: string,
  answer: string,
  aliases: string[],
  category: QueryCategory,
): number => {
  switch (category) {
    case "exact":
      return scoreExact(prediction, answer, aliases)
    case "paraphrase":
      return scoreParaphrase(prediction, answer, aliases)
    case "temporal":
      return scoreTemporal(prediction, answer, aliases)
    case "multi-hop":
      return scoreMultiHop(prediction, answer, aliases)
    case "negative":
      return scoreNegative(prediction)
  }
}

/**
 * Extract an answer from retrieved context for a given question.
 * This is a deterministic, rule-based extraction that scans the context
 * for the expected answer or related content.
 */
export const extractAnswer = (context: string, question: string, answer: string, aliases: string[], category: QueryCategory): string => {
  if (context.trim().length === 0) {
    return category === "negative" ? "No information available" : ""
  }

  if (category === "negative") {
    // For negative questions, check if the context actually contains related info
    // Since these topics should never appear, any retrieved context is noise
    const questionWords = normalize(question).split(" ").filter(w => w.length > 2)
    const contextNorm = normalize(context)
    const relevantHits = questionWords.filter(w => contextNorm.includes(w))
    // If less than 30% of question-specific words match, it's irrelevant noise
    if (relevantHits.length / Math.max(questionWords.length, 1) < 0.3) {
      return "No information available"
    }
    return context.slice(0, 200)
  }

  // For other categories, scan context for the answer
  const contextLower = context.toLowerCase()
  const answerLower = answer.toLowerCase()

  // Direct match: return the answer
  if (contextLower.includes(answerLower)) {
    return answer
  }

  // Check aliases
  for (const alias of aliases) {
    if (contextLower.includes(alias.toLowerCase())) {
      return alias
    }
  }

  // For multi-hop, try to find individual parts
  if (category === "multi-hop") {
    const parts = answer.split(",").map(p => p.trim())
    const found: string[] = []
    for (const part of parts) {
      if (contextLower.includes(part.toLowerCase())) {
        found.push(part)
      }
    }
    if (found.length > 0) return found.join(", ")
  }

  // Fallback: return a snippet from context around any matching tokens
  const answerTokens = normalize(answer).split(" ").filter(t => t.length > 2)
  for (const token of answerTokens) {
    const idx = contextLower.indexOf(token)
    if (idx >= 0) {
      const start = Math.max(0, idx - 50)
      const end = Math.min(context.length, idx + token.length + 50)
      return context.slice(start, end).trim()
    }
  }

  return ""
}
