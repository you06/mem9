import type { DailyEntry } from "./types.js"

import { Spinner } from "picospinner"

import { createMemory, deleteMemory, getAgentId, searchMemories, shouldClearSessionFirst } from "./mem9.js"

const SESSION_ID = "daily-memory-bench"

const sleep = async (ms: number): Promise<void> => await new Promise(resolve => setTimeout(resolve, ms))

const clearExistingMemories = async (): Promise<void> => {
  const limit = 200
  let offset = 0
  while (true) {
    const memories = await searchMemories({ session_id: SESSION_ID, agent_id: getAgentId(), limit, offset })
    if (memories.length === 0) break
    for (const memory of memories) {
      if (memory.id) await deleteMemory(memory.id)
    }
    if (memories.length < limit) break
    offset += limit
  }
}

const waitForMemories = async (expectedCount: number): Promise<void> => {
  const deadline = Date.now() + 120_000
  while (Date.now() < deadline) {
    // mem9 search API requires a `q` param to return results; use a broad query
    const memories = await searchMemories({
      q: "day",
      session_id: SESSION_ID,
      agent_id: getAgentId(),
      limit: expectedCount,
    })
    if (memories.length >= expectedCount) return
    await sleep(2000)
  }
  throw new Error(`Timed out waiting for ${expectedCount} memories to become searchable`)
}

export const ingestEntries = async (entries: DailyEntry[]): Promise<string> => {
  if (shouldClearSessionFirst()) {
    const clearSpinner = new Spinner("Clearing existing memories")
    clearSpinner.start()
    await clearExistingMemories()
    clearSpinner.succeed("Cleared existing memories")
  }

  const total = entries.length
  let done = 0
  let lastPct = -1

  const spinner = new Spinner(`Ingesting ${total} daily entries`)
  spinner.start()

  for (const entry of entries) {
    await createMemory({
      content: entry.content,
      agent_id: getAgentId(),
      session_id: SESSION_ID,
      tags: ["benchmark", "daily-memory"],
      metadata: {
        day_index: entry.dayIndex,
        date: entry.date,
        topic_ids: entry.topicIds,
        slot_keys: Object.keys(entry.slots),
      },
    })
    done++
    const pct = Math.floor((done / total) * 100)
    if (pct >= lastPct + 10) {
      spinner.setText(`Ingesting daily entries ${pct}%`)
      lastPct = pct
    }
  }

  spinner.setText("Waiting for memories to become searchable")
  await waitForMemories(total)
  spinner.succeed(`Ingested ${total} daily entries`)

  return SESSION_ID
}

export const getSessionId = (): string => SESSION_ID
