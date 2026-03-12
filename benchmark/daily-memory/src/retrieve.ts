import { getAgentId, getRetrievalLimit, searchMemories } from "./mem9.js"
import { getSessionId } from "./ingest.js"

export const getContext = async (question: string): Promise<string> => {
  const memories = await searchMemories({
    q: question,
    session_id: getSessionId(),
    agent_id: getAgentId(),
    limit: getRetrievalLimit(),
  })

  return memories
    .map((memory, index) => {
      const scoreLabel = typeof memory.score === "number" ? ` score=${memory.score.toFixed(4)}` : ""
      return `#${index + 1}${scoreLabel}\n${memory.content}`
    })
    .join("\n\n")
}
