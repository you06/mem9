import type { BenchmarkQuestion, DailyEntry, DatasetManifest, QueryCategory, TopicFamily } from "./types.js"

import { mkdir, writeFile } from "node:fs/promises"
import { join } from "node:path"

// ---------------------------------------------------------------------------
// Deterministic PRNG (xoshiro128** variant, seeded)
// ---------------------------------------------------------------------------

class Rng {
  private s: Uint32Array

  constructor(seed: number) {
    this.s = new Uint32Array(4)
    this.s[0] = seed >>> 0
    this.s[1] = (seed * 2654435761) >>> 0
    this.s[2] = (seed * 2246822519) >>> 0
    this.s[3] = (seed * 3266489917) >>> 0
    // warm up
    for (let i = 0; i < 16; i++) this.next()
  }

  next(): number {
    const s = this.s
    const result = (((s[1] * 5) << 7 | (s[1] * 5) >>> 25) * 9) >>> 0
    const t = (s[1] << 9) >>> 0
    s[2] ^= s[0]
    s[3] ^= s[1]
    s[1] ^= s[2]
    s[0] ^= s[3]
    s[2] ^= t
    s[3] = (s[3] << 11 | s[3] >>> 21) >>> 0
    return result / 0x100000000
  }

  /** Inclusive integer in [lo, hi]. */
  int(lo: number, hi: number): number {
    return lo + Math.floor(this.next() * (hi - lo + 1))
  }

  pick<T>(arr: readonly T[]): T {
    return arr[this.int(0, arr.length - 1)]
  }

  shuffle<T>(arr: T[]): T[] {
    for (let i = arr.length - 1; i > 0; i--) {
      const j = this.int(0, i)
      const tmp = arr[i]
      arr[i] = arr[j]
      arr[j] = tmp
    }
    return arr
  }
}

// ---------------------------------------------------------------------------
// Topic family definitions
// ---------------------------------------------------------------------------

interface TopicDef {
  name: string
  template: string
  slotKeys: string[]
  slotPools: Record<string, string[]>
}

const TOPIC_DEFS: TopicDef[] = [
  {
    name: "standup-meeting",
    template: "Had a standup meeting today. {{person}} reported progress on the {{project}} project. The current blocker is {{blocker}}. Next milestone is {{milestone}}.",
    slotKeys: ["person", "project", "blocker", "milestone"],
    slotPools: {
      person: ["Alice", "Bob", "Carlos", "Diana", "Ethan", "Fiona", "George", "Hannah", "Ivan", "Julia", "Kevin", "Laura", "Marcus", "Nina", "Oscar"],
      project: ["Phoenix", "Titan", "Aurora", "Neptune", "Falcon", "Orion", "Vega", "Atlas", "Cosmos", "Horizon"],
      blocker: ["CI pipeline timeout", "flaky integration test", "missing API credentials", "dependency version conflict", "memory leak in staging", "TLS certificate expiry", "rate limiter misconfiguration", "database migration rollback"],
      milestone: ["v2.0 beta release", "load test sign-off", "security audit completion", "public API launch", "data migration cutover", "feature flag rollout", "SOC2 certification deadline", "Q3 demo day"],
    },
  },
  {
    name: "code-review",
    template: "Reviewed PR #{{prNumber}} by {{author}} today. The change touches {{component}} and introduces {{feature}}. Requested {{changeType}} before merging.",
    slotKeys: ["prNumber", "author", "component", "feature", "changeType"],
    slotPools: {
      prNumber: ["142", "287", "315", "423", "508", "619", "734", "891", "956", "1024", "1103", "1247"],
      author: ["alice", "bob", "carlos", "diana", "ethan", "fiona", "george", "hannah"],
      component: ["auth middleware", "payment gateway", "search indexer", "notification service", "user profile API", "rate limiter", "caching layer", "analytics pipeline"],
      feature: ["retry with exponential backoff", "JWT token rotation", "batch upsert endpoint", "streaming response support", "circuit breaker pattern", "connection pooling", "request deduplication", "graceful shutdown handler"],
      changeType: ["additional unit tests", "error handling improvements", "documentation updates", "benchmark results", "migration rollback plan", "load test evidence"],
    },
  },
  {
    name: "deployment",
    template: "Deployed {{service}} to {{environment}} at {{time}}. Build number {{buildNum}}. Rollback plan: {{rollback}}. Deployment took {{duration}} minutes.",
    slotKeys: ["service", "environment", "time", "buildNum", "rollback", "duration"],
    slotPools: {
      service: ["api-gateway", "user-service", "billing-engine", "search-service", "notification-hub", "analytics-collector", "auth-provider", "cdn-edge"],
      environment: ["staging", "production-us", "production-eu", "production-ap", "canary-ring"],
      time: ["09:15 UTC", "11:30 UTC", "14:45 UTC", "16:00 UTC", "18:30 UTC", "21:00 UTC", "03:00 UTC"],
      buildNum: ["b1847", "b2031", "b2256", "b2419", "b2587", "b2734", "b2901", "b3045", "b3198", "b3367"],
      rollback: ["revert to previous image tag", "feature flag disable", "DNS failover to blue stack", "database snapshot restore"],
      duration: ["3", "5", "7", "12", "15", "22"],
    },
  },
  {
    name: "incident",
    template: "Incident report: {{severity}} severity incident on {{system}}. Root cause was {{rootCause}}. Impact lasted {{impactDuration}} and affected {{usersAffected}} users. Resolved by {{resolver}}.",
    slotKeys: ["severity", "system", "rootCause", "impactDuration", "usersAffected", "resolver"],
    slotPools: {
      severity: ["P1", "P2", "P3", "P4"],
      system: ["payment processing", "user authentication", "search backend", "notification delivery", "file storage", "API gateway", "database cluster"],
      rootCause: ["connection pool exhaustion", "misconfigured rate limit", "expired TLS certificate", "OOM kill on worker pod", "corrupted cache entry", "DNS propagation delay", "deadlock in transaction handler"],
      impactDuration: ["12 minutes", "45 minutes", "2 hours", "4 hours", "30 minutes"],
      usersAffected: ["150", "1200", "8500", "23000", "340", "5600"],
      resolver: ["Alice", "Bob", "Carlos", "Diana", "Ethan", "on-call SRE team"],
    },
  },
  {
    name: "reading-notes",
    template: "Read chapter {{chapter}} of \"{{bookTitle}}\" by {{bookAuthor}}. Key takeaway: {{takeaway}}. Plan to apply this to {{application}}.",
    slotKeys: ["chapter", "bookTitle", "bookAuthor", "takeaway", "application"],
    slotPools: {
      chapter: ["3", "5", "7", "9", "11", "14", "18", "22"],
      bookTitle: ["Designing Data-Intensive Applications", "The Pragmatic Programmer", "Staff Engineer", "System Design Interview", "Building Microservices", "Accelerate", "The Phoenix Project", "Release It!"],
      bookAuthor: ["Martin Kleppmann", "David Thomas", "Will Larson", "Alex Xu", "Sam Newman", "Nicole Forsgren", "Gene Kim", "Michael Nygard"],
      takeaway: ["event sourcing simplifies audit trails", "small batch sizes reduce risk", "technical strategy requires organizational alignment", "back-of-envelope estimation prevents over-engineering", "service mesh adds operational overhead", "deployment frequency correlates with stability", "WIP limits improve throughput", "bulkheads prevent cascade failures"],
      application: ["our event pipeline redesign", "the migration project", "quarterly planning process", "capacity planning model", "microservice decomposition", "CI/CD pipeline improvements", "incident response workflow", "monitoring strategy"],
    },
  },
  {
    name: "meeting-notes",
    template: "Architecture review meeting with {{attendees}}. Discussed {{topic}}. Decision: {{decision}}. Action item assigned to {{assignee}} due by {{dueDate}}.",
    slotKeys: ["attendees", "topic", "decision", "assignee", "dueDate"],
    slotPools: {
      attendees: ["platform team", "backend guild", "SRE and infra leads", "product and engineering", "security working group", "data team and stakeholders"],
      topic: ["database sharding strategy", "API versioning approach", "event bus migration", "observability stack upgrade", "multi-region failover", "data retention policy", "GraphQL adoption", "zero-trust network rollout"],
      decision: ["adopt consistent hashing for shard key", "use URL path versioning for public APIs", "migrate to Kafka from RabbitMQ", "standardize on OpenTelemetry", "implement active-active with CRDTs", "enforce 90-day retention by default", "start with a BFF gateway layer", "deploy mTLS between all services"],
      assignee: ["Alice", "Bob", "Carlos", "Diana", "Ethan", "Fiona"],
      dueDate: ["end of sprint", "next Friday", "end of month", "Q3 milestone", "before the freeze window"],
    },
  },
  {
    name: "exercise-log",
    template: "Workout today: {{exerciseType}} for {{duration}} minutes. Felt {{feeling}}. Heart rate peak: {{heartRate}} bpm. Location: {{location}}.",
    slotKeys: ["exerciseType", "duration", "feeling", "heartRate", "location"],
    slotPools: {
      exerciseType: ["running", "cycling", "swimming", "weight training", "yoga", "hiking", "rowing", "HIIT session"],
      duration: ["25", "35", "45", "60", "75", "90"],
      feeling: ["energized", "exhausted but satisfied", "strong", "sluggish at first then great", "sore from yesterday", "like a personal best"],
      heartRate: ["145", "158", "167", "172", "138", "181", "155"],
      location: ["gym", "park trail", "home", "community pool", "riverside path", "mountain trail"],
    },
  },
  {
    name: "cooking",
    template: "Cooked {{dish}} for {{occasion}}. Key ingredient: {{ingredient}}. Cooking time was {{cookTime}} minutes. Rating: {{rating}}/10. Notes: {{notes}}.",
    slotKeys: ["dish", "occasion", "ingredient", "cookTime", "rating", "notes"],
    slotPools: {
      dish: ["pasta carbonara", "chicken tikka masala", "vegetable stir fry", "beef bourguignon", "sushi rolls", "mushroom risotto", "thai green curry", "shakshuka"],
      occasion: ["dinner", "lunch", "weekend brunch", "meal prep Sunday"],
      ingredient: ["smoked pancetta", "garam masala blend", "fresh ginger root", "red wine reduction", "nori sheets", "arborio rice", "lemongrass stalks", "harissa paste"],
      cookTime: ["20", "35", "45", "90", "120", "60"],
      rating: ["6", "7", "8", "9", "10"],
      notes: ["needs more salt next time", "perfect consistency", "slightly overcooked the protein", "best batch yet", "try adding chili flakes", "reduce liquid by a quarter next time"],
    },
  },
  {
    name: "travel-planning",
    template: "Planning trip to {{destination}}. Flight: {{airline}} departing {{departureTime}}. Hotel: {{hotel}} for {{nights}} nights at ${{price}}/night. Must visit: {{attraction}}.",
    slotKeys: ["destination", "airline", "departureTime", "hotel", "nights", "price", "attraction"],
    slotPools: {
      destination: ["Tokyo", "Lisbon", "Vancouver", "Cape Town", "Barcelona", "Seoul", "Reykjavik", "Buenos Aires"],
      airline: ["Delta", "United", "ANA", "Lufthansa", "Emirates", "Qantas", "KLM", "Singapore Airlines"],
      departureTime: ["06:30 AM", "10:15 AM", "02:45 PM", "06:00 PM", "11:30 PM"],
      hotel: ["Grand Hyatt", "Marriott Waterfront", "Hilton Central", "boutique guesthouse Azul", "Airbnb loft downtown"],
      nights: ["3", "5", "7", "10", "14"],
      price: ["120", "185", "240", "310", "95", "420"],
      attraction: ["Tsukiji outer market", "Belem Tower", "Stanley Park seawall", "Table Mountain cable car", "La Sagrada Familia", "Gyeongbokgung Palace", "Blue Lagoon geothermal spa", "La Boca street art"],
    },
  },
  {
    name: "finance",
    template: "Budget review: {{category}} spending this month is ${{amount}}, which is {{trend}} compared to last month. Set savings target at ${{savingsTarget}}. Note: {{note}}.",
    slotKeys: ["category", "amount", "trend", "savingsTarget", "note"],
    slotPools: {
      category: ["groceries", "dining out", "transportation", "subscriptions", "utilities", "entertainment", "healthcare", "education"],
      amount: ["320", "485", "150", "89", "210", "175", "430", "560"],
      trend: ["12% higher", "8% lower", "roughly flat", "25% higher", "15% lower", "doubled"],
      savingsTarget: ["500", "750", "1000", "1200", "800", "1500"],
      note: ["cancel unused streaming service", "switch to annual billing for cloud tools", "insurance renewal coming up", "tax filing deadline next month", "rebalance investment portfolio", "emergency fund at 4 months now"],
    },
  },
  {
    name: "learning",
    template: "Studied {{subject}} today. Completed {{resource}} module {{moduleNum}}. Key concept: {{concept}}. Practice exercise score: {{score}}%. Next: {{nextStep}}.",
    slotKeys: ["subject", "resource", "moduleNum", "concept", "score", "nextStep"],
    slotPools: {
      subject: ["Rust ownership model", "Kubernetes networking", "distributed consensus", "WebAssembly basics", "category theory for programmers", "advanced SQL window functions", "TLA+ model checking", "eBPF tracing"],
      resource: ["Coursera course", "MIT OCW lecture", "O'Reilly workshop", "Udemy bootcamp", "official docs tutorial", "YouTube series"],
      moduleNum: ["2", "4", "6", "8", "10", "12"],
      concept: ["borrow checker lifetime annotations", "pod-to-pod communication via CNI", "Raft leader election protocol", "linear memory model", "functors and natural transformations", "PARTITION BY with ROWS BETWEEN", "temporal logic of actions", "BPF ring buffer maps"],
      score: ["72", "85", "91", "68", "95", "78", "88"],
      nextStep: ["implement a linked list exercise", "deploy multi-node cluster lab", "write a simple Paxos simulator", "compile Rust to WASM target", "solve Category Theory for Programmers exercises", "optimize slow analytical query", "verify a mutual exclusion algorithm", "trace syscall latency in production"],
    },
  },
  {
    name: "garden",
    template: "Garden log: {{action}} the {{plant}} today. Soil moisture at {{moisture}}%. Applied {{treatment}}. Weather: {{weather}}. Expected harvest in {{harvestTime}}.",
    slotKeys: ["action", "plant", "moisture", "treatment", "weather", "harvestTime"],
    slotPools: {
      action: ["transplanted", "pruned", "watered deeply", "fertilized", "staked", "mulched around", "propagated cuttings of"],
      plant: ["tomatoes", "basil", "sunflowers", "jalapeños", "blueberry bushes", "zucchini", "lavender", "strawberries"],
      moisture: ["35", "50", "65", "80", "42", "58"],
      treatment: ["compost tea", "fish emulsion fertilizer", "neem oil spray", "bone meal", "Epsom salt solution", "diatomaceous earth"],
      weather: ["sunny and warm", "overcast with light rain", "hot and humid", "mild and breezy", "afternoon thunderstorms"],
      harvestTime: ["2 weeks", "4 weeks", "6 weeks", "8 weeks", "3 months"],
    },
  },
  {
    name: "side-project",
    template: "Side project \"{{projectName}}\" update: implemented {{feature}} using {{technology}}. Lines of code: {{loc}}. Current status: {{status}}. GitHub stars: {{stars}}.",
    slotKeys: ["projectName", "feature", "technology", "loc", "status", "stars"],
    slotPools: {
      projectName: ["Shellfish", "Nimbus CLI", "PageRank Lite", "TinyLang", "Bonsai DB", "Pixel Forge", "Quill Notes", "ZapLog"],
      feature: ["real-time sync engine", "plugin system with hot reload", "WASM-based sandbox", "SQLite virtual table adapter", "markdown preview renderer", "custom diff algorithm", "incremental parser", "undo/redo stack"],
      technology: ["Rust + Tokio", "Go + gRPC", "TypeScript + Bun", "Zig + WASI", "Python + FastAPI", "Elixir + Phoenix", "Swift + Vapor", "C++ 20 coroutines"],
      loc: ["450", "820", "1300", "2100", "3500", "5200"],
      status: ["alpha — core works", "beta — needs docs", "MVP ready", "polishing CLI UX", "blocked on upstream bug", "preparing v1.0 release"],
      stars: ["12", "47", "128", "256", "510", "3", "89"],
    },
  },
  {
    name: "health",
    template: "Health check-in: slept {{sleepHours}} hours (quality: {{sleepQuality}}). Energy level: {{energy}}/10. Took {{supplement}}. Hydration: {{water}} glasses. Mood: {{mood}}.",
    slotKeys: ["sleepHours", "sleepQuality", "energy", "supplement", "water", "mood"],
    slotPools: {
      sleepHours: ["5.5", "6", "6.5", "7", "7.5", "8", "8.5", "9"],
      sleepQuality: ["poor — woke up twice", "fair", "good", "excellent — deep sleep", "restless"],
      energy: ["3", "5", "6", "7", "8", "9"],
      supplement: ["vitamin D 2000 IU", "magnesium glycinate", "omega-3 fish oil", "zinc + vitamin C", "B-complex", "melatonin 3mg"],
      water: ["4", "6", "8", "10", "12"],
      mood: ["calm and focused", "slightly anxious", "motivated", "tired but steady", "great after morning walk", "neutral"],
    },
  },
  {
    name: "mentor-session",
    template: "Mentoring session with {{mentee}}. Discussed {{mentorTopic}}. Recommended {{recommendation}}. Follow-up scheduled for {{followUp}}. Key feedback: {{feedback}}.",
    slotKeys: ["mentee", "mentorTopic", "recommendation", "followUp", "feedback"],
    slotPools: {
      mentee: ["Sam", "Taylor", "Jordan", "Riley", "Morgan", "Casey", "Quinn", "Avery"],
      mentorTopic: ["career growth to staff level", "handling difficult stakeholder conversations", "system design interview prep", "transitioning from IC to management", "building technical credibility in a new team", "prioritizing tech debt vs features"],
      recommendation: ["write an engineering strategy doc", "shadow the on-call rotation", "present at the next architecture review", "start a weekly writing habit", "build a brag document", "pair program with senior engineers"],
      followUp: ["next Tuesday", "in two weeks", "end of month", "after the sprint retrospective"],
      feedback: ["great progress on communication skills", "needs more practice with system design trade-offs", "ready for a stretch project", "should seek more cross-team visibility", "strong technical depth, work on breadth"],
    },
  },
  {
    name: "conference",
    template: "Attended \"{{talkTitle}}\" by {{speaker}} at {{event}}. Key insight: {{insight}}. Related to our work on {{relevance}}. Rating: {{talkRating}}/5.",
    slotKeys: ["talkTitle", "speaker", "event", "insight", "relevance", "talkRating"],
    slotPools: {
      talkTitle: ["Scaling Without Regret", "The Art of On-Call", "Zero-Copy Deserialization", "Rethinking Observability", "Why Your Cache Is Lying to You", "Building Resilient Distributed Systems", "The Cost of Consistency", "Event Sourcing in Practice"],
      speaker: ["Dr. Sarah Chen", "James Okafor", "Maria Gonzalez", "Priya Patel", "Liam O'Brien", "Yuki Tanaka", "Aisha Mohammed", "Thomas Weber"],
      event: ["KubeCon", "QCon", "Strange Loop", "GopherCon", "RustConf", "SREcon", "Hydra distributed computing conf", "local tech meetup"],
      insight: ["linearizability is rarely worth the latency cost", "structured logging beats unstructured by 10x for debugging", "arena allocators eliminate GC pauses", "distributed tracing requires sampling strategy up front", "stale cache reads cause more outages than cache misses", "chaos engineering must be gradual", "CRDTs enable true offline-first apps", "event replay simplifies disaster recovery"],
      relevance: ["our database migration", "observability overhaul", "memory allocator tuning", "tracing infrastructure", "caching strategy review", "reliability engineering roadmap", "sync engine design", "backup and recovery process"],
      talkRating: ["3", "4", "5"],
    },
  },
  {
    name: "tool-evaluation",
    template: "Evaluated {{toolName}} ({{toolType}}). Tested against {{comparison}}. Result: {{verdict}}. Key metric: {{metric}}. License: {{license}}.",
    slotKeys: ["toolName", "toolType", "comparison", "verdict", "metric", "license"],
    slotPools: {
      toolName: ["Grafana Alloy", "Clickhouse", "DragonflyDB", "Deno 2", "Turso", "Neon Postgres", "Temporal.io", "Buf Connect"],
      toolType: ["telemetry collector", "OLAP database", "Redis-compatible cache", "JavaScript runtime", "edge SQLite", "serverless Postgres", "workflow orchestrator", "gRPC framework"],
      comparison: ["our current Prometheus + Thanos stack", "existing PostgreSQL analytics DB", "Redis 7 cluster", "Node.js 22", "local SQLite", "RDS Aurora", "Airflow DAGs", "standard gRPC-Go"],
      verdict: ["promising but not production-ready for us", "clear winner on query latency", "drop-in replacement with 3x memory savings", "migration path is smooth", "edge replication is the killer feature", "branching databases could transform our dev workflow", "significantly simpler than current solution", "schema-first approach aligns with our conventions"],
      metric: ["p99 latency: 12ms vs 45ms", "ingestion throughput: 2M events/sec", "memory usage: 1.2GB vs 4.1GB", "cold start: 50ms vs 300ms", "replication lag: <5ms globally", "connection time: 8ms vs 120ms", "workflow execution: 200ms overhead", "code generation: 40% fewer lines"],
      license: ["Apache 2.0", "BSL 1.1", "MIT", "AGPL 3.0", "proprietary with free tier", "dual SSPL/commercial"],
    },
  },
  {
    name: "1-on-1",
    template: "1:1 with {{manager}}. Discussed {{oneOnOneTopic}}. Feedback received: {{managerFeedback}}. My ask: {{myAsk}}. Next steps: {{nextSteps}}.",
    slotKeys: ["manager", "oneOnOneTopic", "managerFeedback", "myAsk", "nextSteps"],
    slotPools: {
      manager: ["Sarah", "Mike", "Priya", "Tom", "Rachel", "David"],
      oneOnOneTopic: ["quarterly goals progress", "promotion timeline", "team dynamics concern", "project ownership scope", "workload balance", "cross-team collaboration friction"],
      managerFeedback: ["strong execution, increase visibility of work", "great debugging on the outage, write a post-mortem blog post", "need to delegate more", "technical leadership is solid, focus on stakeholder communication", "consider mentoring more junior engineers"],
      myAsk: ["dedicated time for tech debt reduction", "budget for conference attendance", "clearer definition of staff engineer expectations", "support to lead the platform migration", "help resolving cross-team dependency"],
      nextSteps: ["draft a project proposal by Friday", "schedule a skip-level with VP", "present tech debt plan to the team", "set up weekly sync with partner team", "write a self-review for mid-cycle"],
    },
  },
  {
    name: "documentation",
    template: "Updated {{docType}} for {{docTarget}}. Added section on {{section}}. Reviewed by {{reviewer}}. Published to {{publishTarget}}. Word count: {{wordCount}}.",
    slotKeys: ["docType", "docTarget", "section", "reviewer", "publishTarget", "wordCount"],
    slotPools: {
      docType: ["runbook", "architecture decision record", "API reference", "onboarding guide", "troubleshooting playbook", "design proposal"],
      docTarget: ["payment service", "auth flow", "data pipeline", "deployment process", "monitoring setup", "incident response"],
      section: ["failure modes and recovery steps", "request flow diagram", "rate limiting configuration", "common error codes and fixes", "alerting thresholds", "rollback procedures"],
      reviewer: ["Alice", "Bob", "Carlos", "Diana", "Ethan"],
      publishTarget: ["internal wiki", "GitHub repo docs folder", "Notion workspace", "Confluence space", "README.md"],
      wordCount: ["800", "1200", "1800", "2500", "3200"],
    },
  },
  {
    name: "music-practice",
    template: "Music practice: {{instrument}} for {{practiceDuration}} minutes. Worked on {{piece}} by {{composer}}. Focus area: {{focusArea}}. Tempo: {{tempo}} BPM. Progress: {{progress}}.",
    slotKeys: ["instrument", "practiceDuration", "piece", "composer", "focusArea", "tempo", "progress"],
    slotPools: {
      instrument: ["piano", "guitar", "violin", "drums", "bass", "saxophone"],
      practiceDuration: ["20", "30", "45", "60", "90"],
      piece: ["Nocturne Op. 9 No. 2", "Stairway to Heaven solo", "Bach Partita No. 2", "Take Five", "Spanish Romance", "Autumn Leaves"],
      composer: ["Chopin", "Jimmy Page", "J.S. Bach", "Dave Brubeck", "Anonymous", "Joseph Kosma"],
      focusArea: ["left hand arpeggios", "string bending accuracy", "bowing dynamics", "hi-hat independence", "walking bass line", "improvisation over changes"],
      tempo: ["60", "80", "100", "120", "140", "160"],
      progress: ["nailed the tricky passage", "still struggling with the bridge", "consistent improvement", "ready to perform", "need to slow down and rebuild", "memorization complete"],
    },
  },
  {
    name: "pet-care",
    template: "Pet update: {{petName}} ({{petType}}) had {{petEvent}} today. Vet says: {{vetNote}}. Fed {{petFood}}. Weight: {{petWeight}} lbs. Next appointment: {{nextAppt}}.",
    slotKeys: ["petName", "petType", "petEvent", "vetNote", "petFood", "petWeight", "nextAppt"],
    slotPools: {
      petName: ["Luna", "Milo", "Cleo", "Biscuit", "Nori", "Ziggy", "Pepper", "Tofu"],
      petType: ["golden retriever", "tabby cat", "French bulldog", "Maine Coon", "beagle", "Siamese cat"],
      petEvent: ["annual checkup", "grooming session", "training class", "dental cleaning", "allergy flare-up", "new trick learned"],
      vetNote: ["all vaccinations current", "slightly overweight, reduce treats", "healthy coat and teeth", "recommend joint supplement", "mild ear infection, prescribed drops"],
      petFood: ["grain-free kibble", "raw food diet portion", "salmon and sweet potato mix", "prescription digestive formula"],
      petWeight: ["15", "22", "35", "48", "62", "10", "28"],
      nextAppt: ["in 3 months", "in 6 months", "next month for follow-up", "annual checkup next year"],
    },
  },
  {
    name: "home-improvement",
    template: "Home project: {{homeProject}} in the {{room}}. Materials cost: ${{materialCost}}. Time spent: {{timeSpent}} hours. Difficulty: {{difficulty}}/10. Lesson learned: {{lesson}}.",
    slotKeys: ["homeProject", "room", "materialCost", "timeSpent", "difficulty", "lesson"],
    slotPools: {
      homeProject: ["installed floating shelves", "replaced light fixtures", "painted accent wall", "assembled standing desk", "weatherstripped windows", "organized cable management", "mounted TV bracket", "refinished hardwood floor patch"],
      room: ["home office", "kitchen", "living room", "bedroom", "garage", "bathroom"],
      materialCost: ["45", "85", "120", "210", "65", "340", "28"],
      timeSpent: ["1.5", "3", "4.5", "6", "8", "2"],
      difficulty: ["3", "5", "6", "7", "8", "9"],
      lesson: ["measure three times, drill once", "LED color temperature matters more than brightness", "primer is not optional", "watch assembly videos at 0.5x speed", "foam tape beats silicone for drafts", "velcro ties are superior to zip ties", "use a stud finder, not guessing", "rent a floor sander instead of doing it by hand"],
    },
  },
  {
    name: "language-study",
    template: "Language study: {{language}} — {{studyActivity}} for {{studyDuration}} minutes. New vocabulary: {{vocabWord}} (meaning: {{vocabMeaning}}). Streak: {{streak}} days. Proficiency goal: {{profGoal}}.",
    slotKeys: ["language", "studyActivity", "studyDuration", "vocabWord", "vocabMeaning", "streak", "profGoal"],
    slotPools: {
      language: ["Japanese", "Spanish", "Mandarin", "Korean", "French", "German"],
      studyActivity: ["flashcard review", "shadowing podcast", "grammar exercises", "writing practice", "conversation exchange", "reading native article"],
      studyDuration: ["15", "25", "30", "45", "60"],
      vocabWord: ["仕方がない (shikata ga nai)", "madrugada", "差不多 (chàbùduō)", "괜찮아 (gwaenchana)", "dépaysement", "Feierabend"],
      vocabMeaning: ["it can't be helped", "early morning / dawn", "almost / close enough", "it's okay", "disorientation in a foreign place", "end-of-workday freedom"],
      streak: ["7", "14", "30", "60", "90", "120", "180"],
      profGoal: ["JLPT N3 by December", "B2 conversational", "HSK 4 certification", "TOPIK II level 4", "DELF B1", "Goethe B1"],
    },
  },
  {
    name: "volunteer",
    template: "Volunteered at {{organization}} today. Activity: {{volunteerActivity}}. Duration: {{volunteerDuration}} hours. People helped: {{peopleHelped}}. Coordinator: {{coordinator}}. Takeaway: {{volunteerTakeaway}}.",
    slotKeys: ["organization", "volunteerActivity", "volunteerDuration", "peopleHelped", "coordinator", "volunteerTakeaway"],
    slotPools: {
      organization: ["local food bank", "Habitat for Humanity", "Code.org workshop", "community garden", "animal shelter", "literacy program"],
      volunteerActivity: ["sorting donations", "building framing", "teaching kids to code", "planting vegetable beds", "walking dogs", "tutoring reading"],
      volunteerDuration: ["2", "3", "4", "6", "8"],
      peopleHelped: ["8", "15", "25", "40", "12", "30"],
      coordinator: ["Maria", "James", "Aisha", "Chen", "Rosa", "Patrick"],
      volunteerTakeaway: ["small acts compound over time", "teamwork with strangers is energizing", "kids learn faster than you expect", "physical labor is meditative", "every animal deserves kindness", "reading transforms opportunities"],
    },
  },
  {
    name: "podcast-notes",
    template: "Listened to \"{{podcastTitle}}\" episode {{episodeNum}}: \"{{episodeTitle}}\" with guest {{podcastGuest}}. Key quote: \"{{keyQuote}}\". Actionable idea: {{actionableIdea}}.",
    slotKeys: ["podcastTitle", "episodeNum", "episodeTitle", "podcastGuest", "keyQuote", "actionableIdea"],
    slotPools: {
      podcastTitle: ["Software Engineering Daily", "Changelog", "CoRecursive", "Hanselminutes", "Developer Tea", "Ship It!", "Go Time", "Syntax.fm"],
      episodeNum: ["142", "289", "73", "401", "515", "88", "312", "198"],
      episodeTitle: ["The Future of Edge Computing", "Building Developer Tools", "Incident Management Done Right", "Open Source Sustainability", "Writing Better Error Messages"],
      podcastGuest: ["Kelsey Hightower", "Julia Evans", "Charity Majors", "Mitchell Hashimoto", "Jessie Frazelle", "Bryan Cantrill"],
      keyQuote: ["The best monitoring is the kind nobody has to think about", "If you can't explain it simply, your abstraction is wrong", "On-call should not feel like punishment", "Every company eventually builds a platform team", "Errors are the user interface of your system"],
      actionableIdea: ["add structured context to all error returns", "set up a team-wide blameless post-mortem template", "create an internal developer experience survey", "adopt OpenTelemetry trace propagation", "write user-facing error messages before code"],
    },
  },
  {
    name: "movie-review",
    template: "Watched \"{{movieTitle}}\" ({{movieYear}}, dir. {{director}}). Genre: {{genre}}. Rating: {{movieRating}}/10. Standout: {{standout}}. Would recommend to: {{recommendTo}}.",
    slotKeys: ["movieTitle", "movieYear", "director", "genre", "movieRating", "standout", "recommendTo"],
    slotPools: {
      movieTitle: ["Interstellar", "Parasite", "The Grand Budapest Hotel", "Arrival", "Everything Everywhere All at Once", "Dune Part Two", "Past Lives", "Oppenheimer"],
      movieYear: ["2014", "2019", "2014", "2016", "2022", "2024", "2023", "2023"],
      director: ["Christopher Nolan", "Bong Joon-ho", "Wes Anderson", "Denis Villeneuve", "Daniel Kwan", "Denis Villeneuve", "Celine Song", "Christopher Nolan"],
      genre: ["sci-fi drama", "thriller", "comedy drama", "sci-fi", "action comedy", "epic sci-fi", "romantic drama", "historical drama"],
      movieRating: ["8", "9", "10", "7", "9", "8"],
      standout: ["Hans Zimmer's organ score", "the basement flooding sequence", "symmetrical framing throughout", "the heptapod language design", "the multiverse fight choreography", "sandworm riding sequence", "the in-between spaces motif", "Trinity test sequence"],
      recommendTo: ["anyone who loves hard sci-fi", "thriller fans who enjoy social commentary", "Wes Anderson completists", "fans of thoughtful sci-fi", "everyone, no exceptions", "epic cinema lovers", "anyone reflecting on paths not taken", "history and physics enthusiasts"],
    },
  },
  {
    name: "weather-impact",
    template: "Weather today: {{weatherCondition}}, {{temperature}}°F. Impact on plans: {{weatherImpact}}. Adjusted by: {{adjustment}}. Tomorrow forecast: {{forecast}}.",
    slotKeys: ["weatherCondition", "temperature", "weatherImpact", "adjustment", "forecast"],
    slotPools: {
      weatherCondition: ["heavy rain", "clear skies", "snow flurries", "dense fog", "thunderstorms", "overcast and windy", "heatwave", "mild and sunny"],
      temperature: ["28", "42", "55", "68", "75", "85", "92", "101"],
      weatherImpact: ["cancelled outdoor run", "perfect for park work session", "delayed commute by 30 minutes", "low visibility advisory", "power flickered twice", "stayed indoors all day", "drank extra water, worked near AC", "enjoyed lunch outside"],
      adjustment: ["moved workout to gym treadmill", "took laptop to the park bench", "left 45 minutes early", "worked from home instead", "charged all devices preemptively", "rearranged tasks to indoor focus", "scheduled meetings for cool morning hours", "extended lunch break for fresh air"],
      forecast: ["clearing by afternoon", "rain expected overnight", "warming trend through the week", "another cold front incoming", "sunny and mild", "more storms possible"],
    },
  },
  {
    name: "shopping",
    template: "Purchased {{item}} from {{store}} for ${{itemPrice}}. Reason: {{purchaseReason}}. Compared with {{alternative}} which was ${{altPrice}}. Satisfaction: {{satisfaction}}.",
    slotKeys: ["item", "store", "itemPrice", "purchaseReason", "alternative", "altPrice", "satisfaction"],
    slotPools: {
      item: ["mechanical keyboard", "noise-cancelling headphones", "ergonomic mouse", "ultrawide monitor", "standing desk mat", "webcam", "USB-C hub", "desk lamp"],
      store: ["Amazon", "Best Buy", "Micro Center", "B&H Photo", "local electronics shop"],
      itemPrice: ["79", "129", "249", "349", "449", "59", "89", "45"],
      purchaseReason: ["old one broke", "ergonomic upgrade", "WFH setup improvement", "needed for streaming", "reducing cable clutter", "reducing eye strain"],
      alternative: ["Logitech model", "Sony XM5", "MX Master 3S", "Dell 34-inch", "Topo mat", "Elgato Facecam"],
      altPrice: ["65", "299", "99", "449", "99", "169"],
      satisfaction: ["very happy, great tactile feel", "good noise cancelling but tight fit", "perfect for wrist comfort", "incredible screen real estate", "feet feel much better now", "sharp image quality"],
    },
  },
]

// ---------------------------------------------------------------------------
// Filler / padding sentences to reach target word count
// ---------------------------------------------------------------------------

const FILLER_SENTENCES: string[] = [
  "Felt productive throughout the day.",
  "Need to follow up on this tomorrow.",
  "Overall a solid day of progress.",
  "Energy levels were decent after lunch.",
  "Made a note to revisit this next week.",
  "The weather was pleasant during my walk.",
  "Grabbed coffee from the corner shop.",
  "Had a brief chat with a colleague about unrelated topics.",
  "Reminder to update the shared spreadsheet.",
  "Looking forward to the weekend plans.",
  "Caught up on email backlog during a break.",
  "Listened to ambient music while working.",
  "Took a short stretch break mid-afternoon.",
  "Water intake was better today than yesterday.",
  "Need to restock office supplies soon.",
  "The new desk arrangement is working well.",
  "Double-checked calendar for tomorrow's meetings.",
  "Made progress on inbox zero — down to 14 unread.",
  "Reviewed and archived old bookmarks.",
  "Set phone to do-not-disturb for deep work block.",
  "Reflected on priorities before bed.",
  "The commute was uneventful today.",
  "Charged all devices overnight.",
  "Read a few pages of a novel before sleep.",
  "The sunset was particularly vivid this evening.",

  // Longer paragraph-length fillers for context-window stress testing.
  // These must stay free of proper nouns, specific dates, numbers, project
  // names, or technical details that could overlap with topic slot values.

  "The office was quieter than usual this morning, which made it easier to concentrate on routine tasks. Sometimes the ambient hum of a busy workspace helps, but today the silence felt refreshing and allowed deeper focus without interruption.",

  "The commute home felt longer than usual, probably because traffic was heavier in the late afternoon. Sat in the car listening to a podcast about general productivity habits and thinking about how to structure the rest of the evening around errands and relaxation.",

  "Lunch was simple today — leftover soup from the weekend heated up in the microwave. It was satisfying enough, though not particularly exciting. Made a mental note to try that new salad place everyone keeps mentioning whenever the weather warms up a bit more.",

  "Spent a few minutes tidying the desk area before settling in for the afternoon. Clearing away old papers and rearranging the monitor stand made the whole workspace feel more inviting. Small environmental changes can make a surprisingly big difference in mood.",

  "Took a longer walk during the midday break than usual, looping around the block twice instead of once. The fresh air and movement helped reset focus for the second half of the day. Walking without headphones for once was a nice change of pace.",

  "The vending machine in the break room was out of the usual snack options, so ended up grabbing a granola bar from the bottom shelf instead. It was actually quite good — oats and dried fruit with a hint of cinnamon. Might become the new go-to choice.",

  "Rain started around mid-afternoon and the sound of it against the windows was oddly soothing. Watched the droplets trail down the glass for a minute before turning back to the screen. Rainy days have a particular rhythm that can be either calming or draining depending on mood.",

  "Had a brief hallway conversation with someone from a different floor about the building temperature. Apparently the heating system has been inconsistent all week. Agreed that layering clothes is the safest strategy when the thermostat seems to have a mind of its own.",

  "Noticed the houseplant on the windowsill is finally growing a new leaf after weeks of looking dormant. Gave it a bit of extra water and rotated the pot so the new growth faces the light. Small signs of progress in unexpected places can be oddly motivating.",

  "The elevator was out of service for part of the morning, which meant taking the stairs. It was a minor inconvenience but doubled as a bit of exercise. By the third trip up and down, the legs were definitely feeling it. Resolved to take stairs more often regardless.",

  "Tried a different tea blend this afternoon instead of the usual coffee. It had a mild floral note that was pleasant without being overpowering. Not sure it provided the same alertness boost, but it was a nice change from the standard caffeine routine.",

  "Spent the last few minutes of the workday organizing browser tabs and closing out windows that had been open for days. Digital clutter accumulates just as easily as physical clutter, and periodic cleanup sessions help maintain a sense of order and reduce cognitive load.",

  "The parking lot was nearly empty by the time the workday wrapped up. There is something peaceful about being one of the last to leave — the building settles into quiet and the transition from work mode to personal time feels more deliberate and intentional.",

  "Glanced at the whiteboard near the entrance on the way out and noticed someone had drawn a small cartoon in the corner. It was a stick figure holding a coffee mug with steam rising from it. These little anonymous contributions always bring a small smile.",

  "Dinner was a quick stir-fry thrown together from whatever vegetables were left in the fridge. The key to a good weeknight meal seems to be having a reliable sauce base and not overthinking the ingredient combination. Simplicity often wins over ambition in the kitchen.",

  "Sat outside for a few minutes after dinner watching the sky shift colors as the sun went down. The transition from warm oranges to deep blues never gets old, even on unremarkable evenings. Taking a moment to notice these things feels like a worthwhile habit to maintain.",

  "Before winding down for the night, laid out clothes and packed a bag for the next morning. Front-loading small decisions the evening before makes the start of the day smoother and removes friction from the morning routine when willpower is still warming up.",

  "The neighborhood was unusually quiet during the evening walk. Most houses had lights on but curtains drawn, giving the whole street a calm, settled feeling. The cool air carried a faint smell of someone grilling nearby, mixing with the scent of damp pavement.",
]

// ---------------------------------------------------------------------------
// Generator
// ---------------------------------------------------------------------------

const buildTopicFamilies = (numTopics: number): TopicFamily[] => {
  const families: TopicFamily[] = []
  for (let i = 0; i < numTopics; i++) {
    const def = TOPIC_DEFS[i % TOPIC_DEFS.length]
    families.push({
      id: i,
      name: `${def.name}-${i}`,
      template: def.template,
      slotKeys: def.slotKeys,
    })
  }
  return families
}

const fillSlots = (rng: Rng, def: TopicDef, usedPerSlot: Map<string, Set<string>>): Record<string, string> => {
  const filled: Record<string, string> = {}
  for (const key of def.slotKeys) {
    const pool = def.slotPools[key]
    const usedKey = `${def.name}:${key}`
    let used = usedPerSlot.get(usedKey)
    if (!used) {
      used = new Set()
      usedPerSlot.set(usedKey, used)
    }
    // Try to pick an unused value; fall back if pool is exhausted
    let value: string | undefined
    const available = pool.filter(v => !used.has(v))
    if (available.length > 0) {
      value = rng.pick(available)
    } else {
      // Reset and pick fresh
      used.clear()
      value = rng.pick(pool)
    }
    used.add(value)
    filled[key] = value
  }
  return filled
}

const renderTemplate = (template: string, slots: Record<string, string>): string => {
  let result = template
  for (const [key, value] of Object.entries(slots)) {
    result = result.replace(`{{${key}}}`, value)
  }
  return result
}

const wordCount = (text: string): number => text.split(/\s+/).filter(w => w.length > 0).length

const formatDate = (startDate: Date, dayIndex: number): string => {
  const d = new Date(startDate)
  d.setDate(d.getDate() + dayIndex)
  return d.toISOString().slice(0, 10)
}

const WEEKDAY_NAMES = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]

const generateEntries = (
  rng: Rng,
  numDays: number,
  numTopics: number,
  wordsPerDay: number,
  startDate: Date,
): DailyEntry[] => {
  const families = buildTopicFamilies(numTopics)
  const usedPerSlot = new Map<string, Set<string>>()
  const entries: DailyEntry[] = []

  for (let day = 0; day < numDays; day++) {
    const date = formatDate(startDate, day)
    const dayOfWeek = new Date(startDate)
    dayOfWeek.setDate(dayOfWeek.getDate() + day)
    const weekday = WEEKDAY_NAMES[dayOfWeek.getDay()]

    // Pick 2-4 topic families for this day
    const topicCount = rng.int(2, 4)
    const shuffled = rng.shuffle([...families])
    const dayTopics = shuffled.slice(0, topicCount)

    const sections: string[] = []
    const topicIds: number[] = []
    const slots: Record<string, string> = {}

    for (const family of dayTopics) {
      const defIndex = family.id % TOPIC_DEFS.length
      const def = TOPIC_DEFS[defIndex]
      const filled = fillSlots(rng, def, usedPerSlot)
      const rendered = renderTemplate(family.template, filled)
      sections.push(rendered)
      topicIds.push(family.id)
      for (const [key, value] of Object.entries(filled)) {
        slots[`${family.id}:${key}`] = value
      }
    }

    // Pad with filler to reach target word count
    let body = sections.join("\n\n")
    while (wordCount(body) < wordsPerDay) {
      body += " " + rng.pick(FILLER_SENTENCES)
    }

    const header = `# Day ${day + 1} — ${weekday}, ${date}\n\n`
    const content = header + body

    entries.push({ dayIndex: day, date, content, topicIds, slots })
  }

  return entries
}

// ---------------------------------------------------------------------------
// Question generation
// ---------------------------------------------------------------------------

const generateQuestions = (rng: Rng, entries: DailyEntry[], questionsPerCategory: number = 25): BenchmarkQuestion[] => {
  const questions: BenchmarkQuestion[] = []
  let qid = 0

  const addQ = (category: QueryCategory, question: string, answer: string, aliases: string[], sourceDays: number[]): void => {
    questions.push({ id: `q${qid++}`, category, question, answer, aliases, sourceDays })
  }

  const getTopicDef = (topicId: number): TopicDef => TOPIC_DEFS[topicId % TOPIC_DEFS.length]

  // Shared generator for template-based categories (exact & paraphrase)
  type SlotTemplate = { topicName: string, questionTemplate: string, answerSlot: string }
  const generateFromTemplates = (category: QueryCategory, templates: SlotTemplate[]): void => {
    for (const entry of rng.shuffle([...entries])) {
      for (const topicId of entry.topicIds) {
        const def = getTopicDef(topicId)
        const matched = templates.filter(t => t.topicName === def.name)
        if (matched.length === 0) continue
        const tmpl = rng.pick(matched)
        const value = entry.slots[`${topicId}:${tmpl.answerSlot}`]
        if (!value) continue
        addQ(category, tmpl.questionTemplate.replace("{{date}}", entry.date), value, [value.toLowerCase()], [entry.dayIndex])
      }
    }
  }

  // --- EXACT ATTRIBUTE questions ---
  generateFromTemplates("exact", [
    { topicName: "standup-meeting", questionTemplate: "On {{date}}, who reported progress in the standup meeting?", answerSlot: "person" },
    { topicName: "standup-meeting", questionTemplate: "What project was discussed in the standup on {{date}}?", answerSlot: "project" },
    { topicName: "code-review", questionTemplate: "What was the PR number reviewed on {{date}}?", answerSlot: "prNumber" },
    { topicName: "code-review", questionTemplate: "Who authored the PR reviewed on {{date}}?", answerSlot: "author" },
    { topicName: "deployment", questionTemplate: "Which service was deployed on {{date}}?", answerSlot: "service" },
    { topicName: "deployment", questionTemplate: "What was the build number for the deployment on {{date}}?", answerSlot: "buildNum" },
    { topicName: "incident", questionTemplate: "What was the severity of the incident on {{date}}?", answerSlot: "severity" },
    { topicName: "incident", questionTemplate: "What was the root cause of the incident on {{date}}?", answerSlot: "rootCause" },
    { topicName: "reading-notes", questionTemplate: "What book was being read on {{date}}?", answerSlot: "bookTitle" },
    { topicName: "exercise-log", questionTemplate: "What type of exercise was done on {{date}}?", answerSlot: "exerciseType" },
    { topicName: "cooking", questionTemplate: "What dish was cooked on {{date}}?", answerSlot: "dish" },
    { topicName: "travel-planning", questionTemplate: "What destination was being planned for travel on {{date}}?", answerSlot: "destination" },
    { topicName: "side-project", questionTemplate: "What side project was worked on {{date}}?", answerSlot: "projectName" },
    { topicName: "health", questionTemplate: "How many hours of sleep were logged on {{date}}?", answerSlot: "sleepHours" },
    { topicName: "learning", questionTemplate: "What subject was studied on {{date}}?", answerSlot: "subject" },
    { topicName: "music-practice", questionTemplate: "What instrument was practiced on {{date}}?", answerSlot: "instrument" },
    { topicName: "pet-care", questionTemplate: "What is the pet's name mentioned on {{date}}?", answerSlot: "petName" },
  ])

  // --- PARAPHRASE questions ---
  generateFromTemplates("paraphrase", [
    { topicName: "standup-meeting", questionTemplate: "During the daily sync around {{date}}, which team member gave their status update?", answerSlot: "person" },
    { topicName: "code-review", questionTemplate: "Around {{date}}, which part of the codebase was changed in the pull request that was reviewed?", answerSlot: "component" },
    { topicName: "deployment", questionTemplate: "Near {{date}}, which environment received a new deployment?", answerSlot: "environment" },
    { topicName: "incident", questionTemplate: "Around {{date}}, how many users were impacted by the production incident?", answerSlot: "usersAffected" },
    { topicName: "cooking", questionTemplate: "What was the special ingredient used when cooking around {{date}}?", answerSlot: "ingredient" },
    { topicName: "exercise-log", questionTemplate: "Where did the workout take place around {{date}}?", answerSlot: "location" },
    { topicName: "travel-planning", questionTemplate: "Which airline was booked for the trip planned around {{date}}?", answerSlot: "airline" },
    { topicName: "finance", questionTemplate: "What spending category was reviewed in the budget check around {{date}}?", answerSlot: "category" },
    { topicName: "meeting-notes", questionTemplate: "What architectural topic was the focus of the review meeting around {{date}}?", answerSlot: "topic" },
    { topicName: "side-project", questionTemplate: "What tech stack was used for the hobby project worked on around {{date}}?", answerSlot: "technology" },
    { topicName: "learning", questionTemplate: "What key concept was learned while studying around {{date}}?", answerSlot: "concept" },
  ])

  // --- TEMPORAL questions ---
  const topicIdList = [...new Set(entries.flatMap(e => e.topicIds))]
  for (const topicId of rng.shuffle([...topicIdList])) {
    const def = getTopicDef(topicId)
    const topicLabel = def.name.replace(/-/g, " ")
    const daysWithTopic = entries.filter(e => e.topicIds.includes(topicId)).sort((a, b) => a.dayIndex - b.dayIndex)
    if (daysWithTopic.length < 2) continue

    const first = daysWithTopic[0]
    addQ("temporal", `When was the first time a ${topicLabel} entry appeared in the daily log?`, first.date, [first.date, `day ${first.dayIndex + 1}`], [first.dayIndex])

    const last = daysWithTopic[daysWithTopic.length - 1]
    addQ("temporal", `When was the most recent ${topicLabel} entry in the daily log?`, last.date, [last.date, `day ${last.dayIndex + 1}`], [last.dayIndex])

    // "before/after" comparison questions between pairs of days
    if (daysWithTopic.length >= 3) {
      const shuffledDays = rng.shuffle([...daysWithTopic])
      for (let i = 0; i + 1 < shuffledDays.length; i += 2) {
        const dayA = shuffledDays[i]
        const dayB = shuffledDays[i + 1]
        if (dayA.dayIndex === dayB.dayIndex) continue
        const earlier = dayA.dayIndex < dayB.dayIndex ? dayA : dayB
        const later = dayA.dayIndex < dayB.dayIndex ? dayB : dayA
        addQ("temporal", `Which ${topicLabel} entry came first: the one on ${dayA.date} or the one on ${dayB.date}?`, `${earlier.date} came before ${later.date}`, [earlier.date], [earlier.dayIndex, later.dayIndex])
      }
    }
  }

  // --- MULTI-HOP questions ---
  for (const topicId of rng.shuffle([...topicIdList])) {
    const def = getTopicDef(topicId)
    const daysWithTopic = entries.filter(e => e.topicIds.includes(topicId)).sort((a, b) => a.dayIndex - b.dayIndex)
    if (daysWithTopic.length < 2) continue

    const shuffledDays = rng.shuffle([...daysWithTopic])
    for (let i = 0; i + 1 < shuffledDays.length; i += 2) {
      const day1 = shuffledDays[i]
      const day2 = shuffledDays[i + 1]
      if (day1.dayIndex === day2.dayIndex) continue

      for (const slotKey of rng.shuffle([...def.slotKeys])) {
        const fullKey = `${topicId}:${slotKey}`
        const val1 = day1.slots[fullKey]
        const val2 = day2.slots[fullKey]
        if (!val1 || !val2 || val1 === val2) continue

        const topicLabel = def.name.replace(/-/g, " ")
        addQ("multi-hop", `Compare the ${topicLabel} entries on ${day1.date} and ${day2.date}: what was the ${slotKey.replace(/([A-Z])/g, " $1").toLowerCase()} on each day?`, `${day1.date}: ${val1}, ${day2.date}: ${val2}`, [val1.toLowerCase(), val2.toLowerCase()], [day1.dayIndex, day2.dayIndex])
        break // one question per day-pair
      }
    }
  }

  // --- NEGATIVE questions ---
  const negativeTopics = [
    "What cryptocurrency trades were logged in the daily journal?",
    "When did the daily log mention a scuba diving session?",
    "What space launch was discussed in any daily entry?",
    "Which Formula 1 race results were recorded?",
    "When was ice fishing mentioned in the daily notes?",
    "What knitting patterns were tried according to the daily logs?",
    "Which opera performances were attended based on the daily entries?",
    "What drone photography sessions were logged?",
    "When was beekeeping discussed in the daily journal?",
    "What board game tournament results were recorded?",
    "Which wine tasting notes appear in the daily log?",
    "When was pottery class mentioned in any daily entry?",
    "What rock climbing routes were attempted according to the logs?",
    "Which astronomy observations were noted in the daily journal?",
    "When was paragliding discussed in any entry?",
  ]
  for (const q of rng.shuffle([...negativeTopics]).slice(0, 15)) {
    addQ("negative", q, "No information available", ["no information", "not mentioned", "no record", "none"], [])
  }

  // --- Balance: trim each active category to questionsPerCategory ---
  const balanced: BenchmarkQuestion[] = []
  for (const cat of ["exact", "paraphrase", "temporal", "multi-hop"] as const) {
    balanced.push(...rng.shuffle(questions.filter(q => q.category === cat)).slice(0, questionsPerCategory))
  }
  balanced.push(...questions.filter(q => q.category === "negative"))

  return rng.shuffle(balanced)
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

export interface GenerateOptions {
  seed: number
  numDays: number
  numTopics: number
  wordsPerDay: number
  startDate: string
  outDir: string
  /** Target number of questions per active category (default: 25). */
  questionsPerCategory?: number
}

export const generate = async (opts: GenerateOptions): Promise<DatasetManifest> => {
  const rng = new Rng(opts.seed)
  const start = new Date(opts.startDate)
  const entries = generateEntries(rng, opts.numDays, opts.numTopics, opts.wordsPerDay, start)
  const questions = generateQuestions(rng, entries, opts.questionsPerCategory)

  const manifest: DatasetManifest = {
    seed: opts.seed,
    numDays: opts.numDays,
    numTopics: opts.numTopics,
    wordsPerDay: opts.wordsPerDay,
    startDate: opts.startDate,
    entries,
    questions,
  }

  // Write corpus files to disk
  const corpusDir = join(opts.outDir, "corpus")
  await mkdir(corpusDir, { recursive: true })
  for (const entry of entries) {
    const filename = `${entry.date}.md`
    await writeFile(join(corpusDir, filename), entry.content, "utf-8")
  }

  // Write manifest
  await writeFile(join(opts.outDir, "manifest.json"), JSON.stringify(manifest, null, 2), "utf-8")

  // Write questions separately for convenience
  await writeFile(join(opts.outDir, "questions.json"), JSON.stringify(questions, null, 2), "utf-8")

  return manifest
}
