#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"

# ---------------------------------------------------------------------------
# Configuration (override via env vars)
# ---------------------------------------------------------------------------
MEM9_BASE_URL="${MEM9_BASE_URL:-https://api.mem9.ai}"
MEM9_BASE_URL="${MEM9_BASE_URL%/}"
MEM9_SPACE_ID=""
SEED="${BENCH_DM_SEED:-42}"
NUM_DAYS="${BENCH_DM_DAYS:-30}"
NUM_TOPICS="${BENCH_DM_TOPICS:-10}"
WORDS_PER_DAY="${BENCH_DM_WORDS:-200}"
START_DATE="${BENCH_DM_START_DATE:-2024-01-01}"
MAX_QUESTIONS="${BENCH_DM_MAX_QUESTIONS:-20}"
PROMPT_TIMEOUT="${BENCH_DM_TIMEOUT:-120}"
CORPUS_DIR="${BENCH_DM_CORPUS_DIR:-}"
PROFILE_A="dm_e2e_a"
PROFILE_B="dm_e2e_b"
PORT_A=52789
PORT_B=53789
GATEWAY_TOKEN="dm-bench-$(date +%s)"

# ---------------------------------------------------------------------------
# Phase 1: Preflight checks
# ---------------------------------------------------------------------------
echo "=== Phase 1: Preflight checks ==="

if [[ -z "${CLAUDE_CODE_TOKEN:-}" ]]; then
  echo "ERROR: CLAUDE_CODE_TOKEN is required but not set."
  echo "  export CLAUDE_CODE_TOKEN='your-api-key'"
  exit 1
fi

for cmd in curl openclaw python3; do
  command -v "$cmd" >/dev/null 2>&1 || {
    echo "ERROR: $cmd is required but not installed."
    exit 1
  }
done

echo "    All preflight checks passed."

# ---------------------------------------------------------------------------
# Phase 2: Generate corpus
# ---------------------------------------------------------------------------
echo "=== Phase 2: Generate corpus ==="

RUN_TS="$(date -u +%Y%m%d-%H%M%S)"

if [[ -z "$CORPUS_DIR" ]]; then
  CORPUS_DIR="$ROOT/benchmark/results/dm-e2e-${RUN_TS}-corpus"
  echo "    Generating corpus → $CORPUS_DIR"
  echo "    seed=$SEED days=$NUM_DAYS topics=$NUM_TOPICS words/day=$WORDS_PER_DAY"

  cd "$ROOT/benchmark/daily-memory"

  if [[ ! -d node_modules ]]; then
    echo "    Installing daily-memory dependencies..."
    npm install --silent 2>&1 | tail -1
  fi

  npx tsx "$ROOT/benchmark/scripts/generate-corpus.mts" \
    --seed "$SEED" \
    --days "$NUM_DAYS" \
    --topics "$NUM_TOPICS" \
    --words "$WORDS_PER_DAY" \
    --start-date "$START_DATE" \
    --out-dir "$CORPUS_DIR"

  cd "$ROOT"
else
  echo "    Using existing corpus: $CORPUS_DIR"
  if [[ ! -f "$CORPUS_DIR/manifest.json" ]]; then
    echo "ERROR: $CORPUS_DIR/manifest.json not found."
    exit 1
  fi
fi

MANIFEST_FILE="$CORPUS_DIR/manifest.json"
echo "    Manifest: $MANIFEST_FILE"

# ---------------------------------------------------------------------------
# Phase 3: Cleanup leftover profiles
# ---------------------------------------------------------------------------
echo "=== Phase 3: Cleanup leftover profiles ==="

for profile in "$PROFILE_A" "$PROFILE_B"; do
  if openclaw --profile "$profile" health >/dev/null 2>&1; then
    echo "    Stopping leftover gateway for profile: $profile"
    openclaw --profile "$profile" gateway stop 2>/dev/null || true
  fi
  profile_dir="$HOME/.openclaw-${profile}"
  workspace_dir="$HOME/.openclaw/workspace-${profile}"
  if [[ -d "$profile_dir" ]]; then
    echo "    Removing profile dir: $profile_dir"
    rm -rf "$profile_dir"
  fi
  if [[ -d "$workspace_dir" ]]; then
    echo "    Removing workspace dir: $workspace_dir"
    rm -rf "$workspace_dir"
  fi
done

echo "    Cleanup complete."

# ---------------------------------------------------------------------------
# Phase 4: Provision fresh mem9 space
# ---------------------------------------------------------------------------
echo "=== Phase 4: Provision mem9 space ==="

echo "    mem9 base URL: $MEM9_BASE_URL"
echo "    Provisioning fresh mem9 space..."
TENANT_RESP=$(curl -sf -X POST "${MEM9_BASE_URL}/v1alpha1/mem9s")
MEM9_SPACE_ID=$(echo "$TENANT_RESP" | python3 -c "import sys,json; print(json.load(sys.stdin)['id'])")

if [[ -z "$MEM9_SPACE_ID" || "$MEM9_SPACE_ID" == "None" ]]; then
  echo "ERROR: Failed to provision mem9 space:"
  echo "$TENANT_RESP"
  exit 1
fi

echo "    Fresh space ID: $MEM9_SPACE_ID"

# ---------------------------------------------------------------------------
# Phase 5: Create profiles
# ---------------------------------------------------------------------------
echo "=== Phase 5: Create OpenClaw profiles ==="

echo "--- Configuring profile A (baseline, port $PORT_A)"
openclaw --profile "$PROFILE_A" config set gateway.mode local
openclaw --profile "$PROFILE_A" config set gateway.port "$PORT_A"
openclaw --profile "$PROFILE_A" config set gateway.auth.token "$GATEWAY_TOKEN"
openclaw --profile "$PROFILE_A" config set agents.defaults.model.primary "anthropic/claude-sonnet-4-6"
echo "ANTHROPIC_API_KEY=${CLAUDE_CODE_TOKEN}" > "$HOME/.openclaw-${PROFILE_A}/.env"
echo "    Wrote API key to $HOME/.openclaw-${PROFILE_A}/.env"

echo "--- Configuring profile B (treatment, port $PORT_B)"
openclaw --profile "$PROFILE_B" config set gateway.mode local
openclaw --profile "$PROFILE_B" config set gateway.port "$PORT_B"
openclaw --profile "$PROFILE_B" config set gateway.auth.token "$GATEWAY_TOKEN"
openclaw --profile "$PROFILE_B" config set agents.defaults.model.primary "anthropic/claude-sonnet-4-6"
echo "ANTHROPIC_API_KEY=${CLAUDE_CODE_TOKEN}" > "$HOME/.openclaw-${PROFILE_B}/.env"
echo "    Wrote API key to $HOME/.openclaw-${PROFILE_B}/.env"

echo "--- Installing mem9 plugin into profile B"
openclaw --profile "$PROFILE_B" plugins install --link "$ROOT/openclaw-plugin"
openclaw --profile "$PROFILE_B" config set --strict-json plugins.allow '["mem9"]'
openclaw --profile "$PROFILE_B" config set plugins.slots.memory mem9
openclaw --profile "$PROFILE_B" config set plugins.entries.mem9.enabled true
openclaw --profile "$PROFILE_B" config set plugins.entries.mem9.config.apiUrl "$MEM9_BASE_URL"
openclaw --profile "$PROFILE_B" config set plugins.entries.mem9.config.tenantID "$MEM9_SPACE_ID"

openclaw --profile "$PROFILE_A" daemon install
openclaw --profile "$PROFILE_B" daemon install
openclaw --profile "$PROFILE_A" daemon restart
openclaw --profile "$PROFILE_B" daemon restart
openclaw --profile "$PROFILE_A" gateway restart
openclaw --profile "$PROFILE_B" gateway restart

# ---------------------------------------------------------------------------
# Phase 6: Workspace setup
# ---------------------------------------------------------------------------
echo "=== Phase 6: Workspace setup ==="

for profile in "$PROFILE_A" "$PROFILE_B"; do
  ws_dir="$HOME/.openclaw/workspace-${profile}"
  mkdir -p "$ws_dir"
  cp "$ROOT/benchmark/workspace/SOUL.md" "$ws_dir/"
  cp "$ROOT/benchmark/workspace/IDENTITY.md" "$ws_dir/"
  cp "$ROOT/benchmark/workspace/USER.md" "$ws_dir/"
  echo "    Copied base workspace files to $ws_dir"
done

# Copy daily corpus files ONLY to Profile A workspace
WS_A="$HOME/.openclaw/workspace-${PROFILE_A}"
mkdir -p "$WS_A/daily-notes"
cp "$CORPUS_DIR/corpus"/*.md "$WS_A/daily-notes/"
CORPUS_COUNT=$(ls "$CORPUS_DIR/corpus"/*.md 2>/dev/null | wc -l)
echo "    Copied $CORPUS_COUNT daily corpus files to Profile A: $WS_A/daily-notes/"

# Add a context file so Profile A's agent knows where the daily notes are
cat > "$WS_A/NOTES.md" << 'CONTEXT_EOF'
# Daily Notes

My daily activity logs are saved in the `daily-notes/` directory.
Each file is named `day-NNN_YYYY-MM-DD.md` and contains that day's activities,
meetings, work, and personal notes. Search these files to answer questions
about past activities.
CONTEXT_EOF
echo "    Created NOTES.md context file in Profile A workspace"

# ---------------------------------------------------------------------------
# Phase 7: Smart-ingest corpus into mem9 for Profile B
# ---------------------------------------------------------------------------
echo "=== Phase 7: Smart-ingest corpus into mem9 ==="

python3 "$ROOT/benchmark/scripts/smart-ingest-corpus.py" \
  --corpus-dir "$CORPUS_DIR/corpus" \
  --base-url "$MEM9_BASE_URL" \
  --tenant-id "$MEM9_SPACE_ID" \
  --agent-id "daily-memory-bench"

# Allow a short settling period for smart-ingest processing
echo "    Waiting 10s for smart-ingest processing..."
sleep 10

# ---------------------------------------------------------------------------
# Phase 8: Start gateways
# ---------------------------------------------------------------------------
echo "=== Phase 8: Start gateways ==="

GW_A_LOG="/tmp/dm-e2e-gw-a.log"
GW_B_LOG="/tmp/dm-e2e-gw-b.log"

echo "--- Starting gateway A (baseline) on port $PORT_A"
nohup openclaw --profile "$PROFILE_A" gateway > "$GW_A_LOG" 2>&1 &
GW_A_PID=$!
echo "    Gateway A pid: $GW_A_PID  log: $GW_A_LOG"

echo "--- Starting gateway B (treatment) on port $PORT_B"
nohup openclaw --profile "$PROFILE_B" gateway > "$GW_B_LOG" 2>&1 &
GW_B_PID=$!
echo "    Gateway B pid: $GW_B_PID  log: $GW_B_LOG"

echo "--- Waiting for gateways to be healthy..."
for gw_port in "$PORT_A" "$PORT_B"; do
  for i in $(seq 1 60); do
    if curl -sf "http://localhost:${gw_port}/health" >/dev/null 2>&1; then
      echo "    Gateway on port $gw_port ready."
      break
    fi
    if [[ "$gw_port" == "$PORT_A" ]] && ! kill -0 "$GW_A_PID" 2>/dev/null; then
      echo "ERROR: Gateway A exited unexpectedly. Logs:"; tail -30 "$GW_A_LOG"
      exit 1
    fi
    if [[ "$gw_port" == "$PORT_B" ]] && ! kill -0 "$GW_B_PID" 2>/dev/null; then
      echo "ERROR: Gateway B exited unexpectedly. Logs:"; tail -30 "$GW_B_LOG"
      exit 1
    fi
    sleep 1
  done
  if ! curl -sf "http://localhost:${gw_port}/health" >/dev/null 2>&1; then
    echo "ERROR: Gateway on port $gw_port failed to start within 60s."
    exit 1
  fi
done

# ---------------------------------------------------------------------------
# Phase 9: Run benchmark questions
# ---------------------------------------------------------------------------
echo "=== Phase 9: Drive benchmark questions ==="

RESULTS_DIR="$ROOT/benchmark/results/dm-e2e-${RUN_TS}"
mkdir -p "$RESULTS_DIR"

python3 "$ROOT/benchmark/scripts/drive-daily-memory-e2e.py" \
  --manifest-file "$MANIFEST_FILE" \
  --results-dir "$RESULTS_DIR" \
  --profile-a "$PROFILE_A" \
  --profile-b "$PROFILE_B" \
  --timeout "$PROMPT_TIMEOUT" \
  --max-questions "$MAX_QUESTIONS"

# ---------------------------------------------------------------------------
# Phase 10: Summary
# ---------------------------------------------------------------------------
echo ""
echo "============================================================"
echo "  Daily-Memory E2E Benchmark complete!"
echo "============================================================"
echo ""
echo "  Configuration:"
echo "    seed=$SEED  days=$NUM_DAYS  topics=$NUM_TOPICS  words/day=$WORDS_PER_DAY"
echo "    max_questions=$MAX_QUESTIONS  timeout=${PROMPT_TIMEOUT}s"
echo ""
echo "  mem9:"
echo "    Base URL:  $MEM9_BASE_URL"
echo "    Space ID:  $MEM9_SPACE_ID"
echo ""
echo "  Output:"
echo "    Corpus:      $CORPUS_DIR"
echo "    Results:     $RESULTS_DIR"
echo "    HTML report: $RESULTS_DIR/report.html"
echo "    Transcript:  $RESULTS_DIR/transcript.md"
echo "    JSON output: $RESULTS_DIR/benchmark-results.json"
echo ""
echo "  Running processes:"
echo "    Gateway A    pid=$GW_A_PID   port=$PORT_A (baseline/files)"
echo "    Gateway B    pid=$GW_B_PID   port=$PORT_B (treatment/mem9)"
echo ""
echo "  Web UIs:"
echo "    Baseline:    http://localhost:$PORT_A  (password: $GATEWAY_TOKEN)"
echo "    Treatment:   http://localhost:$PORT_B  (password: $GATEWAY_TOKEN)"
echo "============================================================"
