#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# ---------------------------------------------------------------------------
# Configuration (override via env vars)
# ---------------------------------------------------------------------------
MEM9_BASE_URL="${MEM9_BASE_URL:-https://api.mem9.ai}"
MEM9_BASE_URL="${MEM9_BASE_URL%/}"
MEM9_SPACE_ID=""
SEED="${BENCH_DM_SEED:-42}"
NUM_DAYS="${BENCH_DM_DAYS:-30}"
NUM_TOPICS="${BENCH_DM_TOPICS:-10}"
WORDS_PER_DAY="${BENCH_DM_WORDS:-500}"
START_DATE="${BENCH_DM_START_DATE:-2024-01-01}"
MAX_QUESTIONS="${BENCH_DM_MAX_QUESTIONS:-20}"
PROMPT_TIMEOUT="${BENCH_DM_TIMEOUT:-300}"
SKIP_NEGATIVE="${BENCH_DM_SKIP_NEGATIVE:-true}"
CORPUS_DIR="${BENCH_DM_CORPUS_DIR:-}"
FAST_INGEST="${BENCH_DM_FAST_INGEST:-0}"
CONTEXT_ENGINE="${BENCH_DM_CONTEXT_ENGINE:-0}"
LLM_MODEL="${BENCH_LLM_MODEL:-anthropic/claude-sonnet-4-6}"
PROFILE_A="mem9_bench_a"
PROFILE_B="mem9_bench_b"
PORT_A=52789
PORT_B=53789
GATEWAY_TOKEN="bench"
OC_AUTH="${OC_AUTH:-$HOME/.openclaw/agents/main/agent/auth-profiles.json}"
RUN_TS=""
MANIFEST_FILE=""
RESULTS_DIR=""

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

cleanup_leftover_processes() {
  echo "--- Cleaning up leftover processes from previous runs..."
  for profile in "$PROFILE_A" "$PROFILE_B"; do
    openclaw --profile "$profile" gateway stop 2>/dev/null || true
    openclaw --profile "$profile" daemon stop 2>/dev/null || true
    openclaw --profile "$profile" daemon uninstall 2>/dev/null || true
  done
}

# ---------------------------------------------------------------------------
# Phase 1: Preflight checks
# ---------------------------------------------------------------------------
phase_preflight() {
  echo "=== Phase 1: Preflight checks ==="

  if [[ -z "${CLAUDE_CODE_TOKEN:-}" && ! -f "$OC_AUTH" ]]; then
    echo "ERROR: No Anthropic API key found. Provide one of:"
    echo "  1. export CLAUDE_CODE_TOKEN='sk-ant-...'"
    echo "  2. Configure anthropic:default in OpenClaw (openclaw auth login anthropic)"
    exit 1
  fi

  if [[ -n "${CLAUDE_CODE_TOKEN:-}" ]]; then
    echo "    API key resolved (${CLAUDE_CODE_TOKEN:0:12}...)"
  fi
  if [[ -f "$OC_AUTH" ]]; then
    echo "    Auth profiles found: $OC_AUTH"
  fi

  for cmd in curl openclaw python3; do
    command -v "$cmd" >/dev/null 2>&1 || {
      echo "ERROR: $cmd is required but not installed."
      exit 1
    }
  done

  echo "    All preflight checks passed."
}

# ---------------------------------------------------------------------------
# Phase 2: Generate corpus
# ---------------------------------------------------------------------------
phase_generate_corpus() {
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

    npx tsx "$SCRIPT_DIR/generate-corpus.mts" \
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
}

# ---------------------------------------------------------------------------
# Phase 3: Cleanup leftover profiles
# ---------------------------------------------------------------------------
phase_cleanup_profiles() {
  echo "=== Phase 3: Cleanup leftover profiles ==="

  for profile in "$PROFILE_A" "$PROFILE_B"; do
    echo "    Stopping leftover gateway/daemon for profile: $profile"
    openclaw --profile "$profile" gateway stop 2>/dev/null || true
    openclaw --profile "$profile" daemon stop 2>/dev/null || true
    openclaw --profile "$profile" daemon uninstall 2>/dev/null || true
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

  # Kill any orphan processes still holding the benchmark ports
  for port in "$PORT_A" "$PORT_B"; do
    pid=$(ss -tlnp "sport = :${port}" 2>/dev/null | grep -oP 'pid=\K[0-9]+' | head -1 || true)
    if [[ -n "$pid" ]]; then
      echo "    Killing orphan process $pid on port $port"
      kill "$pid" 2>/dev/null || true
      sleep 1
    fi
  done

  echo "    Cleanup complete."
}

# ---------------------------------------------------------------------------
# Phase 4: Provision fresh mem9 space
# ---------------------------------------------------------------------------
phase_provision_space() {
  echo "=== Phase 4: Provision mem9 space ==="

  echo "    mem9 base URL: $MEM9_BASE_URL"
  echo "    Provisioning fresh mem9 space..."
  local tenant_resp
  tenant_resp=$(curl -sf -X POST "${MEM9_BASE_URL}/v1alpha1/mem9s")
  MEM9_SPACE_ID=$(echo "$tenant_resp" | python3 -c "import sys,json; print(json.load(sys.stdin)['id'])")

  if [[ -z "$MEM9_SPACE_ID" || "$MEM9_SPACE_ID" == "None" ]]; then
    echo "ERROR: Failed to provision mem9 space:"
    echo "$tenant_resp"
    exit 1
  fi

  echo "    Fresh space ID: $MEM9_SPACE_ID"
}

# ---------------------------------------------------------------------------
# Phase 5: Create profiles
# ---------------------------------------------------------------------------
phase_create_profiles() {
  echo "=== Phase 5: Create OpenClaw profiles ==="

  echo "--- Configuring profile A (baseline, port $PORT_A)"
  openclaw --profile "$PROFILE_A" config set gateway.mode local
  openclaw --profile "$PROFILE_A" config set gateway.port "$PORT_A"
  openclaw --profile "$PROFILE_A" config set gateway.auth.token "$GATEWAY_TOKEN"
  openclaw --profile "$PROFILE_A" config set agents.defaults.model.primary "$LLM_MODEL"
  # openclaw --profile "$PROFILE_A" config set agents.defaults.memorySearch.provider local
  if [[ -n "${CLAUDE_CODE_TOKEN:-}" ]]; then
    echo "ANTHROPIC_API_KEY=${CLAUDE_CODE_TOKEN}" > "$HOME/.openclaw-${PROFILE_A}/.env"
    echo "    Wrote API key to $HOME/.openclaw-${PROFILE_A}/.env"
  fi

  # Copy auth-profiles.json so the profile's daemon can authenticate
  if [[ -f "$OC_AUTH" ]]; then
    mkdir -p "$HOME/.openclaw-${PROFILE_A}/agents/main/agent"
    cp "$OC_AUTH" "$HOME/.openclaw-${PROFILE_A}/agents/main/agent/auth-profiles.json"
    echo "    Copied auth-profiles.json to profile A"
  fi

  echo "--- Configuring profile B (treatment, port $PORT_B)"
  openclaw --profile "$PROFILE_B" config set gateway.mode local
  openclaw --profile "$PROFILE_B" config set gateway.port "$PORT_B"
  openclaw --profile "$PROFILE_B" config set gateway.auth.token "$GATEWAY_TOKEN"
  openclaw --profile "$PROFILE_B" config set agents.defaults.model.primary "$LLM_MODEL"
  if [[ -n "${CLAUDE_CODE_TOKEN:-}" ]]; then
    echo "ANTHROPIC_API_KEY=${CLAUDE_CODE_TOKEN}" > "$HOME/.openclaw-${PROFILE_B}/.env"
    echo "    Wrote API key to $HOME/.openclaw-${PROFILE_B}/.env"
  fi

  # Copy auth-profiles.json so the profile's daemon can authenticate
  if [[ -f "$OC_AUTH" ]]; then
    mkdir -p "$HOME/.openclaw-${PROFILE_B}/agents/main/agent"
    cp "$OC_AUTH" "$HOME/.openclaw-${PROFILE_B}/agents/main/agent/auth-profiles.json"
    echo "    Copied auth-profiles.json to profile B"
  fi

  echo "--- Installing mem9 plugin into profile B"
  openclaw --profile "$PROFILE_B" plugins install --link "$ROOT/openclaw-plugin"
  openclaw --profile "$PROFILE_B" config set --strict-json plugins.allow '["mem9"]'
  openclaw --profile "$PROFILE_B" config set plugins.slots.memory mem9
  openclaw --profile "$PROFILE_B" config set plugins.entries.mem9.enabled true
  openclaw --profile "$PROFILE_B" config set plugins.entries.mem9.config.apiUrl "$MEM9_BASE_URL"
  openclaw --profile "$PROFILE_B" config set plugins.entries.mem9.config.tenantID "$MEM9_SPACE_ID"

  if [[ "$CONTEXT_ENGINE" == "1" ]]; then
    echo "    Enabling contextEngine slot for profile B"
    openclaw --profile "$PROFILE_B" config set plugins.slots.contextEngine mem9
  fi

  openclaw --profile "$PROFILE_A" daemon install
  openclaw --profile "$PROFILE_B" daemon install
  openclaw --profile "$PROFILE_A" daemon restart
  openclaw --profile "$PROFILE_B" daemon restart
  openclaw --profile "$PROFILE_A" gateway restart
  openclaw --profile "$PROFILE_B" gateway restart

  echo "--- Waiting for gateways to be healthy..."
  for gw_port in "$PORT_A" "$PORT_B"; do
    for i in $(seq 1 60); do
      if curl -sf "http://localhost:${gw_port}/health" >/dev/null 2>&1; then
        echo "    Gateway on port $gw_port ready."
        break
      fi
      sleep 1
    done
    if ! curl -sf "http://localhost:${gw_port}/health" >/dev/null 2>&1; then
      echo "ERROR: Gateway on port $gw_port failed to start within 60s."
      exit 1
    fi
  done
}

# ---------------------------------------------------------------------------
# Phase 6: Workspace setup
# ---------------------------------------------------------------------------
phase_workspace_setup() {
  echo "=== Phase 6: Workspace setup ==="

  for profile in "$PROFILE_A" "$PROFILE_B"; do
    ws_dir="$HOME/.openclaw/workspace-${profile}"
    mkdir -p "$ws_dir"
    cp "$ROOT/benchmark/workspace/SOUL.md" "$ws_dir/"
    cp "$ROOT/benchmark/workspace/IDENTITY.md" "$ws_dir/"
    cp "$ROOT/benchmark/workspace/USER.md" "$ws_dir/"
    echo "    Copied base workspace files to $ws_dir"
  done

  if [[ "$FAST_INGEST" == "1" ]]; then
    # Copy daily corpus files to both profile workspaces
    for profile in "$PROFILE_A" "$PROFILE_B"; do
      ws_dir="$HOME/.openclaw/workspace-${profile}"
      mkdir -p "$ws_dir/memory"
      cp "$CORPUS_DIR/corpus"/*.md "$ws_dir/memory/"
      echo "    Copied corpus files to $ws_dir/memory/"
    done
    CORPUS_COUNT=$(ls "$CORPUS_DIR/corpus"/*.md 2>/dev/null | wc -l)
    echo "    Total: $CORPUS_COUNT daily corpus files per profile"

    # Add a context file so the agent knows where the daily notes are
    for profile in "$PROFILE_A" "$PROFILE_B"; do
      ws_dir="$HOME/.openclaw/workspace-${profile}"
      cat > "$ws_dir/NOTES.md" << 'CONTEXT_EOF'
# Daily Notes

My daily activity logs are saved in the `memory/` directory.
Each file is named `YYYY-MM-DD.md` and contains that day's activities,
meetings, work, and personal notes. Search these files to answer questions
about past activities.
CONTEXT_EOF
    done
    echo "    Created NOTES.md context file in both profile workspaces"
  fi

  # Build memory index when using fast ingest
  if [[ "$FAST_INGEST" == "1" ]]; then
    echo "=== Phase 6b: Build memory index for both profiles ==="
    for profile in "$PROFILE_A" "$PROFILE_B"; do
      echo "    Building index for $profile..."
      openclaw --profile "$profile" memory index --force
      openclaw --profile "$profile" memory status
    done
  fi
}

# ---------------------------------------------------------------------------
# Phase 7: Ingest corpus into mem9 for Profile B
# ---------------------------------------------------------------------------
phase_ingest_corpus() {
  if [[ "$FAST_INGEST" == "1" ]]; then
    echo "=== Phase 7: Import corpus into mem9 ==="

    python3 "$SCRIPT_DIR/smart-ingest-corpus.py" \
      --corpus-dir "$CORPUS_DIR/corpus" \
      --base-url "$MEM9_BASE_URL" \
      --tenant-id "$MEM9_SPACE_ID" \
      --agent-id "daily-memory-bench"

    # Allow a short settling period for import processing
    echo "    Waiting 10s for import processing..."
    sleep 10
  else
    echo "=== Phase 7: Chat-ingest corpus into both profiles ==="

    python3 "$SCRIPT_DIR/chat-ingest-corpus.py" \
      --corpus-dir "$CORPUS_DIR/corpus" \
      --profile-a "$PROFILE_A" \
      --profile-b "$PROFILE_B" \
      --timeout "$PROMPT_TIMEOUT"
  fi
}

# ---------------------------------------------------------------------------
# Phase 8: Run benchmark questions
# ---------------------------------------------------------------------------
phase_drive_questions() {
  echo "=== Phase 8: Drive benchmark questions ==="

  RESULTS_DIR="$ROOT/benchmark/results/dm-e2e-${RUN_TS}"
  mkdir -p "$RESULTS_DIR"

  python3 "$SCRIPT_DIR/drive-daily-memory-e2e.py" \
    --manifest-file "$MANIFEST_FILE" \
    --results-dir "$RESULTS_DIR" \
    --profile-a "$PROFILE_A" \
    --profile-b "$PROFILE_B" \
    --timeout "$PROMPT_TIMEOUT" \
    --max-questions "$MAX_QUESTIONS" \
    --skip-negative "$SKIP_NEGATIVE"
}

# ---------------------------------------------------------------------------
# Phase 9: Summary
# ---------------------------------------------------------------------------
phase_summary() {
  echo ""
  echo "============================================================"
  echo "  Daily-Memory E2E Benchmark complete!"
  echo "============================================================"
  echo ""
  echo "  Configuration:"
  echo "    seed=$SEED  days=$NUM_DAYS  topics=$NUM_TOPICS  words/day=$WORDS_PER_DAY"
  echo "    max_questions=$MAX_QUESTIONS  timeout=${PROMPT_TIMEOUT}s  skip_negative=$SKIP_NEGATIVE  fast_ingest=$FAST_INGEST  context_engine=$CONTEXT_ENGINE"
  echo "    model=$LLM_MODEL"
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
  echo "  Gateways (managed by systemd):"
  echo "    Gateway A    port=$PORT_A (baseline/files)"
  echo "    Gateway B    port=$PORT_B (treatment/mem9)"
  echo ""
  echo "  Web UIs:"
  echo "    Baseline:    http://localhost:$PORT_A  (password: $GATEWAY_TOKEN)"
  echo "    Treatment:   http://localhost:$PORT_B  (password: $GATEWAY_TOKEN)"
  echo "============================================================"
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
main() {
  cleanup_leftover_processes
  phase_preflight
  phase_generate_corpus
  phase_cleanup_profiles
  phase_provision_space
  phase_create_profiles
  phase_workspace_setup
  phase_ingest_corpus
  phase_drive_questions
  phase_summary
}

main "$@"
