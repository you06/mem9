// memory_kv.go — K=>V experimental store/recall implementations
// (step 4 of the K=>V refactor).
//
// These are the actual implementations that the public Create/Search
// service methods now delegate to (step 4 replaced internals; the
// public API surface and signatures are unchanged so HTTP/CLI/dashboard
// callers are not affected).
//
// Repository access: the K=>V helpers require methods that live only
// on *tidb.MemoryRepo (UpsertMemoryValue / InsertMemoryKeys /
// RecallKV / MarkKeysExtracted). The service holds a generic
// repository.MemoryRepo interface; we type-assert here. On non-tidb
// backends (postgres, db9) the type assertion fails and the K=>V path
// returns a clear error — there's no fallback because the K=>V
// experiment requires TiDB-specific FTS + VECTOR features.

package service

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/qiffang/mnemos/server/internal/domain"
	"github.com/qiffang/mnemos/server/internal/extractkeys"
	"github.com/qiffang/mnemos/server/internal/keynorm"
	"github.com/qiffang/mnemos/server/internal/repository/tidb"
)

// ErrKVBackendUnsupported is returned by the K=>V helpers when the
// service was constructed with a repository that doesn't satisfy
// *tidb.MemoryRepo (e.g. postgres / db9). Step 4 of the K=>V refactor
// is TiDB-only.
var ErrKVBackendUnsupported = errors.New("service: K=>V path requires the TiDB memory repository")

// kvRepo returns the K=>V-capable repository, or an error if the
// configured backend doesn't support it.
func (s *MemoryService) kvRepo() (*tidb.MemoryRepo, error) {
	r, ok := s.memories.(*tidb.MemoryRepo)
	if !ok {
		return nil, ErrKVBackendUnsupported
	}
	return r, nil
}

// storeKV is the K=>V implementation of "store a single memory":
// embed → extract K → upsert V → insert K → mark extracted.
//
// Behavior contract (step 4 lock):
//   - Always produces exactly 1 V. content_hash dedup at the repo
//     layer handles the "same content seen before" case (returns
//     IsNew=false with the existing V's id).
//   - V embedding is computed eagerly via s.embedder (when configured)
//     so VAL_VEC ablation has the column populated. autoModel skips
//     this — TiDB generates the column server-side.
//   - K extraction is best-effort: when LLM is unavailable or
//     extraction fails, the V is still written; keys_extracted_at is
//     left NULL so the V can be recall'd via VAL_FTS only and a future
//     update can trigger re-extraction.
func (s *MemoryService) storeKV(
	ctx context.Context,
	agentID, appID, content string,
	tags []string,
	metadata []byte,
) (*domain.Memory, error) {
	repo, err := s.kvRepo()
	if err != nil {
		// Backend doesn't support K=>V (postgres / db9 / test mocks).
		// Fall back to legacy repo.Create against the old `memories`
		// table. The caller still gets a valid *domain.Memory; only the
		// K=>V indexing layer is unavailable. Production TiDB deployments
		// always have *tidb.MemoryRepo and won't hit this branch.
		return s.storeLegacyFallback(ctx, agentID, appID, content, tags, metadata)
	}

	// Step 1: embed (eager, so VAL_VEC works in future ablation).
	var embedding []float32
	if s.autoModel == "" && s.embedder != nil {
		v, err := s.embedder.Embed(ctx, content)
		if err != nil {
			return nil, fmt.Errorf("store_kv embed: %w", err)
		}
		embedding = v
	}

	// Step 2: upsert V (content_hash dedup at repo layer).
	now := time.Now()
	mem := &domain.Memory{
		ID:         uuid.New().String(),
		Content:    content,
		Source:     agentID,
		Tags:       tags,
		Metadata:   metadata,
		Embedding:  embedding,
		MemoryType: domain.TypeInsight,
		AgentID:    agentID,
		AppID:      appID,
		State:      domain.StateActive,
		Version:    1,
		UpdatedBy:  agentID,
		CreatedAt:  now,
		UpdatedAt:  now,
	}
	upsert, err := repo.UpsertMemoryValue(ctx, mem)
	if err != nil {
		return nil, fmt.Errorf("store_kv upsert V: %w", err)
	}
	mem.ID = upsert.ID

	// If this was a dup (existing V already had keys), short-circuit:
	// don't re-extract keys, don't bump keys_extracted_at. The existing
	// V's K alias set is already in place.
	if !upsert.IsNew {
		existing, getErr := repo.GetMemoryValueByID(ctx, upsert.ID)
		if getErr != nil {
			return nil, fmt.Errorf("store_kv fetch existing V: %w", getErr)
		}
		return existing, nil
	}

	// Step 3: extract K (best-effort; failure leaves V as orphan).
	keys, extractErr := extractkeys.Extract(ctx, s.llmClient, content, extractkeys.DefaultConfig())
	if extractErr != nil {
		// V is written; K not yet. keys_extracted_at remains NULL.
		// The V is still recall-able via VAL_FTS / VAL_VEC; future
		// re-extract can fill K.
		return mem, nil
	}

	// Step 4: insert K rows (validates source enum defensively).
	if len(keys) > 0 {
		validated, vErr := validateExtractedKeySources(keys)
		if vErr != nil {
			return nil, fmt.Errorf("store_kv validate K source: %w", vErr)
		}
		inputs := extractedKeysToInputs(validated)
		if _, err := repo.InsertMemoryKeys(ctx, upsert.ID, inputs, func() string { return uuid.New().String() }); err != nil {
			return nil, fmt.Errorf("store_kv insert K: %w", err)
		}
	}

	// Step 5: mark V as extracted (success path, including 0 K).
	if err := repo.MarkKeysExtracted(ctx, upsert.ID); err != nil {
		return nil, fmt.Errorf("store_kv mark extracted: %w", err)
	}

	return mem, nil
}

// recallKV is the K=>V implementation of "search memories by query":
// run RecallKV with the given strategy, flatten the hits into
// []domain.Memory so callers that expect the legacy shape are not
// affected.
//
// When strategy is 0, falls back to tidb.StrategyDefaultV1.
func (s *MemoryService) recallKV(
	ctx context.Context,
	query string,
	queryVec []float32,
	strategy tidb.RetrievalStrategy,
	limit int,
) ([]domain.Memory, error) {
	repo, err := s.kvRepo()
	if err != nil {
		return nil, err
	}
	if strategy == 0 {
		strategy = tidb.StrategyDefaultV1
	}
	hits, err := repo.RecallKV(ctx, query, queryVec, strategy, limit)
	if err != nil {
		return nil, fmt.Errorf("recall_kv: %w", err)
	}
	out := make([]domain.Memory, 0, len(hits))
	for _, h := range hits {
		if h.Value == nil {
			continue
		}
		m := *h.Value
		// Attach score so downstream consumers (transform layer) can
		// surface ranking signal if they want.
		score := h.Score
		m.Score = &score
		out = append(out, m)
	}
	return out, nil
}

// recallKVMulti is the multi-query (facet) variant of recallKV: run
// the K=>V recall once per query with a DEEPENED per-query limit, then
// quota-merge the ranked lists down to a bounded shown window.
//
// Motivation: the client-
// side batch MVP proved facet fan-out works but exposed two limits the
// client cannot fix — per-query pools cut shallow by the client limit,
// and an unbounded merged window (mean 16.6, max 68 shown) diluting
// attention. Server-side we deepen each query's pool (limit*2 into
// RecallKV, which itself pools limit*4 before diversity) and cap the
// merged output at limit*2 total: deep per facet, bounded overall.
//
// Merge semantics: round-robin across the per-query ranked lists
// (rank 0 of every query, then rank 1, ...), deduplicating by memory
// id — so every query/facet is guaranteed early representation and a
// single high-scoring facet cannot crowd out the rest. Each returned
// memory carries MatchedQueries: the 0-based indexes of ALL queries
// that recalled it (provenance accumulates across lists even for
// duplicates).
func (s *MemoryService) recallKVMulti(
	ctx context.Context,
	queries []string,
	strategy tidb.RetrievalStrategy,
	limit int,
) ([]domain.Memory, error) {
	perQuery := make([][]domain.Memory, 0, len(queries))
	deepLimit := limit * 2
	for _, q := range queries {
		hits, err := s.recallKV(ctx, q, nil, strategy, deepLimit)
		if err != nil {
			return nil, fmt.Errorf("recall_kv_multi %q: %w", q, err)
		}
		perQuery = append(perQuery, hits)
	}
	merged := mergeMultiQueryResults(perQuery, multiQueryShownCap(limit))
	// Window trace: one line per multi-query
	// search so mean/max shown shifts are attributable to the exact cap
	// in force — limit, relative cap, absolute knob, and what actually
	// applied.
	slog.Info("recall_kv_multi window",
		"queries", len(queries),
		"limit", limit,
		"relative_cap", limit*2,
		"absolute_cap", resolveAbsoluteCap(),
		"effective_cap", multiQueryShownCap(limit),
		"shown", len(merged))
	return merged, nil
}

// multiQueryShownCap bounds the merged multi-query shown window. The
// relative bound is limit*2 ("deep but not drowning"); when
// MNEMO_RECALL_ABSOLUTE_CAP is set (>0) it additionally clamps the
// window to that constant regardless of the caller's limit. Measurement
// showed the relative cap controls the extreme (max shown 68→40) but not the
// mean (~16), because agents legitimately request limit 10-20 for deep
// facet sweeps — whether that mean constitutes harmful dilution is an
// open question the error-bucket analysis decides, so the absolute
// clamp ships as a default-OFF knob.
func multiQueryShownCap(limit int) int {
	return shownCapFor(limit, resolveAbsoluteCap())
}

// shownCapFor composes the relative and absolute bounds. Pure so tests
// pin the composition without touching process-cached env state.
func shownCapFor(limit, absoluteCap int) int {
	window := limit * 2
	if absoluteCap > 0 && absoluteCap < window {
		window = absoluteCap
	}
	return window
}

// parseAbsoluteCap maps a raw MNEMO_RECALL_ABSOLUTE_CAP value to an
// effective absolute cap. Semantics (frozen before implementation):
//   - unset / empty / "0" / unparsable / negative -> 0 (knob OFF,
//     behavior identical to pre-knob builds)
//   - N > 0 -> merged shown window additionally clamped to N
//
// The knob bounds ONLY the post-merge shown window. Per-query recall
// depth (deepLimit = limit*2 into RecallKV, which pools limit*4 before
// diversity) is intentionally untouched — the fuse trims what the
// agent sees, never what the ranker considers.
//
// Pure function (no env read) so tests cover the mapping without
// fighting the process-cached resolver below.
func parseAbsoluteCap(raw string) int {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 0 {
		return 0
	}
	return n
}

// resolveAbsoluteCap reads MNEMO_RECALL_ABSOLUTE_CAP once per process,
// mirroring resolveKeyDedup / resolveDiversityMode in the tidb repo
// layer: recall behavior must not flip mid-flight between requests.
var resolveAbsoluteCap = sync.OnceValue(func() int {
	return parseAbsoluteCap(os.Getenv("MNEMO_RECALL_ABSOLUTE_CAP"))
})

// mergeMultiQueryResults quota-merges per-query ranked lists into one
// bounded list. Pure function (no DB) so the merge semantics are unit-
// testable: round-robin by rank across queries, dedup by memory ID,
// stop at totalCap. MatchedQueries on each output memory records every
// query index whose list contained it.
func mergeMultiQueryResults(perQuery [][]domain.Memory, totalCap int) []domain.Memory {
	if totalCap <= 0 || len(perQuery) == 0 {
		return nil
	}
	// Provenance pass: id -> all query indexes that recalled it.
	prov := make(map[string][]int)
	maxLen := 0
	for qi, list := range perQuery {
		if len(list) > maxLen {
			maxLen = len(list)
		}
		for _, m := range list {
			prov[m.ID] = append(prov[m.ID], qi)
		}
	}
	// Selection pass: round-robin by rank, first occurrence wins.
	out := make([]domain.Memory, 0, totalCap)
	picked := make(map[string]struct{}, totalCap)
	for rank := 0; rank < maxLen && len(out) < totalCap; rank++ {
		for _, list := range perQuery {
			if rank >= len(list) {
				continue
			}
			m := list[rank]
			if _, dup := picked[m.ID]; dup {
				continue
			}
			picked[m.ID] = struct{}{}
			m.MatchedQueries = prov[m.ID]
			out = append(out, m)
			if len(out) >= totalCap {
				break
			}
		}
	}
	return out
}

// storeLegacyFallback writes a single memory through the legacy
// repository.MemoryRepo.Create path. Triggered when kvRepo() fails
// (backend doesn't satisfy *tidb.MemoryRepo). Keeps non-TiDB test
// environments and postgres/db9 deployments functional without
// requiring them to opt into K=>V; production TiDB deployments never
// enter this branch because *tidb.MemoryRepo always satisfies kvRepo.
func (s *MemoryService) storeLegacyFallback(
	ctx context.Context,
	agentID, appID, content string,
	tags []string,
	metadata []byte,
) (*domain.Memory, error) {
	var embedding []float32
	if s.autoModel == "" && s.embedder != nil {
		v, err := s.embedder.Embed(ctx, content)
		if err != nil {
			return nil, fmt.Errorf("store_legacy embed: %w", err)
		}
		embedding = v
	}
	now := time.Now()
	mem := &domain.Memory{
		ID:         uuid.New().String(),
		Content:    content,
		Source:     agentID,
		Tags:       tags,
		Metadata:   metadata,
		Embedding:  embedding,
		MemoryType: domain.TypeInsight,
		AgentID:    agentID,
		AppID:      appID,
		State:      domain.StateActive,
		Version:    1,
		UpdatedBy:  agentID,
		CreatedAt:  now,
		UpdatedAt:  now,
	}
	if err := s.memories.Create(ctx, mem); err != nil {
		return nil, fmt.Errorf("store_legacy: %w", err)
	}
	return mem, nil
}

// storeKVWithAgentKeys is the agent-keys variant of storeKV. When
// `agentKeys` is empty, falls back to the standard storeKV path
// (server-side extractkeys.Extract). When non-empty, validates the
// agent keys (validateAgentKeys), inserts the survivors, and returns
// the accepted-count + rejected list to the caller for sync-response
// surfacing.
//
// Same V-side guarantees as storeKV (embed, content_hash dedup,
// MarkKeysExtracted on success); only the K-extraction step differs.
//
// Dup-V merge semantic (DIFFERENT from storeKV): when content_hash
// dedups to an existing V, InsertMemoryKeys STILL runs with the new
// agent keys. The repository's INSERT IGNORE + uq_value_key_norm
// dedup silently drops same-key_norm duplicates, so corrected keys
// from a retry append cleanly while previously-stored keys remain
// (append-only — explicit key removal is not supported in v1). This
// preserves the "agent learns from rejection and re-stores corrected
// keys" feedback loop.
func (s *MemoryService) storeKVWithAgentKeys(
	ctx context.Context,
	agentID, appID, content string,
	tags []string,
	metadata []byte,
	agentKeys []RetrievalKey,
) (*domain.Memory, int, []RejectedKey, error) {
	if len(agentKeys) == 0 {
		mem, err := s.storeKV(ctx, agentID, appID, content, tags, metadata)
		return mem, 0, nil, err
	}

	repo, err := s.kvRepo()
	if err != nil {
		// Backend doesn't support K=>V. Fall back to legacy create; agent
		// keys are dropped silently because the legacy table doesn't
		// have a K=>V index surface. Caller still gets a valid Memory.
		mem, fbErr := s.storeLegacyFallback(ctx, agentID, appID, content, tags, metadata)
		return mem, 0, nil, fbErr
	}

	// Embed (same as storeKV step 1).
	var embedding []float32
	if s.autoModel == "" && s.embedder != nil {
		v, eErr := s.embedder.Embed(ctx, content)
		if eErr != nil {
			return nil, 0, nil, fmt.Errorf("store_kv_agent_keys embed: %w", eErr)
		}
		embedding = v
	}

	// Upsert V (same as storeKV step 2).
	now := time.Now()
	mem := &domain.Memory{
		ID:         uuid.New().String(),
		Content:    content,
		Source:     agentID,
		Tags:       tags,
		Metadata:   metadata,
		Embedding:  embedding,
		MemoryType: domain.TypeInsight,
		AgentID:    agentID,
		AppID:      appID,
		State:      domain.StateActive,
		Version:    1,
		UpdatedBy:  agentID,
		CreatedAt:  now,
		UpdatedAt:  now,
	}
	upsert, err := repo.UpsertMemoryValue(ctx, mem)
	if err != nil {
		return nil, 0, nil, fmt.Errorf("store_kv_agent_keys upsert V: %w", err)
	}
	mem.ID = upsert.ID

	// Validate agent keys (same whether V is new or dup).
	accepted, rejected := validateAgentKeys(content, agentKeys)

	// Insert keys. INSERT IGNORE on uq_value_key_norm de-duplicates
	// silently, so re-storing the same V with a corrected key list
	// (after the agent saw rejection reasons and retried) appends
	// only the new key_norm rows. Caught while reviewing the
	// dup-V case — without this, the "agent learns from rejection
	// and re-stores" feedback loop wouldn't actually apply the
	// corrected keys.
	//
	// Cap caveat: validateAgentKeys's per-V cap
	// (10) is batch-level only. INSERT IGNORE prevents duplicate
	// key_norm rows but does not bound total distinct keys per V
	// across many retries — an agent that retries with 10 fresh keys
	// each time could grow K beyond 10. A DB-aware cap (count
	// existing rows and reject overflow at INSERT time) is deferred.
	//
	// Merge semantics caveat: this is
	// append-only. If an agent's first store had keys [A, B, C] and
	// the second store has [A, B'], B' is appended but C is NOT
	// deleted. v1 prefers safe accumulation; explicit key removal
	// would need a separate DELETE-by-text endpoint (deferred).
	if len(accepted) > 0 {
		if _, err := repo.InsertMemoryKeys(ctx, upsert.ID, accepted, func() string { return uuid.New().String() }); err != nil {
			return nil, 0, nil, fmt.Errorf("store_kv_agent_keys insert K: %w", err)
		}
	}

	// content_hash dup-V short-circuits the rest: return the existing V
	// (the freshly-built mem hasn't been persisted — UpsertMemoryValue
	// returned the existing id), and skip MarkKeysExtracted since the
	// timestamp is already set from the original store.
	if !upsert.IsNew {
		existing, getErr := repo.GetMemoryValueByID(ctx, upsert.ID)
		if getErr != nil {
			return nil, 0, nil, fmt.Errorf("store_kv_agent_keys fetch existing V: %w", getErr)
		}
		return existing, len(accepted), rejected, nil
	}

	// Mark V as extracted (matches storeKV step 5). We treat agent-
	// provided keys as the extraction signal — even if accepted=0
	// (everything was rejected), we still set keys_extracted_at to
	// avoid an orphan-V backfill loop. Callers see the rejection list
	// in the response and can re-store with corrected keys.
	if err := repo.MarkKeysExtracted(ctx, upsert.ID); err != nil {
		return nil, 0, nil, fmt.Errorf("store_kv_agent_keys mark extracted: %w", err)
	}

	return mem, len(accepted), rejected, nil
}

// extractedKeysToInputs converts the LLM-extract output shape to the
// repository write shape. extractkeys.ExtractedKey has no notion of
// weight; server-extract keys land at the repository's default 1.0.
func extractedKeysToInputs(in []extractkeys.ExtractedKey) []tidb.MemoryKeyInput {
	out := make([]tidb.MemoryKeyInput, len(in))
	for i, k := range in {
		out[i] = tidb.MemoryKeyInput{
			KeyText: k.KeyText,
			KeyNorm: k.KeyNorm,
			Source:  k.Source,
			Weight:  defaultKeyWeight,
		}
	}
	return out
}

// validateExtractedKeySources defensively filters out any keys whose
// source value is not one of the documented enum values. Defends
// against a future LLM prompt regression or a malformed caller
// (belt-and-suspenders even though
// extractkeys.Extract today only emits SourceExtract /
// SourceExtractTranslation / SourceExtractCategory).
func validateExtractedKeySources(in []extractkeys.ExtractedKey) ([]extractkeys.ExtractedKey, error) {
	out := make([]extractkeys.ExtractedKey, 0, len(in))
	for i, k := range in {
		switch k.Source {
		case extractkeys.SourceExtract,
			extractkeys.SourceExtractTranslation,
			extractkeys.SourceExtractCategory,
			extractkeys.SourceUser,
			extractkeys.SourceFeedback:
			out = append(out, k)
		case "":
			// Treat empty as SourceExtract for backward tolerance.
			k.Source = extractkeys.SourceExtract
			out = append(out, k)
		default:
			return nil, fmt.Errorf("invalid K source %q at index %d", k.Source, i)
		}
	}
	return out, nil
}

// ---------------------------------------------------------------------
// Agent-provided K.
//
// When the wire payload includes IngestRequest.Keys, the client (an
// agent) has chosen retrieval phrases using its full context; the
// server skips the extractkeys.Extract LLM call and just validates +
// inserts what the agent sent. The server STILL enforces hard quality
// guards (stop-list, entity overlap, length caps, weight bounds) so
// agent hallucination doesn't poison the index.
// ---------------------------------------------------------------------

const (
	defaultKeyWeight    = 1.0
	minAgentKeyWeight   = 0.1
	maxAgentKeyWeight   = 2.0
	maxAgentKeysPerV    = 10
	maxAgentKeyWords    = 8
	maxAgentKeyTextLen  = 200
	minAgentTransTokens = 2
	minAgentTransChars  = 3
)

// agentKeyStopList contains the single-token generic phrases that are
// too broad to be useful retrieval surfaces. The list is normalized via
// keynorm.NormalizeKey at init time so per-key comparison only needs to
// normalize the input once and then map-lookup (avoids missing variants
// like "User" / "user." / " user " due to case/whitespace differences).
var agentKeyStopList = func() map[string]struct{} {
	raw := []string{
		"user", "home", "work", "project", "team", "company",
		"name", "date", "time", "place", "thing", "item",
		"task", "info", "fact",
	}
	out := make(map[string]struct{}, len(raw))
	for _, s := range raw {
		out[keynorm.NormalizeKey(s)] = struct{}{}
	}
	return out
}()

// agentKeyRejectReason is the controlled vocabulary of reasons surfaced
// to the agent (via the sync POST response's keys_rejected list) so the
// agent can learn what NOT to emit next time.
type agentKeyRejectReason string

const (
	reasonEmptyText           agentKeyRejectReason = "empty_text"
	reasonTextTooLong         agentKeyRejectReason = "text_too_long"
	reasonInvalidSource       agentKeyRejectReason = "invalid_source"
	reasonWeightTooSmall      agentKeyRejectReason = "weight_too_small"
	reasonStopListSingleToken agentKeyRejectReason = "stop_list_single_token"
	reasonNoEntityOverlap     agentKeyRejectReason = "no_entity_overlap"
	reasonWordCountExceeded   agentKeyRejectReason = "word_count_exceeded"
	reasonTranslationTooThin  agentKeyRejectReason = "agent_translation_too_thin"
	reasonPerValueCapReached  agentKeyRejectReason = "per_value_cap_reached"
	reasonDuplicateKeyNorm    agentKeyRejectReason = "duplicate_key_norm"
)

// RejectedKey is one item in the sync POST response's keys_rejected
// list. Agent consumers (the Mem9MemoryStore tool) surface this
// to the model so subsequent stores can avoid the same shape.
type RejectedKey struct {
	Text   string `json:"text"`
	Reason string `json:"reason"`
}

// validateAgentKeys runs the 7-step quality guard over a list of
// caller-supplied RetrievalKey
// values. The same guard runs regardless of trust level (server can't
// distinguish a well-behaved agent from one whose system prompt drifted).
//
// Inputs that pass become MemoryKeyInput ready for InsertMemoryKeys.
// Inputs that fail land in `rejected` with a controlled-vocabulary
// reason. `accepted` is hard-capped at maxAgentKeysPerV; overflow goes
// into `rejected` with reasonPerValueCapReached.
//
// Per-K dedup uses key_norm (matches the repository's
// uq_value_key_norm). Duplicates within the same batch are dropped
// here (second occurrence rejected as reasonDuplicateKeyNorm) so the
// repository INSERT IGNORE doesn't silently swallow them.
//
// content is the V text the keys are about; used for the entity-
// overlap check (step 5).
func validateAgentKeys(content string, keys []RetrievalKey) ([]tidb.MemoryKeyInput, []RejectedKey) {
	contentTokens := agentKeyTokens(keynorm.NormalizeKey(content))
	accepted := make([]tidb.MemoryKeyInput, 0, len(keys))
	rejected := make([]RejectedKey, 0)
	seenNorms := make(map[string]struct{}, len(keys))

	for _, k := range keys {
		// Step 1: text non-empty + len cap.
		text := strings.TrimSpace(k.Text)
		if text == "" {
			rejected = append(rejected, RejectedKey{Text: k.Text, Reason: string(reasonEmptyText)})
			continue
		}
		if utf8.RuneCountInString(text) > maxAgentKeyTextLen {
			rejected = append(rejected, RejectedKey{Text: text, Reason: string(reasonTextTooLong)})
			continue
		}

		// Step 2: source enum (only agent / agent_translation valid here;
		// extract/user/feedback are reserved for non-agent paths).
		if k.Source != extractkeys.SourceAgent && k.Source != extractkeys.SourceAgentTranslation {
			rejected = append(rejected, RejectedKey{Text: text, Reason: string(reasonInvalidSource)})
			continue
		}

		// Step 3: weight resolution.
		weight, ok := resolveAgentKeyWeight(k.Weight)
		if !ok {
			rejected = append(rejected, RejectedKey{Text: text, Reason: string(reasonWeightTooSmall)})
			continue
		}

		// Step 4: normalize + stop-list check.
		norm := keynorm.NormalizeKey(text)
		if _, hit := agentKeyStopList[norm]; hit {
			rejected = append(rejected, RejectedKey{Text: text, Reason: string(reasonStopListSingleToken)})
			continue
		}

		// Step 6 first: word-count cap (cheap; runs before the entity-
		// overlap walk which is more expensive on long V text).
		keyTokens := agentKeyTokens(norm)
		if len(keyTokens) > maxAgentKeyWords {
			rejected = append(rejected, RejectedKey{Text: text, Reason: string(reasonWordCountExceeded)})
			continue
		}

		// Step 5: entity overlap.
		// - SourceAgent: must share at least one token with the V text.
		// - SourceAgentTranslation: relaxed — cross-language keys
		//   won't share tokens (Japanese rendering of an English
		//   proper noun, etc.). But still require >= 2 non-trivial
		//   tokens + >= 3 chars to block translation-as-bypass for
		//   single-token generics.
		if k.Source == extractkeys.SourceAgentTranslation {
			if len(keyTokens) < minAgentTransTokens || utf8.RuneCountInString(norm) < minAgentTransChars {
				rejected = append(rejected, RejectedKey{Text: text, Reason: string(reasonTranslationTooThin)})
				continue
			}
		} else {
			if !shareAnyToken(keyTokens, contentTokens) {
				rejected = append(rejected, RejectedKey{Text: text, Reason: string(reasonNoEntityOverlap)})
				continue
			}
		}

		// Step 7a: in-batch key_norm dedup (matches uq_value_key_norm).
		if _, dup := seenNorms[norm]; dup {
			rejected = append(rejected, RejectedKey{Text: text, Reason: string(reasonDuplicateKeyNorm)})
			continue
		}
		seenNorms[norm] = struct{}{}

		// Step 7b: per-V K count cap. Overflow goes into rejected so the
		// agent learns it sent too many.
		if len(accepted) >= maxAgentKeysPerV {
			rejected = append(rejected, RejectedKey{Text: text, Reason: string(reasonPerValueCapReached)})
			continue
		}

		accepted = append(accepted, tidb.MemoryKeyInput{
			KeyText: text,
			KeyNorm: norm,
			Source:  k.Source,
			Weight:  weight,
		})
	}

	return accepted, rejected
}

// resolveAgentKeyWeight applies the 4-rule weight policy:
//   - missing (=0) → default 1.0
//   - <= 0 (negative) or in (0, 0.1) → reject
//   - in [0.1, 2.0] → preserve
//   - > 2.0 → clamp to 2.0 (lenient, not reject)
//
// Returns (weight, ok). ok=false means caller should reject the key.
func resolveAgentKeyWeight(in float64) (float64, bool) {
	if in == 0 {
		return defaultKeyWeight, true
	}
	if in < minAgentKeyWeight {
		return 0, false
	}
	if in > maxAgentKeyWeight {
		return maxAgentKeyWeight, true
	}
	return in, true
}

// agentKeyTokens splits a normalized string on whitespace and returns the
// distinct non-empty token set. Used by validateAgentKeys for the
// entity-overlap check (step 5) and word-count cap (step 6).
func agentKeyTokens(s string) map[string]struct{} {
	out := make(map[string]struct{})
	for _, tok := range strings.Fields(s) {
		if tok != "" {
			out[tok] = struct{}{}
		}
	}
	return out
}

// shareAnyToken returns true iff a and b share at least one element.
func shareAnyToken(a, b map[string]struct{}) bool {
	if len(a) > len(b) {
		a, b = b, a
	}
	for tok := range a {
		if _, ok := b[tok]; ok {
			return true
		}
	}
	return false
}
