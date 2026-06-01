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
	"time"

	"github.com/google/uuid"
	"github.com/qiffang/mnemos/server/internal/domain"
	"github.com/qiffang/mnemos/server/internal/extractkeys"
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
	agentID, content string,
	tags []string,
	metadata []byte,
) (*domain.Memory, error) {
	repo, err := s.kvRepo()
	if err != nil {
		return nil, err
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
		if _, err := repo.InsertMemoryKeys(ctx, upsert.ID, validated, func() string { return uuid.New().String() }); err != nil {
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

// validateExtractedKeySources defensively filters out any keys whose
// source value is not one of the documented enum values. Defends
// against a future LLM prompt regression or a malformed caller
// (per @Magallan's step-3 review note: belt-and-suspenders even though
// extractkeys.Extract today only emits SourceExtract /
// SourceExtractTranslation).
func validateExtractedKeySources(in []extractkeys.ExtractedKey) ([]extractkeys.ExtractedKey, error) {
	out := make([]extractkeys.ExtractedKey, 0, len(in))
	for i, k := range in {
		switch k.Source {
		case extractkeys.SourceExtract,
			extractkeys.SourceExtractTranslation,
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
