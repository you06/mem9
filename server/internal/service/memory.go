package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/google/uuid"
	"github.com/qiffang/mnemos/server/internal/domain"
	"github.com/qiffang/mnemos/server/internal/embed"
	"github.com/qiffang/mnemos/server/internal/llm"
	"github.com/qiffang/mnemos/server/internal/metrics"
	"github.com/qiffang/mnemos/server/internal/repository"
	"github.com/qiffang/mnemos/server/internal/repository/tidb"
)

const (
	maxContentLen        = 50000
	maxTags              = 20
	maxBulkSize          = 100
	maxBulkDeleteSize    = 1000
	defaultMinScore      = 0.3
	maxLooseSearchTokens = 5

	// secondHopWeight is the RRF weight applied to second-hop vector search results.
	// Lower than 1.0 to prevent indirect matches from outranking direct hits.
	secondHopWeight = 0.3
	// secondHopTopN is the number of top first-hop results used as seeds for second-hop search.
	secondHopTopN = 3
	// secondHopGateScore is the minimum first-hop cosine similarity required to
	// trigger second-hop search. When the best vector result scores below this
	// threshold the query likely has no strong match (e.g. adversarial), so
	// second-hop is skipped to avoid injecting noise.
	secondHopGateScore = 0.5
)

type MemoryService struct {
	memories  repository.MemoryRepo
	embedder  *embed.Embedder
	autoModel string
	ingest    *IngestService
	// llmClient is captured here for the K=>V experimental path
	// (memory_kv.go) which calls extractkeys.Extract directly without
	// going through ingest. Step 4 of the K=>V refactor.
	llmClient *llm.Client
}

func NewMemoryService(memories repository.MemoryRepo, llmClient *llm.Client, embedder *embed.Embedder, autoModel string, ingestMode IngestMode) *MemoryService {
	return &MemoryService{
		memories:  memories,
		embedder:  embedder,
		autoModel: autoModel,
		ingest:    NewIngestService(memories, llmClient, embedder, autoModel, ingestMode),
		llmClient: llmClient,
	}
}

// Create stores a memory. Step 4 of the K=>V refactor replaces the old
// reconciliation pipeline with a direct V/K write path. Behavior change
// from the previous reconciliation-based implementation:
//   - Always produces exactly 1 V. Duplicate content (same hash) returns
//     the existing V's id with count=1.
//   - The previous "may produce 0 or N insights based on LLM merge
//     decisions" semantic is gone; dedup is now mechanical via
//     content_hash.
//   - LLM is now only used for K (retrieval-key) extraction, not for
//     content reconciliation/merging.
//
// appID threads through to the V row (main gained the appId tenant
// schema in #350 while the K=>V branch was in flight).
func (s *MemoryService) Create(ctx context.Context, agentID, appID, content string, tags []string, metadata json.RawMessage) (*domain.Memory, int, error) {
	if err := validateMemoryInput(content, tags); err != nil {
		return nil, 0, err
	}

	writeStart := time.Now()
	mem, err := s.storeKV(ctx, agentID, appID, content, tags, metadata)
	metrics.MemoryWriteDuration.WithLabelValues("create", metricStatus(err)).Observe(time.Since(writeStart).Seconds())
	if err != nil {
		return nil, 0, fmt.Errorf("create k=>v memory: %w", err)
	}
	return mem, 1, nil
}

// CreateWithAgentKeys is the same as Create but accepts the agent-
// provided retrieval keys carried on the wire's `keys` field. When
// `agentKeys` is non-empty, server-side `extractkeys.Extract` is
// skipped; the agent keys are validated by `validateAgentKeys` and
// the survivors written to `memory_keys` with `source = "agent"` /
// `"agent_translation"` and the agent-specified weight. Locked.
//
// Returns the created Memory, the written count (1 unless content_hash
// dedup hit), the list of accepted-K count + rejected-K reasons, and
// any error. Caller (handler) surfaces accepted/rejected counts in
// the sync POST response so the agent can self-correct subsequent
// stores.
//
// When `agentKeys` is empty, behaves identically to `Create` (server-
// side extractkeys.Extract). The returned rejected list is empty in
// that case.
func (s *MemoryService) CreateWithAgentKeys(
	ctx context.Context,
	agentID, appID, content string,
	tags []string,
	metadata json.RawMessage,
	agentKeys []RetrievalKey,
) (*domain.Memory, int, int, []RejectedKey, error) {
	if err := validateMemoryInput(content, tags); err != nil {
		return nil, 0, 0, nil, err
	}

	writeStart := time.Now()
	mem, accepted, rejected, err := s.storeKVWithAgentKeys(ctx, agentID, appID, content, tags, metadata, agentKeys)
	metrics.MemoryWriteDuration.WithLabelValues("create_with_keys", metricStatus(err)).Observe(time.Since(writeStart).Seconds())
	if err != nil {
		return nil, 0, 0, nil, fmt.Errorf("create_with_agent_keys k=>v memory: %w", err)
	}
	return mem, 1, accepted, rejected, nil
}

func (s *MemoryService) CreatePinned(ctx context.Context, agentID, appID, content string, tags []string, metadata json.RawMessage) (*domain.Memory, int, error) {
	memories, err := s.BulkCreate(ctx, agentID, []BulkMemoryInput{
		{
			Content:  content,
			AppID:    appID,
			Tags:     tags,
			Metadata: metadata,
		},
	})
	if err != nil {
		return nil, 0, err
	}
	if len(memories) == 0 {
		return nil, 0, fmt.Errorf("bulk create returned no memories")
	}

	mem := memories[0]
	return &mem, len(memories), nil
}

// Get returns a single memory by ID.
func (s *MemoryService) Get(ctx context.Context, id string) (*domain.Memory, error) {
	return s.memories.GetByID(ctx, id)
}

func (s *MemoryService) List(ctx context.Context, filter domain.MemoryFilter) ([]domain.Memory, int, error) {
	mems, total, err := s.memories.List(ctx, filter)
	if err != nil {
		return nil, 0, err
	}
	return finalizeSearchResults(mems, filter.Query), total, nil
}

// Search retrieves memories. Step 4 of the K=>V refactor replaces all
// previous search variants (autoHybridSearch / hybridSearch /
// ftsOnlySearch / keywordOnlySearch) with a single RecallKV path using
// the V1 default strategy (KEY_EXACT | KEY_FTS | VAL_FTS).
//
// Empty-query (list-by-filter) requests still go through the legacy
// memories.List path — K=>V recall only kicks in when there's a query.
//
// API signature unchanged.
func (s *MemoryService) Search(ctx context.Context, filter domain.MemoryFilter) ([]domain.Memory, int, error) {
	if filter.Query == "" {
		return s.List(ctx, filter)
	}

	slog.Info("memory search (k=>v)", "query_len", len(filter.Query), "retrieval_strategy", filter.RetrievalStrategy)
	limit := filter.Limit
	if limit <= 0 {
		limit = 10
	}
	results, err := s.recallKV(ctx, filter.Query, nil, tidb.RetrievalStrategy(filter.RetrievalStrategy), limit)
	if err != nil {
		return nil, 0, err
	}
	finalized := finalizeSearchResults(results, filter.Query)
	return finalized, len(finalized), nil
}

// SearchMulti is the multi-query (facet) variant of Search: the caller
// supplies several query phrasings (repeated `q` params on the wire)
// covering different facets of one question; each runs the K=>V recall
// with a deepened pool and the results are quota-merged so every facet
// is represented in a bounded shown window. See recallKVMulti for the
// merge semantics and the motivation.
//
// Defensive: zero/one query degrades to the single-query Search path.
func (s *MemoryService) SearchMulti(ctx context.Context, filter domain.MemoryFilter, queries []string) ([]domain.Memory, int, error) {
	if len(queries) <= 1 {
		if len(queries) == 1 {
			filter.Query = queries[0]
		}
		return s.Search(ctx, filter)
	}

	slog.Info("memory search (k=>v multi)",
		"query_count", len(queries),
		"retrieval_strategy", filter.RetrievalStrategy)
	limit := filter.Limit
	if limit <= 0 {
		limit = 10
	}
	results, err := s.recallKVMulti(ctx, queries, tidb.RetrievalStrategy(filter.RetrievalStrategy), limit)
	if err != nil {
		return nil, 0, err
	}
	finalized := finalizeSearchResults(results, strings.Join(queries, " "))
	return finalized, len(finalized), nil
}

// searchLegacyDispatch is the pre-step-4 search dispatcher, kept here
// as dead code for reference / quick rollback during the experimental
// phase. It is no longer reachable from Search.
//
//nolint:unused
func (s *MemoryService) searchLegacyDispatch(ctx context.Context, filter domain.MemoryFilter) ([]domain.Memory, int, error) {
	searchFilter := filter
	searchFilter.SessionID = ""
	searchFilter.Source = ""

	slog.Info("memory search", "query_len", len(filter.Query), "auto_model", s.autoModel, "fts", s.memories.FTSAvailable())
	if s.autoModel != "" {
		return s.autoHybridSearch(ctx, searchFilter)
	}
	if s.embedder != nil {
		return s.hybridSearch(ctx, searchFilter)
	}
	if s.memories.FTSAvailable() {
		return s.ftsOnlySearch(ctx, searchFilter)
	}
	// FTS probe still running (cold start) — fall back to LIKE-based keyword search.
	slog.Warn("search: FTS not yet available, falling back to keyword search")
	return s.keywordOnlySearch(ctx, searchFilter)
}

// ContentKeywordSearch performs direct content substring search for list filters.
// Unlike Search(), it deliberately bypasses vector, FTS, and recall-style
// ranking so UI list search behaves like a content filter.
func (s *MemoryService) ContentKeywordSearch(ctx context.Context, filter domain.MemoryFilter) ([]domain.Memory, int, error) {
	if filter.Query == "" {
		return s.List(ctx, filter)
	}
	return s.keywordOnlySearch(ctx, filter)
}

func (s *MemoryService) SearchCandidates(
	ctx context.Context,
	filter domain.MemoryFilter,
	sourcePool RecallSourcePool,
	opts RecallCandidateOptions,
) ([]RecallCandidate, error) {
	if filter.Query == "" {
		return nil, nil
	}

	searchFilter := filter
	searchFilter.SessionID = ""
	searchFilter.Source = ""

	if s.autoModel != "" {
		return s.autoHybridCandidates(ctx, searchFilter, sourcePool, opts)
	}
	if s.embedder != nil {
		return s.hybridCandidates(ctx, searchFilter, sourcePool, opts)
	}
	if s.memories.FTSAvailable() {
		return s.ftsOnlyCandidates(ctx, searchFilter, sourcePool, opts)
	}
	return s.keywordOnlyCandidates(ctx, searchFilter, sourcePool, opts)
}

const rrfK = 60.0

func rrfMerge(ftsResults, vecResults []domain.Memory) map[string]float64 {
	scores := make(map[string]float64, len(ftsResults)+len(vecResults))
	for rank, m := range ftsResults {
		scores[m.ID] += 1.0 / (rrfK + float64(rank+1))
	}
	for rank, m := range vecResults {
		scores[m.ID] += 1.0 / (rrfK + float64(rank+1))
	}
	return scores
}

func (s *MemoryService) paginate(results []domain.Memory, offset, limit int) ([]domain.Memory, int) {
	return paginateResults(results, offset, limit)
}

func paginateResults(results []domain.Memory, offset, limit int) ([]domain.Memory, int) {
	total := len(results)
	if offset >= total {
		return []domain.Memory{}, total
	}
	end := offset + limit
	if end > total {
		end = total
	}
	return results[offset:end], total
}

func (s *MemoryService) ftsOnlySearch(ctx context.Context, filter domain.MemoryFilter) ([]domain.Memory, int, error) {
	limit := filter.Limit
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	offset := filter.Offset
	if offset < 0 {
		offset = 0
	}
	fetchLimit := limit * 3

	ftsResults, err := s.memories.FTSSearch(ctx, filter.Query, filter, fetchLimit)
	if err != nil {
		return nil, 0, fmt.Errorf("FTS search: %w", err)
	}
	if len(ftsResults) == 0 && shouldRunLooseKeywordFallback(filter.Query) {
		ftsResults, err = s.looseTokenKeywordSearch(ctx, filter, fetchLimit)
		if err != nil {
			return nil, 0, err
		}
	}
	slog.Info("fts search completed", "query_len", len(filter.Query), "results", len(ftsResults))

	page, total := s.paginate(ftsResults, offset, limit)
	return finalizeSearchResults(page, filter.Query), total, nil
}

func observeRecallEmbeddingRequest(embedder *embed.Embedder, err error) {
	model := "unknown"
	if embedder != nil && embedder.Model() != "" {
		model = embedder.Model()
	}
	observeRecallEmbeddingRequestByModel(model, err)
}

func observeRecallAutoEmbeddingRequest(autoModel string, err error, skipped bool) {
	if skipped {
		return
	}
	observeRecallEmbeddingRequestByModel(autoModel, err)
}

func observeRecallEmbeddingRequestByModel(model string, err error) {
	if model == "" {
		model = "unknown"
	}
	status := "success"
	if err != nil {
		status = "error"
	}
	metrics.EmbeddingRequestsTotal.WithLabelValues("query_embedding", model, status).Inc()
}

// is not yet available (e.g., during cold start probe window).
func (s *MemoryService) keywordOnlySearch(ctx context.Context, filter domain.MemoryFilter) ([]domain.Memory, int, error) {
	limit := filter.Limit
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	offset := filter.Offset
	if offset < 0 {
		offset = 0
	}
	fetchLimit := limit * 3

	kwResults, err := s.memories.KeywordSearch(ctx, filter.Query, filter, fetchLimit)
	if err != nil {
		return nil, 0, fmt.Errorf("keyword search: %w", err)
	}
	if len(kwResults) == 0 && shouldRunLooseKeywordFallback(filter.Query) {
		kwResults, err = s.looseTokenKeywordSearch(ctx, filter, fetchLimit)
		if err != nil {
			return nil, 0, err
		}
	}
	slog.Info("keyword search completed (FTS unavailable)", "query_len", len(filter.Query), "results", len(kwResults))

	page, total := s.paginate(kwResults, offset, limit)
	return finalizeSearchResults(page, filter.Query), total, nil
}

func (s *MemoryService) looseTokenKeywordSearch(ctx context.Context, filter domain.MemoryFilter, fetchLimit int) ([]domain.Memory, error) {
	tokens := looseSearchTokens(filter.Query)
	if len(tokens) == 0 {
		return nil, nil
	}
	if fetchLimit <= 0 {
		fetchLimit = 50
	}

	byID := make(map[string]domain.Memory)
	for _, token := range tokens {
		results, err := s.memories.KeywordSearch(ctx, token, filter, fetchLimit)
		if err != nil {
			return nil, fmt.Errorf("keyword token search: %w", err)
		}
		for _, memory := range results {
			if _, ok := byID[memory.ID]; !ok {
				byID[memory.ID] = memory
			}
		}
	}

	memories := make([]domain.Memory, 0, len(byID))
	for _, memory := range byID {
		memories = append(memories, memory)
	}
	sort.SliceStable(memories, func(i, j int) bool {
		leftScore := looseSearchTokenMatchScore(memories[i].Content, tokens)
		rightScore := looseSearchTokenMatchScore(memories[j].Content, tokens)
		if leftScore != rightScore {
			return leftScore > rightScore
		}
		if !memories[i].UpdatedAt.Equal(memories[j].UpdatedAt) {
			return memories[i].UpdatedAt.After(memories[j].UpdatedAt)
		}
		return memories[i].ID < memories[j].ID
	})
	if len(memories) > fetchLimit {
		memories = memories[:fetchLimit]
	}
	return memories, nil
}

var looseSearchStopWords = map[string]struct{}{
	"a": {}, "an": {}, "and": {}, "are": {}, "as": {}, "at": {}, "be": {}, "by": {},
	"did": {}, "do": {}, "does": {}, "for": {}, "from": {}, "has": {}, "have": {},
	"how": {}, "if": {}, "in": {}, "is": {}, "not": {}, "of": {}, "on": {}, "or": {},
	"the": {}, "to": {}, "was": {}, "were": {}, "what": {}, "when": {}, "where": {},
	"whether": {}, "which": {}, "who": {}, "whom": {}, "whose": {}, "why": {}, "with": {},
}

func looseSearchTokens(query string) []string {
	var tokens []string
	seen := make(map[string]struct{})
	var current strings.Builder
	flush := func() {
		if current.Len() == 0 {
			return
		}
		token := current.String()
		current.Reset()
		key := strings.ToLower(token)
		if len(key) < 2 {
			return
		}
		if _, ok := looseSearchStopWords[key]; ok {
			return
		}
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		tokens = append(tokens, token)
	}

	for _, r := range query {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			current.WriteRune(r)
			continue
		}
		flush()
		if len(tokens) >= maxLooseSearchTokens {
			return tokens
		}
	}
	flush()
	if len(tokens) > maxLooseSearchTokens {
		return tokens[:maxLooseSearchTokens]
	}
	return tokens
}

func looseSearchTokenMatchScore(content string, tokens []string) int {
	content = strings.ToLower(content)
	score := 0
	for _, token := range tokens {
		if strings.Contains(content, strings.ToLower(token)) {
			score++
		}
	}
	return score
}

func shouldRunLooseKeywordFallback(query string) bool {
	tokens := looseSearchTokens(query)
	return len(tokens) == 1 || strings.ContainsAny(query, "?？")
}

func (s *MemoryService) ftsOnlyCandidates(ctx context.Context, filter domain.MemoryFilter, sourcePool RecallSourcePool, opts RecallCandidateOptions) ([]RecallCandidate, error) {
	limit := normalizeRecallLimit(filter.Limit, 10)
	fetchLimit := limit * normalizeRecallFetchMultiplier(opts.FetchMultiplier, 3)

	ftsResults, err := s.memories.FTSSearch(ctx, filter.Query, filter, fetchLimit)
	if err != nil {
		return nil, fmt.Errorf("FTS search: %w", err)
	}
	if len(ftsResults) == 0 && shouldRunLooseKeywordFallback(filter.Query) {
		ftsResults, err = s.looseTokenKeywordSearch(ctx, filter, fetchLimit)
		if err != nil {
			return nil, err
		}
	}
	return dedupRecallCandidatesByContent(mergeRecallCandidates(sourcePool, ftsResults, nil, nil)), nil
}

func (s *MemoryService) keywordOnlyCandidates(ctx context.Context, filter domain.MemoryFilter, sourcePool RecallSourcePool, opts RecallCandidateOptions) ([]RecallCandidate, error) {
	limit := normalizeRecallLimit(filter.Limit, 10)
	fetchLimit := limit * normalizeRecallFetchMultiplier(opts.FetchMultiplier, 3)

	kwResults, err := s.memories.KeywordSearch(ctx, filter.Query, filter, fetchLimit)
	if err != nil {
		return nil, fmt.Errorf("keyword search: %w", err)
	}
	if len(kwResults) == 0 && shouldRunLooseKeywordFallback(filter.Query) {
		kwResults, err = s.looseTokenKeywordSearch(ctx, filter, fetchLimit)
		if err != nil {
			return nil, err
		}
	}
	return dedupRecallCandidatesByContent(mergeRecallCandidates(sourcePool, kwResults, nil, nil)), nil
}

func (s *MemoryService) hybridSearch(ctx context.Context, filter domain.MemoryFilter) ([]domain.Memory, int, error) {
	limit := filter.Limit
	if limit <= 0 || limit > 200 {
		limit = 10
	}
	offset := filter.Offset
	if offset < 0 {
		offset = 0
	}
	fetchLimit := limit * 3

	queryVec, err := s.embedder.Embed(ctx, filter.Query)
	observeRecallEmbeddingRequest(s.embedder, err)
	if err != nil {
		return nil, 0, fmt.Errorf("embed query for search: %w", err)
	}

	vecResults, vecErr := s.memories.VectorSearch(ctx, queryVec, filter, fetchLimit)
	if vecErr != nil {
		return nil, 0, fmt.Errorf("vector search: %w", vecErr)
	}

	minScore := filter.MinScore
	if minScore == 0 {
		minScore = defaultMinScore
	}
	if minScore > 0 {
		filtered := vecResults[:0]
		for _, m := range vecResults {
			if m.Score != nil && *m.Score >= minScore {
				filtered = append(filtered, m)
			}
		}
		vecResults = filtered
	}

	var kwResults []domain.Memory
	if s.memories.FTSAvailable() {
		var kwErr error
		kwResults, kwErr = s.memories.FTSSearch(ctx, filter.Query, filter, fetchLimit)
		if kwErr != nil {
			return nil, 0, fmt.Errorf("FTS search: %w", kwErr)
		}
	} else {
		var kwErr error
		kwResults, kwErr = s.memories.KeywordSearch(ctx, filter.Query, filter, fetchLimit)
		if kwErr != nil {
			return nil, 0, fmt.Errorf("keyword search: %w", kwErr)
		}
	}

	if len(vecResults) == 0 && len(kwResults) == 0 && shouldRunLooseKeywordFallback(filter.Query) {
		fallbackResults, fallbackErr := s.looseTokenKeywordSearch(ctx, filter, fetchLimit)
		if fallbackErr != nil {
			return nil, 0, fallbackErr
		}
		kwResults = fallbackResults
	}

	slog.Info("hybrid search completed", "query_len", len(filter.Query), "vec_results", len(vecResults), "kw_results", len(kwResults))

	scores := rrfMerge(kwResults, vecResults)
	mems := collectMems(kwResults, vecResults)
	applyTypeWeights(mems, scores)
	merged := sortByScore(mems, scores)

	page, total := s.paginate(merged, offset, limit)
	return finalizeSearchResults(setScores(page, scores), filter.Query), total, nil
}

func (s *MemoryService) hybridCandidates(ctx context.Context, filter domain.MemoryFilter, sourcePool RecallSourcePool, opts RecallCandidateOptions) ([]RecallCandidate, error) {
	limit := normalizeRecallLimit(filter.Limit, 10)
	fetchLimit := limit * normalizeRecallFetchMultiplier(opts.FetchMultiplier, 3)

	queryVec, err := s.embedder.Embed(ctx, filter.Query)
	observeRecallEmbeddingRequest(s.embedder, err)
	if err != nil {
		return nil, fmt.Errorf("embed query for search: %w", err)
	}

	vecResults, err := s.memories.VectorSearch(ctx, queryVec, filter, fetchLimit)
	if err != nil {
		return nil, fmt.Errorf("vector search: %w", err)
	}
	vecResults = applyMinScore(vecResults, filter.MinScore)

	var kwResults []domain.Memory
	if s.memories.FTSAvailable() {
		kwResults, err = s.memories.FTSSearch(ctx, filter.Query, filter, fetchLimit)
		if err != nil {
			return nil, fmt.Errorf("FTS search: %w", err)
		}
	} else {
		kwResults, err = s.memories.KeywordSearch(ctx, filter.Query, filter, fetchLimit)
		if err != nil {
			return nil, fmt.Errorf("keyword search: %w", err)
		}
	}

	if len(vecResults) == 0 && len(kwResults) == 0 && shouldRunLooseKeywordFallback(filter.Query) {
		kwResults, err = s.looseTokenKeywordSearch(ctx, filter, fetchLimit)
		if err != nil {
			return nil, err
		}
	}

	return dedupRecallCandidatesByContent(mergeRecallCandidates(sourcePool, kwResults, vecResults, nil)), nil
}

func (s *MemoryService) autoHybridSearch(ctx context.Context, filter domain.MemoryFilter) ([]domain.Memory, int, error) {
	limit := filter.Limit
	if limit <= 0 || limit > 200 {
		limit = 10
	}
	offset := filter.Offset
	if offset < 0 {
		offset = 0
	}
	fetchLimit := limit * 3

	vecResults, vecErr := s.memories.AutoVectorSearch(ctx, filter.Query, filter, fetchLimit)
	observeRecallAutoEmbeddingRequest(s.autoModel, vecErr, false)
	if vecErr != nil {
		return nil, 0, fmt.Errorf("auto vector search: %w", vecErr)
	}

	minScore := filter.MinScore
	if minScore == 0 {
		minScore = defaultMinScore
	}
	if minScore > 0 {
		filtered := vecResults[:0]
		for _, m := range vecResults {
			if m.Score != nil && *m.Score >= minScore {
				filtered = append(filtered, m)
			}
		}
		vecResults = filtered
	}

	var kwResults []domain.Memory
	if s.memories.FTSAvailable() {
		var kwErr error
		kwResults, kwErr = s.memories.FTSSearch(ctx, filter.Query, filter, fetchLimit)
		if kwErr != nil {
			return nil, 0, fmt.Errorf("FTS search: %w", kwErr)
		}
	} else {
		var kwErr error
		kwResults, kwErr = s.memories.KeywordSearch(ctx, filter.Query, filter, fetchLimit)
		if kwErr != nil {
			return nil, 0, fmt.Errorf("keyword search: %w", kwErr)
		}
	}

	if len(vecResults) == 0 && len(kwResults) == 0 && shouldRunLooseKeywordFallback(filter.Query) {
		fallbackResults, fallbackErr := s.looseTokenKeywordSearch(ctx, filter, fetchLimit)
		if fallbackErr != nil {
			return nil, 0, fallbackErr
		}
		kwResults = fallbackResults
	}

	slog.Info("auto hybrid search completed", "query_len", len(filter.Query), "vec_results", len(vecResults), "kw_results", len(kwResults))

	scores := rrfMerge(kwResults, vecResults)
	mems := collectMems(kwResults, vecResults)

	// Second-hop: skip when the best first-hop vector score is below the gate
	// threshold — a low score suggests the query has no strong match (e.g.
	// adversarial), so expanding search would mainly inject noise.
	maxVecScore := 0.0
	for _, m := range vecResults {
		if m.Score != nil && *m.Score > maxVecScore {
			maxVecScore = *m.Score
		}
	}
	if maxVecScore >= secondHopGateScore {
		secondHopMems := s.secondHopAutoSearch(ctx, mems, scores, filter, limit, secondHopTopN)
		for rank, m := range secondHopMems {
			scores[m.ID] += secondHopWeight / (rrfK + float64(rank+1))
			if _, exists := mems[m.ID]; !exists {
				mems[m.ID] = m
			}
		}
	}

	applyTypeWeights(mems, scores)
	merged := sortByScore(mems, scores)

	page, total := s.paginate(merged, offset, limit)
	return finalizeSearchResults(setScores(page, scores), filter.Query), total, nil
}

func (s *MemoryService) autoHybridCandidates(
	ctx context.Context,
	filter domain.MemoryFilter,
	sourcePool RecallSourcePool,
	opts RecallCandidateOptions,
) ([]RecallCandidate, error) {
	start := time.Now()
	limit := normalizeRecallLimit(filter.Limit, 10)
	fetchLimit := limit * normalizeRecallFetchMultiplier(opts.FetchMultiplier, 3)

	vectorStart := time.Now()
	vecResults, err := s.memories.AutoVectorSearch(ctx, filter.Query, filter, fetchLimit)
	observeRecallAutoEmbeddingRequest(s.autoModel, err, false)
	vectorDuration := time.Since(vectorStart)
	if err != nil {
		return nil, fmt.Errorf("auto vector search: %w", err)
	}
	vecResults = applyMinScore(vecResults, filter.MinScore)

	var kwResults []domain.Memory
	keywordStart := time.Now()
	if s.memories.FTSAvailable() {
		kwResults, err = s.memories.FTSSearch(ctx, filter.Query, filter, fetchLimit)
		if err != nil {
			return nil, fmt.Errorf("FTS search: %w", err)
		}
	} else {
		kwResults, err = s.memories.KeywordSearch(ctx, filter.Query, filter, fetchLimit)
		if err != nil {
			return nil, fmt.Errorf("keyword search: %w", err)
		}
	}
	keywordDuration := time.Since(keywordStart)

	if len(vecResults) == 0 && len(kwResults) == 0 && shouldRunLooseKeywordFallback(filter.Query) {
		kwResults, err = s.looseTokenKeywordSearch(ctx, filter, fetchLimit)
		if err != nil {
			return nil, err
		}
	}

	var secondHopResults []domain.Memory
	secondHopStart := time.Now()
	if opts.EnableSecondHop {
		maxVecScore := 0.0
		for _, m := range vecResults {
			if m.Score != nil && *m.Score > maxVecScore {
				maxVecScore = *m.Score
			}
		}
		if maxVecScore >= secondHopGateScore {
			scores := rrfMerge(kwResults, vecResults)
			mems := collectMems(kwResults, vecResults)
			topN := opts.SecondHopTopN
			if topN <= 0 {
				topN = secondHopTopN
			}
			secondHopResults = s.secondHopAutoSearch(ctx, mems, scores, filter, limit, topN)
		}
	}
	secondHopDuration := time.Since(secondHopStart)

	slog.InfoContext(ctx, "memory recall candidate search",
		"query_len", len(filter.Query),
		"source_pool", string(sourcePool),
		"memory_type", filter.MemoryType,
		"fetch_limit", fetchLimit,
		"vector_ms", vectorDuration.Milliseconds(),
		"keyword_ms", keywordDuration.Milliseconds(),
		"second_hop_ms", secondHopDuration.Milliseconds(),
		"second_hop_enabled", opts.EnableSecondHop,
		"second_hop_count", len(secondHopResults),
		"total_ms", time.Since(start).Milliseconds(),
	)

	return dedupRecallCandidatesByContent(mergeRecallCandidates(sourcePool, kwResults, vecResults, secondHopResults)), nil
}

// secondHopAutoSearch runs concurrent AutoVectorSearch calls using the top-N
// first-hop results as seed queries. Returns a merged, deduplicated, ranked list
// of second-hop results (excluding seed memories).
func (s *MemoryService) secondHopAutoSearch(
	ctx context.Context,
	firstHopMems map[string]domain.Memory,
	firstHopScores map[string]float64,
	filter domain.MemoryFilter,
	limit int,
	topN int,
) []domain.Memory {
	sorted := sortByScore(firstHopMems, firstHopScores)
	if topN <= 0 {
		topN = secondHopTopN
	}
	if topN > len(sorted) {
		topN = len(sorted)
	}
	if topN == 0 {
		return nil
	}

	seeds := sorted[:topN]
	seedIDs := make(map[string]struct{}, topN)
	for _, m := range seeds {
		seedIDs[m.ID] = struct{}{}
	}

	// Launch concurrent second-hop searches using first-hop embeddings
	// to avoid redundant embedding API calls.
	type hopResult struct {
		results []domain.Memory
		err     error
	}
	ch := make(chan hopResult, topN)
	for _, seed := range seeds {
		if len(seed.Embedding) > 0 {
			go func(vec []float32) {
				results, err := s.memories.VectorSearch(ctx, vec, filter, limit)
				ch <- hopResult{results: results, err: err}
			}(seed.Embedding)
		} else {
			go func(content string) {
				results, err := s.memories.AutoVectorSearch(ctx, content, filter, limit)
				ch <- hopResult{results: results, err: err}
			}(seed.Content)
		}
	}

	// Collect results: deduplicate, exclude seeds, keep best score per ID.
	bestByID := make(map[string]domain.Memory)
	bestScore := make(map[string]float64)
	for i := 0; i < topN; i++ {
		hr := <-ch
		if hr.err != nil {
			slog.Warn("second-hop search failed", "err", hr.err)
			continue
		}
		for _, m := range hr.results {
			if _, isSeed := seedIDs[m.ID]; isSeed {
				continue
			}
			if defaultMinScore > 0 && m.Score != nil && *m.Score < defaultMinScore {
				continue
			}
			sc := 0.0
			if m.Score != nil {
				sc = *m.Score
			}
			if prev, exists := bestScore[m.ID]; !exists || sc > prev {
				bestByID[m.ID] = m
				bestScore[m.ID] = sc
			}
		}
	}

	if len(bestByID) == 0 {
		return nil
	}

	// Sort by cosine similarity to produce a single ranked list for RRF.
	result := make([]domain.Memory, 0, len(bestByID))
	for _, m := range bestByID {
		result = append(result, m)
	}
	sort.Slice(result, func(i, j int) bool {
		return bestScore[result[i].ID] > bestScore[result[j].ID]
	})
	return result
}

func collectMems(kwResults, vecResults []domain.Memory) map[string]domain.Memory {
	mems := make(map[string]domain.Memory, len(kwResults)+len(vecResults))
	for _, m := range kwResults {
		mems[m.ID] = m
	}
	for _, m := range vecResults {
		if _, seen := mems[m.ID]; !seen {
			mems[m.ID] = m
		}
	}
	return mems
}

func sortByScore(mems map[string]domain.Memory, scores map[string]float64) []domain.Memory {
	result := make([]domain.Memory, 0, len(mems))
	for id := range mems {
		result = append(result, mems[id])
	}
	sort.Slice(result, func(i, j int) bool {
		return scores[result[i].ID] > scores[result[j].ID]
	})
	return result
}

// setScores sets the Score field on each memory.
// It preserves the original cosine similarity from vector search when available
// (set by VectorSearch/AutoVectorSearch as 1-distance), falling back to the
// RRF fusion score for keyword-only results.
func setScores(page []domain.Memory, scores map[string]float64) []domain.Memory {
	for i := range page {
		if page[i].Score == nil {
			sc := scores[page[i].ID]
			page[i].Score = &sc
		}
	}
	return page
}

// applyTypeWeights adjusts RRF scores based on memory_type.
// pinned = 1.5x boost (user-explicit memories), insight = 1.0x (standard).
func applyTypeWeights(mems map[string]domain.Memory, scores map[string]float64) {
	for id, m := range mems {
		if m.MemoryType == domain.TypePinned {
			scores[id] *= 1.5
		}
	}
}

// relativeAge returns a human-readable recency string for the given timestamp.
// Returns "just now" for timestamps in the future (clock skew) or under 1 minute.
func relativeAge(t time.Time) string {
	d := time.Since(t)
	if d < 0 {
		return "just now"
	}
	switch {
	case d < time.Minute:
		return "just now"
	case d < time.Hour:
		n := int(d.Minutes())
		if n == 1 {
			return "1 minute ago"
		}
		return fmt.Sprintf("%d minutes ago", n)
	case d < 24*time.Hour:
		n := int(d.Hours())
		if n == 1 {
			return "1 hour ago"
		}
		return fmt.Sprintf("%d hours ago", n)
	case d < 7*24*time.Hour:
		n := int(d.Hours() / 24)
		if n == 1 {
			return "1 day ago"
		}
		return fmt.Sprintf("%d days ago", n)
	case d < 30*24*time.Hour:
		n := int(d.Hours() / (24 * 7))
		if n == 1 {
			return "1 week ago"
		}
		return fmt.Sprintf("%d weeks ago", n)
	case d < 365*24*time.Hour:
		n := int(d.Hours() / (24 * 30))
		if n >= 12 {
			return "1 year ago"
		}
		if n == 1 {
			return "1 month ago"
		}
		return fmt.Sprintf("%d months ago", n)
	default:
		n := int(d.Hours() / (24 * 365))
		if n == 1 {
			return "1 year ago"
		}
		return fmt.Sprintf("%d years ago", n)
	}
}

func populateRelativeAge(memories []domain.Memory) []domain.Memory {
	for i := range memories {
		memories[i].RelativeAge = relativeAge(memories[i].UpdatedAt)
	}
	return memories
}

// Update modifies an existing memory with LWW conflict resolution.
func (s *MemoryService) Update(ctx context.Context, agentName, id, content string, tags []string, metadata json.RawMessage, ifMatch int) (*domain.Memory, error) {
	current, err := s.memories.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}

	if ifMatch > 0 && ifMatch != current.Version {
		slog.Warn("version conflict, applying LWW",
			"memory_id", id,
			"expected_version", ifMatch,
			"actual_version", current.Version,
			"agent", agentName,
		)
	}

	contentChanged := false
	if content != "" {
		if len(content) > maxContentLen {
			return nil, &domain.ValidationError{Field: "content", Message: "too long (max 50000)"}
		}
		current.Content = content
		contentChanged = true
	}
	if tags != nil {
		if len(tags) > maxTags {
			return nil, &domain.ValidationError{Field: "tags", Message: "too many (max 20)"}
		}
		current.Tags = tags
	}
	if metadata != nil {
		current.Metadata = metadata
	}
	current.UpdatedBy = agentName

	if contentChanged && s.autoModel == "" && s.embedder != nil {
		embedding, err := s.embedder.Embed(ctx, current.Content)
		if err != nil {
			return nil, err
		}
		current.Embedding = embedding
	}

	writeStart := time.Now()
	err = s.memories.UpdateOptimistic(ctx, current, 0)
	metrics.MemoryWriteDuration.WithLabelValues("update", metricStatus(err)).Observe(time.Since(writeStart).Seconds())
	if err != nil {
		return nil, err
	}

	updated, err := s.memories.GetByID(ctx, id)
	if err != nil {
		current.Version++
		return current, nil
	}
	return updated, nil
}

func (s *MemoryService) Delete(ctx context.Context, id, agentName string) (int64, error) {
	return s.memories.SoftDelete(ctx, id, agentName)
}

// BulkDelete soft-deletes multiple memories by ID. Returns the number of
// memories actually deleted (already-deleted rows are excluded from the count).
func (s *MemoryService) BulkDelete(ctx context.Context, ids []string, agentName string) (int64, error) {
	unique, err := ValidateBulkDeleteIDs(ids)
	if err != nil {
		return 0, err
	}

	return s.memories.BulkSoftDelete(ctx, unique, agentName)
}

func ValidateBulkDeleteIDs(ids []string) ([]string, error) {
	if len(ids) == 0 {
		return nil, &domain.ValidationError{Field: "ids", Message: "required"}
	}
	if len(ids) > maxBulkDeleteSize {
		return nil, &domain.ValidationError{Field: "ids", Message: "too many (max 1000)"}
	}

	seen := make(map[string]struct{}, len(ids))
	unique := make([]string, 0, len(ids))
	for _, id := range ids {
		if id == "" {
			continue
		}
		if _, ok := seen[id]; !ok {
			seen[id] = struct{}{}
			unique = append(unique, id)
		}
	}
	if len(unique) == 0 {
		return nil, &domain.ValidationError{Field: "ids", Message: "required"}
	}
	return unique, nil
}

func (s *MemoryService) Bootstrap(ctx context.Context, limit int) ([]domain.Memory, error) {
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	return s.memories.ListBootstrap(ctx, limit)
}

// BulkCreate creates multiple memories at once.
func (s *MemoryService) BulkCreate(ctx context.Context, agentName string, items []BulkMemoryInput) ([]domain.Memory, error) {
	if err := ValidateBulkMemoryInputs(items); err != nil {
		return nil, err
	}

	now := time.Now()
	memories := make([]*domain.Memory, 0, len(items))
	for _, item := range items {
		var embedding []float32
		if s.autoModel == "" && s.embedder != nil {
			var err error
			embedding, err = s.embedder.Embed(ctx, item.Content)
			if err != nil {
				return nil, err
			}
		}

		memories = append(memories, &domain.Memory{
			ID:         uuid.New().String(),
			Content:    item.Content,
			Source:     agentName,
			Tags:       item.Tags,
			Metadata:   item.Metadata,
			Embedding:  embedding,
			MemoryType: domain.TypePinned,
			AppID:      item.AppID,
			State:      domain.StateActive,
			Version:    1,
			UpdatedBy:  agentName,
			CreatedAt:  now,
			UpdatedAt:  now,
		})
	}

	writeStart := time.Now()
	err := s.memories.BulkCreate(ctx, memories)
	metrics.MemoryWriteDuration.WithLabelValues("bulk_create", metricStatus(err)).Observe(time.Since(writeStart).Seconds())
	if err != nil {
		return nil, err
	}

	result := make([]domain.Memory, len(memories))
	for i, m := range memories {
		result[i] = *m
	}
	return result, nil
}

func ValidateBulkMemoryInputs(items []BulkMemoryInput) error {
	if len(items) == 0 {
		return &domain.ValidationError{Field: "memories", Message: "required"}
	}
	if len(items) > maxBulkSize {
		return &domain.ValidationError{Field: "memories", Message: "too many (max 100)"}
	}

	for i, item := range items {
		if err := validateMemoryInput(item.Content, item.Tags); err != nil {
			var ve *domain.ValidationError
			if errors.As(err, &ve) {
				ve.Field = "memories[" + strconv.Itoa(i) + "]." + ve.Field
			}
			return err
		}
		appID := item.AppID
		if appID == "" {
			appID = item.AppIDLegacy
		}
		appID = strings.TrimSpace(appID)
		if len(appID) > 100 {
			return &domain.ValidationError{Field: "memories[" + strconv.Itoa(i) + "].appId", Message: "too long (max 100)"}
		}
		items[i].AppID = appID
	}
	return nil
}

// BulkMemoryInput is the input shape for each item in a bulk create request.
type BulkMemoryInput struct {
	Content     string          `json:"content"`
	AppID       string          `json:"appId,omitempty"`
	AppIDLegacy string          `json:"app_id,omitempty"`
	Tags        []string        `json:"tags,omitempty"`
	Metadata    json.RawMessage `json:"metadata,omitempty"`
}

func validateMemoryInput(content string, tags []string) error {
	if content == "" {
		return &domain.ValidationError{Field: "content", Message: "required"}
	}
	if len(content) > maxContentLen {
		return &domain.ValidationError{Field: "content", Message: "too long (max 50000)"}
	}
	if len(tags) > maxTags {
		return &domain.ValidationError{Field: "tags", Message: "too many (max 20)"}
	}
	return nil
}

func (s *MemoryService) CountStats(ctx context.Context) (total int64, last7d int64, err error) {
	return s.memories.CountStats(ctx)
}
