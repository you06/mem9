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
	"github.com/qiffang/mnemos/server/internal/repository"
)

const (
	maxContentLen   = 50000
	maxTags         = 20
	maxBulkSize     = 100
	defaultMinScore = 0.3
	maxGraphExpand  = 10 // cap graph expansion to avoid flooding results with noise
)

type MemoryService struct {
	memories  repository.MemoryRepo
	embedder  *embed.Embedder
	autoModel string
	ingest    *IngestService
	graph     *GraphService
}

func NewMemoryService(memories repository.MemoryRepo, llmClient *llm.Client, embedder *embed.Embedder, autoModel string, ingestMode IngestMode, graphSvc *GraphService) *MemoryService {
	return &MemoryService{
		memories:  memories,
		embedder:  embedder,
		autoModel: autoModel,
		ingest:    NewIngestService(memories, llmClient, embedder, autoModel, ingestMode, graphSvc),
		graph:     graphSvc,
	}
}

func (s *MemoryService) Create(ctx context.Context, agentID, sessionID, content string, tags []string, metadata json.RawMessage) (*domain.Memory, error) {
	if err := validateMemoryInput(content, tags); err != nil {
		return nil, err
	}

	if s.ingest == nil {
		return nil, fmt.Errorf("ingest service not configured")
	}

	if !s.ingest.HasLLM() {
		// Keep no-LLM create as a single write so API semantics remain predictable.
		// This branch intentionally avoids a "create then patch tags/metadata" flow,
		// which could otherwise return an error after content is already persisted.
		var embedding []float32
		if s.autoModel == "" && s.embedder != nil {
			embeddingResult, embedErr := s.embedder.Embed(ctx, content)
			if embedErr != nil {
				return nil, fmt.Errorf("embed raw content: %w", embedErr)
			}
			embedding = embeddingResult
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
			State:      domain.StateActive,
			Version:    1,
			UpdatedBy:  agentID,
			CreatedAt:  now,
			UpdatedAt:  now,
		}
		if err := s.memories.Create(ctx, mem); err != nil {
			return nil, fmt.Errorf("create raw memory: %w", err)
		}
		return mem, nil
	}

	result, err := s.ingest.ReconcileContent(ctx, agentID, agentID, sessionID, []string{content})
	if err != nil {
		return nil, err
	}

	if result.Status == "failed" {
		return nil, fmt.Errorf("content reconciliation failed")
	}
	if len(result.InsightIDs) == 0 {
		return nil, nil
	}

	// Apply user-provided tags/metadata to all created insights.
	for _, id := range result.InsightIDs {
		mem, err := s.memories.GetByID(ctx, id)
		if err != nil {
			continue
		}
		if len(tags) > 0 {
			mem.Tags = tags
		}
		if len(metadata) > 0 {
			mem.Metadata = metadata
		}
		if len(tags) > 0 || len(metadata) > 0 {
			_ = s.memories.UpdateOptimistic(ctx, mem, 0)
		}
	}

	latestID := result.InsightIDs[len(result.InsightIDs)-1]
	mem, getErr := s.memories.GetByID(ctx, latestID)
	if getErr != nil {
		return nil, fmt.Errorf("fetch reconciled memory %s: %w", latestID, getErr)
	}
	return mem, nil

}

// Get returns a single memory by ID.
func (s *MemoryService) Get(ctx context.Context, id string) (*domain.Memory, error) {
	return s.memories.GetByID(ctx, id)
}

func (s *MemoryService) Search(ctx context.Context, filter domain.MemoryFilter) ([]domain.Memory, int, error) {
	if filter.Query == "" {
		mems, total, err := s.memories.List(ctx, filter)
		if err != nil {
			return nil, 0, err
		}
		return populateRelativeAge(mems), total, nil
	}
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

// expandViaGraph takes seed memory IDs from existing hybrid results, performs
// 1-hop graph expansion, fetches the expanded memories, and returns them as
// additional candidates with their graph-derived RRF scores.
func (s *MemoryService) expandViaGraph(ctx context.Context, seedIDs []string, agentID string, limit int) ([]domain.Memory, map[string]float64) {
	if s.graph == nil || len(seedIDs) == 0 {
		return nil, nil
	}
	hits, err := s.graph.ExpandFromMemories(ctx, seedIDs, agentID, limit)
	if err != nil {
		slog.Warn("graph expansion failed, skipping", "err", err)
		return nil, nil
	}
	if len(hits) == 0 {
		return nil, nil
	}

	// Deduplicate memory IDs from graph hits and collect scores.
	hitScores := make(map[string]float64, len(hits))
	var expandedIDs []string
	for _, h := range hits {
		if _, seen := hitScores[h.MemoryID]; !seen {
			expandedIDs = append(expandedIDs, h.MemoryID)
		}
		// Accumulate graph confidence as a score contribution.
		hitScores[h.MemoryID] += h.GraphScore
	}

	// Fetch the actual memory objects for graph-expanded IDs.
	var graphMems []domain.Memory
	for _, id := range expandedIDs {
		mem, err := s.memories.GetByID(ctx, id)
		if err != nil {
			slog.Debug("graph expand: could not fetch memory", "id", id, "err", err)
			continue
		}
		graphMems = append(graphMems, *mem)
	}

	// Convert graph hits to RRF-style scores (ranked by confidence).
	graphRRF := make(map[string]float64, len(graphMems))
	for rank, m := range graphMems {
		graphRRF[m.ID] = 1.0 / (rrfK + float64(rank+1))
	}

	slog.Info("graph expansion completed", "seed_count", len(seedIDs), "expanded", len(graphMems))
	return graphMems, graphRRF
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

// ftsStopWords contains a small set of function words that add noise to BM25
// without carrying content signal. Intentionally conservative — words like
// "who", "where", "when" are kept because they may carry semantic value.
var ftsStopWords = map[string]struct{}{
	"the": {}, "a": {}, "an": {}, "of": {}, "is": {}, "are": {},
	"do": {}, "does": {}, "did": {}, "was": {}, "were": {},
	"in": {}, "on": {}, "at": {}, "to": {}, "for": {}, "and": {},
	"or": {}, "it": {}, "its": {}, "be": {}, "been": {}, "being": {},
	"has": {}, "have": {}, "had": {}, "that": {}, "this": {},
}

// normalizeQueryForFTS cleans a natural-language query for better BM25 matching:
// lowercase, strip punctuation, remove possessive 's, drop function words.
// Falls back to the original query if the result is empty.
func normalizeQueryForFTS(raw string) string {
	raw = strings.ToLower(raw)
	// Strip possessive: "sarah's" → "sarah"
	raw = strings.ReplaceAll(raw, "'s", "")
	raw = strings.ReplaceAll(raw, "\u2019s", "") // curly apostrophe

	var tokens []string
	for _, word := range strings.FieldsFunc(raw, func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r)
	}) {
		if _, stop := ftsStopWords[word]; stop {
			continue
		}
		tokens = append(tokens, word)
	}
	if len(tokens) == 0 {
		return raw // fallback to original
	}
	return strings.Join(tokens, " ")
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

	query := normalizeQueryForFTS(filter.Query)
	ftsResults, err := s.memories.FTSSearch(ctx, query, filter, fetchLimit)
	if err != nil {
		return nil, 0, fmt.Errorf("FTS search: %w", err)
	}
	slog.Info("fts search completed", "query_len", len(filter.Query), "results", len(ftsResults))

	page, total := s.paginate(ftsResults, offset, limit)
	return populateRelativeAge(page), total, nil
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
	slog.Info("keyword search completed (FTS unavailable)", "query_len", len(filter.Query), "results", len(kwResults))

	page, total := s.paginate(kwResults, offset, limit)
	return populateRelativeAge(page), total, nil
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
	fetchLimit := limit * 5

	queryVec, err := s.embedder.Embed(ctx, filter.Query)
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

	slog.Info("hybrid search completed", "query_len", len(filter.Query), "vec_results", len(vecResults), "kw_results", len(kwResults))

	scores := rrfMerge(kwResults, vecResults)
	mems := collectMems(kwResults, vecResults)

	// Graph expansion: boost-only mode. Run 1-hop expansion from top-K seeds,
	// but only boost scores of memories already in hybrid results — do not
	// introduce new candidates, to avoid diluting query-relevant results.
	seedIDs := topKIDs(mems, scores, limit)
	_, graphScores := s.expandViaGraph(ctx, seedIDs, filter.AgentID, fetchLimit)
	for id, gs := range graphScores {
		if _, exists := mems[id]; exists {
			scores[id] += gs
		}
	}

	applyTypeWeights(mems, scores)
	merged := sortByScore(mems, scores)

	page, total := s.paginate(merged, offset, limit)
	return populateRelativeAge(setScores(page, scores)), total, nil
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
	fetchLimit := limit * 5

	vecResults, vecErr := s.memories.AutoVectorSearch(ctx, filter.Query, filter, fetchLimit)
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

	slog.Info("auto hybrid search completed", "query_len", len(filter.Query), "vec_results", len(vecResults), "kw_results", len(kwResults))

	scores := rrfMerge(kwResults, vecResults)
	mems := collectMems(kwResults, vecResults)

	// Graph expansion: boost-only mode (same as hybridSearch above).
	seedIDs := topKIDs(mems, scores, limit)
	_, graphScores := s.expandViaGraph(ctx, seedIDs, filter.AgentID, fetchLimit)
	for id, gs := range graphScores {
		if _, exists := mems[id]; exists {
			scores[id] += gs
		}
	}

	applyTypeWeights(mems, scores)
	merged := sortByScore(mems, scores)

	page, total := s.paginate(merged, offset, limit)
	return populateRelativeAge(setScores(page, scores)), total, nil
}

// topKIDs returns up to k memory IDs sorted by descending score.
func topKIDs(mems map[string]domain.Memory, scores map[string]float64, k int) []string {
	sorted := sortByScore(mems, scores)
	if len(sorted) > k {
		sorted = sorted[:k]
	}
	ids := make([]string, len(sorted))
	for i, m := range sorted {
		ids[i] = m.ID
	}
	return ids
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

	if err := s.memories.UpdateOptimistic(ctx, current, 0); err != nil {
		return nil, err
	}

	updated, err := s.memories.GetByID(ctx, id)
	if err != nil {
		current.Version++
		return current, nil
	}
	return updated, nil
}

func (s *MemoryService) Delete(ctx context.Context, id, agentName string) error {
	return s.memories.SoftDelete(ctx, id, agentName)
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
	if len(items) == 0 {
		return nil, &domain.ValidationError{Field: "memories", Message: "required"}
	}
	if len(items) > maxBulkSize {
		return nil, &domain.ValidationError{Field: "memories", Message: "too many (max 100)"}
	}

	now := time.Now()
	memories := make([]*domain.Memory, 0, len(items))
	for i, item := range items {
		if err := validateMemoryInput(item.Content, item.Tags); err != nil {
			var ve *domain.ValidationError
			if errors.As(err, &ve) {
				ve.Field = "memories[" + strconv.Itoa(i) + "]." + ve.Field
			}
			return nil, err
		}

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
			State:      domain.StateActive,
			Version:    1,
			UpdatedBy:  agentName,
			CreatedAt:  now,
			UpdatedAt:  now,
		})
	}

	if err := s.memories.BulkCreate(ctx, memories); err != nil {
		return nil, err
	}

	result := make([]domain.Memory, len(memories))
	for i, m := range memories {
		result[i] = *m
	}
	return result, nil
}

// BulkMemoryInput is the input shape for each item in a bulk create request.
type BulkMemoryInput struct {
	Content  string          `json:"content"`
	Tags     []string        `json:"tags,omitempty"`
	Metadata json.RawMessage `json:"metadata,omitempty"`
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
