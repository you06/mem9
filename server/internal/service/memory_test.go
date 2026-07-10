package service

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/qiffang/mnemos/server/internal/domain"
)

func floatEqual(a, b float64) bool {
	return math.Abs(a-b) < 1e-9
}

type bulkCreateCaptureRepo struct {
	memoryRepoMock
	bulkCreateCalls [][]domain.Memory
}

func (m *bulkCreateCaptureRepo) BulkCreate(_ context.Context, memories []*domain.Memory) error {
	copied := make([]domain.Memory, len(memories))
	for i, memory := range memories {
		copied[i] = *memory
	}
	m.bulkCreateCalls = append(m.bulkCreateCalls, copied)
	return nil
}

func TestApplyTypeWeights(t *testing.T) {
	tests := []struct {
		name   string
		mems   map[string]domain.Memory
		scores map[string]float64
		want   map[string]float64
	}{
		{
			name: "mixed types weighted",
			mems: map[string]domain.Memory{
				"pinned":  {ID: "pinned", MemoryType: domain.TypePinned},
				"insight": {ID: "insight", MemoryType: domain.TypeInsight},
			},
			scores: map[string]float64{
				"pinned":  1.0,
				"insight": 2.0,
			},
			want: map[string]float64{
				"pinned":  1.5,
				"insight": 2.0,
			},
		},
		{
			name:   "empty input",
			mems:   map[string]domain.Memory{},
			scores: map[string]float64{},
			want:   map[string]float64{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			applyTypeWeights(tt.mems, tt.scores)
			if len(tt.scores) != len(tt.want) {
				t.Fatalf("scores size mismatch: got %d want %d", len(tt.scores), len(tt.want))
			}
			for id, want := range tt.want {
				got, ok := tt.scores[id]
				if !ok {
					t.Fatalf("missing score for %s", id)
				}
				if !floatEqual(got, want) {
					t.Fatalf("score mismatch for %s: got %.12f want %.12f", id, got, want)
				}
			}
		})
	}
}

func TestRrfMerge(t *testing.T) {
	tests := []struct {
		name        string
		ftsResults  []domain.Memory
		vecResults  []domain.Memory
		wantScores  map[string]float64
		wantLen     int
		checkScores bool
	}{
		{
			name:       "disjoint results",
			ftsResults: []domain.Memory{{ID: "a"}, {ID: "b"}},
			vecResults: []domain.Memory{{ID: "c"}},
			wantScores: map[string]float64{
				"a": 1.0 / (rrfK + 1.0),
				"b": 1.0 / (rrfK + 2.0),
				"c": 1.0 / (rrfK + 1.0),
			},
			wantLen:     3,
			checkScores: true,
		},
		{
			name:       "overlapping results",
			ftsResults: []domain.Memory{{ID: "a"}, {ID: "b"}},
			vecResults: []domain.Memory{{ID: "b"}, {ID: "c"}},
			wantScores: map[string]float64{
				"a": 1.0 / (rrfK + 1.0),
				"b": 1.0/(rrfK+2.0) + 1.0/(rrfK+1.0),
				"c": 1.0 / (rrfK + 2.0),
			},
			wantLen:     3,
			checkScores: true,
		},
		{
			name:        "both empty",
			ftsResults:  nil,
			vecResults:  nil,
			wantScores:  map[string]float64{},
			wantLen:     0,
			checkScores: false,
		},
		{
			name:        "one empty",
			ftsResults:  []domain.Memory{{ID: "a"}},
			vecResults:  nil,
			wantScores:  map[string]float64{"a": 1.0 / (rrfK + 1.0)},
			wantLen:     1,
			checkScores: true,
		},
		{
			name:        "single in each",
			ftsResults:  []domain.Memory{{ID: "a"}},
			vecResults:  []domain.Memory{{ID: "b"}},
			wantScores:  map[string]float64{"a": 1.0 / (rrfK + 1.0), "b": 1.0 / (rrfK + 1.0)},
			wantLen:     2,
			checkScores: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scores := rrfMerge(tt.ftsResults, tt.vecResults)
			if len(scores) != tt.wantLen {
				t.Fatalf("score size mismatch: got %d want %d", len(scores), tt.wantLen)
			}
			if !tt.checkScores {
				return
			}
			for id, want := range tt.wantScores {
				got, ok := scores[id]
				if !ok {
					t.Fatalf("missing score for %s", id)
				}
				if !floatEqual(got, want) {
					t.Fatalf("score mismatch for %s: got %.12f want %.12f", id, got, want)
				}
			}
		})
	}
}

func TestValidateMemoryInput(t *testing.T) {
	tooLongContent := strings.Repeat("a", maxContentLen+1)
	tooManyTags := make([]string, maxTags+1)
	for i := range tooManyTags {
		tooManyTags[i] = "tag"
	}

	tests := []struct {
		name        string
		content     string
		tags        []string
		wantErr     bool
		wantField   string
		wantMessage string
	}{
		{
			name:    "valid input",
			content: "ok",
			tags:    []string{"a", "b"},
			wantErr: false,
		},
		{
			name:        "empty content",
			content:     "",
			tags:        nil,
			wantErr:     true,
			wantField:   "content",
			wantMessage: "required",
		},
		{
			name:        "content too long",
			content:     tooLongContent,
			tags:        nil,
			wantErr:     true,
			wantField:   "content",
			wantMessage: "too long (max 50000)",
		},
		{
			name:        "too many tags",
			content:     "ok",
			tags:        tooManyTags,
			wantErr:     true,
			wantField:   "tags",
			wantMessage: "too many (max 20)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateMemoryInput(tt.content, tt.tags)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error")
				}
				var ve *domain.ValidationError
				if !errors.As(err, &ve) {
					t.Fatalf("expected ValidationError, got %T", err)
				}
				if !errors.Is(err, domain.ErrValidation) {
					t.Fatalf("expected ErrValidation unwrap")
				}
				if ve.Field != tt.wantField {
					t.Fatalf("field mismatch: got %s want %s", ve.Field, tt.wantField)
				}
				if ve.Message != tt.wantMessage {
					t.Fatalf("message mismatch: got %s want %s", ve.Message, tt.wantMessage)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestCollectMems(t *testing.T) {
	tests := []struct {
		name       string
		kwResults  []domain.Memory
		vecResults []domain.Memory
		wantLen    int
		wantIDs    []string
		wantKWID   string
		wantKWText string
	}{
		{
			name: "collects from both and dedupes",
			kwResults: []domain.Memory{
				{ID: "shared", Content: "kw"},
				{ID: "kw-only", Content: "kw2"},
			},
			vecResults: []domain.Memory{
				{ID: "shared", Content: "vec"},
				{ID: "vec-only", Content: "vec2"},
			},
			wantLen:    3,
			wantIDs:    []string{"shared", "kw-only", "vec-only"},
			wantKWID:   "shared",
			wantKWText: "kw",
		},
		{
			name:       "empty inputs",
			kwResults:  nil,
			vecResults: nil,
			wantLen:    0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mems := collectMems(tt.kwResults, tt.vecResults)
			if len(mems) != tt.wantLen {
				t.Fatalf("map size mismatch: got %d want %d", len(mems), tt.wantLen)
			}
			for _, id := range tt.wantIDs {
				if _, ok := mems[id]; !ok {
					t.Fatalf("missing memory %s", id)
				}
			}
			if tt.wantKWID != "" {
				if got := mems[tt.wantKWID].Content; got != tt.wantKWText {
					t.Fatalf("kw precedence mismatch: got %s want %s", got, tt.wantKWText)
				}
			}
		})
	}
}

func TestSortByScore(t *testing.T) {
	mems := map[string]domain.Memory{
		"high": {ID: "high"},
		"tie1": {ID: "tie1"},
		"tie2": {ID: "tie2"},
		"low":  {ID: "low"},
	}
	scores := map[string]float64{
		"high": 0.9,
		"tie1": 0.5,
		"tie2": 0.5,
		"low":  0.1,
	}

	result := sortByScore(mems, scores)
	if len(result) != 4 {
		t.Fatalf("result size mismatch: got %d want %d", len(result), 4)
	}
	if result[0].ID != "high" {
		t.Fatalf("expected high score first, got %s", result[0].ID)
	}
	if !floatEqual(scores[result[1].ID], 0.5) || !floatEqual(scores[result[2].ID], 0.5) {
		t.Fatalf("expected tie scores in positions 2 and 3")
	}
	seenTie1 := result[1].ID == "tie1" || result[2].ID == "tie1"
	seenTie2 := result[1].ID == "tie2" || result[2].ID == "tie2"
	if !seenTie1 || !seenTie2 {
		t.Fatalf("expected tie1 and tie2 in top ties, got %s and %s", result[1].ID, result[2].ID)
	}
	if result[3].ID != "low" {
		t.Fatalf("expected low score last, got %s", result[3].ID)
	}
}

// TestSearchColdStartFallbackToKeyword verifies that when no embedder and no
// autoModel are configured and FTS is not yet available (cold start), Search()
// falls back to KeywordSearch instead of returning a hard error.
// TestSearchColdStartFallbackToKeyword and TestSearchFTSOnlyWhenAvailable
// were removed in step 4.1 of the K=>V refactor. Both asserted the
// pre-K=>V Search dispatcher's branching between hybrid/FTS/keyword
// based on availability of embedder/autoModel/FTS. Step 4 replaced
// that dispatcher with a single repo.RecallKV call using the V1 default
// strategy bitmask (0x0B), so the branches no longer exist.
//
// The Search dispatcher's pre-K=>V code is kept as searchLegacyDispatch
// in memory.go (//nolint:unused) for reference and rollback during the
// experimental phase; it will be deleted alongside the experimental retrieval work.

// TestSearchFallsBackToLooseTokensWhenFTSQuestionHasNoRows was removed
// when rebasing the K=>V branch onto main: it asserted the legacy Search
// dispatcher's FTS -> loose-keyword-token fallback, which step 4 of the
// K=>V refactor replaced with repo.RecallKV (mock-incompatible: K=>V
// requires the TiDB repository). Same treatment as the five dispatch
// tests dropped in step 4.1.

// TestSearchEmptyQueryReturnsList verifies that Search() with empty query
// delegates to List() instead of any search path.
func TestSearchEmptyQueryReturnsList(t *testing.T) {
	t.Parallel()

	memRepo := &memoryRepoMock{}
	svc := NewMemoryService(memRepo, nil, nil, "", ModeSmart)

	results, total, err := svc.Search(context.Background(), domain.MemoryFilter{
		Query: "",
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("Search() empty query error: %v", err)
	}
	// List returns nil, 0, nil from mock.
	if total != 0 || len(results) != 0 {
		t.Fatalf("expected empty results from List(), got total=%d results=%d", total, len(results))
	}
}

func TestSearchEmptyQueryPopulatesRelativeAge(t *testing.T) {
	t.Parallel()

	past := time.Now().Add(-5 * time.Minute)
	memRepo := &memoryRepoMock{
		listResults: []domain.Memory{
			{ID: "m1", Content: "hello", UpdatedAt: past, MemoryType: domain.TypeInsight, State: domain.StateActive},
		},
	}
	svc := NewMemoryService(memRepo, nil, nil, "", ModeSmart)

	results, total, err := svc.Search(context.Background(), domain.MemoryFilter{
		Query: "",
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("Search() error: %v", err)
	}
	if total != 1 || len(results) != 1 {
		t.Fatalf("expected 1 result, got total=%d results=%d", total, len(results))
	}
	if results[0].RelativeAge == "" {
		t.Fatal("expected RelativeAge to be populated, got empty string")
	}
}

// TestSearchIgnoresSessionAndSourceFilters was removed in step 4.1 of the
// K=>V refactor. It asserted the legacy Search dispatcher's behavior of
// clearing Source/SessionID filters before forwarding to KeywordSearch.
// Step 4 replaced the dispatcher with repo.RecallKV which does not
// consume those filter fields at all; the V1 strategy (0x0B = fast path
// + K-FTS + V-FTS) is filter-agnostic on the legacy MemoryFilter shape.

func TestContentKeywordSearchBypassesFTSAndVector(t *testing.T) {
	t.Parallel()

	memRepo := &memoryRepoMock{
		ftsAvail: true,
		kwResults: []domain.Memory{
			{ID: "kw-1", Content: "mem9小组负责验证", MemoryType: domain.TypeInsight, State: domain.StateActive},
		},
		ftsSearchHook: func(context.Context, string, domain.MemoryFilter, int) ([]domain.Memory, error) {
			t.Fatal("ContentKeywordSearch must not call FTS")
			return nil, nil
		},
	}
	svc := NewMemoryService(memRepo, nil, nil, "auto-model", ModeSmart)

	results, total, err := svc.ContentKeywordSearch(context.Background(), domain.MemoryFilter{
		Query:     "mem9小组",
		Source:    "console",
		SessionID: "session-1",
		AgentID:   "agent-1",
		Limit:     10,
	})
	if err != nil {
		t.Fatalf("ContentKeywordSearch() error: %v", err)
	}
	if total != 1 || len(results) != 1 || results[0].ID != "kw-1" {
		t.Fatalf("unexpected results: total=%d results=%+v", total, results)
	}
	if memRepo.lastKeywordFilter.Source != "console" {
		t.Fatalf("expected Source filter preserved, got %q", memRepo.lastKeywordFilter.Source)
	}
	if memRepo.lastKeywordFilter.SessionID != "session-1" {
		t.Fatalf("expected SessionID filter preserved, got %q", memRepo.lastKeywordFilter.SessionID)
	}
	if memRepo.lastAutoVectorFilter.Query != "" || memRepo.lastFTSFilter.Query != "" {
		t.Fatalf("direct keyword search unexpectedly touched vector/FTS filters: auto=%+v fts=%+v", memRepo.lastAutoVectorFilter, memRepo.lastFTSFilter)
	}
}

// Under the K=>V refactor, a non-TiDB repository (like memoryRepoMock)
// can't host the K=>V tables, so Create falls back to a single legacy
// raw write (storeLegacyFallback). The observable behavior pinned here
// is the same as the pre-K=>V no-LLM fast path: exactly one raw create,
// content unchanged, insight type.
func TestCreateFallsBackToRawWhenLLMUnavailable(t *testing.T) {
	t.Parallel()

	repo := &memoryRepoMock{}
	svc := NewMemoryService(repo, nil, nil, "", ModeSmart)

	mem, _, err := svc.Create(context.Background(), "agent-1", "", "user prefers dark mode", []string{"prefs"}, json.RawMessage(`{"source":"manual"}`))
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if mem == nil {
		t.Fatal("expected created memory")
	}
	if len(repo.createCalls) != 1 {
		t.Fatalf("expected 1 raw memory create, got %d", len(repo.createCalls))
	}
	if mem.Content != "user prefers dark mode" {
		t.Fatalf("expected raw content unchanged, got %q", mem.Content)
	}
	if mem.MemoryType != domain.TypeInsight {
		t.Fatalf("expected insight memory type, got %s", mem.MemoryType)
	}
}

func TestCreatePinnedUsesBulkCreateSemantics(t *testing.T) {
	t.Parallel()

	repo := &bulkCreateCaptureRepo{}
	svc := NewMemoryService(repo, nil, nil, "", ModeSmart)

	mem, written, err := svc.CreatePinned(
		context.Background(),
		"agent-1",
		"",
		"user prefers pour-over coffee",
		[]string{"preference", "coffee"},
		json.RawMessage(`{"source":"manual"}`),
	)
	if err != nil {
		t.Fatalf("CreatePinned() error = %v", err)
	}
	if mem == nil {
		t.Fatal("expected created memory")
	}
	if written != 1 {
		t.Fatalf("expected 1 written memory, got %d", written)
	}
	if len(repo.bulkCreateCalls) != 1 {
		t.Fatalf("expected 1 bulk create call, got %d", len(repo.bulkCreateCalls))
	}

	created := repo.bulkCreateCalls[0][0]
	if created.MemoryType != domain.TypePinned {
		t.Fatalf("expected pinned memory type, got %s", created.MemoryType)
	}
	if created.Source != "agent-1" {
		t.Fatalf("expected source agent-1, got %q", created.Source)
	}
	if created.UpdatedBy != "agent-1" {
		t.Fatalf("expected updated_by agent-1, got %q", created.UpdatedBy)
	}
	if created.State != domain.StateActive {
		t.Fatalf("expected active state, got %q", created.State)
	}
	if created.Content != "user prefers pour-over coffee" {
		t.Fatalf("expected content preserved, got %q", created.Content)
	}
	if len(created.Tags) != 2 || created.Tags[0] != "preference" || created.Tags[1] != "coffee" {
		t.Fatalf("expected tags preserved, got %v", created.Tags)
	}
	if string(created.Metadata) != `{"source":"manual"}` {
		t.Fatalf("expected metadata preserved, got %s", string(created.Metadata))
	}
	if mem.MemoryType != domain.TypePinned {
		t.Fatalf("expected returned memory type pinned, got %s", mem.MemoryType)
	}
}

// TestCreateRunsReconcilePipeline was removed in step 4.1 of the K=>V
// refactor. The reconciliation pipeline (ingest.ReconcileContent: facts
// extraction + LLM-driven merge/dedup) was the pre-K=>V mechanism for
// "should this new content add/update/skip an existing memory". Step 4
// replaced it with mechanical content_hash dedup at repo.UpsertMemoryValue
// plus multi-K alias generation. The reconciliation pipeline still
// exists in ingest.go but is no longer reachable from service.Create.
//
// No replacement reconciliation test is added — the K=>V store path is
// exercised end-to-end against real TiDB in the manual smoke tests;
// deeper unit coverage requires a *tidb.MemoryRepo fake that the legacy
// memoryRepoMock cannot provide.

func TestRelativeAge(t *testing.T) {
	now := time.Now()

	cases := []struct {
		name string
		t    time.Time
		want string
	}{
		{
			name: "future returns just now",
			t:    now.Add(10 * time.Minute),
			want: "just now",
		},
		{
			name: "30 seconds returns just now",
			t:    now.Add(-30 * time.Second),
			want: "just now",
		},
		{
			name: "1 minute singular",
			t:    now.Add(-90 * time.Second),
			want: "1 minute ago",
		},
		{
			name: "45 minutes plural",
			t:    now.Add(-45 * time.Minute),
			want: "45 minutes ago",
		},
		{
			name: "1 hour singular",
			t:    now.Add(-90 * time.Minute),
			want: "1 hour ago",
		},
		{
			name: "5 hours plural",
			t:    now.Add(-5 * time.Hour),
			want: "5 hours ago",
		},
		{
			name: "1 day singular",
			t:    now.Add(-36 * time.Hour),
			want: "1 day ago",
		},
		{
			name: "3 days plural",
			t:    now.Add(-3 * 24 * time.Hour),
			want: "3 days ago",
		},
		{
			name: "1 week singular",
			t:    now.Add(-10 * 24 * time.Hour),
			want: "1 week ago",
		},
		{
			name: "3 weeks plural",
			t:    now.Add(-25 * 24 * time.Hour),
			want: "3 weeks ago",
		},
		{
			name: "1 month singular",
			t:    now.Add(-45 * 24 * time.Hour),
			want: "1 month ago",
		},
		{
			name: "6 months plural",
			t:    now.Add(-180 * 24 * time.Hour),
			want: "6 months ago",
		},
		{
			name: "364 days caps at 1 year ago",
			t:    now.Add(-364 * 24 * time.Hour),
			want: "1 year ago",
		},
		{
			name: "400 days is 1 year ago",
			t:    now.Add(-400 * 24 * time.Hour),
			want: "1 year ago",
		},
		{
			name: "3 years plural",
			t:    now.Add(-3 * 365 * 24 * time.Hour),
			want: "3 years ago",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := relativeAge(tc.t)
			if got != tc.want {
				t.Errorf("relativeAge() = %q, want %q", got, tc.want)
			}
		})
	}
}
