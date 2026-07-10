package extractkeys

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/qiffang/mnemos/server/internal/llm"
)

// ---------- pure normalize/parse layer ----------

func TestParseLLMResponse_Plain(t *testing.T) {
	raw := `{"native":["a","b"],"translation":["c"]}`
	out, err := parseLLMResponse(raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(out.Native) != 2 || out.Native[0] != "a" {
		t.Errorf("native = %v, want [a b]", out.Native)
	}
	if len(out.Translation) != 1 || out.Translation[0] != "c" {
		t.Errorf("translation = %v, want [c]", out.Translation)
	}
}

func TestParseLLMResponse_StripsMarkdownFences(t *testing.T) {
	raw := "```json\n{\"native\":[\"x\"]}\n```"
	out, err := parseLLMResponse(raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(out.Native) != 1 || out.Native[0] != "x" {
		t.Errorf("native = %v, want [x]", out.Native)
	}
}

func TestParseLLMResponse_InvalidJSON(t *testing.T) {
	if _, err := parseLLMResponse("not json"); err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestNormalizeExtracted_DedupAcrossLanguages(t *testing.T) {
	// "User Lives In" (native) and "user lives in" (translation) collapse
	// under NormalizeKey to the same form; we should keep only one.
	resp := llmResponse{
		Native:      []string{"User Lives In", "Home"},
		Translation: []string{"user lives in", "千叶"},
	}
	out := normalizeExtracted(resp, DefaultConfig())
	if len(out) != 3 {
		t.Fatalf("got %d keys (want 3 after dedup): %+v", len(out), out)
	}
	if out[0].KeyNorm != "user lives in" || out[0].Source != SourceExtract {
		t.Errorf("first key wrong: %+v", out[0])
	}
	if out[1].KeyNorm != "home" || out[1].Source != SourceExtract {
		t.Errorf("second key wrong: %+v", out[1])
	}
	if out[2].KeyNorm != "千叶" || out[2].Source != SourceExtractTranslation {
		t.Errorf("third key wrong: %+v", out[2])
	}
}

func TestNormalizeExtracted_CategoryKeys(t *testing.T) {
	// Category keys (the aggregate-query bridge) are emitted with
	// source=extract_category, ordered AFTER native but BEFORE
	// translation so a hard-cap truncation drops cross-language K
	// before the category K that fixes aggregation recall.
	resp := llmResponse{
		Native:      []string{"Melanie swimming with kids"},
		Category:    []string{"Melanie activities", "Melanie hobbies"},
		Translation: []string{"梅兰妮游泳"},
	}
	out := normalizeExtracted(resp, DefaultConfig())
	if len(out) != 4 {
		t.Fatalf("got %d keys, want 4: %+v", len(out), out)
	}
	if out[0].Source != SourceExtract {
		t.Errorf("out[0] source = %q, want extract", out[0].Source)
	}
	if out[1].Source != SourceExtractCategory || out[1].KeyNorm != "melanie activities" {
		t.Errorf("out[1] wrong (want category 'melanie activities'): %+v", out[1])
	}
	if out[2].Source != SourceExtractCategory || out[2].KeyNorm != "melanie hobbies" {
		t.Errorf("out[2] wrong (want category 'melanie hobbies'): %+v", out[2])
	}
	if out[3].Source != SourceExtractTranslation {
		t.Errorf("out[3] source = %q, want extract_translation (after category)", out[3].Source)
	}
}

func TestNormalizeExtracted_CategoryDisabled(t *testing.T) {
	resp := llmResponse{
		Native:   []string{"Melanie swimming"},
		Category: []string{"Melanie activities"},
	}
	cfg := DefaultConfig()
	cfg.EnableCategoryKeys = false
	out := normalizeExtracted(resp, cfg)
	if len(out) != 1 || out[0].KeyText != "Melanie swimming" {
		t.Fatalf("category disabled should drop category keys; got %+v", out)
	}
}

func TestNormalizeExtracted_CategoryBeatsTranslationUnderCap(t *testing.T) {
	// With a tight cap, category K must survive while translation K is
	// truncated — category fixes the aggregation failure, cross-language
	// is secondary.
	resp := llmResponse{
		Native:      []string{"a specific fact"},
		Category:    []string{"subject activities"},
		Translation: []string{"译文一", "译文二", "译文三"},
	}
	cfg := DefaultConfig()
	cfg.HardCapKeysPerValue = 2
	out := normalizeExtracted(resp, cfg)
	if len(out) != 2 {
		t.Fatalf("want 2 under cap, got %d: %+v", len(out), out)
	}
	if out[1].Source != SourceExtractCategory {
		t.Errorf("category K should survive the cap before translation; got %+v", out)
	}
}

func TestNormalizeExtracted_TranslationDisabled(t *testing.T) {
	resp := llmResponse{
		Native:      []string{"home"},
		Translation: []string{"千叶"},
	}
	cfg := DefaultConfig()
	cfg.EnableTranslation = false
	out := normalizeExtracted(resp, cfg)
	if len(out) != 1 {
		t.Fatalf("got %d keys, want 1 (translation disabled): %+v", len(out), out)
	}
	if out[0].KeyText != "home" {
		t.Errorf("kept wrong key: %+v", out[0])
	}
}

func TestNormalizeExtracted_HardCap(t *testing.T) {
	// Generate 20 distinct native keys, hard cap 5.
	native := make([]string, 20)
	for i := 0; i < 20; i++ {
		native[i] = fmt.Sprintf("key %d", i)
	}
	cfg := DefaultConfig()
	cfg.HardCapKeysPerValue = 5
	out := normalizeExtracted(llmResponse{Native: native}, cfg)
	if len(out) != 5 {
		t.Errorf("got %d keys, want 5 (hard cap)", len(out))
	}
}

func TestNormalizeExtracted_SkipsEmpty(t *testing.T) {
	resp := llmResponse{
		Native: []string{"home", "", "   ", "Chiba"},
	}
	out := normalizeExtracted(resp, DefaultConfig())
	if len(out) != 2 {
		t.Errorf("got %d keys, want 2 (empties skipped): %+v", len(out), out)
	}
}

// ---------- Extract end-to-end with mock LLM ----------

// newMockLLM stands up an httptest server returning the given canned
// completions in order, with optional per-call delay. Subsequent calls
// after the canned list return the last entry.
type mockResp struct {
	body  string
	delay time.Duration
	code  int // 0 = 200
}

func newMockLLM(t *testing.T, responses []mockResp) (*llm.Client, *int64, func()) {
	t.Helper()
	var calls int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt64(&calls, 1)
		_, _ = io.Copy(io.Discard, r.Body)
		idx := int(n) - 1
		if idx >= len(responses) {
			idx = len(responses) - 1
		}
		resp := responses[idx]
		if resp.delay > 0 {
			time.Sleep(resp.delay)
		}
		if resp.code != 0 {
			w.WriteHeader(resp.code)
		}
		// Wrap the canned body in an OpenAI-style chat completion envelope so
		// llm.Client's Complete extracts it from choices[0].message.content.
		env := map[string]any{
			"choices": []map[string]any{{
				"message": map[string]any{"role": "assistant", "content": resp.body},
			}},
		}
		_ = json.NewEncoder(w).Encode(env)
	}))
	t.Cleanup(srv.Close)

	c := llm.New(llm.Config{
		APIKey:  "test",
		BaseURL: srv.URL,
		Model:   "test-model",
	})
	if c == nil {
		t.Fatal("llm.New returned nil")
	}
	return c, &calls, srv.Close
}

func TestExtract_NilLLM(t *testing.T) {
	out, err := Extract(context.Background(), nil, "User lives in Chiba.", DefaultConfig())
	if err == nil || !errors.Is(err, ErrLLMUnavailable) {
		t.Fatalf("got err=%v, want ErrLLMUnavailable", err)
	}
	if out != nil {
		t.Errorf("expected nil keys, got %+v", out)
	}
}

func TestExtract_EmptyContent(t *testing.T) {
	// Should short-circuit without calling LLM.
	out, err := Extract(context.Background(), nil /* nil LLM proves no call */, "   ", DefaultConfig())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out != nil {
		t.Errorf("expected nil keys for empty content, got %+v", out)
	}
}

func TestExtract_HappyPath(t *testing.T) {
	body := `{"native":["user lives in","home","千叶"],"translation":["Chiba","user residence"]}`
	c, calls, _ := newMockLLM(t, []mockResp{{body: body}})

	out, err := Extract(context.Background(), c, "User lives in Chiba Prefecture, Japan.", DefaultConfig())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if atomic.LoadInt64(calls) != 1 {
		t.Errorf("got %d LLM calls, want 1", *calls)
	}
	if len(out) != 5 {
		t.Fatalf("got %d keys, want 5: %+v", len(out), out)
	}
	// Verify Source split.
	nNative, nTrans := 0, 0
	for _, k := range out {
		if k.Source == SourceExtract {
			nNative++
		}
		if k.Source == SourceExtractTranslation {
			nTrans++
		}
		if k.KeyNorm == "" {
			t.Errorf("key %+v has empty KeyNorm", k)
		}
	}
	if nNative != 3 || nTrans != 2 {
		t.Errorf("source split: native=%d, translation=%d (want 3, 2)", nNative, nTrans)
	}
}

func TestExtract_EmptyKeysSuccess(t *testing.T) {
	// LLM legitimately returns 0 keys — Extract must succeed so caller
	// fills keys_extracted_at and backfill stops retrying.
	body := `{"native":[],"translation":[]}`
	c, _, _ := newMockLLM(t, []mockResp{{body: body}})

	out, err := Extract(context.Background(), c, "anything", DefaultConfig())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(out) != 0 {
		t.Errorf("got %d keys, want 0", len(out))
	}
}

func TestExtract_ParseRetrySucceeds(t *testing.T) {
	c, calls, _ := newMockLLM(t, []mockResp{
		{body: "not json at all"},
		{body: `{"native":["x","y"]}`},
	})

	cfg := DefaultConfig()
	cfg.MaxRetries = 1
	out, err := Extract(context.Background(), c, "fact", cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if atomic.LoadInt64(calls) != 2 {
		t.Errorf("got %d LLM calls, want 2 (first parse fail, second succeed)", *calls)
	}
	if len(out) != 2 {
		t.Errorf("got %d keys, want 2", len(out))
	}
}

func TestExtract_ParseFailureAfterRetries(t *testing.T) {
	c, calls, _ := newMockLLM(t, []mockResp{
		{body: "not json"},
		{body: "still not json"},
	})

	cfg := DefaultConfig()
	cfg.MaxRetries = 1
	out, err := Extract(context.Background(), c, "fact", cfg)
	if err == nil {
		t.Fatal("expected error after exhausting retries")
	}
	if out != nil {
		t.Errorf("expected nil keys on failure, got %+v", out)
	}
	if atomic.LoadInt64(calls) != 2 {
		t.Errorf("got %d LLM calls, want 2", *calls)
	}
}

func TestExtract_TimeoutSurfacesError(t *testing.T) {
	// LLM delays 200ms; timeout is 20ms → should error.
	body := `{"native":["x"]}`
	c, _, _ := newMockLLM(t, []mockResp{{body: body, delay: 200 * time.Millisecond}})

	cfg := DefaultConfig()
	cfg.Timeout = 20 * time.Millisecond
	cfg.MaxRetries = 0
	out, err := Extract(context.Background(), c, "fact", cfg)
	if err == nil {
		t.Fatal("expected timeout error")
	}
	if !strings.Contains(err.Error(), "llm call failed") {
		t.Errorf("error doesn't mention llm call failure: %v", err)
	}
	if out != nil {
		t.Errorf("expected nil keys on timeout, got %+v", out)
	}
}

// TestTruncate_UTF8Safe pins the rune-counted truncate so error messages
// containing CJK don't produce mojibake at the boundary.
func TestTruncate_UTF8Safe(t *testing.T) {
	// 5 Chinese chars = 15 bytes in UTF-8.
	s := "用户住在千叶"
	got := truncate(s, 3)
	want := "用户住..."
	if got != want {
		t.Errorf("truncate(%q, 3) = %q, want %q", s, got, want)
	}

	// Mixed ASCII + CJK; first 5 runes are "abc用户".
	got = truncate("abc用户在千叶", 5)
	want = "abc用户..."
	if got != want {
		t.Errorf("truncate mixed = %q, want %q", got, want)
	}

	// Shorter than limit returns unchanged.
	if got := truncate("short", 100); got != "short" {
		t.Errorf("truncate short = %q, want %q", got, "short")
	}
}

func TestExtract_PromptIncludesTranslationWhenEnabled(t *testing.T) {
	// We can't directly inspect the prompt from outside, but we can check
	// the system prompt builder.
	cfg := DefaultConfig()
	cfg.EnableTranslation = true
	sys := buildSystemPrompt(cfg)
	if !strings.Contains(sys, "translation") {
		t.Errorf("translation-enabled system prompt missing translation key")
	}

	cfg.EnableTranslation = false
	sys = buildSystemPrompt(cfg)
	if strings.Contains(sys, "translation") {
		t.Errorf("translation-disabled system prompt unexpectedly includes translation key")
	}
}
