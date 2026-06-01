// Package extractkeys generates query-side retrieval keys (K) from a
// canonical fact (V) for the K=>V recall path.
//
// Each V in memory_values may be reachable through multiple K aliases in
// memory_keys; Extract is the function that produces those aliases when
// store / backfill / V-update paths need them.
//
// Design notes (see #mem9-discussion:9dcf4b01 for the full thread):
//
//   - Keys should be SHORT DECLARATIVE phrases that share token/predicate
//     overlap with the original V. The empirical mem9 v1 weakness is that
//     pure semantic synonyms ("user residence" vs stored "lives in") miss
//     FTS; the K table mitigates by registering surfaces that DO share
//     overlap.
//
//   - Each call should produce both:
//
//   - An entity-word K ("home", "Chiba", "千叶") — short, high recall.
//
//   - A predicate-fragment K with preposition ("user lives in",
//     "用户住在") — matches stored declarative forms.
//
//   - Cross-language expansion is the default. mem9 is cross-session
//     long-term memory; the language of the storing turn and the language
//     of the querying turn are not coupled. Single-language K shards
//     recall by language.
//
//   - Keys with `source = "extract_translation"` are tagged separately so
//     ablation can compare with-translation vs without-translation
//     recall rates.
//
//   - LLM failures are NOT surfaced as orphan V to the caller — they
//     return (nil, error). The caller decides whether to leave V's
//     keys_extracted_at NULL (waiting for backfill) or surface the error
//     to the user. The "extracted 0 keys" case returns ([], nil) and is
//     a successful outcome (fill keys_extracted_at).
package extractkeys

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/qiffang/mnemos/server/internal/keynorm"
	"github.com/qiffang/mnemos/server/internal/llm"
)

// ExtractedKey is one retrieval surface emitted by Extract.
type ExtractedKey struct {
	// KeyText is the original surface; stored in memory_keys.key_text and
	// indexed by FULLTEXT for K-FTS matching.
	KeyText string
	// KeyNorm is keynorm.NormalizeKey(KeyText); stored in
	// memory_keys.key_norm and used for KEY_EXACT fast-path equality.
	KeyNorm string
	// Source is the provenance enum for memory_keys.source.
	// One of "extract" (same-language) or "extract_translation"
	// (cross-language expansion).
	Source string
}

// Source enum values, matching the schema's memory_keys.source ENUM.
const (
	SourceExtract            = "extract"
	SourceExtractTranslation = "extract_translation"
	SourceUser               = "user"     // reserved for client-provided K (v2)
	SourceFeedback           = "feedback" // reserved for query-miss feedback (v2)
)

// Config controls Extract.
type Config struct {
	// MaxKeysPerValue is the soft target. The prompt asks the LLM for
	// this many keys total (across both languages). Default 5.
	MaxKeysPerValue int
	// HardCapKeysPerValue caps the returned slice length defensively in
	// case the LLM ignores the soft target. Default 10.
	HardCapKeysPerValue int
	// EnableTranslation requests cross-language K expansion. When true,
	// keys produced in the language other than the V's primary language
	// are tagged source="extract_translation". Default true.
	EnableTranslation bool
	// Timeout is the per-attempt LLM call timeout. Default 3s.
	Timeout time.Duration
	// MaxRetries is the retry count on LLM error or JSON parse error.
	// Default 1 (so up to 2 attempts total). The retry passes the prior
	// error back to the LLM so it can self-correct.
	MaxRetries int
}

// DefaultConfig returns the V1 default tuning.
func DefaultConfig() Config {
	return Config{
		MaxKeysPerValue:     5,
		HardCapKeysPerValue: 10,
		EnableTranslation:   true,
		Timeout:             3 * time.Second,
		MaxRetries:          1,
	}
}

func (c Config) withDefaults() Config {
	if c.MaxKeysPerValue <= 0 {
		c.MaxKeysPerValue = 5
	}
	if c.HardCapKeysPerValue <= 0 {
		c.HardCapKeysPerValue = 10
	}
	if c.Timeout <= 0 {
		c.Timeout = 3 * time.Second
	}
	if c.MaxRetries < 0 {
		c.MaxRetries = 0
	}
	return c
}

// ErrLLMUnavailable is returned when the caller passed a nil llm.Client.
// The mem9 LLM client returns nil when no API key is configured (per
// repo convention), so this is a normal runtime state — the caller
// should leave keys_extracted_at NULL and let backfill retry once an
// LLM client becomes available.
var ErrLLMUnavailable = errors.New("extractkeys: llm client is not configured")

// Extract returns the K list for a single V content string. It is the
// only public entry point of this package.
//
// Semantics:
//
//   - nil llmClient → returns (nil, ErrLLMUnavailable). Caller leaves
//     memory_values.keys_extracted_at NULL.
//   - LLM timeout/error after retries → returns (nil, err). Caller
//     leaves keys_extracted_at NULL for backfill to retry.
//   - LLM returns 0 keys → returns ([], nil). Caller fills
//     keys_extracted_at (we tried, there was nothing to extract).
//   - LLM returns ≥1 keys → returns (keys, nil), capped at
//     HardCapKeysPerValue. Caller fills keys_extracted_at.
//
// content must be non-empty; an empty content returns ([], nil) without
// calling the LLM.
func Extract(ctx context.Context, llmClient *llm.Client, content string, cfg Config) ([]ExtractedKey, error) {
	if strings.TrimSpace(content) == "" {
		return nil, nil
	}
	if llmClient == nil {
		return nil, ErrLLMUnavailable
	}
	cfg = cfg.withDefaults()

	system := buildSystemPrompt(cfg)
	user := buildUserPrompt(content, cfg)

	var (
		raw       string
		callErr   error
		parsedOut llmResponse
	)
	for attempt := 0; attempt <= cfg.MaxRetries; attempt++ {
		attemptCtx, cancel := context.WithTimeout(ctx, cfg.Timeout)
		raw, callErr = llmClient.CompleteJSONWithScope(attemptCtx, system, user, llm.CallScope{Step: "extract_keys"})
		cancel()
		if callErr != nil {
			if attempt == cfg.MaxRetries {
				return nil, fmt.Errorf("extractkeys: llm call failed after %d attempt(s): %w", attempt+1, callErr)
			}
			continue
		}
		parsedOut, callErr = parseLLMResponse(raw)
		if callErr == nil {
			break
		}
		if attempt == cfg.MaxRetries {
			return nil, fmt.Errorf("extractkeys: parse failed after %d attempt(s): %w (raw=%q)", attempt+1, callErr, truncate(raw, 200))
		}
		// On parse retry, swap the user prompt to surface the error so the
		// LLM can self-correct.
		user = buildRetryPrompt(content, cfg, callErr, raw)
	}

	keys := normalizeExtracted(parsedOut, cfg)
	return keys, nil
}

type llmResponse struct {
	// Same-language K's (same script/language as input content).
	Native []string `json:"native"`
	// Cross-language K's (the other-language expansion).
	Translation []string `json:"translation,omitempty"`
}

func parseLLMResponse(raw string) (llmResponse, error) {
	cleaned := llm.StripMarkdownFences(raw)
	var out llmResponse
	if err := json.Unmarshal([]byte(cleaned), &out); err != nil {
		return llmResponse{}, fmt.Errorf("invalid JSON: %w", err)
	}
	return out, nil
}

// normalizeExtracted converts the LLM's two-bucket output into a flat,
// deduped, capped slice of ExtractedKey ready for INSERT IGNORE into
// memory_keys.
func normalizeExtracted(resp llmResponse, cfg Config) []ExtractedKey {
	seen := make(map[string]struct{})
	out := make([]ExtractedKey, 0, len(resp.Native)+len(resp.Translation))

	add := func(text, source string) {
		text = strings.TrimSpace(text)
		if text == "" {
			return
		}
		norm := keynorm.NormalizeKey(text)
		if norm == "" {
			return
		}
		if _, dup := seen[norm]; dup {
			return
		}
		seen[norm] = struct{}{}
		out = append(out, ExtractedKey{KeyText: text, KeyNorm: norm, Source: source})
	}

	for _, t := range resp.Native {
		add(t, SourceExtract)
	}
	if cfg.EnableTranslation {
		for _, t := range resp.Translation {
			add(t, SourceExtractTranslation)
		}
	}

	if len(out) > cfg.HardCapKeysPerValue {
		out = out[:cfg.HardCapKeysPerValue]
	}
	return out
}

func buildSystemPrompt(cfg Config) string {
	var b strings.Builder
	b.WriteString(`You extract retrieval keys for a single fact that will be stored in long-term memory.

A retrieval key is a SHORT DECLARATIVE phrase that an agent would use to look up this fact later, without already knowing the fact's exact wording. Each key MUST:
- Be a short declarative phrase, NOT a question. ("user lives in" is OK; "where does the user live?" is NOT.)
- Include EITHER a predicate fragment with its preposition ("user lives in", "project uses", "team deploys on") OR a key entity word from the fact ("home", "Chiba", "千叶", "React").
- Share token or predicate overlap with the original fact. Avoid pure semantic synonyms that share no tokens. ("residence" is BAD if the fact says "lives in" because no token overlap; "lives in" or "home location" is GOOD.)
- Be at most 8 words.

Aim for a mix of predicate-fragment keys AND entity-word keys.

`)
	if cfg.EnableTranslation {
		fmt.Fprintf(&b, `Output JSON with two arrays:
{
  "native": ["...","..."],         // keys in the SAME language as the input fact
  "translation": ["...","..."]     // keys in OTHER major languages used by agents (Chinese, English, Japanese — pick the languages most likely to be used to look up this fact in a future session) when the fact contains named entities or person/place/project references; empty array if the fact has no such entities or if the fact's language is the only relevant one
}

Target %d total keys across both arrays. Hard limit %d per array.
`, cfg.MaxKeysPerValue, cfg.HardCapKeysPerValue)
	} else {
		fmt.Fprintf(&b, `Output JSON with one array:
{
  "native": ["...","..."]
}

Target %d keys. Hard limit %d.
`, cfg.MaxKeysPerValue, cfg.HardCapKeysPerValue)
	}

	b.WriteString(`
Output ONLY the JSON object. No prose, no markdown fences, no commentary.`)
	return b.String()
}

func buildUserPrompt(content string, _ Config) string {
	return "Fact:\n" + content
}

func buildRetryPrompt(content string, cfg Config, prevErr error, prevRaw string) string {
	var b strings.Builder
	b.WriteString("Fact:\n")
	b.WriteString(content)
	b.WriteString("\n\nYour previous response could not be parsed as the required JSON.\n")
	b.WriteString("Error: ")
	b.WriteString(prevErr.Error())
	if prevRaw != "" {
		b.WriteString("\nPrevious response (truncated): ")
		b.WriteString(truncate(prevRaw, 200))
	}
	b.WriteString("\n\nReturn ONLY the JSON object described in the system instructions, with no markdown fences.")
	_ = cfg
	return b.String()
}

// truncate returns the first n runes of s, followed by "..." if the
// input was longer. Counting runes (not bytes) keeps multi-byte
// characters whole — error messages and prompt artifacts containing
// CJK text don't end up with mojibake at the truncation boundary.
func truncate(s string, n int) string {
	if utf8.RuneCountInString(s) <= n {
		return s
	}
	rs := []rune(s)
	return string(rs[:n]) + "..."
}
