package service

import (
	"strings"
	"testing"

	"github.com/qiffang/mnemos/server/internal/extractkeys"
)

// validateAgentKeys is the 7-step quality guard.
// These tests pin each rule independently
// so future tuning (e.g. adding entries to the stop-list, changing
// weight bounds) gets caught by a focused failure rather than a vague
// e2e regression.

func TestValidateAgentKeys_HappyPath(t *testing.T) {
	content := "User works at Acme Robotics and joined the perception team in 2024."
	keys := []RetrievalKey{
		{Text: "user works at Acme Robotics", Source: extractkeys.SourceAgent, Weight: 1.0},
		{Text: "Acme Robotics perception team", Source: extractkeys.SourceAgent, Weight: 0.8},
	}
	accepted, rejected := validateAgentKeys(content, keys)
	if len(accepted) != 2 {
		t.Fatalf("expected 2 accepted, got %d", len(accepted))
	}
	if len(rejected) != 0 {
		t.Fatalf("expected 0 rejected, got %d (%+v)", len(rejected), rejected)
	}
	for _, a := range accepted {
		if a.Source != extractkeys.SourceAgent {
			t.Errorf("unexpected source %q on accepted key", a.Source)
		}
	}
}

func TestValidateAgentKeys_EmptyText(t *testing.T) {
	accepted, rejected := validateAgentKeys("hello world", []RetrievalKey{
		{Text: "   ", Source: extractkeys.SourceAgent},
	})
	if len(accepted) != 0 || len(rejected) != 1 {
		t.Fatalf("want accepted=0 rejected=1, got %d/%d", len(accepted), len(rejected))
	}
	if rejected[0].Reason != string(reasonEmptyText) {
		t.Errorf("wrong reason: %s", rejected[0].Reason)
	}
}

func TestValidateAgentKeys_TextTooLong(t *testing.T) {
	long := strings.Repeat("a", maxAgentKeyTextLen+1)
	_, rejected := validateAgentKeys("hello world", []RetrievalKey{
		{Text: long, Source: extractkeys.SourceAgent},
	})
	if len(rejected) != 1 || rejected[0].Reason != string(reasonTextTooLong) {
		t.Fatalf("expected text_too_long, got %+v", rejected)
	}
}

func TestValidateAgentKeys_InvalidSource(t *testing.T) {
	cases := []string{
		"", // empty
		"extract",
		"extract_translation",
		"user",
		"feedback",
		"random_string",
	}
	for _, src := range cases {
		t.Run(src, func(t *testing.T) {
			_, rejected := validateAgentKeys("Acme test", []RetrievalKey{
				{Text: "Acme thing", Source: src},
			})
			if len(rejected) != 1 || rejected[0].Reason != string(reasonInvalidSource) {
				t.Fatalf("source=%q expected invalid_source reject, got %+v", src, rejected)
			}
		})
	}
}

func TestValidateAgentKeys_WeightBounds(t *testing.T) {
	// missing → 1.0
	accepted, _ := validateAgentKeys("Acme test", []RetrievalKey{
		{Text: "Acme thing", Source: extractkeys.SourceAgent},
	})
	if len(accepted) != 1 || accepted[0].Weight != 1.0 {
		t.Fatalf("missing weight should default to 1.0; got accepted=%+v", accepted)
	}

	// negative → reject
	_, rejected := validateAgentKeys("Acme test", []RetrievalKey{
		{Text: "Acme thing", Source: extractkeys.SourceAgent, Weight: -0.5},
	})
	if len(rejected) != 1 || rejected[0].Reason != string(reasonWeightTooSmall) {
		t.Fatalf("negative weight should reject; got %+v", rejected)
	}

	// 0 < w < 0.1 → reject
	_, rejected = validateAgentKeys("Acme test", []RetrievalKey{
		{Text: "Acme thing", Source: extractkeys.SourceAgent, Weight: 0.05},
	})
	if len(rejected) != 1 || rejected[0].Reason != string(reasonWeightTooSmall) {
		t.Fatalf("weight 0.05 should reject; got %+v", rejected)
	}

	// > 2.0 → clamp to 2.0 (not reject)
	accepted, _ = validateAgentKeys("Acme test", []RetrievalKey{
		{Text: "Acme thing", Source: extractkeys.SourceAgent, Weight: 5.0},
	})
	if len(accepted) != 1 || accepted[0].Weight != maxAgentKeyWeight {
		t.Fatalf("weight 5.0 should clamp to 2.0; got %+v", accepted)
	}
}

func TestValidateAgentKeys_StopList(t *testing.T) {
	// Stop list rejection must be normalize-aware: "User" with
	// trailing punctuation / whitespace still hits the rule.
	cases := []string{"user", "User", "USER", " user ", "user.", "home", "Project"}
	for _, k := range cases {
		t.Run(k, func(t *testing.T) {
			_, rejected := validateAgentKeys("Acme test", []RetrievalKey{
				{Text: k, Source: extractkeys.SourceAgent},
			})
			if len(rejected) != 1 || rejected[0].Reason != string(reasonStopListSingleToken) {
				t.Fatalf("stop-list miss for %q, got %+v", k, rejected)
			}
		})
	}
}

func TestValidateAgentKeys_NoEntityOverlap(t *testing.T) {
	// SourceAgent: key tokens must overlap with V tokens.
	_, rejected := validateAgentKeys(
		"Acme Robotics ships lidar calibration tool",
		[]RetrievalKey{
			{Text: "completely unrelated phrase here", Source: extractkeys.SourceAgent},
		},
	)
	if len(rejected) != 1 || rejected[0].Reason != string(reasonNoEntityOverlap) {
		t.Fatalf("expected no_entity_overlap reject, got %+v", rejected)
	}
}

func TestValidateAgentKeys_AgentTranslationOverlapRelaxed(t *testing.T) {
	// SourceAgentTranslation: cross-language K need NOT share tokens
	// with V, but must still have >= 2 non-trivial tokens + >= 3 chars
	// (prevents translation-as-bypass for single-token generics).
	// "アクメ ロボティクス" (Acme Robotics in Japanese katakana) is
	// disjoint from V tokens but should pass — meets length + token
	// requirements.
	accepted, rejected := validateAgentKeys(
		"Acme Robotics ships lidar calibration tool",
		[]RetrievalKey{
			{Text: "アクメ ロボティクス", Source: extractkeys.SourceAgentTranslation},
		},
	)
	if len(accepted) != 1 || len(rejected) != 0 {
		t.Fatalf("cross-language K should pass; got accepted=%d rejected=%+v", len(accepted), rejected)
	}

	// But single-token translation generic → reject.
	_, rejected = validateAgentKeys(
		"Acme Robotics ships lidar calibration tool",
		[]RetrievalKey{
			{Text: "用户", Source: extractkeys.SourceAgentTranslation},
		},
	)
	if len(rejected) != 1 || rejected[0].Reason != string(reasonTranslationTooThin) {
		t.Fatalf("single-token translation should reject; got %+v", rejected)
	}
}

func TestValidateAgentKeys_WordCountCap(t *testing.T) {
	long := "this is a very long key with more than eight words inside it"
	_, rejected := validateAgentKeys("this is the content", []RetrievalKey{
		{Text: long, Source: extractkeys.SourceAgent},
	})
	if len(rejected) != 1 || rejected[0].Reason != string(reasonWordCountExceeded) {
		t.Fatalf("expected word_count_exceeded, got %+v", rejected)
	}
}

func TestValidateAgentKeys_InBatchDedup(t *testing.T) {
	content := "Acme Robotics ships lidar"
	keys := []RetrievalKey{
		{Text: "Acme Robotics", Source: extractkeys.SourceAgent},
		{Text: "ACME robotics", Source: extractkeys.SourceAgent}, // same key_norm
	}
	accepted, rejected := validateAgentKeys(content, keys)
	if len(accepted) != 1 {
		t.Fatalf("expected 1 accepted (dedup), got %d", len(accepted))
	}
	if len(rejected) != 1 || rejected[0].Reason != string(reasonDuplicateKeyNorm) {
		t.Fatalf("expected duplicate_key_norm reject, got %+v", rejected)
	}
}

func TestValidateAgentKeys_PerValueCap(t *testing.T) {
	content := "Acme Robotics ships lidar tools and software and hardware and integrations"
	keys := make([]RetrievalKey, 0, maxAgentKeysPerV+3)
	for i := 0; i < maxAgentKeysPerV+3; i++ {
		// Distinct key_norm per entry by salting with a unique word
		// also present in V.
		w := []string{
			"Acme", "Robotics", "ships", "lidar", "tools",
			"software", "hardware", "integrations", "and",
			"Acme tools", "Robotics ships", "lidar tools", "tools software",
		}[i%13]
		keys = append(keys, RetrievalKey{
			Text:   w + " variant " + string(rune('a'+i)),
			Source: extractkeys.SourceAgent,
		})
	}
	accepted, rejected := validateAgentKeys(content, keys)
	if len(accepted) != maxAgentKeysPerV {
		t.Fatalf("expected accepted=cap=%d, got %d", maxAgentKeysPerV, len(accepted))
	}
	overflowCount := 0
	for _, r := range rejected {
		if r.Reason == string(reasonPerValueCapReached) {
			overflowCount++
		}
	}
	if overflowCount == 0 {
		t.Fatalf("expected at least one per_value_cap_reached, got rejected=%+v", rejected)
	}
}

func TestResolveAgentKeyWeight(t *testing.T) {
	cases := []struct {
		in      float64
		wantOK  bool
		wantOut float64
	}{
		{0, true, 1.0},   // missing → default
		{-1, false, 0},   // negative → reject
		{0.05, false, 0}, // below floor → reject
		{0.1, true, 0.1}, // at floor → preserve
		{1.5, true, 1.5}, // in range → preserve
		{2.0, true, 2.0}, // at ceiling → preserve
		{5.0, true, 2.0}, // above ceiling → clamp
	}
	for _, c := range cases {
		got, ok := resolveAgentKeyWeight(c.in)
		if ok != c.wantOK || (ok && got != c.wantOut) {
			t.Errorf("resolveAgentKeyWeight(%v) = (%v, %v); want (%v, %v)",
				c.in, got, ok, c.wantOut, c.wantOK)
		}
	}
}

// TestValidateExtractedKeySources_AcceptsCategory pins the fix for the
// review P1: extractkeys now emits SourceExtractCategory, and the
// store-path source allowlist must accept it. Before the fix, a
// server-extracted category key made storeKV abort after the V upsert
// with "invalid K source", silently breaking every LLM-extract store
// that produced a category key.
func TestValidateExtractedKeySources_AcceptsCategory(t *testing.T) {
	in := []extractkeys.ExtractedKey{
		{KeyText: "Melanie swimming with kids", KeyNorm: "melanie swimming with kids", Source: extractkeys.SourceExtract},
		{KeyText: "Melanie activities", KeyNorm: "melanie activities", Source: extractkeys.SourceExtractCategory},
		{KeyText: "Melanie hobbies", KeyNorm: "melanie hobbies", Source: extractkeys.SourceExtractCategory},
	}
	out, err := validateExtractedKeySources(in)
	if err != nil {
		t.Fatalf("category-source key must pass validation, got err: %v", err)
	}
	if len(out) != 3 {
		t.Fatalf("want 3 keys kept, got %d: %+v", len(out), out)
	}
	var gotCategory int
	for _, k := range out {
		if k.Source == extractkeys.SourceExtractCategory {
			gotCategory++
		}
	}
	if gotCategory != 2 {
		t.Errorf("want 2 category keys preserved, got %d", gotCategory)
	}
}

func TestValidateExtractedKeySources_RejectsUnknown(t *testing.T) {
	in := []extractkeys.ExtractedKey{
		{KeyText: "ok", KeyNorm: "ok", Source: extractkeys.SourceExtract},
		{KeyText: "bad", KeyNorm: "bad", Source: "not_a_real_source"},
	}
	if _, err := validateExtractedKeySources(in); err == nil {
		t.Fatal("unknown source must still be rejected")
	}
}
