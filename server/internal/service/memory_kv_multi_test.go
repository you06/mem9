package service

import (
	"testing"

	"github.com/qiffang/mnemos/server/internal/domain"
)

// mergeMultiQueryResults is the pure quota-merge behind multi-query
// (facet) recall. These tests pin the contract the analysis asked
// for: every query's top results get early representation (round-robin
// by rank), duplicates collapse to one entry carrying full provenance,
// and the output is bounded by totalCap.

func mq(id string) domain.Memory { return domain.Memory{ID: id, Content: "c-" + id} }

func idsOf(ms []domain.Memory) []string {
	out := make([]string, len(ms))
	for i, m := range ms {
		out[i] = m.ID
	}
	return out
}

func TestMergeMultiQuery_RoundRobinQuota(t *testing.T) {
	// Query 0 has many strong hits; query 1 has two. Round-robin must
	// interleave so query 1's facet is represented early, not appended
	// after all of query 0's list.
	perQuery := [][]domain.Memory{
		{mq("a1"), mq("a2"), mq("a3"), mq("a4")},
		{mq("b1"), mq("b2")},
	}
	got := mergeMultiQueryResults(perQuery, 4)
	want := []string{"a1", "b1", "a2", "b2"}
	gotIDs := idsOf(got)
	for i := range want {
		if gotIDs[i] != want[i] {
			t.Fatalf("round-robin order: got %v want %v", gotIDs, want)
		}
	}
}

func TestMergeMultiQuery_DedupAccumulatesProvenance(t *testing.T) {
	// "shared" is recalled by both queries: one output entry, both
	// query indexes in MatchedQueries.
	perQuery := [][]domain.Memory{
		{mq("shared"), mq("a2")},
		{mq("shared"), mq("b2")},
	}
	got := mergeMultiQueryResults(perQuery, 10)
	if len(got) != 3 {
		t.Fatalf("want 3 unique, got %d: %v", len(got), idsOf(got))
	}
	if got[0].ID != "shared" {
		t.Fatalf("first should be shared, got %v", idsOf(got))
	}
	mp := got[0].MatchedQueries
	if len(mp) != 2 || mp[0] != 0 || mp[1] != 1 {
		t.Errorf("shared provenance = %v, want [0 1]", mp)
	}
	// Single-source entries carry their own index.
	for _, m := range got[1:] {
		if len(m.MatchedQueries) != 1 {
			t.Errorf("%s provenance = %v, want exactly one index", m.ID, m.MatchedQueries)
		}
	}
}

func TestMergeMultiQuery_TotalCap(t *testing.T) {
	perQuery := [][]domain.Memory{
		{mq("a1"), mq("a2"), mq("a3")},
		{mq("b1"), mq("b2"), mq("b3")},
	}
	got := mergeMultiQueryResults(perQuery, 4)
	if len(got) != 4 {
		t.Fatalf("cap not enforced: got %d", len(got))
	}
}

func TestMergeMultiQuery_EmptyAndBounds(t *testing.T) {
	if got := mergeMultiQueryResults(nil, 5); got != nil {
		t.Errorf("nil input -> nil, got %v", got)
	}
	if got := mergeMultiQueryResults([][]domain.Memory{{mq("a")}}, 0); got != nil {
		t.Errorf("zero cap -> nil, got %v", got)
	}
	// One empty list among non-empty ones is skipped, not fatal.
	got := mergeMultiQueryResults([][]domain.Memory{{}, {mq("b1")}}, 5)
	if len(got) != 1 || got[0].ID != "b1" {
		t.Errorf("empty list handling: got %v", idsOf(got))
	}
}

// MNEMO_RECALL_ABSOLUTE_CAP knob (frozen
// semantics): unset/0/garbage/negative = OFF (window stays limit*2);
// N>0 clamps the merged shown window to min(limit*2, N). It bounds the
// post-merge window ONLY — per-query recall depth is untouched (that
// lives in recallKVMulti's deepLimit, not in this composition).

func TestParseAbsoluteCap(t *testing.T) {
	cases := []struct {
		raw  string
		want int
	}{
		{"", 0},        // unset -> off
		{"  ", 0},      // blank -> off
		{"0", 0},       // explicit zero -> off
		{"-3", 0},      // negative -> off (never widen, never panic)
		{"twelve", 0},  // unparsable -> off, fail-safe
		{"12.5", 0},    // non-integer -> off
		{"12", 12},     // the candidate A/B value
		{" 8 ", 8},     // trimmed
		{"1000", 1000}, // large values allowed; min() makes them no-ops
	}
	for _, c := range cases {
		if got := parseAbsoluteCap(c.raw); got != c.want {
			t.Errorf("parseAbsoluteCap(%q) = %d, want %d", c.raw, got, c.want)
		}
	}
}

func TestShownCapFor(t *testing.T) {
	cases := []struct {
		limit, abs, want int
	}{
		{10, 0, 20},   // knob off -> relative cap, pre-knob behavior
		{20, 0, 40},   // knob off at the max-shown shape (limit 20 -> 40)
		{20, 12, 12},  // knob clamps a deep sweep
		{10, 12, 12},  // knob below relative cap -> clamps
		{5, 12, 10},   // relative cap already tighter -> knob is a no-op
		{6, 12, 12},   // boundary: equal -> unchanged
		{10, 100, 20}, // huge knob never widens the window
	}
	for _, c := range cases {
		if got := shownCapFor(c.limit, c.abs); got != c.want {
			t.Errorf("shownCapFor(%d, %d) = %d, want %d", c.limit, c.abs, got, c.want)
		}
	}
}

func TestResolveAbsoluteCap_DefaultOff(t *testing.T) {
	// Process-default pin: with the env var unset (the test process
	// never sets it), the cached resolver must report OFF and the
	// effective window must be byte-identical to pre-knob builds.
	if got := resolveAbsoluteCap(); got != 0 {
		t.Fatalf("default must be off, got %d", got)
	}
	if got := multiQueryShownCap(10); got != 20 {
		t.Fatalf("default window must stay limit*2, got %d", got)
	}
}
