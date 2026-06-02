package tidb

import (
	"testing"
)

// ---------- RetrievalStrategy bitmask ----------

func TestRetrievalStrategy_BitValues(t *testing.T) {
	// These values are an API contract with the service / handler layers
	// and the ablation harness. Changing them is a breaking change.
	tests := []struct {
		name string
		bit  RetrievalStrategy
		want uint8
	}{
		{"KEY_EXACT", StrategyKeyExact, 0x01},
		{"KEY_FTS", StrategyKeyFTS, 0x02},
		{"KEY_VEC", StrategyKeyVec, 0x04},
		{"VAL_FTS", StrategyValFTS, 0x08},
		{"VAL_VEC", StrategyValVec, 0x10},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if uint8(tc.bit) != tc.want {
				t.Errorf("%s = 0x%02X, want 0x%02X", tc.name, uint8(tc.bit), tc.want)
			}
		})
	}
}

func TestRetrievalStrategy_DefaultV1(t *testing.T) {
	// Step 4.5 turned on the vector bits: V1 default is now all 5
	// strategy bits (fast path + K-FTS + K-VEC + V-FTS + V-VEC).
	// @tmgg06 observed FTS-only recall was unstable when query
	// tokens didn't overlap stored K text; vector recall provides
	// semantic-match fallback. Vector paths use VEC_EMBED_COSINE_DISTANCE
	// when autoModel is configured (server-side embedding) or
	// VEC_COSINE_DISTANCE with caller queryVec otherwise.
	want := RetrievalStrategy(0x1F) // = KEY_EXACT | KEY_FTS | KEY_VEC | VAL_FTS | VAL_VEC
	if StrategyDefaultV1 != want {
		t.Errorf("StrategyDefaultV1 = 0x%02X, want 0x%02X (all 5 bits)",
			uint8(StrategyDefaultV1), uint8(want))
	}

	// Spot check individual bits.
	if StrategyDefaultV1&StrategyKeyExact == 0 {
		t.Error("V1 default missing KEY_EXACT")
	}
	if StrategyDefaultV1&StrategyKeyFTS == 0 {
		t.Error("V1 default missing KEY_FTS")
	}
	if StrategyDefaultV1&StrategyKeyVec == 0 {
		t.Error("V1 default missing KEY_VEC (step 4.5 turned this on)")
	}
	if StrategyDefaultV1&StrategyValFTS == 0 {
		t.Error("V1 default missing VAL_FTS")
	}
	if StrategyDefaultV1&StrategyValVec == 0 {
		t.Error("V1 default missing VAL_VEC (step 4.5 turned this on)")
	}
}

// ---------- RRF merge ----------

func TestRRFMerge_Empty(t *testing.T) {
	if got := rrfMerge(nil, 10); got != nil {
		t.Errorf("rrfMerge(nil) = %v, want nil", got)
	}
	if got := rrfMerge([][]string{}, 10); got != nil {
		t.Errorf("rrfMerge([]) = %v, want nil", got)
	}
}

func TestRRFMerge_SingleListPreservesOrder(t *testing.T) {
	in := [][]string{{"a", "b", "c"}}
	got := rrfMerge(in, 10)
	if len(got) != 3 {
		t.Fatalf("len = %d, want 3", len(got))
	}
	if got[0].id != "a" || got[1].id != "b" || got[2].id != "c" {
		t.Errorf("order = %v, want [a b c]", ids(got))
	}
	// Scores must be strictly decreasing with rank.
	if !(got[0].score > got[1].score && got[1].score > got[2].score) {
		t.Errorf("scores not strictly decreasing: %v", scores(got))
	}
}

func TestRRFMerge_OverlapBoosts(t *testing.T) {
	// "a" is hit by BOTH lists at rank 0; "b" only by first; "c" only by second.
	// "a" should rank first because it gets credit from both lists.
	in := [][]string{
		{"a", "b"},
		{"a", "c"},
	}
	got := rrfMerge(in, 10)
	if len(got) != 3 {
		t.Fatalf("len = %d, want 3", len(got))
	}
	if got[0].id != "a" {
		t.Errorf("first = %s, want a (gets RRF credit from both lists)", got[0].id)
	}
	// a's score must equal 1/61 + 1/61 = 2/61
	// b and c each get 1/62
	wantA := 1.0/(rrfK+1) + 1.0/(rrfK+1)
	if got[0].score != wantA {
		t.Errorf("a score = %g, want %g", got[0].score, wantA)
	}
}

func TestRRFMerge_TieBreakByID(t *testing.T) {
	// Two ids with identical scores: tie-broken by id ascending.
	in := [][]string{{"z", "a"}}
	got := rrfMerge(in, 10)
	// z and a have different ranks here so scores differ — construct a true tie:
	in = [][]string{{"z"}, {"a"}}
	got = rrfMerge(in, 10)
	if len(got) != 2 {
		t.Fatalf("len = %d, want 2", len(got))
	}
	if got[0].id != "a" {
		t.Errorf("tie break: first = %s, want a (id ascending)", got[0].id)
	}
	if got[0].score != got[1].score {
		t.Errorf("expected tied scores, got %g vs %g", got[0].score, got[1].score)
	}
}

func TestRRFMerge_RespectsLimit(t *testing.T) {
	in := [][]string{{"a", "b", "c", "d", "e", "f"}}
	got := rrfMerge(in, 3)
	if len(got) != 3 {
		t.Errorf("len = %d, want 3", len(got))
	}
}

func TestRRFMerge_Deterministic(t *testing.T) {
	// Same input → same output regardless of map iteration order.
	in := [][]string{{"a", "b", "c"}, {"c", "b", "a"}, {"b"}}
	first := rrfMerge(in, 10)
	for i := 0; i < 100; i++ {
		next := rrfMerge(in, 10)
		if len(first) != len(next) {
			t.Fatalf("length varies across runs")
		}
		for j := range first {
			if first[j].id != next[j].id || first[j].score != next[j].score {
				t.Errorf("non-deterministic at idx %d: %+v vs %+v", j, first[j], next[j])
			}
		}
	}
}

// ---------- helpers ----------

func ids(rs []rankedMemory) []string {
	out := make([]string, len(rs))
	for i, r := range rs {
		out[i] = r.id
	}
	return out
}

func scores(rs []rankedMemory) []float64 {
	out := make([]float64, len(rs))
	for i, r := range rs {
		out[i] = r.score
	}
	return out
}
