package tidb

import (
	"testing"

	"github.com/qiffang/mnemos/server/internal/domain"
)

// ---------- RetrievalStrategy bitmask ----------

func TestRetrievalStrategy_BitValues(t *testing.T) {
	// These values are an API contract with the service / handler layers
	// and the recall-tuning env. Changing them is a breaking change.
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
	// The vector bits are on: V1 default is now all 5
	// strategy bits (fast path + K-FTS + K-VEC + V-FTS + V-VEC).
	// FTS-only recall was observed to be unstable when query
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

// ---------- facet-diversity rerank (MMR) ----------

func mem(id, content string) *domain.Memory {
	return &domain.Memory{ID: id, Content: content}
}

func resultIDs(rs []RecallKVResult) []string {
	out := make([]string, len(rs))
	for i, r := range rs {
		out[i] = r.Value.ID
	}
	return out
}

func TestDiversityRerank_BreaksUpDominantCluster(t *testing.T) {
	// RRF order is pottery-saturated (the q15 failure shape): 4 pottery
	// variants rank above the single swimming/camping facts. With limit
	// 3 and plain RRF the agent would see 3 pottery V's and miss
	// swimming/camping. MMR must surface the distinct facets.
	candidates := []RecallKVResult{
		{Value: mem("pot1", "Melanie made a pottery bowl in class"), Score: 0.0164},
		{Value: mem("pot2", "Melanie made another pottery bowl"), Score: 0.0162},
		{Value: mem("pot3", "Melanie enjoys pottery as therapy"), Score: 0.0161},
		{Value: mem("pot4", "Melanie pottery cup glaze work"), Score: 0.0160},
		{Value: mem("swim", "Melanie went swimming with her kids"), Score: 0.0158},
		{Value: mem("camp", "Melanie family camping trip mountains"), Score: 0.0156},
	}
	out := diversityRerank(candidates, 3, diversityAlways)
	if len(out) != 3 {
		t.Fatalf("want 3 results, got %d", len(out))
	}
	// Rank 0 is always the top-RRF candidate (relevance preserved).
	if out[0].Value.ID != "pot1" {
		t.Errorf("rank0 = %s, want pot1 (top RRF preserved)", out[0].Value.ID)
	}
	// The other two slots should NOT both be pottery — swimming and/or
	// camping must surface over near-duplicate pottery variants.
	got := resultIDs(out)
	hasDistinct := false
	for _, id := range got[1:] {
		if id == "swim" || id == "camp" {
			hasDistinct = true
		}
	}
	if !hasDistinct {
		t.Errorf("diversity rerank kept only pottery cluster: %v", got)
	}
}

func TestDiversityRerank_DegradesToRRFWhenNoDuplicates(t *testing.T) {
	// All facets distinct → MMR should preserve pure RRF order.
	candidates := []RecallKVResult{
		{Value: mem("a", "alpha beta gamma delta"), Score: 1.0},
		{Value: mem("b", "epsilon zeta eta theta"), Score: 0.9},
		{Value: mem("c", "iota kappa lambda mu"), Score: 0.8},
	}
	out := diversityRerank(candidates, 3, diversityAlways)
	got := resultIDs(out)
	want := []string{"a", "b", "c"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("no-duplicate rerank changed RRF order: got %v want %v", got, want)
		}
	}
}

func TestDiversityRerank_FastPathStaysRank0(t *testing.T) {
	candidates := []RecallKVResult{
		{Value: mem("fast", "exact key match content here"), Score: mathInf, FromFast: true},
		{Value: mem("b", "exact key match content here too"), Score: 0.9},
		{Value: mem("c", "completely different facet words"), Score: 0.8},
	}
	out := diversityRerank(candidates, 2, diversityAlways)
	if out[0].Value.ID != "fast" {
		t.Errorf("fast-path hit must stay rank 0, got %s", out[0].Value.ID)
	}
}

func TestDiversityRerank_LimitAndEmpty(t *testing.T) {
	if got := diversityRerank(nil, 5, diversityAlways); got != nil {
		t.Errorf("nil candidates → nil, got %v", got)
	}
	single := []RecallKVResult{{Value: mem("x", "one"), Score: 1.0}}
	if got := diversityRerank(single, 5, diversityAlways); len(got) != 1 || got[0].Value.ID != "x" {
		t.Errorf("single candidate passthrough failed: %v", got)
	}
}

// ---------- diversity rerank modes (off / always / auto) ----------

func TestDiversityRerank_OffIsPureRRF(t *testing.T) {
	// "off" must return RRF order untouched, even on a cluster-saturated
	// pool that "always"/"auto" would reshuffle.
	candidates := []RecallKVResult{
		{Value: mem("pot1", "Melanie made a pottery bowl in class"), Score: 0.0164},
		{Value: mem("pot2", "Melanie made another pottery bowl"), Score: 0.0162},
		{Value: mem("pot3", "Melanie enjoys pottery as therapy"), Score: 0.0161},
		{Value: mem("swim", "Melanie went swimming with her kids"), Score: 0.0158},
	}
	out := diversityRerank(candidates, 3, diversityOff)
	got := resultIDs(out)
	want := []string{"pot1", "pot2", "pot3"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("off mode changed RRF order: got %v want %v", got, want)
		}
	}
}

func TestDiversityRerank_AutoEngagesOnSaturatedPool(t *testing.T) {
	// Cluster-saturated pool (pottery-heavy, the aggregation shape):
	// auto SHOULD engage MMR and surface the distinct facet.
	candidates := []RecallKVResult{
		{Value: mem("pot1", "Melanie made a pottery bowl in class"), Score: 0.0164},
		{Value: mem("pot2", "Melanie made another pottery bowl today"), Score: 0.0162},
		{Value: mem("pot3", "Melanie enjoys pottery bowl making"), Score: 0.0161},
		{Value: mem("pot4", "Melanie pottery bowl glaze class"), Score: 0.0160},
		{Value: mem("swim", "Melanie went swimming with her kids"), Score: 0.0158},
	}
	out := diversityRerank(candidates, 3, diversityAuto)
	got := resultIDs(out)
	hasSwim := false
	for _, id := range got {
		if id == "swim" {
			hasSwim = true
		}
	}
	if !hasSwim {
		t.Errorf("auto on saturated pool should surface swim, got %v", got)
	}
}

func TestDiversityRerank_AutoSkipsDiversePool(t *testing.T) {
	// Facet-diverse pool (single-answer shape): auto should NOT rerank;
	// it leaves RRF order intact so a relevant near-neighbor isn't traded
	// for a less-relevant distinct one.
	candidates := []RecallKVResult{
		{Value: mem("a", "Caroline career counseling mental health"), Score: 0.0164},
		{Value: mem("b", "totally unrelated weather forecast rain"), Score: 0.0162},
		{Value: mem("c", "stock market quarterly earnings report"), Score: 0.0160},
		{Value: mem("d", "recipe for sourdough bread baking"), Score: 0.0158},
	}
	out := diversityRerank(candidates, 3, diversityAuto)
	got := resultIDs(out)
	want := []string{"a", "b", "c"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("auto on diverse pool should preserve RRF order: got %v want %v", got, want)
		}
	}
}

func TestPoolIsClusterSaturated(t *testing.T) {
	tok := func(ss ...string) []map[string]struct{} {
		out := make([]map[string]struct{}, len(ss))
		for i, s := range ss {
			out[i] = contentTokenSet(&domain.Memory{Content: s})
		}
		return out
	}
	saturated := tok(
		"Melanie pottery bowl class",
		"Melanie pottery bowl glaze",
		"Melanie pottery bowl making",
		"Melanie swimming kids",
	)
	if !poolIsClusterSaturated(saturated) {
		t.Error("pottery-heavy pool should be cluster-saturated")
	}
	diverse := tok(
		"career counseling mental health",
		"weather forecast rain today",
		"stock market earnings report",
		"sourdough bread baking recipe",
	)
	if poolIsClusterSaturated(diverse) {
		t.Error("facet-diverse pool should NOT be cluster-saturated")
	}
}

func TestParseDiversityMode(t *testing.T) {
	// Pure parse — no env read — so it is stable under the recall-tuning env
	// (a dev/CI shell with MNEMO_RECALL_DIVERSITY=off must not break it).
	cases := map[string]string{
		"off":     diversityOff,
		"always":  diversityAlways,
		"auto":    diversityAuto,
		"OFF":     diversityOff,  // case-insensitive
		"  auto ": diversityAuto, // trimmed
		"":        diversityAuto, // empty → default
		"bogus":   diversityAuto, // unknown → default
	}
	for in, want := range cases {
		if got := parseDiversityMode(in); got != want {
			t.Errorf("parseDiversityMode(%q) = %q, want %q", in, got, want)
		}
	}
}

// ---------- key-side dedup (MNEMO_RECALL_KEY_DEDUP) ----------

func TestDedupRankedIDs(t *testing.T) {
	in := []string{"v1", "v2", "v1", "v3", "v2", "v1"}
	got := dedupRankedIDs(in)
	want := []string{"v1", "v2", "v3"}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("order not preserved: got %v, want %v", got, want)
		}
	}
}

func TestDedupRankedIDs_NoDuplicates(t *testing.T) {
	in := []string{"a", "b", "c"}
	got := dedupRankedIDs(in)
	if len(got) != 3 || got[0] != "a" || got[2] != "c" {
		t.Fatalf("unique list should pass through unchanged: %v", got)
	}
}

func TestDedupRankedIDs_Empty(t *testing.T) {
	if got := dedupRankedIDs(nil); len(got) != 0 {
		t.Errorf("nil -> empty, got %v", got)
	}
}

func TestRrfMerge_DedupRemovesKeyCountBias(t *testing.T) {
	// "gen" appears 3x in one source list (matched by 3 keys) while
	// "spec" appears once at a better-than-some rank. Without dedup gen's
	// summed RRF inflates; dedup collapses gen to its single best rank so
	// the gap to spec shrinks to one rank.
	biased := []string{"gen", "spec", "gen", "gen"}
	scoresBiased := scoresOf(rrfMerge([][]string{biased}, 10))
	if scoresBiased["gen"] <= scoresBiased["spec"] {
		t.Fatalf("expected key-count bias to favor gen without dedup: %v", scoresBiased)
	}
	deduped := dedupRankedIDs(biased)
	if len(deduped) != 2 {
		t.Fatalf("dedup should leave 2 ids, got %v", deduped)
	}
	scoresDedup := scoresOf(rrfMerge([][]string{deduped}, 10))
	gapBiased := scoresBiased["gen"] - scoresBiased["spec"]
	gapDedup := scoresDedup["gen"] - scoresDedup["spec"]
	if gapDedup >= gapBiased {
		t.Errorf("dedup should shrink the gen-vs-spec gap: biased=%.5f dedup=%.5f", gapBiased, gapDedup)
	}
}

func scoresOf(rs []rankedMemory) map[string]float64 {
	out := make(map[string]float64, len(rs))
	for _, r := range rs {
		out[r.id] = r.score
	}
	return out
}

func TestParseKeyDedup(t *testing.T) {
	// Pure parse — no env read — stable under the recall-tuning env.
	// Default is ON: only explicit off-ish values disable dedup.
	off := []string{"0", "false", "off", "no", "OFF", " off "}
	for _, v := range off {
		if parseKeyDedup(v) {
			t.Errorf("parseKeyDedup(%q) = true, want false", v)
		}
	}
	on := []string{"1", "true", "on", "yes", "", "bogus"}
	for _, v := range on {
		if !parseKeyDedup(v) {
			t.Errorf("parseKeyDedup(%q) = false, want true (default on)", v)
		}
	}
}
