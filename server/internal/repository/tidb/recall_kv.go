// recall_kv.go — K=>V recall query: fast path + RRF over (up to) four
// candidate sources. Gated by the RetrievalStrategy bitmask.
//
// Strategy semantics:
//
//   bit 0  KEY_EXACT   key_norm = NormalizeKey(query) — SHORT-CIRCUIT.
//                      If set AND a key_norm row matches, return the V
//                      it points to and SKIP RRF entirely.
//   bit 1  KEY_FTS     FTS_MATCH_WORD(query, key_text) → value_id
//   bit 2  KEY_VEC     VEC_COSINE_DISTANCE(key_embedding, q_vec) → value_id
//   bit 3  VAL_FTS     FTS_MATCH_WORD(query, content)   → id
//   bit 4  VAL_VEC     VEC_COSINE_DISTANCE(embedding,   q_vec) → id
//
// Bits 1..4 are RRF candidates: each enabled bit contributes its own
// ranking list and the merge sums 1/(k + rank) with k=60. KEY_EXACT is
// NOT part of RRF — it's a binary short-circuit.
//
// V1 default = 0x0B = KEY_EXACT | KEY_FTS | VAL_FTS. Vector paths are
// available behind their bits but recommended off until ablation
// demonstrates they earn their cost.

package tidb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"os"
	"sort"
	"strings"
	"sync"

	"github.com/qiffang/mnemos/server/internal/domain"
	"github.com/qiffang/mnemos/server/internal/keynorm"
)

// RetrievalStrategy is the bitmask of recall paths to enable.
type RetrievalStrategy uint8

const (
	StrategyKeyExact RetrievalStrategy = 1 << iota // 0x01
	StrategyKeyFTS                                 // 0x02
	StrategyKeyVec                                 // 0x04
	StrategyValFTS                                 // 0x08
	StrategyValVec                                 // 0x10
)

// StrategyDefaultV1 is the V1 production default: fast path + all 4
// RRF candidate paths (KEY_FTS / KEY_VEC / VAL_FTS / VAL_VEC). The
// vector bits were turned on after FTS-only recall was observed to be
// unstable for queries whose tokens don't overlap stored K text.
// Vector paths use VEC_EMBED_COSINE_DISTANCE under autoModel
// (TiDB embeds the query text server-side), or VEC_COSINE_DISTANCE
// against the caller-provided queryVec otherwise.
const StrategyDefaultV1 = StrategyKeyExact | StrategyKeyFTS | StrategyKeyVec | StrategyValFTS | StrategyValVec

// rrfK is the standard RRF damping constant (Cormack et al. 2009).
const rrfK = 60.0

// fanout is the per-path candidate cap before RRF fusion. Each enabled
// path fetches at most this many rows; final results are LIMIT'd at the
// caller's request after RRF merge.
const fanout = 128

// diversityRerankMultiplier sizes the RRF candidate pool that feeds the
// facet-diversity rerank. We RRF-rank `limit * multiplier` candidates,
// hydrate them, MMR-rerank for facet diversity, then truncate to limit.
// 4× gives the rerank room to swap a near-duplicate out for a distinct
// facet without re-querying.
const diversityRerankMultiplier = 4

// diversityLambda is the MMR trade-off: final score =
// lambda*relevance - (1-lambda)*max_similarity_to_selected. Higher =
// more relevance-faithful, lower = more diverse. 0.7 keeps RRF order
// dominant while breaking up same-facet clusters. Tunable.
//
// Motivation: aggregate queries
// like "Melanie activities" pulled top-5 saturated by one facet cluster
// (pottery had 15 stored V rows), so swimming/beach/camping never
// entered the shown window even though they were recalled deeper in the
// pool. MMR penalizes a candidate that closely repeats an
// already-selected one, letting distinct facets surface.
const diversityLambda = 0.7

// Diversity rerank modes, selected by MNEMO_RECALL_DIVERSITY:
//
//   - "off":    no rerank; pure RRF order (pre-880ffa6 behavior).
//   - "always": apply MMR to every query (the original 880ffa6 default).
//   - "auto":   apply MMR ONLY when the candidate pool is cluster-
//     saturated (the aggregation signature). Single-answer
//     queries — whose pools are facet-diverse — skip the
//     rerank and keep pure-RRF precision.
//
// Default is "auto". Validation showed global "always" MMR helped
// aggregation shown-coverage (q15 0/4→2/4) but cost precision on
// single-answer categories (cat1/2/3/5 dipped), because diversity is
// medicine for list questions and mild poison for single-answer ones.
// "auto" gates MMR on pool shape so it only fires where it helps.
const (
	diversityOff    = "off"
	diversityAlways = "always"
	diversityAuto   = "auto"
)

// autoDupThreshold is the Jaccard above which two pooled V's count as
// "near-duplicate" (same facet) for the "auto" gate.
const autoDupThreshold = 0.30

// autoDensityThreshold is the fraction of the pool that must be
// near-duplicate (each sharing ≥autoDupThreshold with some other
// candidate) for "auto" mode to engage MMR. Below this the pool is
// treated as facet-diverse (single-answer shape) and left in RRF order.
const autoDensityThreshold = 0.30

// parseDiversityMode maps a raw MNEMO_RECALL_DIVERSITY value to a mode.
// Unknown or empty values fall back to "auto". Pure (no env read) so it
// is testable without the cached resolver inheriting the ambient
// recall-tuning env.
func parseDiversityMode(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case diversityOff:
		return diversityOff
	case diversityAlways:
		return diversityAlways
	case diversityAuto:
		return diversityAuto
	default:
		return diversityAuto
	}
}

// resolveDiversityMode reads MNEMO_RECALL_DIVERSITY once at first use.
var resolveDiversityMode = sync.OnceValue(func() string {
	return parseDiversityMode(os.Getenv("MNEMO_RECALL_DIVERSITY"))
})

// parseKeyDedup maps a raw MNEMO_RECALL_KEY_DEDUP value to on/off.
// Pure (no env read) so it is testable without the cached resolver
// inheriting the ambient recall-tuning env — same pattern as
// parseDiversityMode.
//
// Default is ON (empty / unrecognized): the smoke test confirmed dedup
// is the canonical-correct RRF behavior and removes the key-count bias
// that buried specific facts
// under key-rich generic ones (e.g. "Melanie painted" surfaced the horse
// V and demoted the generic "abstract painting" V). Set
// MNEMO_RECALL_KEY_DEDUP=off to recover the pre-b98fc1b key-count-biased
// scoring for the tuning baseline.
func parseKeyDedup(raw string) bool {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "0", "false", "off", "no":
		return false
	default:
		return true
	}
}

// resolveKeyDedup reads MNEMO_RECALL_KEY_DEDUP once at first use. When
// true (the default), each key-side candidate list is collapsed to one
// entry per memory_value_id (best rank), removing the key-count bias
// described at the call site.
var resolveKeyDedup = sync.OnceValue(func() bool {
	return parseKeyDedup(os.Getenv("MNEMO_RECALL_KEY_DEDUP"))
})

// dedupRankedIDs collapses a relevance-ranked id list to its first
// occurrence of each id, preserving order. Because the SQL already
// orders by the source's relevance, the first occurrence IS the best
// rank for that value. Used to make a key-side candidate list a proper
// RRF source (each value ranked at most once within the source).
func dedupRankedIDs(ids []string) []string {
	seen := make(map[string]struct{}, len(ids))
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}

// RecallKVResult is one hit returned by RecallKV: a V with its RRF
// score (or the singular fast-path hit, which has score = +Inf so it
// always ranks first when mixed back into downstream code).
type RecallKVResult struct {
	Value    *domain.Memory
	Score    float64
	FromFast bool // true iff produced by KEY_EXACT short-circuit
}

// RecallKV runs the K=>V recall pipeline. Returns up to `limit` results
// ranked by RRF score (or just the fast-path hit if that fired).
//
// queryVec MAY be nil. Vector paths gracefully degrade to no-candidates
// when queryVec is nil even if their bit is set.
//
// The returned slice length MAY be smaller than `limit` even when more
// than `limit` rows were RRF-ranked: hydrateValues filters on
// state='active' to avoid surfacing soft-deleted rows under a race
// where a concurrent writer flipped state between candidate fetch and
// hydration. Callers should treat the result size as best-effort.
func (r *MemoryRepo) RecallKV(
	ctx context.Context,
	query string,
	queryVec []float32,
	strategy RetrievalStrategy,
	limit int,
) ([]RecallKVResult, error) {
	if limit <= 0 {
		limit = 10
	}
	if strategy == 0 {
		return nil, fmt.Errorf("recall_kv: no retrieval strategy bits set")
	}

	// 1. KEY_EXACT short-circuit.
	if strategy&StrategyKeyExact != 0 {
		hit, err := r.fastPathLookup(ctx, query)
		if err != nil {
			return nil, err
		}
		if hit != nil {
			return []RecallKVResult{{Value: hit, Score: 1.0, FromFast: true}}, nil
		}
	}

	// 2. Collect per-path candidate id-lists, then RRF.
	// Each list is []string of memory_value ids ranked best-first.
	var candidateLists [][]string

	// The key-side paths SELECT memory_value_id FROM memory_keys without
	// DISTINCT, so a value matched by N keys appears N times and rrfMerge
	// sums each occurrence — a key-count bias that rewards key-rich values
	// (e.g. generic facts carrying category keys) over specific ones.
	// When MNEMO_RECALL_KEY_DEDUP is on, collapse each key-side list to
	// the value's best (first) rank so each source ranks each value once,
	// matching canonical RRF. Cross-source duplicates are intentionally
	// preserved (KEY_FTS + KEY_VEC + VAL_FTS + VAL_VEC co-occurrence is
	// the point of RRF). Val-side paths SELECT memory_values.id and are
	// already unique.
	keyDedup := resolveKeyDedup()

	if strategy&StrategyKeyFTS != 0 {
		ids, err := r.keyFTSCandidates(ctx, query, fanout)
		if err != nil {
			return nil, err
		}
		if keyDedup {
			ids = dedupRankedIDs(ids)
		}
		candidateLists = append(candidateLists, ids)
	}

	if strategy&StrategyKeyVec != 0 && (queryVec != nil || r.autoModel != "") {
		ids, err := r.keyVecCandidates(ctx, query, queryVec, fanout)
		if err != nil {
			return nil, err
		}
		if keyDedup {
			ids = dedupRankedIDs(ids)
		}
		candidateLists = append(candidateLists, ids)
	}

	if strategy&StrategyValFTS != 0 {
		ids, err := r.valFTSCandidates(ctx, query, fanout)
		if err != nil {
			return nil, err
		}
		candidateLists = append(candidateLists, ids)
	}

	if strategy&StrategyValVec != 0 && (queryVec != nil || r.autoModel != "") {
		ids, err := r.valVecCandidates(ctx, query, queryVec, fanout)
		if err != nil {
			return nil, err
		}
		candidateLists = append(candidateLists, ids)
	}

	// RRF-rank a POOL larger than the caller's limit so the diversity
	// rerank below has candidates to swap in. When the pool == limit
	// (multiplier 1) this collapses to the old pure-RRF behavior.
	pool := limit * diversityRerankMultiplier
	if pool < limit {
		pool = limit
	}
	merged := rrfMerge(candidateLists, pool)
	if len(merged) == 0 {
		return nil, nil
	}

	// 3. Hydrate the pooled V's in a single SELECT.
	values, err := r.hydrateValues(ctx, merged)
	if err != nil {
		return nil, err
	}
	ranked := make([]RecallKVResult, 0, len(merged))
	for _, m := range merged {
		v, ok := values[m.id]
		if !ok {
			continue
		}
		ranked = append(ranked, RecallKVResult{Value: v, Score: m.score, FromFast: false})
	}

	// 4. Facet-diversity rerank (MMR) then truncate to the caller's
	//    limit, so distinct facets aren't crowded out of the shown
	//    window by a single over-represented cluster. Mode (off/always/
	//    auto) is resolved from MNEMO_RECALL_DIVERSITY; "auto" gates the
	//    rerank on pool shape so single-answer queries keep RRF precision.
	return diversityRerank(ranked, limit, resolveDiversityMode()), nil
}

// fastPathLookup runs the KEY_EXACT path: WHERE key_norm = ? LIMIT 1,
// JOINed to memory_values for state='active' filtering so K rows
// belonging to a soft-deleted V don't surface here. This matches the
// K-FTS / K-VEC paths which apply the same JOIN filter. Returns
// (nil, nil) on miss.
func (r *MemoryRepo) fastPathLookup(ctx context.Context, query string) (*domain.Memory, error) {
	norm := keynorm.NormalizeKey(query)
	if norm == "" {
		return nil, nil
	}
	var valueID string
	err := r.db.QueryRowContext(ctx,
		`SELECT mk.memory_value_id
		 FROM memory_keys mk
		 JOIN memory_values mv ON mv.id = mk.memory_value_id
		 WHERE mk.key_norm = ? AND mv.state = 'active'
		 LIMIT 1`,
		norm,
	).Scan(&valueID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("recall_kv fast path: %w", err)
	}
	return r.GetMemoryValueByID(ctx, valueID)
}

// keyFTSCandidates returns memory_value_id ranked by FTS_MATCH_WORD
// score on memory_keys.key_text.
//
// TiDB constraint (Error 1221): FTS_MATCH_WORD must be used ALONE in
// WHERE — no AND clauses are permitted. The K-side state filter ("only
// surface keys belonging to active V's") is therefore enforced
// downstream in hydrateValues (which JOINs memory_values with
// state='active'), not here.
func (r *MemoryRepo) keyFTSCandidates(ctx context.Context, query string, limit int) ([]string, error) {
	safeQ := ftsSafeLiteral(query)
	if safeQ == "" {
		return nil, nil
	}
	q := `SELECT memory_value_id
		FROM memory_keys
		WHERE fts_match_word('` + safeQ + `', key_text)
		ORDER BY fts_match_word('` + safeQ + `', key_text) DESC, id
		LIMIT ?`
	return scanIDList(ctx, r.db, q, limit)
}

// valFTSCandidates returns memory_values.id ranked by FTS on content.
// State filter applied downstream in hydrateValues (see TiDB Error 1221
// note on keyFTSCandidates).
func (r *MemoryRepo) valFTSCandidates(ctx context.Context, query string, limit int) ([]string, error) {
	safeQ := ftsSafeLiteral(query)
	if safeQ == "" {
		return nil, nil
	}
	q := `SELECT id
		FROM memory_values
		WHERE fts_match_word('` + safeQ + `', content)
		ORDER BY fts_match_word('` + safeQ + `', content) DESC, id
		LIMIT ?`
	return scanIDList(ctx, r.db, q, limit)
}

// keyVecCandidates: K-side vector search (KEY_VEC).
// When autoModel is configured, uses VEC_EMBED_COSINE_DISTANCE with
// the query text (TiDB server-side embeds via EMBED_TEXT). Otherwise
// falls back to VEC_COSINE_DISTANCE against the caller-provided
// queryVec. queryVec is permitted to be nil under autoModel.
func (r *MemoryRepo) keyVecCandidates(ctx context.Context, query string, queryVec []float32, limit int) ([]string, error) {
	if r.autoModel != "" {
		q := `SELECT mk.memory_value_id
			FROM memory_keys mk
			JOIN memory_values mv ON mv.id = mk.memory_value_id
			WHERE mk.key_embedding IS NOT NULL AND mv.state = 'active'
			ORDER BY VEC_EMBED_COSINE_DISTANCE(mk.key_embedding, ?) ASC
			LIMIT ?`
		return scanIDList(ctx, r.db, q, query, limit)
	}
	vec := vecToString(queryVec)
	if vec == nil {
		return nil, nil
	}
	q := `SELECT mk.memory_value_id
		FROM memory_keys mk
		JOIN memory_values mv ON mv.id = mk.memory_value_id
		WHERE mk.key_embedding IS NOT NULL AND mv.state = 'active'
		ORDER BY VEC_COSINE_DISTANCE(mk.key_embedding, ?) ASC
		LIMIT ?`
	return scanIDList(ctx, r.db, q, vec, limit)
}

// valVecCandidates: V-side vector search (VAL_VEC). Same autoModel /
// queryVec switching as keyVecCandidates.
func (r *MemoryRepo) valVecCandidates(ctx context.Context, query string, queryVec []float32, limit int) ([]string, error) {
	if r.autoModel != "" {
		q := `SELECT id
			FROM memory_values
			WHERE embedding IS NOT NULL AND state = 'active'
			ORDER BY VEC_EMBED_COSINE_DISTANCE(embedding, ?) ASC
			LIMIT ?`
		return scanIDList(ctx, r.db, q, query, limit)
	}
	vec := vecToString(queryVec)
	if vec == nil {
		return nil, nil
	}
	q := `SELECT id
		FROM memory_values
		WHERE embedding IS NOT NULL AND state = 'active'
		ORDER BY VEC_COSINE_DISTANCE(embedding, ?) ASC
		LIMIT ?`
	return scanIDList(ctx, r.db, q, vec, limit)
}

func scanIDList(ctx context.Context, db *sql.DB, q string, args ...any) ([]string, error) {
	rows, err := db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("recall_kv scan id list: %w", err)
	}
	defer rows.Close()
	out := make([]string, 0, 64)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("recall_kv scan id: %w", err)
		}
		out = append(out, id)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// rankedMemory is one entry in the RRF-merged list before hydration.
type rankedMemory struct {
	id    string
	score float64
}

// diversityRerank applies Maximal Marginal Relevance (MMR) over an
// RRF-ranked, hydrated candidate pool, then returns the top `limit`.
//
// MMR greedily selects: at each step pick the candidate maximizing
//
//	diversityLambda*relevance - (1-diversityLambda)*max_sim(c, selected)
//
// where relevance is the RRF score divide-by-max normalized (see the
// implementation note below for why divide-by-max, not min-max) so it's
// comparable to similarity ∈ [0,1], and similarity is token-Jaccard on
// V content. The effect: once a facet cluster is represented in the
// selected set, further near-duplicates from that cluster are penalized,
// letting distinct facets surface into the truncated window.
//
// `candidates` is assumed already sorted by RRF score descending (as
// rrfMerge produces). The first pick is therefore always the top-RRF V,
// so pure-relevance order is preserved at rank 1 and diversity only
// reshuffles the tail — the behavior degrades gracefully to plain RRF
// when the pool has no near-duplicates.
func diversityRerank(candidates []RecallKVResult, limit int, mode string) []RecallKVResult {
	truncate := func() []RecallKVResult {
		if len(candidates) > limit {
			return candidates[:limit]
		}
		return candidates
	}

	if mode == diversityOff || len(candidates) <= 1 || limit <= 0 {
		return truncate()
	}

	// Pre-tokenize each candidate's content once (shared by the auto
	// gate and the MMR loop below).
	tokens := make([]map[string]struct{}, len(candidates))
	for i, c := range candidates {
		tokens[i] = contentTokenSet(c.Value)
	}

	// "auto" gate: only engage MMR when the pool is cluster-saturated.
	// A single-answer query's pool is facet-diverse — reranking it just
	// trades a relevant near-neighbor for a less-relevant distinct one,
	// hurting precision. Measure the near-duplicate density and bail to
	// pure RRF when it's low.
	if mode == diversityAuto && !poolIsClusterSaturated(tokens) {
		return truncate()
	}

	// Relevance = score / maxScore (divide-by-max, NOT min-max). This is
	// deliberate: when an aggregate query matches every facet's category
	// key the RRF scores are all close together, and we want the rerank
	// to treat them as "similarly relevant" so the diversity term
	// decides order. Min-max would stretch those near-equal scores
	// across [0,1] and re-amplify the relevance gap the category keys
	// just flattened — defeating the rerank. Divide-by-max keeps close
	// scores close (all ≈1.0) while still spreading a genuinely wide
	// relevance range. FromFast hits carry +Inf and must stay rank 0.
	maxS := 0.0
	for _, c := range candidates {
		if c.Score != mathInf && c.Score > maxS {
			maxS = c.Score
		}
	}
	rel := func(c RecallKVResult) float64 {
		if c.FromFast || c.Score == mathInf {
			return 1.0
		}
		if maxS <= 0 {
			return 1.0
		}
		return c.Score / maxS
	}

	n := len(candidates)
	selected := make([]int, 0, limit)
	picked := make([]bool, n)
	if limit > n {
		limit = n
	}

	for len(selected) < limit {
		best := -1
		bestScore := math.Inf(-1)
		for i := 0; i < n; i++ {
			if picked[i] {
				continue
			}
			var maxSim float64
			for _, s := range selected {
				if sim := jaccard(tokens[i], tokens[s]); sim > maxSim {
					maxSim = sim
				}
			}
			mmr := diversityLambda*rel(candidates[i]) - (1-diversityLambda)*maxSim
			if mmr > bestScore {
				bestScore = mmr
				best = i
			}
		}
		if best < 0 {
			break
		}
		picked[best] = true
		selected = append(selected, best)
	}

	out := make([]RecallKVResult, 0, len(selected))
	for _, idx := range selected {
		out = append(out, candidates[idx])
	}
	return out
}

// poolIsClusterSaturated reports whether enough of the candidate pool is
// made of near-duplicates (each sharing ≥autoDupThreshold Jaccard with
// at least one other candidate) to justify the MMR rerank under "auto"
// mode. This is the "aggregation signature": a query like "Melanie
// activities" pulls many V's from the same facet cluster (pottery
// variants), whereas a single-answer query pulls a facet-diverse pool.
// Reranking the latter only trades relevance for spurious diversity.
func poolIsClusterSaturated(tokens []map[string]struct{}) bool {
	n := len(tokens)
	if n <= 2 {
		return false
	}
	dupCount := 0
	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			if i == j {
				continue
			}
			if jaccard(tokens[i], tokens[j]) >= autoDupThreshold {
				dupCount++
				break // i has at least one near-duplicate; count once
			}
		}
	}
	return float64(dupCount)/float64(n) >= autoDensityThreshold
}

// mathInf is +Inf, the score FromFast hits carry.
var mathInf = math.Inf(1)

// contentTokenSet lowercases V content and returns its distinct word
// set for Jaccard similarity. Used only for facet de-duplication, so a
// crude whitespace/punctuation split is sufficient.
func contentTokenSet(v *domain.Memory) map[string]struct{} {
	set := make(map[string]struct{})
	if v == nil {
		return set
	}
	for _, w := range strings.FieldsFunc(strings.ToLower(v.Content), func(r rune) bool {
		return !('a' <= r && r <= 'z') && !('0' <= r && r <= '9') && r < 0x80
	}) {
		if len(w) > 2 { // drop very short tokens (a, in, of) that don't carry facet signal
			set[w] = struct{}{}
		}
	}
	return set
}

// jaccard is |A∩B| / |A∪B| over two token sets; 0 when either is empty.
func jaccard(a, b map[string]struct{}) float64 {
	if len(a) == 0 || len(b) == 0 {
		return 0
	}
	inter := 0
	for t := range a {
		if _, ok := b[t]; ok {
			inter++
		}
	}
	union := len(a) + len(b) - inter
	if union == 0 {
		return 0
	}
	return float64(inter) / float64(union)
}

// rrfMerge applies Reciprocal Rank Fusion (k=60) across N candidate
// lists. Each list contributes 1/(k + rank+1) to its members; final
// score is the sum across lists. Returns the top `limit` value ids
// ordered by score desc, breaking ties by id for determinism.
//
// Lists may have overlapping ids — that's the point: a V hit by both
// K-FTS and V-FTS gets credit from both, ranking it above a V hit only
// once.
func rrfMerge(lists [][]string, limit int) []rankedMemory {
	if len(lists) == 0 {
		return nil
	}
	scores := make(map[string]float64)
	for _, list := range lists {
		for rank, id := range list {
			scores[id] += 1.0 / (rrfK + float64(rank+1))
		}
	}
	merged := make([]rankedMemory, 0, len(scores))
	for id, s := range scores {
		merged = append(merged, rankedMemory{id: id, score: s})
	}
	sort.Slice(merged, func(i, j int) bool {
		if merged[i].score == merged[j].score {
			return merged[i].id < merged[j].id
		}
		return merged[i].score > merged[j].score
	})
	if len(merged) > limit {
		merged = merged[:limit]
	}
	return merged
}

// hydrateValues fetches the full memory_values rows for the given ranked
// ids in a single SELECT and returns them keyed by id.
func (r *MemoryRepo) hydrateValues(ctx context.Context, ranked []rankedMemory) (map[string]*domain.Memory, error) {
	if len(ranked) == 0 {
		return nil, nil
	}
	placeholders := make([]string, len(ranked))
	args := make([]any, len(ranked))
	for i, rm := range ranked {
		placeholders[i] = "?"
		args[i] = rm.id
	}
	q := `SELECT ` + memoryValueColumns + ` FROM memory_values
		WHERE id IN (` + strings.Join(placeholders, ",") + `)
		  AND state = 'active'`
	rows, err := r.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("recall_kv hydrate values: %w", err)
	}
	defer rows.Close()
	out := make(map[string]*domain.Memory, len(ranked))
	for rows.Next() {
		m, _, err := scanMemoryValueRow(rows)
		if err != nil {
			return nil, err
		}
		out[m.ID] = m
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}
