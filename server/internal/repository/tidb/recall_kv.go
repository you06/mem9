// recall_kv.go — K=>V recall query: fast path + RRF over (up to) four
// candidate sources. Gated by the RetrievalStrategy bitmask.
//
// Strategy semantics (locked in #mem9-discussion:9dcf4b01):
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
	"sort"
	"strings"

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
// RRF candidate paths (KEY_FTS / KEY_VEC / VAL_FTS / VAL_VEC). Step
// 4.5 turned on the vector bits after @tmgg06 observed FTS-only
// recall was unstable for queries whose tokens don't overlap stored
// K text. Vector paths use VEC_EMBED_COSINE_DISTANCE under autoModel
// (TiDB embeds the query text server-side), or VEC_COSINE_DISTANCE
// against the caller-provided queryVec otherwise.
const StrategyDefaultV1 = StrategyKeyExact | StrategyKeyFTS | StrategyKeyVec | StrategyValFTS | StrategyValVec

// rrfK is the standard RRF damping constant (Cormack et al. 2009).
const rrfK = 60.0

// fanout is the per-path candidate cap before RRF fusion. Each enabled
// path fetches at most this many rows; final results are LIMIT'd at the
// caller's request after RRF merge.
const fanout = 128

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

	if strategy&StrategyKeyFTS != 0 {
		ids, err := r.keyFTSCandidates(ctx, query, fanout)
		if err != nil {
			return nil, err
		}
		candidateLists = append(candidateLists, ids)
	}

	if strategy&StrategyKeyVec != 0 && (queryVec != nil || r.autoModel != "") {
		ids, err := r.keyVecCandidates(ctx, query, queryVec, fanout)
		if err != nil {
			return nil, err
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

	merged := rrfMerge(candidateLists, limit)
	if len(merged) == 0 {
		return nil, nil
	}

	// 3. Hydrate the top-N V's in a single SELECT and re-order by RRF.
	values, err := r.hydrateValues(ctx, merged)
	if err != nil {
		return nil, err
	}
	out := make([]RecallKVResult, 0, len(merged))
	for _, m := range merged {
		v, ok := values[m.id]
		if !ok {
			continue
		}
		out = append(out, RecallKVResult{Value: v, Score: m.score, FromFast: false})
	}
	return out, nil
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
// score on memory_keys.key_text. JOINs memory_values to filter on V's
// state = 'active' (per the agreed "K state inherits V" rule).
func (r *MemoryRepo) keyFTSCandidates(ctx context.Context, query string, limit int) ([]string, error) {
	safeQ := ftsSafeLiteral(query)
	if safeQ == "" {
		return nil, nil
	}
	q := `SELECT mk.memory_value_id
		FROM memory_keys mk
		JOIN memory_values mv ON mv.id = mk.memory_value_id
		WHERE fts_match_word('` + safeQ + `', mk.key_text)
		  AND mv.state = 'active'
		ORDER BY fts_match_word('` + safeQ + `', mk.key_text) DESC, mk.id
		LIMIT ?`
	return scanIDList(ctx, r.db, q, limit)
}

// valFTSCandidates returns memory_values.id ranked by FTS on content.
func (r *MemoryRepo) valFTSCandidates(ctx context.Context, query string, limit int) ([]string, error) {
	safeQ := ftsSafeLiteral(query)
	if safeQ == "" {
		return nil, nil
	}
	q := `SELECT id
		FROM memory_values
		WHERE fts_match_word('` + safeQ + `', content)
		  AND state = 'active'
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
