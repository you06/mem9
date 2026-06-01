// memory_key.go — CRUD for memory_keys (the K table of the K=>V refactor).
//
// One V (memory_values row) can carry multiple K (memory_keys rows).
// The unique constraint `uq_value_key_norm (memory_value_id, key_norm)`
// guarantees no two K's under the same V collide on normalized form;
// INSERT IGNORE leverages it to make batch upserts idempotent.

package tidb

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/qiffang/mnemos/server/internal/extractkeys"
)

// MemoryKey is the projection of a memory_keys row used by callers
// outside this package (currently only the recall path, indirectly).
type MemoryKey struct {
	ID        string
	ValueID   string
	KeyText   string
	KeyNorm   string
	Source    string
	Weight    float32
	CreatedAt time.Time
	// Embedding is intentionally omitted from the default projection —
	// it's a VECTOR(1536) column that callers don't usually need to
	// hydrate; recall paths read it via dedicated vector-search SQL.
}

// InsertMemoryKeys batch-inserts K rows under a given V. Returns the
// number of rows that were actually inserted (others were already
// present under uq_value_key_norm and were silently dropped by INSERT
// IGNORE).
//
// idGenerator must produce a fresh row id for each call; pass a
// uuid.NewString-style closure. Keeping id generation out of this
// function avoids importing a uuid package here.
//
// `keys` may be empty — returns (0, nil) without touching the database.
// This is the legitimate "extract returned 0 K" path; the caller has
// already decided to commit the orphan-V → has-no-keys transition.
func (r *MemoryRepo) InsertMemoryKeys(
	ctx context.Context,
	valueID string,
	keys []extractkeys.ExtractedKey,
	idGenerator func() string,
) (int64, error) {
	if len(keys) == 0 {
		return 0, nil
	}
	if idGenerator == nil {
		return 0, fmt.Errorf("insert memory_keys: idGenerator is nil")
	}

	// Single multi-row INSERT IGNORE. uq_value_key_norm silently drops
	// any (value_id, key_norm) duplicates without raising errors.
	placeholders := make([]string, 0, len(keys))
	args := make([]any, 0, len(keys)*6)
	for _, k := range keys {
		placeholders = append(placeholders, "(?, ?, ?, ?, ?, ?, NOW())")
		args = append(args, idGenerator(), valueID, k.KeyText, k.KeyNorm, k.Source, 1.0)
	}

	sqlQuery := `INSERT IGNORE INTO memory_keys
		(id, memory_value_id, key_text, key_norm, source, weight, created_at)
		VALUES ` + strings.Join(placeholders, ",")

	res, err := r.db.ExecContext(ctx, sqlQuery, args...)
	if err != nil {
		return 0, fmt.Errorf("insert memory_keys: %w", err)
	}
	n, _ := res.RowsAffected()
	return n, nil
}

// DeleteMemoryKeysByValue removes all K rows under a V. Used by the V
// update path when "full re-extract" is preferred over diff-based
// updates. The recall path becomes V-only (VAL_FTS / VAL_VEC) for this
// V until backfill re-runs extractkeys.Extract and InsertMemoryKeys.
//
// Note: memory_keys.fk_memory_keys_value ON DELETE CASCADE means
// deleting a memory_values row will also delete its keys automatically.
// This method exists for the case where the V row stays but the K rows
// need to be wiped (e.g. content changed and old K is stale).
func (r *MemoryRepo) DeleteMemoryKeysByValue(ctx context.Context, valueID string) (int64, error) {
	res, err := r.db.ExecContext(ctx,
		`DELETE FROM memory_keys WHERE memory_value_id = ?`, valueID)
	if err != nil {
		return 0, fmt.Errorf("delete memory_keys by value: %w", err)
	}
	n, _ := res.RowsAffected()
	return n, nil
}

// ListMemoryKeysByValue returns all K rows under a V, ordered by
// created_at ASC then id ASC for deterministic enumeration. Used by
// backfill verification and by tests; the recall path does not call
// this — it joins memory_keys via dedicated FTS / VEC queries.
func (r *MemoryRepo) ListMemoryKeysByValue(ctx context.Context, valueID string) ([]MemoryKey, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT id, memory_value_id, key_text, key_norm, source, weight, created_at
		 FROM memory_keys
		 WHERE memory_value_id = ?
		 ORDER BY created_at ASC, id ASC`,
		valueID,
	)
	if err != nil {
		return nil, fmt.Errorf("list memory_keys by value: %w", err)
	}
	defer rows.Close()

	out := make([]MemoryKey, 0, 8)
	for rows.Next() {
		var k MemoryKey
		var source sql.NullString
		if err := rows.Scan(
			&k.ID, &k.ValueID, &k.KeyText, &k.KeyNorm, &source, &k.Weight, &k.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan memory_key: %w", err)
		}
		k.Source = source.String
		out = append(out, k)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}
