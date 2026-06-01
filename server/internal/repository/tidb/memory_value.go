// memory_value.go — CRUD for memory_values (the V table of the K=>V refactor).
//
// See server/schema.sql memory_values definition. Schema-coupled methods
// kept in this file; recall (4-path RRF query) lives in recall_kv.go.
//
// memory_values mirrors the existing memories table column-for-column
// PLUS content_hash (sha256 dedup key) and keys_extracted_at (orphan-V
// marker). During the additive migration (step 1), the old memories
// table is still authoritative; step 2 (service layer) will introduce
// double-writes that populate both tables.

package tidb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/qiffang/mnemos/server/internal/domain"
	"github.com/qiffang/mnemos/server/internal/keynorm"
)

// memoryValueColumns matches the memory_values column order used by the
// SELECT / scan paths. Extending the table requires updating this string
// and scanMemoryValueRow.
const memoryValueColumns = `id, content, content_hash, source, tags, metadata, embedding, memory_type, agent_id, session_id, state, version, updated_by, created_at, updated_at, superseded_by, keys_extracted_at`

// UpsertMemoryValueResult is the return value of UpsertMemoryValue.
// IsNew is true iff this call inserted a new row; when false, ID
// references an existing row that matches content_hash.
type UpsertMemoryValueResult struct {
	ID    string
	IsNew bool
}

// UpsertMemoryValue inserts a V row, or returns the existing V's ID when
// the normalized content collides. The dedup key is content_hash =
// keynorm.HashValue(content); concurrent writers with the same hash will
// resolve to the same row.
//
// IDIOM: INSERT ... ON DUPLICATE KEY UPDATE id = LAST_INSERT_ID(id)
// returns the existing row's id via LAST_INSERT_ID() when the unique
// constraint fires. This avoids a separate SELECT FOR UPDATE round-trip.
// (See server/internal/repository/tidb/AGENTS.md — INSERT ... ON
// DUPLICATE KEY UPDATE is the expected upsert pattern.)
//
// Embedding column behavior follows the existing memories rules: when
// autoModel != "", the column is generated server-side, so the INSERT
// omits it.
func (r *MemoryRepo) UpsertMemoryValue(ctx context.Context, m *domain.Memory) (UpsertMemoryValueResult, error) {
	hash := keynorm.HashValue(m.Content)
	tagsJSON := marshalTags(m.Tags)
	memoryType := string(m.MemoryType)
	if memoryType == "" {
		memoryType = string(domain.TypePinned)
	}

	var (
		res sql.Result
		err error
	)
	if r.autoModel != "" {
		res, err = r.db.ExecContext(ctx,
			`INSERT INTO memory_values
				(id, content, content_hash, source, tags, metadata, memory_type, agent_id, session_id, state, version, updated_by, created_at, updated_at)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', ?, ?, NOW(), NOW())
			 ON DUPLICATE KEY UPDATE id = LAST_INSERT_ID(id)`,
			m.ID, m.Content, hash, nullString(m.Source),
			tagsJSON, nullJSON(m.Metadata), memoryType, nullString(m.AgentID), nullString(m.SessionID),
			m.Version, nullString(m.UpdatedBy),
		)
	} else {
		res, err = r.db.ExecContext(ctx,
			`INSERT INTO memory_values
				(id, content, content_hash, source, tags, metadata, embedding, memory_type, agent_id, session_id, state, version, updated_by, created_at, updated_at)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', ?, ?, NOW(), NOW())
			 ON DUPLICATE KEY UPDATE id = LAST_INSERT_ID(id)`,
			m.ID, m.Content, hash, nullString(m.Source),
			tagsJSON, nullJSON(m.Metadata), vecToString(m.Embedding), memoryType, nullString(m.AgentID), nullString(m.SessionID),
			m.Version, nullString(m.UpdatedBy),
		)
	}
	if err != nil {
		return UpsertMemoryValueResult{}, fmt.Errorf("upsert memory_value: %w", err)
	}

	// rowsAffected is 1 on insert, 2 on update-via-dup-key (MySQL quirk).
	// We treat anything that isn't a fresh insert (rowsAffected != 1) as
	// IsNew=false and resolve the existing id via SELECT.
	rowsAffected, _ := res.RowsAffected()
	if rowsAffected == 1 {
		return UpsertMemoryValueResult{ID: m.ID, IsNew: true}, nil
	}

	// Duplicate. Resolve the existing id by hash.
	var existingID string
	err = r.db.QueryRowContext(ctx,
		`SELECT id FROM memory_values WHERE content_hash = ?`, hash,
	).Scan(&existingID)
	if err != nil {
		return UpsertMemoryValueResult{}, fmt.Errorf("resolve duplicate memory_value id: %w", err)
	}
	return UpsertMemoryValueResult{ID: existingID, IsNew: false}, nil
}

// MarkKeysExtracted stamps keys_extracted_at on a V to signal that K
// extraction has finished for this V (success path, including
// "extracted 0 keys"). Caller passes the V id returned by
// UpsertMemoryValue.
//
// On a V update that requires re-extraction, set keys_extracted_at NULL
// via ResetKeysExtracted instead.
func (r *MemoryRepo) MarkKeysExtracted(ctx context.Context, valueID string) error {
	_, err := r.db.ExecContext(ctx,
		`UPDATE memory_values SET keys_extracted_at = NOW() WHERE id = ?`, valueID)
	if err != nil {
		return fmt.Errorf("mark keys_extracted: %w", err)
	}
	return nil
}

// ResetKeysExtracted clears keys_extracted_at, marking the V as orphan
// again. Used by the V-update path (per the agreed async-re-extract
// design): when V's content changes, leave the old K rows in place
// transiently and let backfill re-extract.
func (r *MemoryRepo) ResetKeysExtracted(ctx context.Context, valueID string) error {
	_, err := r.db.ExecContext(ctx,
		`UPDATE memory_values SET keys_extracted_at = NULL WHERE id = ?`, valueID)
	if err != nil {
		return fmt.Errorf("reset keys_extracted: %w", err)
	}
	return nil
}

// OrphanValue is the minimal V info needed by the backfill job.
type OrphanValue struct {
	ID      string
	Content string
}

// ListOrphanValues returns up to `limit` V rows with keys_extracted_at
// IS NULL (i.e. K extraction has never succeeded or was reset by a V
// update). Ordered by id ASC for deterministic pagination.
//
// Backfill workers consume this list, call extractkeys.Extract for each,
// insert the resulting K rows, then call MarkKeysExtracted to drop them
// from this list on next iteration.
func (r *MemoryRepo) ListOrphanValues(ctx context.Context, limit int) ([]OrphanValue, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := r.db.QueryContext(ctx,
		`SELECT id, content FROM memory_values
		 WHERE keys_extracted_at IS NULL AND state = 'active'
		 ORDER BY id ASC
		 LIMIT ?`, limit)
	if err != nil {
		return nil, fmt.Errorf("list orphan values: %w", err)
	}
	defer rows.Close()

	out := make([]OrphanValue, 0, limit)
	for rows.Next() {
		var ov OrphanValue
		if err := rows.Scan(&ov.ID, &ov.Content); err != nil {
			return nil, fmt.Errorf("scan orphan value: %w", err)
		}
		out = append(out, ov)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// GetMemoryValueByID fetches a single V row by id.
func (r *MemoryRepo) GetMemoryValueByID(ctx context.Context, id string) (*domain.Memory, error) {
	row := r.db.QueryRowContext(ctx,
		`SELECT `+memoryValueColumns+` FROM memory_values WHERE id = ? AND state = 'active'`,
		id,
	)
	m, _, err := scanMemoryValueRow(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, domain.ErrNotFound
		}
		return nil, err
	}
	return m, nil
}

// scanMemoryValueRow scans one memory_values row into a domain.Memory
// using the same field set as scanMemory (existing memories table) plus
// the K=>V-specific content_hash and keys_extracted_at columns. The
// second return value is keys_extracted_at (NULL → zero time).
func scanMemoryValueRow(scanner interface {
	Scan(dest ...any) error
}) (*domain.Memory, time.Time, error) {
	var (
		m                                                                      domain.Memory
		hash                                                                   string
		source, memoryType, agentID, sessionID, state, updatedBy, supersededBy sql.NullString
		tagsJSON, metadataJSON, embeddingStr                                   []byte
		keysExtracted                                                          sql.NullTime
	)
	err := scanner.Scan(
		&m.ID, &m.Content, &hash, &source,
		&tagsJSON, &metadataJSON, &embeddingStr, &memoryType, &agentID, &sessionID, &state, &m.Version, &updatedBy,
		&m.CreatedAt, &m.UpdatedAt, &supersededBy, &keysExtracted,
	)
	if err != nil {
		return nil, time.Time{}, err
	}
	m.Source = source.String
	m.MemoryType = domain.MemoryType(memoryType.String)
	if m.MemoryType == "" {
		m.MemoryType = domain.TypePinned
	}
	m.AgentID = agentID.String
	m.SessionID = sessionID.String
	m.State = domain.MemoryState(state.String)
	if m.State == "" {
		m.State = domain.StateActive
	}
	m.UpdatedBy = updatedBy.String
	m.SupersededBy = supersededBy.String
	m.Tags = unmarshalTags(tagsJSON)
	m.Metadata = unmarshalRawJSON(metadataJSON)
	m.Embedding = parseVecString(embeddingStr)
	_ = hash // column-only; not part of domain.Memory
	if keysExtracted.Valid {
		return &m, keysExtracted.Time, nil
	}
	return &m, time.Time{}, nil
}
