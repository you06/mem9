package tidb

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"

	"github.com/qiffang/mnemos/server/internal/domain"
	internaltenant "github.com/qiffang/mnemos/server/internal/tenant"
)

// GraphRepo implements repository.GraphRepo using TiDB with CTE for graph traversal.
type GraphRepo struct {
	db *sql.DB
}

func NewGraphRepo(db *sql.DB) *GraphRepo {
	return &GraphRepo{db: db}
}

// UpsertFromMemory replaces all graph data for the given memoryID, then inserts
// the supplied entities and edges. Entity dedup uses normalized_name + agent_id.
//
// Entity ID resolution: after each INSERT ON DUPLICATE KEY, we SELECT the actual
// row's ID (which may be the pre-existing row if dedup kicked in). Edge writes
// use these resolved IDs so that joins in ExpandFromMemories never produce dangling edges.
func (r *GraphRepo) UpsertFromMemory(ctx context.Context, memoryID, agentID, sessionID string, entities []domain.GraphEntity, edges []domain.GraphEdge) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("graph upsert begin tx: %w", err)
	}
	defer tx.Rollback()

	// Remove old edges for this memory (idempotent re-indexing).
	_, err = tx.ExecContext(ctx, `DELETE FROM graph_edges WHERE source_memory_id = ?`, memoryID)
	if err != nil {
		if internaltenant.IsTableNotFoundError(err) {
			slog.Debug("graph_edges table not ready, skipping graph upsert")
			return nil
		}
		return fmt.Errorf("graph delete old edges: %w", err)
	}

	// Upsert entities and build a map from caller's entity ID → actual DB entity ID.
	// The caller's ID is only used if the entity is brand new; on duplicate key the
	// DB keeps the old row's ID.
	entityUpsertStmt, err := tx.PrepareContext(ctx,
		`INSERT INTO graph_entities (id, agent_id, session_id, canonical_name, normalized_name, entity_type, mentions, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, 1, NOW(), NOW())
		 ON DUPLICATE KEY UPDATE mentions = mentions + 1, updated_at = NOW()`)
	if err != nil {
		if internaltenant.IsTableNotFoundError(err) {
			slog.Debug("graph_entities table not ready, skipping graph upsert")
			return nil
		}
		return fmt.Errorf("graph entity prepare: %w", err)
	}
	defer entityUpsertStmt.Close()

	entityResolveStmt, err := tx.PrepareContext(ctx,
		`SELECT id FROM graph_entities WHERE normalized_name = ? AND agent_id = ? LIMIT 1`)
	if err != nil {
		return fmt.Errorf("graph entity resolve prepare: %w", err)
	}
	defer entityResolveStmt.Close()

	// callerID → resolvedID
	idMap := make(map[string]string, len(entities))

	for _, e := range entities {
		_, err = entityUpsertStmt.ExecContext(ctx,
			e.ID, nullString(agentID), nullString(sessionID),
			e.CanonicalName, e.NormalizedName, string(e.EntityType),
		)
		if err != nil {
			return fmt.Errorf("graph entity upsert %q: %w", e.CanonicalName, err)
		}

		// Resolve the actual ID that ended up in the DB.
		var resolvedID string
		if err := entityResolveStmt.QueryRowContext(ctx, e.NormalizedName, agentID).Scan(&resolvedID); err != nil {
			return fmt.Errorf("graph entity resolve %q: %w", e.NormalizedName, err)
		}
		idMap[e.ID] = resolvedID
	}

	// Insert new edges with resolved entity IDs.
	if len(edges) > 0 {
		edgeStmt, err := tx.PrepareContext(ctx,
			`INSERT INTO graph_edges (id, agent_id, session_id, src_entity_id, relation, dst_entity_id, dst_literal, source_memory_id, confidence, created_at)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NOW())`)
		if err != nil {
			return fmt.Errorf("graph edge prepare: %w", err)
		}
		defer edgeStmt.Close()

		for _, edge := range edges {
			srcID := idMap[edge.SrcEntityID]
			if srcID == "" {
				srcID = edge.SrcEntityID // fallback if caller didn't register this entity
			}
			var dstID string
			if edge.DstEntityID != "" {
				dstID = idMap[edge.DstEntityID]
				if dstID == "" {
					dstID = edge.DstEntityID
				}
			}

			_, err = edgeStmt.ExecContext(ctx,
				edge.ID, nullString(agentID), nullString(sessionID),
				srcID, edge.Relation,
				nullString(dstID), nullString(edge.DstLiteral),
				edge.SourceMemoryID, edge.Confidence,
			)
			if err != nil {
				return fmt.Errorf("graph edge insert: %w", err)
			}
		}
	}

	return tx.Commit()
}

// DeleteByMemory removes all edges linked to the given memory and cleans up
// orphan entities (entities with no remaining edges).
func (r *GraphRepo) DeleteByMemory(ctx context.Context, memoryID string) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("graph delete begin tx: %w", err)
	}
	defer tx.Rollback()

	// Collect entity IDs referenced by the edges we're about to delete.
	rows, err := tx.QueryContext(ctx,
		`SELECT DISTINCT src_entity_id FROM graph_edges WHERE source_memory_id = ?
		 UNION
		 SELECT DISTINCT dst_entity_id FROM graph_edges WHERE source_memory_id = ? AND dst_entity_id IS NOT NULL`,
		memoryID, memoryID,
	)
	if err != nil {
		if internaltenant.IsTableNotFoundError(err) {
			return nil
		}
		return fmt.Errorf("graph collect entity ids: %w", err)
	}
	var entityIDs []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return fmt.Errorf("graph scan entity id: %w", err)
		}
		entityIDs = append(entityIDs, id)
	}
	rows.Close()

	// Delete edges.
	_, err = tx.ExecContext(ctx, `DELETE FROM graph_edges WHERE source_memory_id = ?`, memoryID)
	if err != nil {
		return fmt.Errorf("graph delete edges: %w", err)
	}

	// Clean up orphan entities: entities that no longer appear in any edge.
	if len(entityIDs) > 0 {
		placeholders := strings.Repeat("?,", len(entityIDs))
		placeholders = placeholders[:len(placeholders)-1]
		args := make([]any, len(entityIDs))
		for i, id := range entityIDs {
			args[i] = id
		}
		_, err = tx.ExecContext(ctx,
			fmt.Sprintf(`DELETE FROM graph_entities WHERE id IN (%s)
				AND id NOT IN (SELECT src_entity_id FROM graph_edges WHERE src_entity_id IN (%s))
				AND id NOT IN (SELECT dst_entity_id FROM graph_edges WHERE dst_entity_id IN (%s) AND dst_entity_id IS NOT NULL)`,
				placeholders, placeholders, placeholders),
			append(append(args, args...), args...)...,
		)
		if err != nil {
			return fmt.Errorf("graph cleanup orphan entities: %w", err)
		}
	}

	return tx.Commit()
}

// ExpandFromMemories performs 1-hop graph expansion from the given seed memory IDs.
// It finds entities referenced by the seed memories' edges, then follows edges from
// those entities to discover related memories.
func (r *GraphRepo) ExpandFromMemories(ctx context.Context, seedMemoryIDs []string, agentID string, limit int) ([]domain.GraphHit, error) {
	if len(seedMemoryIDs) == 0 {
		return nil, nil
	}
	if limit <= 0 {
		limit = 20
	}

	seedPlaceholders := strings.Repeat("?,", len(seedMemoryIDs))
	seedPlaceholders = seedPlaceholders[:len(seedPlaceholders)-1]

	args := make([]any, 0, len(seedMemoryIDs)*3+3)
	for _, id := range seedMemoryIDs {
		args = append(args, id)
	}
	for _, id := range seedMemoryIDs {
		args = append(args, id)
	}

	// Optional agent_id filter for the expanded_edges CTE.
	agentFilter := ""
	if agentID != "" {
		agentFilter = " AND ge.agent_id = ?"
	}

	// CTE: find entities from seed edges, then expand 1 hop to find related edges
	// pointing to different memories.
	query := fmt.Sprintf(`
		WITH seed_entities AS (
			SELECT DISTINCT src_entity_id AS entity_id FROM graph_edges WHERE source_memory_id IN (%s)
			UNION
			SELECT DISTINCT dst_entity_id FROM graph_edges WHERE source_memory_id IN (%s) AND dst_entity_id IS NOT NULL
		),
		expanded_edges AS (
			SELECT ge.source_memory_id, ge.relation, ge.confidence,
				   e_src.canonical_name AS src_name,
				   COALESCE(e_dst.canonical_name, ge.dst_literal) AS dst_name
			FROM graph_edges ge
			JOIN seed_entities se ON ge.src_entity_id = se.entity_id OR ge.dst_entity_id = se.entity_id
			JOIN graph_entities e_src ON ge.src_entity_id = e_src.id
			LEFT JOIN graph_entities e_dst ON ge.dst_entity_id = e_dst.id
			WHERE ge.source_memory_id NOT IN (%s)%s
		)
		SELECT source_memory_id, src_name, relation, dst_name, MAX(confidence) AS score
		FROM expanded_edges
		GROUP BY source_memory_id, src_name, relation, dst_name
		ORDER BY score DESC
		LIMIT ?`,
		seedPlaceholders, seedPlaceholders, seedPlaceholders, agentFilter)

	// Add seedMemoryIDs for the NOT IN clause.
	for _, id := range seedMemoryIDs {
		args = append(args, id)
	}
	if agentID != "" {
		args = append(args, agentID)
	}
	args = append(args, limit)

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		if internaltenant.IsTableNotFoundError(err) {
			slog.Debug("graph tables not ready, skipping graph expansion")
			return nil, nil
		}
		return nil, fmt.Errorf("graph expand: %w", err)
	}
	defer rows.Close()

	var hits []domain.GraphHit
	for rows.Next() {
		var h domain.GraphHit
		var dstName sql.NullString
		if err := rows.Scan(&h.MemoryID, &h.EntityName, &h.Relation, &dstName, &h.GraphScore); err != nil {
			return nil, fmt.Errorf("graph expand scan: %w", err)
		}
		if dstName.Valid {
			h.TargetName = dstName.String
		}
		hits = append(hits, h)
	}
	return hits, rows.Err()
}

// FindEntityByName looks up an entity by normalized name and agent_id.
// Returns nil if not found.
func (r *GraphRepo) FindEntityByName(ctx context.Context, normalizedName, agentID string) (*domain.GraphEntity, error) {
	row := r.db.QueryRowContext(ctx,
		`SELECT id, agent_id, session_id, canonical_name, normalized_name, entity_type, mentions, created_at, updated_at
		 FROM graph_entities
		 WHERE normalized_name = ? AND (agent_id = ? OR agent_id IS NULL)
		 LIMIT 1`,
		normalizedName, agentID,
	)

	var e domain.GraphEntity
	var aid, sid sql.NullString
	err := row.Scan(&e.ID, &aid, &sid, &e.CanonicalName, &e.NormalizedName, &e.EntityType, &e.Mentions, &e.CreatedAt, &e.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		if internaltenant.IsTableNotFoundError(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("graph find entity: %w", err)
	}
	if aid.Valid {
		e.AgentID = aid.String
	}
	if sid.Valid {
		e.SessionID = sid.String
	}
	return &e, nil
}
