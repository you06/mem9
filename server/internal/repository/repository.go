package repository

import (
	"context"
	"time"

	"github.com/qiffang/mnemos/server/internal/domain"
)

// MemoryRepo defines storage operations for memories.
type MemoryRepo interface {
	Create(ctx context.Context, m *domain.Memory) error
	GetByID(ctx context.Context, id string) (*domain.Memory, error)
	UpdateOptimistic(ctx context.Context, m *domain.Memory, expectedVersion int) error
	SoftDelete(ctx context.Context, id, agentName string) error
	ArchiveMemory(ctx context.Context, id, supersededBy string) error
	ArchiveAndCreate(ctx context.Context, archiveID, supersededBy string, newMem *domain.Memory) error
	SetState(ctx context.Context, id string, state domain.MemoryState) error
	List(ctx context.Context, f domain.MemoryFilter) (memories []domain.Memory, total int, err error)
	Count(ctx context.Context) (int, error)
	BulkCreate(ctx context.Context, memories []*domain.Memory) error

	// VectorSearch performs ANN search using cosine distance with a pre-computed vector.
	VectorSearch(ctx context.Context, queryVec []float32, f domain.MemoryFilter, limit int) ([]domain.Memory, error)

	// AutoVectorSearch performs ANN search using VEC_EMBED_COSINE_DISTANCE with a plain-text query.
	// TiDB Serverless auto-embeds the query text.
	AutoVectorSearch(ctx context.Context, queryText string, f domain.MemoryFilter, limit int) ([]domain.Memory, error)

	KeywordSearch(ctx context.Context, query string, f domain.MemoryFilter, limit int) ([]domain.Memory, error)

	// FTSSearch performs full-text search using FTS_MATCH_WORD with BM25 ranking.
	// Results include a fts_score field used for RRF merge.
	FTSSearch(ctx context.Context, query string, f domain.MemoryFilter, limit int) ([]domain.Memory, error)
	// FTSAvailable reports whether full-text search is usable on this database.
	FTSAvailable() bool

	ListBootstrap(ctx context.Context, limit int) ([]domain.Memory, error)
}

// TenantRepo manages tenant records in the control plane DB.
type TenantRepo interface {
	Create(ctx context.Context, t *domain.Tenant) error
	GetByID(ctx context.Context, id string) (*domain.Tenant, error)
	GetByName(ctx context.Context, name string) (*domain.Tenant, error)
	UpdateStatus(ctx context.Context, id string, status domain.TenantStatus) error
	UpdateSchemaVersion(ctx context.Context, id string, version int) error
}

// UploadTaskRepo manages upload task records in the control plane DB.
type UploadTaskRepo interface {
	Create(ctx context.Context, task *domain.UploadTask) error
	GetByID(ctx context.Context, taskID string) (*domain.UploadTask, error)
	ListByTenant(ctx context.Context, tenantID string) ([]domain.UploadTask, error)
	UpdateStatus(ctx context.Context, taskID string, status domain.TaskStatus, errorMsg string) error
	UpdateProgress(ctx context.Context, taskID string, doneChunks int) error
	UpdateTotalChunks(ctx context.Context, taskID string, totalChunks int) error
	FetchPending(ctx context.Context, limit int) ([]domain.UploadTask, error)
	ResetProcessing(ctx context.Context, staleTimeout time.Duration) (int64, error)
}

// SessionRepo handles raw session message storage and search.
// Search methods accept domain.MemoryFilter for consistency with MemoryRepo.
// All search methods return []domain.Memory (projected as TypeSession rows).
// BulkCreate silently skips MySQL 1146 (table not yet migrated) at DEBUG level.
type SessionRepo interface {
	BulkCreate(ctx context.Context, sessions []*domain.Session) error
	PatchTags(ctx context.Context, sessionID, contentHash string, tags []string) error
	AutoVectorSearch(ctx context.Context, query string, f domain.MemoryFilter, limit int) ([]domain.Memory, error)
	VectorSearch(ctx context.Context, queryVec []float32, f domain.MemoryFilter, limit int) ([]domain.Memory, error)
	FTSSearch(ctx context.Context, query string, f domain.MemoryFilter, limit int) ([]domain.Memory, error)
	KeywordSearch(ctx context.Context, query string, f domain.MemoryFilter, limit int) ([]domain.Memory, error)
	FTSAvailable() bool
	// ListBySessionIDs returns raw session rows for the given session IDs, ordered by
	// session_id ASC, created_at ASC, seq ASC, id ASC. At most limitPerSession rows are
	// returned per session_id. Returns ErrNotSupported on non-TiDB backends.
	ListBySessionIDs(ctx context.Context, sessionIDs []string, limitPerSession int) ([]*domain.Session, error)
}

// GraphRepo is a derived index over memories, storing entities and relationships
// extracted from memory content. Graph is NOT a source of truth — every edge
// traces back to a source_memory_id in the memories table.
//
// The interface is memory-granular: callers pass a memory ID and its content;
// the implementation manages internal entity/edge bookkeeping. This keeps the
// service layer decoupled from graph internals and allows swapping TiDB CTE
// for a dedicated graph DB (Neo4j, Neptune, etc.) by implementing this interface.
type GraphRepo interface {
	// UpsertFromMemory extracts entities and edges from a memory's content and
	// stores them in the graph index. Re-calling with the same memoryID replaces
	// all previously extracted edges for that memory.
	UpsertFromMemory(ctx context.Context, memoryID, agentID, sessionID string, entities []domain.GraphEntity, edges []domain.GraphEdge) error

	// DeleteByMemory removes all graph edges (and orphan entities) linked to a memory.
	DeleteByMemory(ctx context.Context, memoryID string) error

	// ExpandFromMemories performs 1-hop graph expansion from the given seed memory IDs.
	// Returns additional memory IDs (via source_memory_id on edges) that are related
	// to the seeds, along with graph hit metadata for scoring/merge.
	ExpandFromMemories(ctx context.Context, seedMemoryIDs []string, agentID string, limit int) ([]domain.GraphHit, error)
}
