package domain

import "time"

// EntityType classifies graph entities.
type EntityType string

const (
	EntityPerson EntityType = "person"
	EntityPlace  EntityType = "place"
	EntityOrg    EntityType = "organization"
	EntityEvent  EntityType = "event"
	EntityWork   EntityType = "work"
	EntityProduct EntityType = "product"
	EntityPet    EntityType = "pet"
)

// GraphEntity represents a named entity node in the graph index.
type GraphEntity struct {
	ID             string     `json:"id"`
	AgentID        string     `json:"agent_id,omitempty"`
	SessionID      string     `json:"session_id,omitempty"`
	CanonicalName  string     `json:"canonical_name"`
	NormalizedName string     `json:"normalized_name"`
	EntityType     EntityType `json:"entity_type"`
	Mentions       int        `json:"mentions"`
	CreatedAt      time.Time  `json:"created_at"`
	UpdatedAt      time.Time  `json:"updated_at"`
}

// GraphEdge represents a directed relationship between entities (or an entity and a literal).
type GraphEdge struct {
	ID             string  `json:"id"`
	AgentID        string  `json:"agent_id,omitempty"`
	SessionID      string  `json:"session_id,omitempty"`
	SrcEntityID    string  `json:"src_entity_id"`
	Relation       string  `json:"relation"`
	DstEntityID    string  `json:"dst_entity_id,omitempty"` // non-empty when target is an entity
	DstLiteral     string  `json:"dst_literal,omitempty"`   // non-empty when target is a literal value
	SourceMemoryID string  `json:"source_memory_id"`
	Confidence     float64 `json:"confidence"`
	CreatedAt      time.Time `json:"created_at"`
}

// GraphHit represents a graph search result that maps back to a memory.
type GraphHit struct {
	MemoryID    string  `json:"memory_id"`
	EntityName  string  `json:"entity_name,omitempty"`
	Relation    string  `json:"relation,omitempty"`
	TargetName  string  `json:"target_name,omitempty"`
	GraphScore  float64 `json:"graph_score"`
}
