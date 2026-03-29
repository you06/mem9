package service

import (
	"context"
	"fmt"
	"log/slog"
	"regexp"
	"strings"

	"github.com/google/uuid"
	"github.com/qiffang/mnemos/server/internal/domain"
	"github.com/qiffang/mnemos/server/internal/llm"
	"github.com/qiffang/mnemos/server/internal/repository"
)

// GraphService handles entity/relation extraction from memory content
// and writes graph index entries via GraphRepo.
type GraphService struct {
	graph repository.GraphRepo
	llm   *llm.Client
}

func NewGraphService(graph repository.GraphRepo, llmClient *llm.Client) *GraphService {
	if graph == nil {
		return nil
	}
	return &GraphService{graph: graph, llm: llmClient}
}

// IndexMemory extracts entities and relations from a memory's content
// and writes them to the graph index. Safe to call asynchronously.
func (s *GraphService) IndexMemory(ctx context.Context, memoryID, agentID, sessionID, content string) error {
	if s == nil || s.llm == nil {
		return nil
	}

	extracted, err := s.extractGraph(ctx, content)
	if err != nil {
		return fmt.Errorf("graph extract: %w", err)
	}
	if len(extracted.Entities) == 0 {
		return nil
	}

	// Build domain entities with normalized names and generated IDs.
	// Also build a name→ID map so edges can reference entities by name.
	nameToID := make(map[string]string, len(extracted.Entities))
	entities := make([]domain.GraphEntity, 0, len(extracted.Entities))
	for _, e := range extracted.Entities {
		normalized := NormalizeEntityName(e.Name)
		if normalized == "" {
			continue
		}
		id := uuid.New().String()
		nameToID[strings.ToLower(strings.TrimSpace(e.Name))] = id
		entities = append(entities, domain.GraphEntity{
			ID:             id,
			CanonicalName:  e.Name,
			NormalizedName: normalized,
			EntityType:     toEntityType(e.Type),
		})
	}

	// Build domain edges, resolving src/dst names to entity IDs.
	edges := make([]domain.GraphEdge, 0, len(extracted.Relations))
	for _, rel := range extracted.Relations {
		srcKey := strings.ToLower(strings.TrimSpace(rel.Source))
		srcID, ok := nameToID[srcKey]
		if !ok {
			slog.Debug("graph: skipping relation with unknown source entity", "source", rel.Source)
			continue
		}

		edge := domain.GraphEdge{
			ID:             uuid.New().String(),
			SrcEntityID:    srcID,
			Relation:       rel.Relation,
			SourceMemoryID: memoryID,
			Confidence:     1.0,
		}

		// Check if target is a known entity or a literal.
		dstKey := strings.ToLower(strings.TrimSpace(rel.Target))
		if dstID, found := nameToID[dstKey]; found {
			edge.DstEntityID = dstID
		} else {
			edge.DstLiteral = rel.Target
		}

		edges = append(edges, edge)
	}

	if err := s.graph.UpsertFromMemory(ctx, memoryID, agentID, sessionID, entities, edges); err != nil {
		return fmt.Errorf("graph upsert: %w", err)
	}

	slog.Info("graph indexed memory", "memory_id", memoryID, "entities", len(entities), "edges", len(edges))
	return nil
}

// DeleteMemory removes all graph data associated with a memory.
func (s *GraphService) DeleteMemory(ctx context.Context, memoryID string) error {
	if s == nil {
		return nil
	}
	return s.graph.DeleteByMemory(ctx, memoryID)
}

// ExpandFromMemories performs 1-hop graph expansion from seed memory IDs.
func (s *GraphService) ExpandFromMemories(ctx context.Context, seedMemoryIDs []string, agentID string, limit int) ([]domain.GraphHit, error) {
	if s == nil {
		return nil, nil
	}
	return s.graph.ExpandFromMemories(ctx, seedMemoryIDs, agentID, limit)
}

// --- LLM extraction ---

type extractedEntity struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type extractedRelation struct {
	Source   string `json:"source"`
	Relation string `json:"relation"`
	Target   string `json:"target"`
}

type graphExtractResponse struct {
	Entities  []extractedEntity  `json:"entities"`
	Relations []extractedRelation `json:"relations"`
}

func (s *GraphService) extractGraph(ctx context.Context, content string) (*graphExtractResponse, error) {
	systemPrompt := `You are an entity-relationship extraction engine. Given a memory statement, extract named entities and their relationships.

## Entity Types (only extract these types)
- person: Named individuals, people mentioned by name or clear reference
- place: Cities, countries, restaurants, parks, addresses
- organization: Companies, schools, teams, institutions
- event: Named events, trips, milestones (e.g., "Tokyo trip", "wedding")
- work: Jobs, projects, roles (e.g., "ML project", "backend engineer")
- product: Named products, tools, services (e.g., "VS Code", "iPhone")
- pet: Named pets

## Rules
1. Only extract entities that are explicitly named or clearly identifiable in the text.
2. Each entity must have a "name" (as mentioned in the text) and a "type" from the list above.
3. For relationships, use the exact entity names as "source" and "target".
4. Relationships should be concise verb phrases: "is sister of", "lives in", "works at", "owns".
5. If the target of a relationship is not a named entity (e.g., a date, number), still include it as "target".
6. If no entities or relationships can be extracted, return empty arrays.
7. Preserve the original language of entity names.

## Output Format
Return ONLY valid JSON. No markdown fences, no explanation.

{"entities": [{"name": "Alice", "type": "person"}, {"name": "Tokyo", "type": "place"}], "relations": [{"source": "Alice", "relation": "visited", "target": "Tokyo"}]}`

	userPrompt := fmt.Sprintf("Extract entities and relationships from this memory:\n\n%s", content)

	raw, err := s.llm.CompleteJSON(ctx, systemPrompt, userPrompt)
	if err != nil {
		return nil, fmt.Errorf("graph extraction LLM call: %w", err)
	}

	parsed, err := llm.ParseJSON[graphExtractResponse](raw)
	if err != nil {
		// One retry.
		raw2, retryErr := s.llm.CompleteJSON(ctx, systemPrompt,
			"Your previous response was invalid JSON:\n"+raw+"\n\nFix it and return ONLY the corrected JSON.\n\n"+userPrompt)
		if retryErr != nil {
			return nil, fmt.Errorf("graph extraction retry: %w", retryErr)
		}
		parsed, err = llm.ParseJSON[graphExtractResponse](raw2)
		if err != nil {
			return nil, fmt.Errorf("graph extraction parse after retry: %w", err)
		}
	}

	return &parsed, nil
}

// NormalizeEntityName applies case-fold + trim + collapse internal whitespace.
var multiSpaceRe = regexp.MustCompile(`\s+`)

func NormalizeEntityName(name string) string {
	name = strings.TrimSpace(name)
	name = strings.ToLower(name)
	name = multiSpaceRe.ReplaceAllString(name, " ")
	return name
}

// toEntityType maps LLM output to domain.EntityType, defaulting to empty string
// for unrecognized types (which will still be stored).
func toEntityType(t string) domain.EntityType {
	switch strings.ToLower(strings.TrimSpace(t)) {
	case "person":
		return domain.EntityPerson
	case "place":
		return domain.EntityPlace
	case "organization", "org":
		return domain.EntityOrg
	case "event":
		return domain.EntityEvent
	case "work":
		return domain.EntityWork
	case "product":
		return domain.EntityProduct
	case "pet":
		return domain.EntityPet
	default:
		return domain.EntityType(strings.ToLower(strings.TrimSpace(t)))
	}
}
