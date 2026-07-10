// Package service: reconcile.go implements the reconcile v0.1
// proposal layer — comparing a newly stored V against similar existing
// V's of the same agent and proposing DISTINCT / DUPLICATE / UPDATES /
// CONFLICTS actions.
//
// v0.1 redesign (after the first 340-V dry-run audit): the LLM no
// longer chooses the ACTION.
// It only classifies the RELATION between two facts; the action is
// derived in code, and mapRelationToAction is the ONLY place that can
// produce UPDATES. The first dry-run showed the systematic failure
// mode of action-choosing: "adds detail / refines / generalizes"
// relations were labeled UPDATES, which on apply would archive the old
// V and silently destroy information (a generic "is a mother"
// superseding a dated museum-visit fact; vague/relative facts eating
// dated ones; distinct repeated events collapsed into one progression).
// With the relation schema those mistakes are structurally impossible.
//
// Deterministic guards (all unit-tested, no LLM discretion):
//   - information preservation: OLD content carrying date/number
//     tokens absent from NEW can never be superseded;
//   - same-event: state transitions across different occurrences of a
//     repeated activity are not UPDATES;
//   - topology (rule 6, ApplyTopologyGuard — runs AFTER pair-level
//     mapping, over tentative UPDATES only): one old V claimed by >1
//     UPDATES demotes the whole group to CONFLICTS — the structural
//     fingerprint of distinct repeated events being collapsed, and a
//     shape `superseded_by` (single column) could not represent anyway;
//   - habit/instance is its own relation and never supersedes.
//
// Apply (archive / supersede / key migration) remains a separate,
// flag-locked step gated on a second dry-run audit.
package service

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/qiffang/mnemos/server/internal/domain"
	"github.com/qiffang/mnemos/server/internal/llm"
)

// Reconcile actions (derived in code, never chosen by the LLM).
const (
	ReconcileDistinct  = "DISTINCT"
	ReconcileDuplicate = "DUPLICATE"
	ReconcileUpdates   = "UPDATES"
	ReconcileConflicts = "CONFLICTS"
)

// Relations the LLM may assign. Only relationStateTransition can ever
// become UPDATES, and only with same_event + confidence + guards.
const (
	relationUnrelated       = "unrelated"
	relationSameFact        = "same_fact"
	relationAddsDetail      = "adds_detail"
	relationGeneralizes     = "generalizes"
	relationStateTransition = "state_transition"
	relationContradicts     = "contradicts"
	relationInstanceOfHabit = "instance_of_habit"
)

// Confidence floors. A wrong supersede silently hides a fact from
// recall, so UPDATES demands more than DUPLICATE.
const (
	reconcileDuplicateMinConfidence = 0.70
	reconcileUpdatesMinConfidence   = 0.80
)

// ReconcileProposal is one audited row of the dry-run output: what the
// pipeline WOULD do to (NewValueID, CandidateID) if apply were enabled.
type ReconcileProposal struct {
	NewValueID  string `json:"new_value_id"`
	CandidateID string `json:"candidate_id"`
	Action      string `json:"action"`
	// Relation is the LLM's classification; Action is derived from it
	// in mapRelationToAction. Kept for audit.
	Relation  string `json:"relation,omitempty"`
	SameEvent bool   `json:"same_event,omitempty"`
	Entity    string `json:"entity,omitempty"`
	Slot      string `json:"slot,omitempty"`
	// Survivor is "new" or "old" — which V remains active.
	Survivor string `json:"survivor,omitempty"`
	// KeysToMove are the loser's / superseded V's key_text values that
	// would migrate to the surviving / new V (source='reconcile').
	KeysToMove []string `json:"keys_to_move,omitempty"`
	// MetadataPreview is the V-level audit record that would be written
	// onto the surviving V's metadata on apply.
	MetadataPreview json.RawMessage `json:"metadata_preview,omitempty"`
	Confidence      float64         `json:"confidence"`
	Reason          string          `json:"reason"`
	// Demoted marks a deterministic downgrade after relation mapping
	// (guard hit); OriginalAction records what it would have been and
	// DemotedBy names the guard: "confidence", "same_event",
	// "info_preservation", "topology".
	Demoted        bool   `json:"demoted,omitempty"`
	OriginalAction string `json:"original_action,omitempty"`
	DemotedBy      string `json:"demoted_by,omitempty"`
	// SupersederCount is set by the topology guard on demoted rows:
	// how many tentative UPDATES targeted the same old V.
	SupersederCount int `json:"superseder_count,omitempty"`
	// OmittedByLLM marks a candidate the LLM returned NO verdict for.
	// Such pairs become explicit DISTINCT rows so the audit table has
	// exactly one row per (new, candidate) pair.
	OmittedByLLM bool `json:"omitted_by_llm,omitempty"`
}

// reconcileLLMVerdict is the raw per-candidate JSON the LLM returns.
type reconcileLLMVerdict struct {
	CandidateID string  `json:"candidate_id"`
	Relation    string  `json:"relation"`
	SameEvent   bool    `json:"same_event"`
	Entity      string  `json:"entity"`
	Slot        string  `json:"slot"`
	Survivor    string  `json:"survivor"`
	Confidence  float64 `json:"confidence"`
	Reason      string  `json:"reason"`
}

// ProposeReconcile runs the LLM relation judgment for one new V
// against its candidate set and returns guard-checked, audit-ready
// proposals. Read-only: no repository writes happen here.
//
// NOTE: the cross-proposal topology guard (rule 6) spans the proposals
// of MANY new V's — callers must run ApplyTopologyGuard over the
// accumulated batch after all ProposeReconcile calls.
func ProposeReconcile(
	ctx context.Context,
	llmClient *llm.Client,
	runID string,
	newV domain.Memory,
	candidates []domain.Memory,
	keysByValue map[string][]string,
) ([]ReconcileProposal, error) {
	if llmClient == nil {
		return nil, fmt.Errorf("reconcile: llm client is not configured")
	}
	if len(candidates) == 0 {
		return nil, nil
	}

	system := reconcileSystemPrompt()
	user := reconcileUserPrompt(newV, candidates)

	callCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	raw, err := llmClient.CompleteJSONWithScope(callCtx, system, user, llm.CallScope{Step: "reconcile_propose"})
	if err != nil {
		return nil, fmt.Errorf("reconcile: llm call: %w", err)
	}
	verdicts, err := parseReconcileVerdicts(raw)
	if err != nil {
		return nil, fmt.Errorf("reconcile: parse: %w", err)
	}

	byID := make(map[string]domain.Memory, len(candidates))
	for _, c := range candidates {
		byID[c.ID] = c
	}

	out := make([]ReconcileProposal, 0, len(verdicts))
	for _, v := range verdicts {
		cand, ok := byID[v.CandidateID]
		if !ok {
			// LLM hallucinated an id — drop, never guess.
			continue
		}
		p := ReconcileProposal{
			NewValueID:  newV.ID,
			CandidateID: v.CandidateID,
			Relation:    strings.ToLower(strings.TrimSpace(v.Relation)),
			SameEvent:   v.SameEvent,
			Entity:      strings.TrimSpace(v.Entity),
			Slot:        strings.TrimSpace(v.Slot),
			Survivor:    strings.ToLower(strings.TrimSpace(v.Survivor)),
			Confidence:  v.Confidence,
			Reason:      strings.TrimSpace(v.Reason),
		}
		p = mapRelationToAction(p, newV.Content, cand.Content)
		p = resolveSurvivorAndKeys(p, newV, cand, keysByValue)
		p.MetadataPreview = metadataPreview(runID, p)
		out = append(out, p)
	}
	return fillOmittedCandidates(out, newV, candidates), nil
}

// mapRelationToAction derives the action from the LLM's relation plus
// the deterministic pair-level guards. Pure function; the ONLY
// producer of UPDATES in the codebase:
//
//	same_fact (conf >= 0.70)           -> DUPLICATE
//	state_transition AND same_event
//	  AND conf >= 0.80 AND entity+slot
//	  AND info-preservation passes     -> UPDATES (tentative — topology
//	                                      guard still runs over the batch)
//	  guard misses                     -> CONFLICTS (keep both, flagged)
//	  missing entity/slot              -> DISTINCT
//	contradicts                        -> CONFLICTS
//	adds_detail / generalizes /
//	instance_of_habit / unrelated /
//	unknown                            -> DISTINCT
func mapRelationToAction(p ReconcileProposal, newContent, oldContent string) ReconcileProposal {
	switch p.Relation {
	case relationSameFact:
		if p.Confidence < reconcileDuplicateMinConfidence {
			p.Action = ReconcileDistinct
			p.Demoted = true
			p.OriginalAction = ReconcileDuplicate
			p.DemotedBy = "confidence"
			return p
		}
		p.Action = ReconcileDuplicate
		return p
	case relationContradicts:
		p.Action = ReconcileConflicts
		return p
	case relationStateTransition:
		switch {
		case p.Entity == "" || p.Slot == "":
			p.Action = ReconcileDistinct
			p.Demoted = true
			p.OriginalAction = ReconcileUpdates
			p.DemotedBy = "confidence"
		case !p.SameEvent:
			p.Action = ReconcileConflicts
			p.Demoted = true
			p.OriginalAction = ReconcileUpdates
			p.DemotedBy = "same_event"
		case p.Confidence < reconcileUpdatesMinConfidence:
			p.Action = ReconcileConflicts
			p.Demoted = true
			p.OriginalAction = ReconcileUpdates
			p.DemotedBy = "confidence"
		case !infoPreserved(oldContent, newContent):
			// OLD carries dates/numbers the NEW lacks: superseding
			// would destroy information. Keep both and flag.
			p.Action = ReconcileConflicts
			p.Demoted = true
			p.OriginalAction = ReconcileUpdates
			p.DemotedBy = "info_preservation"
		default:
			p.Action = ReconcileUpdates
		}
		return p
	default:
		// adds_detail, generalizes, instance_of_habit, unrelated, and
		// any unknown relation: keep both facts, no action. High
		// confidence does not change this — these relations are
		// structurally barred from UPDATES.
		p.Action = ReconcileDistinct
		return p
	}
}

// dateNumberToken matches the deterministic "information tokens" the
// preservation guard protects: digit-bearing tokens (dates, counts,
// times, years), month names, and spelled-out small numbers ("two
// children"). Over-firing is acceptable — it demotes toward CONFLICTS
// (keep both), the conservative direction. Named places/objects are protected by
// the relation narrowing + same_event requirement rather than token
// matching, which would over-fire on capitalized sentence starts.
var dateNumberToken = regexp.MustCompile(`(?i)\b(?:\d[\d:./-]*|january|february|march|april|may|june|july|august|september|october|november|december|one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve)\b`)

// infoPreserved reports whether every date/number token present in the
// OLD content also appears as an EXACT token in the NEW content. If
// not, the new fact is less specific than what it would replace and
// must not supersede it.
//
// v0.1.1: exact-token matching, not substring — the first v0.1 audit
// admitted a supersede because OLD's day token "2" was a substring of
// NEW's "2023" (the pottery case, where the dataset itself treats the
// 2-July signup and the ~7-July workshop as two separately-questioned
// facts; archiving the signup would destroy an answer source).
func infoPreserved(oldContent, newContent string) bool {
	oldTokens := dateNumberToken.FindAllString(strings.ToLower(oldContent), -1)
	if len(oldTokens) == 0 {
		return true
	}
	newTokens := make(map[string]struct{})
	for _, t := range dateNumberToken.FindAllString(strings.ToLower(newContent), -1) {
		newTokens[t] = struct{}{}
	}
	for _, t := range oldTokens {
		if _, ok := newTokens[t]; !ok {
			return false
		}
	}
	return true
}

// ApplyTopologyGuard enforces rule 6 over an accumulated proposal
// batch: if more than one tentative UPDATES targets the SAME old
// (candidate) V, the whole group demotes to CONFLICTS, with
// SupersederCount recorded for the audit. It MUST run after pair-level
// mapping (it counts only rows that survived as UPDATES — counting raw
// noise proposals would over-demote).
//
// Rationale: distinct repeated events (three camping trips all
// "superseding" one plan) produce exactly this shape, and
// `superseded_by` is a single column — multiple superseders have no
// legal representation. Genuine state CHAINS (researching -> applied
// -> chose) also hit this guard in pairwise form; chain resolution is
// v1 work, and until then keeping both facts flagged is the
// conservative, lossless choice. Pure function; order-preserving.
func ApplyTopologyGuard(proposals []ReconcileProposal) []ReconcileProposal {
	updatesPerOld := make(map[string]int)
	for _, p := range proposals {
		if p.Action == ReconcileUpdates {
			updatesPerOld[p.CandidateID]++
		}
	}
	out := make([]ReconcileProposal, len(proposals))
	for i, p := range proposals {
		if p.Action == ReconcileUpdates && updatesPerOld[p.CandidateID] > 1 {
			p.Action = ReconcileConflicts
			p.Demoted = true
			p.OriginalAction = ReconcileUpdates
			p.DemotedBy = "topology"
			p.SupersederCount = updatesPerOld[p.CandidateID]
			p.Survivor = ""
			p.KeysToMove = nil
			p.MetadataPreview = nil
		}
		out[i] = p
	}
	return out
}

// fillOmittedCandidates guarantees exactly one audit row per
// (new, candidate) pair: any candidate the LLM failed to return a
// verdict for becomes an explicit DISTINCT row flagged OmittedByLLM.
func fillOmittedCandidates(proposals []ReconcileProposal, newV domain.Memory, candidates []domain.Memory) []ReconcileProposal {
	seen := make(map[string]struct{}, len(proposals))
	for _, p := range proposals {
		seen[p.CandidateID] = struct{}{}
	}
	for _, c := range candidates {
		if _, ok := seen[c.ID]; ok {
			continue
		}
		proposals = append(proposals, ReconcileProposal{
			NewValueID:   newV.ID,
			CandidateID:  c.ID,
			Action:       ReconcileDistinct,
			Reason:       "LLM returned no verdict for this candidate",
			OmittedByLLM: true,
		})
	}
	return proposals
}

// resolveSurvivorAndKeys fills Survivor and KeysToMove. DUPLICATE ->
// canonical survivor; UPDATES -> new V survives, old keys migrate;
// CONFLICTS/DISTINCT -> nothing moves.
func resolveSurvivorAndKeys(p ReconcileProposal, newV, cand domain.Memory, keysByValue map[string][]string) ReconcileProposal {
	switch p.Action {
	case ReconcileDuplicate:
		newRich := richness(newV, keysByValue)
		oldRich := richness(cand, keysByValue)
		if newRich > oldRich {
			p.Survivor = "new"
			p.KeysToMove = keysByValue[cand.ID]
		} else {
			// Tie keeps old: existing references stay stable.
			p.Survivor = "old"
			p.KeysToMove = keysByValue[newV.ID]
		}
	case ReconcileUpdates:
		p.Survivor = "new"
		p.KeysToMove = keysByValue[cand.ID]
	default:
		p.Survivor = ""
		p.KeysToMove = nil
	}
	return p
}

// richness is the canonical-survivor heuristic. v0.1: CONTENT length
// dominates — the first dry-run showed key count picking shorter,
// lossier survivors ("Family time matters" beating the sentence that
// carried the actual detail). Key count and metadata break ties.
func richness(m domain.Memory, keysByValue map[string][]string) int {
	return len(m.Content)*100 + len(keysByValue[m.ID])*10 + len(m.Metadata)
}

// metadataPreview renders the V-level audit record that apply would
// write onto the surviving V (memory_keys has no metadata column — the
// audit lives on memory_values.metadata; keys rows only get
// source='reconcile').
func metadataPreview(runID string, p ReconcileProposal) json.RawMessage {
	if p.Action != ReconcileDuplicate && p.Action != ReconcileUpdates && p.Action != ReconcileConflicts {
		return nil
	}
	rec := map[string]any{
		"reconcile_run_id": runID,
		"reconcile_action": strings.ToLower(p.Action),
	}
	switch p.Action {
	case ReconcileDuplicate:
		if p.Survivor == "new" {
			rec["from_value_id"] = p.CandidateID
		} else {
			rec["from_value_id"] = p.NewValueID
		}
		rec["migrated_keys"] = p.KeysToMove
	case ReconcileUpdates:
		rec["from_value_id"] = p.CandidateID
		rec["migrated_keys"] = p.KeysToMove
	case ReconcileConflicts:
		rec["conflicts_with"] = p.CandidateID
	}
	b, err := json.Marshal(rec)
	if err != nil {
		return nil
	}
	return b
}

func parseReconcileVerdicts(raw string) ([]reconcileLLMVerdict, error) {
	cleaned := llm.StripMarkdownFences(raw)
	var out struct {
		Verdicts []reconcileLLMVerdict `json:"verdicts"`
	}
	if err := json.Unmarshal([]byte(cleaned), &out); err != nil {
		return nil, fmt.Errorf("invalid JSON: %w", err)
	}
	return out.Verdicts, nil
}

func reconcileSystemPrompt() string {
	return `You compare ONE newly stored long-term memory fact (NEW) against EXISTING memory facts from the same agent. For each existing fact, classify the RELATION between NEW and that fact. You do NOT decide what happens to either fact — only name the relation.

Relations:
- unrelated: different facts about different things.
- same_fact: the SAME assertion, merely reworded. Not "related", not "overlapping" — the same fact.
- adds_detail: NEW says the same thing as the existing fact plus extra specifics (a date, a place, names, a count). The existing fact remains true.
- generalizes: NEW is a vaguer or broader statement of the existing fact (drops specifics the existing fact has).
- state_transition: the SAME entity's SAME attribute moved to a later state of the SAME plan/event/process — e.g. "plans to attend X" then "attended X"; "researching agencies" then "applied to agencies". Added items, new preferences, or other instances of a recurring activity are NOT state transitions.
- contradicts: the two facts cannot both be current and you cannot tell which is.
- instance_of_habit: one fact is a recurring habit/tradition and the other is one specific occurrence of it.

Also report:
- same_event: true ONLY if both facts clearly refer to the same single occurrence/plan/process. For repeated activities (trips, parades, workshops), different occurrences mean same_event=false.
- entity: the subject. slot: the attribute in question (state_transition only).
- survivor: for same_fact only — "new" or "old", whichever wording carries more information.
- confidence: 0.0-1.0 for the relation classification.
- reason: one sentence.

Be precise about specificity: dropping a date or detail is "generalizes", never "state_transition". Different occurrences of similar activities are separate facts, not progressions.

Output ONLY JSON:
{"verdicts":[{"candidate_id":"...","relation":"...","same_event":false,"entity":"...","slot":"...","survivor":"","confidence":0.0,"reason":"..."}]}
The "relation" value must be exactly one of: unrelated, same_fact, adds_detail, generalizes, state_transition, contradicts, instance_of_habit. ("same_event" is a separate boolean field, never a relation value.)
One verdict per existing fact. No prose, no fences.`
}

func reconcileUserPrompt(newV domain.Memory, candidates []domain.Memory) string {
	var b strings.Builder
	b.WriteString("NEW fact:\n")
	fmt.Fprintf(&b, "  %s\n\nEXISTING facts:\n", strings.TrimSpace(newV.Content))
	for _, c := range candidates {
		fmt.Fprintf(&b, "- candidate_id: %s\n  %s\n", c.ID, strings.TrimSpace(c.Content))
	}
	return b.String()
}
