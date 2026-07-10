package service

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/qiffang/mnemos/server/internal/domain"
)

// v0.1 contract pins:
// mapRelationToAction is the ONLY producer of UPDATES, and the barred
// relations can never reach it regardless of confidence. The topology
// guard runs after pair-level mapping, over tentative UPDATES only.

func TestMapRelation_BarredRelationsNeverUpdate(t *testing.T) {
	// Contract pin: adds_detail / generalizes /
	// instance_of_habit / unrelated / unknown cannot become UPDATES
	// even at confidence 0.99 with perfect entity/slot/same_event.
	for _, rel := range []string{
		relationAddsDetail, relationGeneralizes, relationInstanceOfHabit,
		relationUnrelated, "merge_everything",
	} {
		p := mapRelationToAction(ReconcileProposal{
			Relation: rel, SameEvent: true, Entity: "Caroline", Slot: "x", Confidence: 0.99,
		}, "new content", "old content")
		if p.Action != ReconcileDistinct {
			t.Errorf("relation %q must map to DISTINCT, got %s", rel, p.Action)
		}
	}
}

func TestMapRelation_StateTransitionGuards(t *testing.T) {
	base := ReconcileProposal{Relation: relationStateTransition, SameEvent: true, Entity: "Caroline", Slot: "attendance", Confidence: 0.9}

	// All guards pass -> tentative UPDATES.
	p := mapRelationToAction(base, "Caroline attended the conference", "Caroline plans to attend the conference")
	if p.Action != ReconcileUpdates || p.Demoted {
		t.Fatalf("clean state_transition must be UPDATES: %+v", p)
	}

	// same_event=false -> CONFLICTS (different occurrences are not progressions).
	q := base
	q.SameEvent = false
	p = mapRelationToAction(q, "new", "old")
	if p.Action != ReconcileConflicts || p.DemotedBy != "same_event" {
		t.Fatalf("same_event=false -> CONFLICTS: %+v", p)
	}

	// Low confidence -> CONFLICTS.
	q = base
	q.Confidence = 0.79
	p = mapRelationToAction(q, "new", "old")
	if p.Action != ReconcileConflicts || p.DemotedBy != "confidence" {
		t.Fatalf("conf<0.80 -> CONFLICTS: %+v", p)
	}

	// Missing entity/slot -> DISTINCT.
	q = base
	q.Slot = ""
	p = mapRelationToAction(q, "new", "old")
	if p.Action != ReconcileDistinct || !p.Demoted {
		t.Fatalf("missing slot -> DISTINCT: %+v", p)
	}

	// Info-preservation: OLD has a date the NEW lacks -> CONFLICTS.
	p = mapRelationToAction(base, "Caroline is transitioning", "Caroline started transitioning around June 2020")
	if p.Action != ReconcileConflicts || p.DemotedBy != "info_preservation" {
		t.Fatalf("date loss must demote to CONFLICTS: %+v", p)
	}

	// Info preserved when NEW keeps the tokens.
	p = mapRelationToAction(base, "Caroline attended the conference around July 10, 2023", "Caroline is going to a conference in July 2023")
	if p.Action != ReconcileUpdates {
		t.Fatalf("tokens preserved -> UPDATES allowed: %+v", p)
	}
}

func TestMapRelation_SameFactAndContradicts(t *testing.T) {
	p := mapRelationToAction(ReconcileProposal{Relation: relationSameFact, Confidence: 0.71}, "n", "o")
	if p.Action != ReconcileDuplicate {
		t.Fatalf("confident same_fact -> DUPLICATE: %+v", p)
	}
	p = mapRelationToAction(ReconcileProposal{Relation: relationSameFact, Confidence: 0.69}, "n", "o")
	if p.Action != ReconcileDistinct || p.DemotedBy != "confidence" {
		t.Fatalf("low-confidence same_fact -> DISTINCT: %+v", p)
	}
	p = mapRelationToAction(ReconcileProposal{Relation: relationContradicts, Confidence: 0.5}, "n", "o")
	if p.Action != ReconcileConflicts {
		t.Fatalf("contradicts -> CONFLICTS at any confidence: %+v", p)
	}
}

func TestInfoPreserved(t *testing.T) {
	cases := []struct {
		old, new string
		want     bool
	}{
		{"went hiking around 18 August 2023", "went hiking and met people", false},
		{"went hiking around 18 August 2023", "had a bad encounter on the 18 August 2023 hike", true},
		{"has two children", "has children", false},
		{"likes painting", "loves painting a lot", true}, // no protected tokens in old
		{"camping the week of June 27", "camping in June", false},
		// v0.1.1 exact-token pin (the pottery case): OLD's day token
		// "2" must NOT be satisfied by being a substring of NEW's
		// "2023" — the 2-July signup and ~7-July workshop are two
		// separately-questioned dataset facts.
		{"signed up for a pottery class on 2 July 2023", "took her kids to a pottery workshop around July 7, 2023", false},
	}
	for _, c := range cases {
		if got := infoPreserved(c.old, c.new); got != c.want {
			t.Errorf("infoPreserved(%q, %q) = %v, want %v", c.old, c.new, got, c.want)
		}
	}
}

func TestApplyTopologyGuard(t *testing.T) {
	// Contract pin: two tentative UPDATES on the same old V -> both
	// become CONFLICTS with superseder_count=2; the single-superseder
	// UPDATES and unrelated rows stay untouched.
	in := []ReconcileProposal{
		{NewValueID: "n1", CandidateID: "old-plan", Action: ReconcileUpdates},
		{NewValueID: "n2", CandidateID: "old-plan", Action: ReconcileUpdates},
		{NewValueID: "n3", CandidateID: "other-old", Action: ReconcileUpdates},
		{NewValueID: "n4", CandidateID: "old-plan", Action: ReconcileDistinct},
	}
	out := ApplyTopologyGuard(in)
	for _, p := range out[:2] {
		if p.Action != ReconcileConflicts || p.DemotedBy != "topology" || p.SupersederCount != 2 {
			t.Fatalf("multi-superseder must demote with count: %+v", p)
		}
		if p.KeysToMove != nil || p.Survivor != "" || p.MetadataPreview != nil {
			t.Fatalf("demoted row must carry no apply payload: %+v", p)
		}
	}
	if out[2].Action != ReconcileUpdates || out[2].Demoted {
		t.Fatalf("single-superseder UPDATES must survive: %+v", out[2])
	}
	if out[3].Action != ReconcileDistinct {
		t.Fatalf("non-UPDATES rows untouched: %+v", out[3])
	}
}

func TestResolveSurvivorAndKeys_ContentDominant(t *testing.T) {
	// v0.1 pin: content richness outweighs key count. "old" has more
	// keys but much shorter content — the longer content must win.
	newV := domain.Memory{ID: "new", Content: "Melanie cherishes time with her family and feels alive and happy when with them."}
	oldV := domain.Memory{ID: "old", Content: "Family time matters to Melanie."}
	keys := map[string][]string{
		"new": {"k1"},
		"old": {"k1", "k2", "k3", "k4", "k5"},
	}
	p := resolveSurvivorAndKeys(ReconcileProposal{Action: ReconcileDuplicate, NewValueID: "new", CandidateID: "old"}, newV, oldV, keys)
	if p.Survivor != "new" {
		t.Fatalf("longer content must win survivor despite fewer keys: %+v", p)
	}
	// UPDATES: new survives, old keys migrate.
	p = resolveSurvivorAndKeys(ReconcileProposal{Action: ReconcileUpdates, NewValueID: "new", CandidateID: "old"}, newV, oldV, keys)
	if p.Survivor != "new" || len(p.KeysToMove) != 5 {
		t.Fatalf("UPDATES survivor/keys: %+v", p)
	}
	// CONFLICTS: nothing moves.
	p = resolveSurvivorAndKeys(ReconcileProposal{Action: ReconcileConflicts}, newV, oldV, keys)
	if p.Survivor != "" || p.KeysToMove != nil {
		t.Fatalf("CONFLICTS must not migrate: %+v", p)
	}
}

func TestMetadataPreviewShape(t *testing.T) {
	p := ReconcileProposal{Action: ReconcileUpdates, NewValueID: "new", CandidateID: "old", Survivor: "new", KeysToMove: []string{"k1"}}
	raw := metadataPreview("run-1", p)
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	if m["reconcile_run_id"] != "run-1" || m["reconcile_action"] != "updates" || m["from_value_id"] != "old" {
		t.Fatalf("preview: %v", m)
	}
	if metadataPreview("run-1", ReconcileProposal{Action: ReconcileDistinct}) != nil {
		t.Fatal("DISTINCT must have no metadata preview")
	}
}

func TestParseReconcileVerdicts(t *testing.T) {
	raw := `{"verdicts":[{"candidate_id":"c1","relation":"state_transition","same_event":true,"entity":"Caroline","slot":"adoption","survivor":"","confidence":0.9,"reason":"progressed"}]}`
	vs, err := parseReconcileVerdicts(raw)
	if err != nil || len(vs) != 1 || vs[0].Relation != "state_transition" || !vs[0].SameEvent {
		t.Fatalf("parse: %+v %v", vs, err)
	}
	if _, err := parseReconcileVerdicts("```json\n" + raw + "\n```"); err != nil {
		t.Fatalf("fenced: %v", err)
	}
	if _, err := parseReconcileVerdicts("not json"); err == nil {
		t.Fatal("invalid JSON must error")
	}
}

func TestFillOmittedCandidates(t *testing.T) {
	newV := domain.Memory{ID: "new"}
	cands := []domain.Memory{{ID: "c1"}, {ID: "c2"}, {ID: "c3"}}
	in := []ReconcileProposal{{NewValueID: "new", CandidateID: "c1", Action: ReconcileDuplicate}}
	out := fillOmittedCandidates(in, newV, cands)
	if len(out) != 3 {
		t.Fatalf("want one row per candidate pair, got %d", len(out))
	}
	omitted := 0
	for _, p := range out {
		if p.OmittedByLLM {
			omitted++
			if p.Action != ReconcileDistinct {
				t.Errorf("omitted rows must be DISTINCT, got %s", p.Action)
			}
		}
	}
	if omitted != 2 {
		t.Errorf("want 2 omitted rows, got %d", omitted)
	}
}

func TestReconcilePromptHygiene(t *testing.T) {
	sys := reconcileSystemPrompt()
	// v0.1: prompt asks for RELATIONS, never actions.
	for _, must := range []string{"relation", "same_fact", "state_transition", "adds_detail", "generalizes", "instance_of_habit", "same_event", "separate facts, not progressions", "never a relation value"} {
		if !strings.Contains(sys, must) {
			t.Errorf("system prompt missing %q", must)
		}
	}
	for _, never := range []string{"LoCoMo", "locomo", "benchmark", "score", "category", "UPDATES", "DUPLICATE", "ARCHIVE"} {
		if strings.Contains(sys, never) {
			t.Errorf("system prompt must not contain %q (actions are code-side)", never)
		}
	}
}
