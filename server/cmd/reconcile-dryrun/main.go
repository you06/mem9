// reconcile-dryrun replays the reconcile v0 proposal pipeline over an
// existing tenant corpus and emits the audit table — WITHOUT writing
// anything. This is the gate artifact the frozen spec requires before
// apply mode is allowed: humans audit the proposed actions; DUPLICATE
// precision must reach >=90% and UPDATES is held to a stricter standard.
//
// Replay semantics: V's are processed in created_at order; each V is
// treated as "newly stored" and compared only against EARLIER V's —
// approximating what the async post-store hook would have seen at the
// time. Candidates come from the real RecallKV path (the same recall
// the production hook would use), so the audit also exercises
// active-only filtering on every recall path; any non-active hit is
// counted as a violation (expected 0).
//
// Usage:
//
//	MNEMO_LLM_API_KEY=... MNEMO_LLM_BASE_URL=... MNEMO_LLM_MODEL=... \
//	go run ./cmd/reconcile-dryrun \
//	  -dsn 'user:pass@tcp(host:4000)/db?parseTime=true' \
//	  -agent 'your-agent-namespace' \
//	  -out reconcile-dryrun.jsonl
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/qiffang/mnemos/server/internal/domain"
	"github.com/qiffang/mnemos/server/internal/llm"
	"github.com/qiffang/mnemos/server/internal/repository/tidb"
	"github.com/qiffang/mnemos/server/internal/service"
)

func main() {
	dsn := flag.String("dsn", "", "tenant DB DSN (required)")
	agent := flag.String("agent", "", "agent_id namespace to replay (required)")
	out := flag.String("out", "reconcile-dryrun.jsonl", "audit JSONL output path")
	maxCandidates := flag.Int("candidates", 5, "max candidates per new V")
	maxValues := flag.Int("max-values", 0, "process at most N values (0 = all)")
	flag.Parse()
	if *dsn == "" || *agent == "" {
		flag.Usage()
		os.Exit(2)
	}

	llmClient := llm.New(llm.Config{
		APIKey:  os.Getenv("MNEMO_LLM_API_KEY"),
		BaseURL: os.Getenv("MNEMO_LLM_BASE_URL"),
		Model:   os.Getenv("MNEMO_LLM_MODEL"),
	})
	if llmClient == nil {
		fmt.Fprintln(os.Stderr, "error: MNEMO_LLM_API_KEY (and friends) must be set — the proposal layer needs an LLM")
		os.Exit(2)
	}

	db, err := sql.Open("mysql", *dsn)
	if err != nil {
		fatal("open db: %v", err)
	}
	defer db.Close()
	ctx := context.Background()
	if err := db.PingContext(ctx); err != nil {
		fatal("ping db: %v", err)
	}
	repo := tidb.NewMemoryRepo(db, "", true, "")

	values, err := loadValues(ctx, db, *agent)
	if err != nil {
		fatal("load values: %v", err)
	}
	keysByValue, err := loadKeys(ctx, db, *agent)
	if err != nil {
		fatal("load keys: %v", err)
	}
	fmt.Printf("corpus: %d active V, %d V with keys\n", len(values), len(keysByValue))

	runID := fmt.Sprintf("dryrun-%s", time.Now().UTC().Format("20060102T150405Z"))
	seenAt := make(map[string]int, len(values)) // id -> replay order index
	activeViolations := 0
	type pendingRow struct {
		p           service.ReconcileProposal
		newContent  string
		candContent string
	}
	var pending []pendingRow
	candContentByID := make(map[string]string)

	for i, v := range values {
		if *maxValues > 0 && i >= *maxValues {
			break
		}
		seenAt[v.ID] = i

		// Candidate recall via the production path. Query = content head
		// (FTS-friendly); the hook would use the same surface.
		query := contentHead(v.Content, 160)
		hits, err := repo.RecallKV(ctx, query, nil, tidb.StrategyDefaultV1, *maxCandidates*3)
		if err != nil {
			fatal("recall for %s: %v", v.ID, err)
		}
		candidates := make([]domain.Memory, 0, *maxCandidates)
		for _, h := range hits {
			if h.Value == nil || h.Value.ID == v.ID {
				continue
			}
			if h.Value.State != domain.StateActive {
				activeViolations++ // expected 0: every recall path must filter active-only
				continue
			}
			// Replay constraint: only EARLIER V's were visible at "store time".
			at, seen := seenAt[h.Value.ID]
			if !seen || at >= i {
				continue
			}
			candidates = append(candidates, *h.Value)
			if len(candidates) >= *maxCandidates {
				break
			}
		}
		if len(candidates) == 0 {
			continue
		}

		proposals, err := service.ProposeReconcile(ctx, llmClient, runID, v, candidates, keysByValue)
		if err != nil {
			fmt.Fprintf(os.Stderr, "warn: propose for %s: %v (fail-open, skipping)\n", v.ID, err)
			continue
		}
		for _, c := range candidates {
			candContentByID[c.ID] = c.Content
		}
		for _, p := range proposals {
			pending = append(pending, pendingRow{
				p:           p,
				newContent:  contentHead(v.Content, 120),
				candContent: contentHead(candContentByID[p.CandidateID], 120),
			})
		}
		if (i+1)%25 == 0 {
			fmt.Printf("  ...%d/%d values\n", i+1, len(values))
		}
	}

	// Rule 6 (topology guard) runs over the FULL accumulated batch,
	// after pair-level mapping — it counts tentative UPDATES only.
	all := make([]service.ReconcileProposal, len(pending))
	for i, r := range pending {
		all[i] = r.p
	}
	all = service.ApplyTopologyGuard(all)
	for i := range pending {
		pending[i].p = all[i]
	}

	f, err := os.Create(*out)
	if err != nil {
		fatal("create out: %v", err)
	}
	defer f.Close()
	enc := json.NewEncoder(f)

	actionCounts := map[string]int{}
	relationCounts := map[string]int{}
	demoteReasons := map[string]int{}
	multiSupersederRows := 0
	proposalsTotal := 0
	for _, r := range pending {
		proposalsTotal++
		actionCounts[r.p.Action]++
		if r.p.Relation != "" {
			relationCounts[r.p.Relation]++
		}
		if r.p.Demoted {
			demoteReasons[r.p.DemotedBy]++
		}
		if r.p.SupersederCount > 1 {
			multiSupersederRows++
		}
		if err := enc.Encode(auditRow{
			ReconcileProposal: r.p,
			NewContent:        r.newContent,
			CandContent:       r.candContent,
		}); err != nil {
			fatal("write audit row: %v", err)
		}
	}

	fmt.Printf("\nrun_id: %s\nproposals: %d\n", runID, proposalsTotal)
	fmt.Println("actions:")
	for _, a := range []string{service.ReconcileDistinct, service.ReconcileDuplicate, service.ReconcileUpdates, service.ReconcileConflicts} {
		fmt.Printf("  %-10s %d\n", a, actionCounts[a])
	}
	fmt.Printf("relations: %v\n", relationCounts)
	fmt.Printf("demotions by guard: %v\n", demoteReasons)
	fmt.Printf("multi-superseder rows (topology-demoted): %d\n", multiSupersederRows)
	fmt.Printf("active-only violations (must be 0): %d\naudit table: %s\n", activeViolations, *out)
	if activeViolations > 0 {
		os.Exit(1)
	}
}

type auditRow struct {
	service.ReconcileProposal
	NewContent  string `json:"new_content"`
	CandContent string `json:"candidate_content"`
}

func loadValues(ctx context.Context, db *sql.DB, agent string) ([]domain.Memory, error) {
	rows, err := db.QueryContext(ctx,
		`SELECT id, content, COALESCE(metadata, ''), created_at, state
		 FROM memory_values
		 WHERE agent_id = ? AND state = 'active'
		 ORDER BY created_at, id`, agent)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.Memory
	for rows.Next() {
		var m domain.Memory
		var meta string
		if err := rows.Scan(&m.ID, &m.Content, &meta, &m.CreatedAt, &m.State); err != nil {
			return nil, err
		}
		if meta != "" {
			m.Metadata = json.RawMessage(meta)
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

func loadKeys(ctx context.Context, db *sql.DB, agent string) (map[string][]string, error) {
	rows, err := db.QueryContext(ctx,
		`SELECT mk.memory_value_id, mk.key_text
		 FROM memory_keys mk JOIN memory_values mv ON mv.id = mk.memory_value_id
		 WHERE mv.agent_id = ?`, agent)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string][]string)
	for rows.Next() {
		var id, key string
		if err := rows.Scan(&id, &key); err != nil {
			return nil, err
		}
		out[id] = append(out[id], key)
	}
	return out, rows.Err()
}

func contentHead(s string, n int) string {
	s = strings.TrimSpace(s)
	if len(s) <= n {
		return s
	}
	return s[:n]
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "error: "+format+"\n", args...)
	os.Exit(1)
}
