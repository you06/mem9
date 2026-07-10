package handler

import (
	"net/url"
	"testing"
)

// parseSearchQueries is the wire-side half of multi-query recall:
// repeated `q` params, trimmed and deduped. Single-q behavior must be
// byte-identical to the old q.Get("q") path; the cap lives in the
// multi-query branch, not here.

func TestParseSearchQueries_SingleAndEmpty(t *testing.T) {
	qs := parseSearchQueries(url.Values{"q": {" hello "}})
	if len(qs) != 1 || qs[0] != "hello" {
		t.Fatalf("single: got %v", qs)
	}
	if qs := parseSearchQueries(url.Values{}); len(qs) != 0 {
		t.Fatalf("absent q: got %v", qs)
	}
	if qs := parseSearchQueries(url.Values{"q": {"  "}}); len(qs) != 0 {
		t.Fatalf("blank q: got %v", qs)
	}
}

func TestParseSearchQueries_MultiDedupOrder(t *testing.T) {
	qs := parseSearchQueries(url.Values{"q": {"a", "b", " a ", "c", ""}})
	want := []string{"a", "b", "c"}
	if len(qs) != len(want) {
		t.Fatalf("got %v want %v", qs, want)
	}
	for i := range want {
		if qs[i] != want[i] {
			t.Fatalf("order/dedup: got %v want %v", qs, want)
		}
	}
}

// The cap is NOT enforced at parse time — primary-only branches (chain,
// content-keyword, scanAll) keep the old q.Get("q") semantics of
// ignoring extra values, and the K=>V multi-query branch enforces
// maxSearchQueries itself (see listMemories). Parse just collects.
func TestParseSearchQueries_NoParseTimeCap(t *testing.T) {
	six := url.Values{"q": {"a", "b", "c", "d", "e", "f"}}
	if qs := parseSearchQueries(six); len(qs) != 6 {
		t.Fatalf("parse must not cap, got %v", qs)
	}
	// Duplicates still collapse.
	dups := url.Values{"q": {"a", "b", "c", "d", "e", "a"}}
	if qs := parseSearchQueries(dups); len(qs) != 5 {
		t.Fatalf("dedup: got %v", qs)
	}
}
