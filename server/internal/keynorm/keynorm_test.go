package keynorm

import "testing"

func TestNormalizeKey(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		// Empty + whitespace-only.
		{name: "empty", in: "", want: ""},
		{name: "whitespace_only", in: "   \t\n  ", want: ""},

		// Basic English: lowercase + strip terminal punctuation.
		{name: "english_simple", in: "User lives in Chiba.", want: "user lives in chiba"},
		{name: "english_caps", in: "My HOME", want: "my home"},
		{name: "english_question", in: "Where does the user live?", want: "where does the user live"},
		{name: "english_exclamation", in: "User PREFERS Python!", want: "user prefers python"},

		// Apostrophe (single quote) replaced with space per V1 rule. The
		// possessive "user's home" splits into "user s home"; this is the
		// intentional safe-failure mode that keeps it distinct from the
		// plural "users home" (no KEY_EXACT collision across that boundary).
		{name: "english_apostrophe", in: "user's home", want: "user s home"},
		{name: "english_double_quote", in: `she said "hi"`, want: "she said hi"},

		// Whitespace folding (multiple spaces, leading/trailing).
		{name: "english_multispace", in: "  hello   world  ", want: "hello world"},
		{name: "english_tabs_newlines", in: "hello\tworld\nfoo", want: "hello world foo"},

		// Chinese: lowercase no-op, fullwidth punctuation stripped, fullwidth space folded.
		{name: "chinese_simple", in: "用户住在千叶。", want: "用户住在千叶"},
		{name: "chinese_question", in: "用户的家在哪里？", want: "用户的家在哪里"},
		{name: "chinese_comma", in: "用户喜欢 Python，部署 TiDB。", want: "用户喜欢 python 部署 tidb"},
		{name: "chinese_fullwidth_space", in: "用户 住在 千叶", want: "用户 住在 千叶"},
		{name: "chinese_enum_comma", in: "苹果、橘子、香蕉。", want: "苹果 橘子 香蕉"},

		// Mixed CN/EN.
		{name: "mixed_simple", in: "User住在Chiba。", want: "user住在chiba"},

		// NFKC: fullwidth ASCII → halfwidth ASCII.
		{name: "fullwidth_alphanumeric", in: "ＵＳＥＲ ＬＩＶＥＳ ＩＮ Ｃｈｉｂａ", want: "user lives in chiba"},
		{name: "fullwidth_digits", in: "ＴｉＤＢ２０２４", want: "tidb2024"},

		// Typographic / curly quotes replaced with space.
		{name: "curly_double", in: `they said “hello”`, want: "they said hello"},
		{name: "curly_single", in: "users‘ home", want: "users home"},
		{name: "curly_apostrophe_mid", in: "user’s home", want: "user s home"},

		// Punctuation NOT stripped: structural connectors stay.
		{name: "hyphen_kept", in: "rate-limit user", want: "rate-limit user"},
		{name: "underscore_kept", in: "kind_skill memory", want: "kind_skill memory"},
		{name: "colon_kept", in: "type:memory", want: "type:memory"},
		{name: "slash_kept", in: "src/main.go", want: "src/main go"}, // . inside token still stripped → "main go"
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got := NormalizeKey(tc.in)
			if got != tc.want {
				t.Errorf("NormalizeKey(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestNormalizeKey_Idempotent(t *testing.T) {
	cases := []string{
		"User lives in Chiba.",
		"用户住在千叶。",
		"  hello   world  ",
		"ＵＳＥＲ ＬＩＶＥＳ ＩＮ Ｃｈｉｂａ",
		"user's home",
		"rate-limit user",
		"",
	}
	for _, in := range cases {
		once := NormalizeKey(in)
		twice := NormalizeKey(once)
		if once != twice {
			t.Errorf("NormalizeKey not idempotent for %q: %q → %q", in, once, twice)
		}
	}
}

// TestCanonicalizeValue documents the V-dedup semantics: NFKC + lowercase
// + whitespace fold + trim, but NO punctuation strip. Two facts that
// differ only in punctuation must NOT collide.
func TestCanonicalizeValue(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{name: "empty", in: "", want: ""},
		{name: "whitespace_only", in: "  \t\n  ", want: ""},

		// Punctuation kept — these differ from NormalizeKey output.
		{name: "english_period", in: "User lives in Chiba.", want: "user lives in chiba."},
		{name: "english_apostrophe", in: "user's home", want: "user's home"},
		{name: "english_question", in: "Where?", want: "where?"},
		{name: "chinese_period", in: "用户住在千叶。", want: "用户住在千叶。"},
		{name: "chinese_enum", in: "苹果、橘子、香蕉。", want: "苹果、橘子、香蕉。"},

		// NFKC + lowercase still apply.
		{name: "caps", in: "User Lives In Chiba", want: "user lives in chiba"},
		{name: "fullwidth", in: "ＵＳＥＲ ＬＩＶＥＳ", want: "user lives"},

		// Whitespace fold still applies.
		{name: "multispace", in: "  hello   world  ", want: "hello world"},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got := CanonicalizeValue(tc.in)
			if got != tc.want {
				t.Errorf("CanonicalizeValue(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

// TestCanonicalizeValue_PunctuationDifferentiates is the critical contract:
// two facts that differ only in punctuation hash to different values. This
// is the property NormalizeKey would have violated, which is why we have
// a separate CanonicalizeValue.
func TestCanonicalizeValue_PunctuationDifferentiates(t *testing.T) {
	pairs := []struct {
		a, b string
	}{
		{"A. B.", "A B"},
		{"User's home", "Users home"},
		{"hello, world", "hello world"},
		{"苹果、橘子", "苹果 橘子"},
	}
	for _, p := range pairs {
		ha := HashValue(p.a)
		hb := HashValue(p.b)
		if ha == hb {
			t.Errorf("punctuation should differentiate: HashValue(%q) == HashValue(%q) == %q", p.a, p.b, ha)
		}
	}
}

func TestHashValue_DeterministicAndIdempotent(t *testing.T) {
	in := "User Lives in Chiba."
	h1 := HashValue(in)
	h2 := HashValue(in)
	if h1 != h2 {
		t.Errorf("HashValue not deterministic: %q vs %q", h1, h2)
	}
	if len(h1) != 64 {
		t.Errorf("HashValue length = %d, want 64 (hex sha256)", len(h1))
	}

	// CanonicalizeValue is idempotent.
	once := CanonicalizeValue(in)
	twice := CanonicalizeValue(once)
	if once != twice {
		t.Errorf("CanonicalizeValue not idempotent: %q → %q", once, twice)
	}
}

// TestNormalizeKey_FastPathSymmetry documents the contract the keynorm
// package promises to extract_keys.go and the query side: a stored
// memory_keys.key_norm and a query-side normalized form match exactly
// when their source text means the same thing.
func TestNormalizeKey_FastPathSymmetry(t *testing.T) {
	pairs := []struct {
		stored, queried string
	}{
		{"User lives in Chiba", "user lives in chiba."},
		{"用户住在千叶", "  用户住在千叶 ！"},
		{`she said "hello"`, "she said hello"},
		// Apostrophe-bearing strings normalize identically regardless of
		// which side of the lookup contributed the apostrophe.
		{"user's home", "user’s home"},
		{"ＵＳＥＲ ＬＩＶＥＳ", "user lives"},
	}
	for _, p := range pairs {
		s := NormalizeKey(p.stored)
		q := NormalizeKey(p.queried)
		if s != q {
			t.Errorf("fast-path symmetry broken: stored %q → %q, queried %q → %q", p.stored, s, p.queried, q)
		}
	}
}
