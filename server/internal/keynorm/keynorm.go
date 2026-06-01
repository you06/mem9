// Package keynorm normalizes K (key) text for the K=>V memory recall path.
//
// The same NormalizeKey function is applied on both sides of the KEY_EXACT
// fast path:
//   - extractor side: when extract_keys.go produces a K, it stores the
//     normalized form in memory_keys.key_norm
//   - query side: when a caller wants KEY_EXACT lookup, NormalizeKey is
//     applied to the user query before the WHERE key_norm = ? equality
//     check
//
// Keeping normalization in one place avoids the failure mode where extract
// and query disagree on what "normalized" means and KEY_EXACT silently
// misses what should have been a hit.
//
// Rules (frozen for V1):
//  1. Unicode NFKC
//  2. ASCII-lowercase
//  3. Strip a fixed punctuation set and REPLACE each with a single ASCII
//     space: . , ! ? ' " ， 。 ！ ？ 、 " " ' '
//     (Replace-with-space, not drop. Drop joins words across punctuation
//     boundaries — e.g. "苹果、橘子" would become "苹果橘子" which
//     collapses two distinct enum items into one token. Replace-with-space
//     keeps semantic separators intact.)
//  4. Fold runs of whitespace (including fullwidth) into a single ASCII space
//  5. Trim leading and trailing whitespace
//
// One consequence of replace-with-space: contractions split.
// "user's home" -> "user s home", not "users home". This is the safe
// failure mode: "user's home" and the plural "users home" normalize to
// distinct forms ("user s home" vs "users home"), so KEY_EXACT will not
// wrong-match across the apostrophe boundary.
//
// Explicitly NOT done:
//   - No stemming. KEY_EXACT is literal equality; stemming would make
//     "user lives" and "user living" collide which we don't want.
//   - No stop-word removal. Same reason.
//   - No removal of structural punctuation like - _ : / which often
//     carry meaning inside tokens ("rate-limit", "deploy:prod").
package keynorm

import (
	"strings"
	"unicode"

	"golang.org/x/text/unicode/norm"
)

// stripRunes is the set of punctuation characters removed during
// normalization. Kept as a map for O(1) lookup; the set is small enough
// that the allocation cost is negligible.
var stripRunes = map[rune]struct{}{
	// ASCII sentence punctuation.
	'.':  {},
	',':  {},
	'!':  {},
	'?':  {},
	'\'': {},
	'"':  {},

	// CJK fullwidth equivalents.
	'，': {},
	'。': {},
	'！': {},
	'？': {},
	'、': {},

	// Curly / typographic quotes that appear in copy-pasted text.
	'“': {}, // “
	'”': {}, // ”
	'‘': {}, // ‘
	'’': {}, // ’
}

// NormalizeKey converts a raw key surface (or a user query) into the
// canonical form stored in memory_keys.key_norm. It is safe to call
// repeatedly; NormalizeKey(NormalizeKey(s)) == NormalizeKey(s).
//
// An empty input returns an empty string.
func NormalizeKey(s string) string {
	if s == "" {
		return ""
	}

	// Step 1: Unicode NFKC. Folds fullwidth latin, ligatures, and
	// compatibility forms into their canonical compositions.
	s = norm.NFKC.String(s)

	// Step 2-4 in one pass: lowercase, strip-and-replace punctuation with
	// space, fold consecutive whitespace.
	var b strings.Builder
	b.Grow(len(s))
	prevSpace := true // treat leading whitespace as already-seen so we don't emit a leading space
	for _, r := range s {
		if _, drop := stripRunes[r]; drop {
			if !prevSpace {
				b.WriteRune(' ')
				prevSpace = true
			}
			continue
		}
		if unicode.IsSpace(r) {
			if !prevSpace {
				b.WriteRune(' ')
				prevSpace = true
			}
			continue
		}
		b.WriteRune(unicode.ToLower(r))
		prevSpace = false
	}

	// Step 5: trim trailing space if any (leading was suppressed above).
	return strings.TrimRight(b.String(), " ")
}
