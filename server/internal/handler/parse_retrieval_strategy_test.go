package handler

import (
	"errors"
	"testing"

	"github.com/qiffang/mnemos/server/internal/domain"
)

func TestParseRetrievalStrategy(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    uint8
		wantErr bool
	}{
		{"empty defaults to zero", "", 0, false},
		{"whitespace defaults to zero", "   ", 0, false},
		{"decimal default V1", "31", 0x1F, false},
		{"hex default V1", "0x1F", 0x1F, false},
		{"hex lowercase", "0x1f", 0x1F, false},
		{"hex pre-step-4.5 default", "0x0B", 0x0B, false},
		{"single bit KEY_EXACT", "0x01", 0x01, false},
		{"single bit VAL_VEC", "0x10", 0x10, false},
		{"zero parses to zero", "0", 0, false},

		// rejected values
		{"out-of-range bit 0x20", "0x20", 0, true},
		{"out-of-range decimal 64", "64", 0, true},
		{"out-of-range 255", "255", 0, true},
		{"negative number", "-1", 0, true},
		{"non-numeric", "abc", 0, true},
		{"non-numeric leading", "0xZZ", 0, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseRetrievalStrategy(tc.input)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("parseRetrievalStrategy(%q) = (%v, nil), want error", tc.input, got)
				}
				var ve *domain.ValidationError
				if !errors.As(err, &ve) {
					t.Errorf("err = %T (%v), want *domain.ValidationError so handler maps it to 400", err, err)
				} else if ve.Field != "retrieval_strategy" {
					t.Errorf("ValidationError.Field = %q, want %q", ve.Field, "retrieval_strategy")
				}
				return
			}
			if err != nil {
				t.Fatalf("parseRetrievalStrategy(%q) unexpected error: %v", tc.input, err)
			}
			if got != tc.want {
				t.Errorf("parseRetrievalStrategy(%q) = 0x%02X, want 0x%02X", tc.input, got, tc.want)
			}
		})
	}
}
