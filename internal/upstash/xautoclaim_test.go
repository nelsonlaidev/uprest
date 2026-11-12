package upstash

import (
	"reflect"
	"testing"
)

func TestRewriteXAutoClaimZeroCount(t *testing.T) {
	for _, test := range []struct {
		name    string
		options []any
		changed bool
	}{
		{"zero count", []any{"COUNT", "0"}, true},
		{"case insensitive", []any{"count", "0", "justid"}, true},
		{"JUSTID first", []any{"JUSTID", "COUNT", "0"}, true},
		{"POST body count", []any{"COUNT", []byte("0")}, true},
		{"POST body option", []any{"COUNT", "0", []byte("JUSTID")}, true},
		{"default count", nil, false},
		{"default count JUSTID", []any{"JUSTID"}, false},
		{"positive count", []any{"COUNT", "1"}, false},
		{"negative count", []any{"COUNT", "-1"}, false},
		{"invalid integer", []any{"COUNT", "0.0"}, false},
		{"noncanonical integer", []any{"COUNT", "00"}, false},
		{"missing count", []any{"JUSTID", "COUNT"}, false},
		{"unknown option", []any{"COUNT", "0", "UNKNOWN"}, false},
		{"invalid option type", []any{"COUNT", "0", nil}, false},
		{"count override", []any{"COUNT", "0", "COUNT", "1"}, false},
		{"duplicate count", []any{"COUNT", "0", "COUNT", "0"}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			// Option-looking key/group/consumer names must remain literal arguments.
			command := append([]any{"xautoclaim", "COUNT", "JUSTID", "COUNT", "0", "0-0"}, test.options...)
			original := append([]any(nil), command...)
			got, changed := RewriteXAutoClaimZeroCount(command)
			want := original

			if test.changed {
				want = append([]any{"EVAL", xautoClaimZeroCountScript, 1}, command[1:]...)
			}

			if changed != test.changed || !reflect.DeepEqual(got, want) {
				t.Fatalf("got %#v, changed=%v; want %#v, changed=%v", got, changed, want, test.changed)
			}

			if !reflect.DeepEqual(command, original) {
				t.Fatal("input command changed")
			}
		})
	}

	for _, command := range [][]any{
		nil,
		{"XAUTOCLAIM"},
		{"XCLAIM", "key", "group", "consumer", "0", "0-0", "COUNT", "0"},
	} {
		if got, changed := RewriteXAutoClaimZeroCount(command); changed || !reflect.DeepEqual(got, command) {
			t.Fatalf("unexpected rewrite of %#v: %#v", command, got)
		}
	}
}
