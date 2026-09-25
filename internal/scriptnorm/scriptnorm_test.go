package scriptnorm

import (
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
)

func TestNormalize(t *testing.T) {
	tests := []struct {
		name        string
		script      string
		want        string
		wantChanged bool
	}{
		{
			name:   "no shebang",
			script: "return ARGV[1]",
			want:   "return ARGV[1]",
		},
		{
			name:   "shebang after first line",
			script: "\n#!lua flags=allow-key-locking\nreturn 1",
			want:   "\n#!lua flags=allow-key-locking\nreturn 1",
		},
		{
			name:   "all supported flags",
			script: "#!lua flags=no-writes,allow-oom,allow-stale,no-cluster,allow-cross-slot-keys\nreturn 1",
			want:   "#!lua flags=no-writes,allow-oom,allow-stale,no-cluster,allow-cross-slot-keys\nreturn 1",
		},
		{
			name:        "mixed flags",
			script:      "#!lua flags=no-writes,allow-key-locking,allow-stale\nreturn 1",
			want:        "#!lua flags=no-writes,allow-stale\nreturn 1",
			wantChanged: true,
		},
		{
			name:        "only unknown flags",
			script:      "#!lua flags=allow-key-locking,FUTURE_flag\nreturn 1",
			want:        "return 1",
			wantChanged: true,
		},
		{
			name:        "CRLF preserved",
			script:      "#!lua flags=allow-oom,allow-key-locking\r\nreturn 1\r\n",
			want:        "#!lua flags=allow-oom\r\nreturn 1\r\n",
			wantChanged: true,
		},
		{
			name:        "no trailing newline",
			script:      "#!lua flags=no-writes,allow-key-locking",
			want:        "#!lua flags=no-writes",
			wantChanged: true,
		},
		{
			name:        "binary body preserved",
			script:      "#!lua flags=allow-key-locking\n\x00return '\u4e16\u754c'\r\n",
			want:        "\x00return '\u4e16\u754c'\r\n",
			wantChanged: true,
		},
		{
			name:   "empty flags are malformed",
			script: "#!lua flags=\nreturn 1",
			want:   "#!lua flags=\nreturn 1",
		},
		{
			name:   "empty flag is malformed",
			script: "#!lua flags=no-writes,\nreturn 1",
			want:   "#!lua flags=no-writes,\nreturn 1",
		},
		{
			name:   "whitespace is malformed",
			script: "#!lua flags=no-writes, allow-oom\nreturn 1",
			want:   "#!lua flags=no-writes, allow-oom\nreturn 1",
		},
		{
			name:   "different shebang is unchanged",
			script: "#!lua name=library\nreturn 1",
			want:   "#!lua name=library\nreturn 1",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, changed := Normalize(test.script)

			if got != test.want || changed != test.wantChanged {
				t.Fatalf("got (%q, %t), want (%q, %t)", got, changed, test.want, test.wantChanged)
			}
		})
	}
}

func TestNormalizeCommand(t *testing.T) {
	normalizer := New()
	original := "#!lua flags=allow-key-locking\nreturn ARGV[1]"
	normalized := "return ARGV[1]"

	eval := []any{"eval", original, "0", "value"}
	normalizer.NormalizeCommand(eval)

	if eval[1] != normalized {
		t.Fatalf("got script %q, want %q", eval[1], normalized)
	}

	evalSHA := []any{"EVALSHA", strings.ToUpper(scriptSHA(original)), "0"}
	normalizer.NormalizeCommand(evalSHA)

	if evalSHA[1] != scriptSHA(normalized) {
		t.Fatalf("got hash %q, want %q", evalSHA[1], scriptSHA(normalized))
	}

	scriptLoad := []any{"SCRIPT", "LOAD", "#!lua flags=no-writes,allow-key-locking\nreturn 1"}
	normalizer.NormalizeCommand(scriptLoad)

	if scriptLoad[2] != "#!lua flags=no-writes\nreturn 1" {
		t.Fatalf("unexpected SCRIPT LOAD command: %#v", scriptLoad)
	}

	evalRO := []any{"EVAL_RO", "#!lua flags=no-writes,allow-key-locking\nreturn 1", "0"}
	normalizer.NormalizeCommand(evalRO)

	if evalRO[1] != "#!lua flags=no-writes\nreturn 1" {
		t.Fatalf("unexpected EVAL_RO command: %#v", evalRO)
	}

	binaryEval := []any{"EVAL", []byte("#!lua flags=allow-key-locking\nreturn 1"), "0"}
	normalizer.NormalizeCommand(binaryEval)

	if !reflect.DeepEqual(binaryEval[1], []byte("return 1")) {
		t.Fatalf("unexpected binary EVAL command: %#v", binaryEval)
	}

	binaryEvalSHA := []any{"EVALSHA", []byte(strings.ToUpper(scriptSHA("#!lua flags=allow-key-locking\nreturn 1"))), "0"}
	normalizer.NormalizeCommand(binaryEvalSHA)

	if !reflect.DeepEqual(binaryEvalSHA[1], []byte(scriptSHA("return 1"))) {
		t.Fatalf("unexpected binary EVALSHA command: %#v", binaryEvalSHA)
	}

	unknownSHA := []any{"EVALSHA_RO", strings.Repeat("a", 40), "0"}
	normalizer.NormalizeCommand(unknownSHA)

	if unknownSHA[1] != strings.Repeat("a", 40) {
		t.Fatalf("unexpected unknown hash rewrite: %#v", unknownSHA)
	}

	fcall := []any{"FCALL", "function_name", "0"}
	wantFCall := append([]any(nil), fcall...)
	normalizer.NormalizeCommand(fcall)

	if !reflect.DeepEqual(fcall, wantFCall) {
		t.Fatalf("got %#v, want %#v", fcall, wantFCall)
	}

	fcallRO := []any{"FCALL_RO", "function_name", "0"}
	wantFCallRO := append([]any(nil), fcallRO...)
	normalizer.NormalizeCommand(fcallRO)

	if !reflect.DeepEqual(fcallRO, wantFCallRO) {
		t.Fatalf("got %#v, want %#v", fcallRO, wantFCallRO)
	}
}

func TestNormalizerBoundsAliases(t *testing.T) {
	normalizer := New()

	for i := range maxAliases + 1 {
		command := []any{"EVAL", "#!lua flags=allow-key-locking\nreturn " + strconv.Itoa(i), "0"}

		normalizer.NormalizeCommand(command)
	}

	if len(normalizer.aliases) != 1 {
		t.Fatalf("got %d aliases after rollover, want 1", len(normalizer.aliases))
	}
}

func TestNormalizerConcurrentUse(t *testing.T) {
	normalizer := New()
	waitGroup := sync.WaitGroup{}

	for i := range 64 {
		waitGroup.Add(1)

		go func() {
			defer waitGroup.Done()

			original := "#!lua flags=allow-key-locking\nreturn " + strconv.Itoa(i)
			normalized := "return " + strconv.Itoa(i)
			eval := []any{"EVAL", original, "0"}

			normalizer.NormalizeCommand(eval)

			evalSHA := []any{"EVALSHA", scriptSHA(original), "0"}

			normalizer.NormalizeCommand(evalSHA)

			if evalSHA[1] != scriptSHA(normalized) {
				t.Errorf("got hash %q, want %q", evalSHA[1], scriptSHA(normalized))
			}
		}()
	}

	waitGroup.Wait()
}
