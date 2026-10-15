package upstash

import (
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestValidateCommandRejectsConnectionStateChanges(t *testing.T) {
	for _, name := range []string{
		"AUTH", "HELLO", "SELECT", "QUIT", "RESET", "READONLY", "READWRITE", "ASKING",
		"MULTI", "EXEC", "DISCARD", "WATCH", "UNWATCH",
		"SUBSCRIBE", "PSUBSCRIBE", "SSUBSCRIBE", "UNSUBSCRIBE", "PUNSUBSCRIBE", "SUNSUBSCRIBE", "MONITOR",
		"SYNC", "PSYNC", "REPLCONF", "HIMPORT",
	} {
		t.Run(name, func(t *testing.T) {
			for _, spelling := range []string{name, strings.ToLower(name)} {
				command := []any{spelling, "private argument"}

				if err := ValidateCommand(command); !errors.Is(err, ErrUnsupportedCommand) {
					t.Fatalf("got %v for %q, want unsupported command", err, spelling)
				}

				if !reflect.DeepEqual(command, []any{spelling, "private argument"}) {
					t.Fatalf("validation changed command: %#v", command)
				}
			}
		})
	}
}

func TestValidateCommand(t *testing.T) {
	for _, test := range []struct {
		name      string
		command   []any
		wantError error
	}{
		{"empty command", nil, ErrInvalidCommand},
		{"empty name", []any{""}, ErrInvalidCommand},
		{"non-string name", []any{1}, ErrInvalidCommand},
		{"missing CLIENT subcommand", []any{"CLIENT"}, ErrUnsupportedCommand},
		{"empty CLIENT subcommand", []any{"CLIENT", ""}, ErrUnsupportedCommand},
		{"non-string CLIENT subcommand", []any{"CLIENT", 1}, ErrUnsupportedCommand},
		{"unknown CLIENT subcommand", []any{"CLIENT", "FUTURE"}, ErrUnsupportedCommand},
		{"CLIENT REPLY", []any{"cLiEnT", "rEpLy", "OFF"}, ErrUnsupportedCommand},
		{"CLIENT TRACKING", []any{"CLIENT", "TRACKING", "ON"}, ErrUnsupportedCommand},
		{"CLIENT SETNAME", []any{"CLIENT", "SETNAME", "name"}, ErrUnsupportedCommand},
		{"CLIENT SETINFO", []any{"CLIENT", "SETINFO", "LIB-NAME", "name"}, ErrUnsupportedCommand},
		{"CLIENT KILL", []any{"CLIENT", "KILL", "ID", "1"}, ErrUnsupportedCommand},
		{"CLIENT PAUSE", []any{"CLIENT", "PAUSE", "1"}, ErrUnsupportedCommand},
		{"CLIENT body subcommand", []any{"CLIENT", []byte("rEpLy"), "OFF"}, ErrUnsupportedCommand},
		{"SCRIPT DEBUG YES", []any{"SCRIPT", "DEBUG", "YES"}, ErrUnsupportedCommand},
		{"SCRIPT DEBUG NO", []any{"sCrIpT", "dEbUg", "NO"}, ErrUnsupportedCommand},
		{"SCRIPT DEBUG SYNC", []any{"SCRIPT", "DEBUG", "SYNC"}, ErrUnsupportedCommand},
		{"SCRIPT DEBUG missing mode", []any{"SCRIPT", "DEBUG"}, ErrUnsupportedCommand},
		{"SCRIPT DEBUG body subcommand", []any{"SCRIPT", []byte("dEbUg"), "YES"}, ErrUnsupportedCommand},
		{"PING", []any{"PING"}, nil},
		{"ECHO", []any{"ECHO", "AUTH"}, nil},
		{"command word as key", []any{"GET", "SELECT"}, nil},
		{"command word as value", []any{"SET", "key", "CLIENT REPLY OFF"}, nil},
		{"Lua contents", []any{"EVAL", "return 'MONITOR'", "0"}, nil},
		{"read-only Lua", []any{"EVAL_RO", "return 'SELECT'", "0"}, nil},
		{"EVALSHA", []any{"EVALSHA", "hash", "0"}, nil},
		{"EVALSHA_RO", []any{"EVALSHA_RO", "hash", "0"}, nil},
		{"FCALL", []any{"FCALL", "name", "0"}, nil},
		{"FCALL_RO", []any{"FCALL_RO", "name", "0"}, nil},
		{"SCRIPT LOAD", []any{"SCRIPT", "LOAD", "return 'DEBUG'"}, nil},
		{"SCRIPT EXISTS", []any{"SCRIPT", "EXISTS", "hash"}, nil},
		{"missing SCRIPT subcommand", []any{"SCRIPT"}, nil},
		{"non-string SCRIPT subcommand", []any{"SCRIPT", 1}, nil},
		{"PUBLISH", []any{"PUBLISH", "channel", "SUBSCRIBE"}, nil},
		{"PUBSUB", []any{"PUBSUB", "CHANNELS"}, nil},
		{"blocking command", []any{"BLPOP", "key", "0"}, nil},
		{"blocking stream", []any{"XREAD", "BLOCK", "0", "STREAMS", "key", "$"}, nil},
		{"cleanup", []any{"FLUSHDB"}, nil},
		{"administration", []any{"CONFIG", "GET", "timeout"}, nil},
		{"unknown command", []any{"FUTURE.COMMAND", "AUTH"}, nil},
		{"CLIENT query body subcommand", []any{"CLIENT", []byte("gEtNaMe")}, nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			before := append([]any(nil), test.command...)

			if err := ValidateCommand(test.command); !errors.Is(err, test.wantError) {
				t.Fatalf("got %v, want %v", err, test.wantError)
			}

			if !reflect.DeepEqual(test.command, before) {
				t.Fatalf("validation changed command: %#v", test.command)
			}
		})
	}

	for _, subcommand := range []string{"GETNAME", "GETREDIR", "ID", "INFO", "LIST", "TRACKINGINFO", "HELP"} {
		t.Run("CLIENT "+subcommand, func(t *testing.T) {
			if err := ValidateCommand([]any{"cLiEnT", strings.ToLower(subcommand)}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestDecodeCommand(t *testing.T) {
	tests := []struct {
		name string
		body string
		want []any
	}{
		{"string arguments", `["SET","a","1"]`, []any{"SET", "a", "1"}},
		{"numeric argument", `["SETEX","a",100,"value"]`, []any{"SETEX", "a", "100", "value"}},
		{"boolean argument", `["SET","a",true]`, []any{"SET", "a", "true"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := DecodeCommand(strings.NewReader(test.body))

			if err != nil {
				t.Fatal(err)
			}

			if !reflect.DeepEqual(got, test.want) {
				t.Fatalf("got %#v, want %#v", got, test.want)
			}
		})
	}
}

func TestDecodeCommandRejectsInvalidInput(t *testing.T) {
	for _, body := range []string{
		``,
		`{}`,
		`[]`,
		`[1,"a"]`,
		`["","a"]`,
		`["SET","a",null]`,
		`["SET","a",{}]`,
		`["SET","a",[]]`,
		`["PING"] ["PING"]`,
	} {
		t.Run(body, func(t *testing.T) {
			if _, err := DecodeCommand(strings.NewReader(body)); err == nil {
				t.Fatal("expected a command error")
			}
		})
	}
}

func TestDecodePipeline(t *testing.T) {
	got, err := DecodePipeline(strings.NewReader(`[["SET","a",1],["GET","a"],["EXPIRE","a",true]]`))

	if err != nil {
		t.Fatal(err)
	}

	want := [][]any{{"SET", "a", "1"}, {"GET", "a"}, {"EXPIRE", "a", "true"}}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v, want %#v", got, want)
	}
}

func TestDecodePipelineRejectsInvalidInput(t *testing.T) {
	for _, body := range []string{
		``,
		`{}`,
		`null`,
		`[]`,
		`[[]]`,
		`[["GET","a"],null]`,
		`[["SET","a",{}]]`,
		`[["PING"]][["PING"]]`,
	} {
		t.Run(body, func(t *testing.T) {
			if _, err := DecodePipeline(strings.NewReader(body)); err == nil {
				t.Fatal("expected a pipeline error")
			}
		})
	}
}

func TestRewriteScanWithType(t *testing.T) {
	tests := []struct {
		name    string
		command []any
		want    []any
	}{
		{
			name:    "minimal command",
			command: []any{"SCAN", "0", "WITHTYPE"},
			want:    []any{"EVAL", scanWithTypeScript, 0, "0"},
		},
		{
			name:    "case insensitive options",
			command: []any{"scan", "42", "match", "prefix:*", "COUNT", "10", "type", "string", "withtype"},
			want:    []any{"EVAL", scanWithTypeScript, 0, "42", "match", "prefix:*", "COUNT", "10", "type", "string"},
		},
		{
			name:    "with type before standard options",
			command: []any{"SCAN", "0", "WITHTYPE", "MATCH", "prefix:*", "COUNT", "100"},
			want:    []any{"EVAL", scanWithTypeScript, 0, "0", "MATCH", "prefix:*", "COUNT", "100"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			original := append([]any(nil), test.command...)
			got, changed := RewriteScanWithType(test.command)

			if !changed {
				t.Fatal("expected command rewrite")
			}

			if !reflect.DeepEqual(got, test.want) {
				t.Fatalf("got %#v, want %#v", got, test.want)
			}

			if !reflect.DeepEqual(test.command, original) {
				t.Fatalf("input command changed: got %#v, want %#v", test.command, original)
			}
		})
	}
}

func TestRewriteScanWithTypeLeavesUnsupportedCommandsUnchanged(t *testing.T) {
	tests := []struct {
		name    string
		command []any
	}{
		{"standard scan", []any{"SCAN", "0", "COUNT", "10"}},
		{"different command", []any{"GET", "WITHTYPE"}},
		{"match value", []any{"SCAN", "0", "MATCH", "WITHTYPE"}},
		{"count value", []any{"SCAN", "0", "COUNT", "WITHTYPE"}},
		{"type value", []any{"SCAN", "0", "TYPE", "WITHTYPE"}},
		{"duplicate with type", []any{"SCAN", "0", "WITHTYPE", "WITHTYPE"}},
		{"missing option value", []any{"SCAN", "0", "WITHTYPE", "MATCH"}},
		{"unknown option", []any{"SCAN", "0", "WITHTYPE", "UNKNOWN"}},
		{"non-string option", []any{"SCAN", "0", "WITHTYPE", 1}},
		{"non-string command", []any{[]byte("SCAN"), "0", "WITHTYPE"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			original := append([]any(nil), test.command...)
			got, changed := RewriteScanWithType(test.command)

			if changed {
				t.Fatal("unexpected command rewrite")
			}

			if !reflect.DeepEqual(got, original) || !reflect.DeepEqual(test.command, original) {
				t.Fatalf("command changed: got %#v, input %#v, want %#v", got, test.command, original)
			}
		})
	}
}

func TestDecodePathCommand(t *testing.T) {
	tests := []struct {
		name  string
		path  string
		query string
		body  []byte
		want  []any
	}{
		{
			name: "URL-decoded arguments",
			path: "/set/hello%20world/a%2Fb",
			want: []any{"set", "hello world", "a/b"},
		},
		{
			name: "empty trailing argument",
			path: "/set/key/",
			want: []any{"set", "key", ""},
		},
		{
			name: "empty POST body argument",
			path: "/set/key",
			body: []byte{},
			want: []any{"set", "key", []byte{}},
		},
		{
			name:  "raw body before ordered query arguments",
			path:  "/set/key",
			query: "EX=100&NX",
			body:  []byte{0x00, 0xff},
			want:  []any{"set", "key", []byte{0x00, 0xff}, "EX", "100", "NX"},
		},
		{
			name:  "query decoding",
			path:  "/echo",
			query: "hello+world=a%2Fb",
			want:  []any{"echo", "hello world", "a/b"},
		},
		{
			name:  "query token is not a command argument",
			path:  "/get/key",
			query: "_token=secret",
			want:  []any{"get", "key"},
		},
		{
			name:  "query options retain order around token",
			path:  "/set/key/value",
			query: "EX=100&_token=secret&NX",
			want:  []any{"set", "key", "value", "EX", "100", "NX"},
		},
		{
			name:  "encoded and repeated query token",
			path:  "/get/key",
			query: "%5Ftoken=first&_token=second",
			want:  []any{"get", "key"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := DecodePathCommand(test.path, test.query, test.body)

			if err != nil {
				t.Fatal(err)
			}

			if !reflect.DeepEqual(got, test.want) {
				t.Fatalf("got %#v, want %#v", got, test.want)
			}
		})
	}
}

func TestDecodePathCommandRejectsInvalidInput(t *testing.T) {
	for _, path := range []string{"", "/", "ping", "/get/%zz"} {
		t.Run(path, func(t *testing.T) {
			if _, err := DecodePathCommand(path, "", nil); err == nil {
				t.Fatal("expected a path command error")
			}
		})
	}

	if _, err := DecodePathCommand("/get/key", "bad=%zz", nil); err == nil {
		t.Fatal("expected a query argument error")
	}
}
