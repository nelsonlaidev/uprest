package upstash

import (
	"reflect"
	"strings"
	"testing"
)

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
