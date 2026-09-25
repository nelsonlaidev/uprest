package upstash

import (
	"reflect"
	"testing"
)

func TestEncodeBase64Result(t *testing.T) {
	value := []any{"hello", "OK", nil, int64(3), []any{"world", ""}}
	want := []any{"aGVsbG8=", "T0s=", nil, int64(3), []any{"d29ybGQ=", ""}}

	if got := EncodeBase64Result(value); !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v, want %#v", got, want)
	}

	if got := EncodeBase64Result("OK"); got != "OK" {
		t.Fatalf("got %#v, want OK", got)
	}
}
