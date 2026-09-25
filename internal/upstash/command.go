package upstash

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/url"
	"strconv"
	"strings"
)

var ErrInvalidCommand = errors.New("command must be a JSON array beginning with a command name and containing only scalar arguments")
var ErrInvalidPipeline = errors.New("pipeline must be a non-empty JSON array of commands")
var ErrInvalidPathCommand = errors.New("path must contain a command and valid URL-encoded arguments")

func DecodeCommand(body io.Reader) ([]any, error) {
	decoder := json.NewDecoder(body)
	decoder.UseNumber()

	var values []any

	if err := decoder.Decode(&values); err != nil {
		return nil, err
	}

	var extra any

	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return nil, ErrInvalidCommand
		}

		return nil, err
	}

	if len(values) == 0 {
		return nil, ErrInvalidCommand
	}

	name, ok := values[0].(string)

	if !ok || name == "" {
		return nil, ErrInvalidCommand
	}

	for i, value := range values {
		switch v := value.(type) {
		case string:
		case json.Number:
			values[i] = v.String()
		case bool:
			values[i] = strconv.FormatBool(v)
		default:
			return nil, ErrInvalidCommand
		}
	}

	return values, nil
}

func DecodePipeline(body io.Reader) ([][]any, error) {
	decoder := json.NewDecoder(body)

	var rows []json.RawMessage

	if err := decoder.Decode(&rows); err != nil {
		return nil, err
	}

	var extra any

	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return nil, ErrInvalidPipeline
		}

		return nil, err
	}

	if len(rows) == 0 {
		return nil, ErrInvalidPipeline
	}

	commands := make([][]any, len(rows))

	for i, row := range rows {
		command, err := DecodeCommand(bytes.NewReader(row))

		if err != nil {
			return nil, ErrInvalidPipeline
		}

		commands[i] = command
	}

	return commands, nil
}

// DecodePathCommand converts an escaped URL path, optional body, and query
// parameters into Redis command arguments. A non-nil body is inserted before
// query arguments, matching the Upstash REST API path-command contract.
// The _token authentication parameter is not a command argument.
func DecodePathCommand(escapedPath string, rawQuery string, body []byte) ([]any, error) {
	if !strings.HasPrefix(escapedPath, "/") {
		return nil, ErrInvalidPathCommand
	}

	escapedSegments := strings.Split(strings.TrimPrefix(escapedPath, "/"), "/")

	if len(escapedSegments) == 0 || escapedSegments[0] == "" {
		return nil, ErrInvalidPathCommand
	}

	command := make([]any, 0, len(escapedSegments)+1)

	for _, escapedSegment := range escapedSegments {
		segment, err := url.PathUnescape(escapedSegment)

		if err != nil {
			return nil, ErrInvalidPathCommand
		}

		command = append(command, segment)
	}

	if body != nil {
		command = append(command, body)
	}

	if rawQuery == "" {
		return command, nil
	}

	for field := range strings.SplitSeq(rawQuery, "&") {
		if field == "" {
			continue
		}

		escapedName, escapedValue, hasValue := strings.Cut(field, "=")
		name, err := url.QueryUnescape(escapedName)

		if err != nil {
			return nil, ErrInvalidPathCommand
		}

		if name == "_token" {
			continue
		}

		command = append(command, name)

		if hasValue {
			value, err := url.QueryUnescape(escapedValue)

			if err != nil {
				return nil, ErrInvalidPathCommand
			}

			command = append(command, value)
		}
	}

	return command, nil
}
