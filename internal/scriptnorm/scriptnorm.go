package scriptnorm

import (
	"crypto/sha1" //nolint:gosec // G505: Redis script cache IDs are SHA-1
	"encoding/hex"
	"strings"
	"sync"
)

const shebangPrefix = "#!lua flags="
const maxAliases = 1024

var supportedFlags = map[string]struct{}{
	"no-writes":             {},
	"allow-oom":             {},
	"allow-stale":           {},
	"no-cluster":            {},
	"allow-cross-slot-keys": {},
}

type Normalizer struct {
	mu      sync.RWMutex
	aliases map[string]string
}

func New() *Normalizer {
	return &Normalizer{
		aliases: make(map[string]string),
	}
}

// Normalize removes unsupported flags from a leading Redis Lua shebang.
func Normalize(script string) (string, bool) {
	if !strings.HasPrefix(script, shebangPrefix) {
		return script, false
	}

	line, remainder, hasLineEnding := strings.Cut(script, "\n")
	lineEnding := ""

	if hasLineEnding {
		lineEnding = "\n"

		var hasCarriageReturn bool

		line, hasCarriageReturn = strings.CutSuffix(line, "\r")

		if hasCarriageReturn {
			lineEnding = "\r\n"
		}
	}

	flags := strings.Split(strings.TrimPrefix(line, shebangPrefix), ",")

	for _, flag := range flags {
		if !validFlagName(flag) {
			return script, false
		}
	}

	kept := make([]string, 0, len(flags))

	for _, flag := range flags {
		if _, ok := supportedFlags[flag]; ok {
			kept = append(kept, flag)
		}
	}

	if len(kept) == len(flags) {
		return script, false
	}

	if len(kept) == 0 {
		return remainder, true
	}

	return shebangPrefix + strings.Join(kept, ",") + lineEnding + remainder, true
}

// NormalizeCommand mutates script arguments and resolves hashes for scripts it changed.
func (n *Normalizer) NormalizeCommand(command []any) bool {
	if len(command) == 0 {
		return false
	}

	name, ok := command[0].(string)

	if !ok {
		return false
	}

	switch strings.ToUpper(name) {
	case "EVAL", "EVAL_RO":
		return n.normalizeScriptArgument(command, 1)

	case "EVALSHA", "EVALSHA_RO":
		return n.normalizeSHAArgument(command, 1)

	case "SCRIPT":
		if len(command) <= 1 {
			return false
		}

		subcommand, ok := command[1].(string)

		if ok && strings.EqualFold(subcommand, "LOAD") {
			return n.normalizeScriptArgument(command, 2)
		}
	}

	return false
}

func (n *Normalizer) normalizeScriptArgument(command []any, index int) bool {
	if len(command) <= index {
		return false
	}

	var script string
	var binary bool

	switch value := command[index].(type) {
	case string:
		script = value

	case []byte:
		script = string(value)
		binary = true

	default:
		return false
	}

	normalized, changed := Normalize(script)

	if !changed {
		return false
	}

	n.rememberAlias(scriptSHA(script), scriptSHA(normalized))

	if binary {
		command[index] = []byte(normalized)
	} else {
		command[index] = normalized
	}

	return true
}

func (n *Normalizer) normalizeSHAArgument(command []any, index int) bool {
	if len(command) <= index {
		return false
	}

	var hash string
	var binary bool

	switch value := command[index].(type) {
	case string:
		hash = value

	case []byte:
		hash = string(value)
		binary = true

	default:
		return false
	}

	n.mu.RLock()
	normalized, ok := n.aliases[strings.ToLower(hash)]
	n.mu.RUnlock()

	if !ok {
		return false
	}

	if binary {
		command[index] = []byte(normalized)
	} else {
		command[index] = normalized
	}

	return true
}

func (n *Normalizer) rememberAlias(original string, normalized string) {
	n.mu.Lock()

	defer n.mu.Unlock()

	if len(n.aliases) >= maxAliases {
		clear(n.aliases)
	}

	n.aliases[original] = normalized
}

func validFlagName(flag string) bool {
	if flag == "" {
		return false
	}

	for _, character := range flag {
		lowercase := character >= 'a' && character <= 'z'
		uppercase := character >= 'A' && character <= 'Z'
		digit := character >= '0' && character <= '9'

		if !lowercase && !uppercase && !digit && character != '-' && character != '_' {
			return false
		}
	}

	return true
}

func scriptSHA(script string) string {
	// Redis defines script cache identifiers as SHA-1 digests.
	digest := sha1.Sum([]byte(script)) //nolint:gosec // G401: Redis requires SHA-1 script digests

	return hex.EncodeToString(digest[:])
}
