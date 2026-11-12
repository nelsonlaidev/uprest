package upstash

import "strings"

// Redis rejects COUNT 0 before looking up the stream's group. Let it validate
// the original command (including ACLs, idle time and start ID), then use a
// read-only group lookup before returning Upstash's empty claim result.
// Never substitute a positive count: that could claim or delete pending entries.
const xautoClaimZeroCountScript = `local result = redis.pcall("XAUTOCLAIM", KEYS[1], unpack(ARGV))
if result.err ~= "ERR COUNT must be > 0" then
  return result
end

local pending = redis.pcall("XPENDING", KEYS[1], ARGV[1])
if pending.err then
  return pending
end

return {"0-0", {}, {}}`

// RewriteXAutoClaimZeroCount implements Upstash's COUNT 0 no-op. Only one
// canonical COUNT 0 option, with optional JUSTID, is translated; other commands
// retain Redis's validation and execution behavior.
func RewriteXAutoClaimZeroCount(command []any) ([]any, bool) {
	if len(command) < 8 || !strings.EqualFold(xautoClaimArgument(command[0]), "XAUTOCLAIM") {
		return command, false
	}

	zeroCount := false

	for index := 6; index < len(command); index++ {
		switch strings.ToUpper(xautoClaimArgument(command[index])) {
		case "COUNT":
			if zeroCount || index+1 >= len(command) || xautoClaimArgument(command[index+1]) != "0" {
				return command, false
			}

			zeroCount = true
			index++

		case "JUSTID":
		default:
			return command, false
		}
	}

	if !zeroCount {
		return command, false
	}

	rewritten := make([]any, 0, len(command)+2)
	rewritten = append(rewritten, "EVAL", xautoClaimZeroCountScript, 1, command[1])
	rewritten = append(rewritten, command[2:]...)

	return rewritten, true
}

func xautoClaimArgument(value any) string {
	switch argument := value.(type) {
	case string:
		return argument
	case []byte:
		return string(argument)
	default:
		return ""
	}
}
