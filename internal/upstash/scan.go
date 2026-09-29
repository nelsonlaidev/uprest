package upstash

import "strings"

const scanWithTypeScript = `local result = redis.call("SCAN", unpack(ARGV))
local typed = {}

for _, key in ipairs(result[2]) do
  typed[#typed + 1] = key
  typed[#typed + 1] = redis.call("TYPE", key)["ok"]
end

return {result[1], typed}`

// RewriteScanWithType converts Upstash's SCAN WITHTYPE extension into a
// parameterized Redis script. The returned command replaces the input when
// changed; malformed or standard SCAN commands are returned unchanged.
func RewriteScanWithType(command []any) ([]any, bool) {
	if len(command) < 3 {
		return command, false
	}

	name, ok := command[0].(string)

	if !ok || !strings.EqualFold(name, "SCAN") {
		return command, false
	}

	arguments := make([]any, 0, len(command)-2)
	arguments = append(arguments, command[1])
	withType := false

	for index := 2; index < len(command); {
		option, ok := command[index].(string)

		if !ok {
			return command, false
		}

		switch {
		case strings.EqualFold(option, "WITHTYPE"):
			if withType {
				return command, false
			}

			withType = true
			index++

		case strings.EqualFold(option, "MATCH"), strings.EqualFold(option, "COUNT"), strings.EqualFold(option, "TYPE"):
			if index+1 >= len(command) {
				return command, false
			}

			arguments = append(arguments, command[index], command[index+1])
			index += 2

		default:
			return command, false
		}
	}

	if !withType {
		return command, false
	}

	rewritten := make([]any, 0, len(arguments)+3)
	rewritten = append(rewritten, "EVAL", scanWithTypeScript, 0)
	rewritten = append(rewritten, arguments...)

	return rewritten, true
}
