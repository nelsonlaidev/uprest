package upstash

import "encoding/base64"

func EncodeBase64Result(value any) any {
	switch result := value.(type) {
	case string:
		if result == "OK" {
			return result
		}

		return base64.StdEncoding.EncodeToString([]byte(result))

	case []any:
		encoded := make([]any, len(result))

		for i, item := range result {
			if value, ok := item.(string); ok {
				encoded[i] = base64.StdEncoding.EncodeToString([]byte(value))
			} else {
				encoded[i] = EncodeBase64Result(item)
			}
		}

		return encoded

	default:
		return value
	}
}
