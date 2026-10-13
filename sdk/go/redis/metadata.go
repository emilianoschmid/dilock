package redis

import (
	"bytes"
	"encoding/json"
)

// hasDuplicateTopLevelJSONField rejects ambiguous namespace metadata objects.
// Nested extension objects retain their own independent field scopes.
func hasDuplicateTopLevelJSONField(raw []byte) bool {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return false
	}
	seen := make(map[string]struct{})
	for decoder.More() {
		token, err = decoder.Token()
		if err != nil {
			return false
		}
		name, ok := token.(string)
		if !ok {
			return false
		}
		if _, exists := seen[name]; exists {
			return true
		}
		seen[name] = struct{}{}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return false
		}
	}
	return false
}
