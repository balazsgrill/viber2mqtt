package main

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"
)

// renderMessage fills a notification message template.
// JSON payloads resolve {field} / {nested.field}; raw payloads only {payload}.
// Missing fields become "" (with a warning for named fields).
func renderMessage(template string, payload []byte, notifID string) string {
	trimmed := strings.TrimSpace(string(payload))
	var doc map[string]any
	if json.Unmarshal([]byte(trimmed), &doc) == nil {
		return renderJSON(template, doc, notifID)
	}
	var val any
	if json.Unmarshal([]byte(trimmed), &val) == nil {
		// valid JSON scalar/array: expose only {payload}
		trimmed = string(payload)
		if strings.Contains(template, "{payload}") {
			return strings.ReplaceAll(template, "{payload}", valAsString(val))
		}
		log.Printf("notify %s: payload is JSON %s but template uses named placeholders; left as-is", notifID, classify(val))
		return template
	}
	// not JSON at all
	if strings.Contains(template, "{payload}") {
		return strings.ReplaceAll(template, "{payload}", string(payload))
	}
	log.Printf("notify %s: payload is not JSON; template placeholders left as literals", notifID)
	return template
}

func renderJSON(template string, doc map[string]any, notifID string) string {
	return replacePlaceholders(template, func(field string) (string, bool) {
		v, ok := lookupPath(doc, field)
		if !ok {
			log.Printf("notify %s: field %q missing from payload", notifID, field)
			return "", true // missing field -> empty string
		}
		return valAsString(v), true
	})
}

// replacePlaceholders substitutes {name} tokens. Unknown names are left as literals.
func replacePlaceholders(template string, resolve func(name string) (string, bool)) string {
	var sb strings.Builder
	i := 0
	for i < len(template) {
		open := strings.IndexByte(template[i:], '{')
		if open < 0 {
			sb.WriteString(template[i:])
			break
		}
		open += i
		sb.WriteString(template[i:open])
		close := strings.IndexByte(template[open:], '}')
		if close < 0 {
			sb.WriteString(template[open:])
			break
		}
		close += open
		name := template[open+1 : close]
		if !isPlaceholderName(name) {
			sb.WriteString(template[open : close+1])
			i = close + 1
			continue
		}
		if val, ok := resolve(name); ok {
			sb.WriteString(val)
		} else {
			sb.WriteString(template[open : close+1])
		}
		i = close + 1
	}
	return sb.String()
}

// isPlaceholderName: identifier chars and dots, no spaces, not empty.
func isPlaceholderName(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		switch {
		case r == '.':
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '_':
		default:
			return false
		}
	}
	return true
}

func lookupPath(doc map[string]any, field string) (any, bool) {
	var cur any = doc
	for _, part := range strings.Split(field, ".") {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil, false
		}
		cur, ok = m[part]
		if !ok {
			return nil, false
		}
	}
	return cur, true
}

func valAsString(v any) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	case bool:
		if t {
			return "true"
		}
		return "false"
	case float64:
		if t == float64(int64(t)) {
			return strings.TrimSuffix(fmt.Sprintf("%.1f", t), ".0")
		}
		return fmt.Sprintf("%v", t)
	default:
		b, _ := json.Marshal(t)
		return string(b)
	}
}

func classify(v any) string {
	switch v.(type) {
	case string:
		return "string"
	case float64:
		return "number"
	case bool:
		return "bool"
	case []any:
		return "array"
	default:
		return "object"
	}
}
