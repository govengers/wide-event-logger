package wideevent

import "strings"

// conflictValueKey holds a value whose key is also the prefix of other keys: "db" = "pg" next to
// "db.operation" = "update" becomes {"db": {"_value": "pg", "operation": "update"}}.
const conflictValueKey = "_value"

// object is a map nestFields created itself. Its own type keeps a map[string]any the caller set as a
// value apart, so nesting never writes into the caller's map.
type object map[string]any

// nestFields expands dot-separated keys into nested maps: "db.operation" = "update" becomes
// {"db": {"operation": "update"}}. No value is lost when a key is also a prefix of other keys, and the
// result does not depend on the order of the fields.
func nestFields(fields []Field) map[string]any {
	root := object{}
	for _, field := range fields {
		insertNested(root, strings.Split(field.Key, "."), field.Value)
	}
	return plainMaps(root)
}

func insertNested(node object, path []string, value any) {
	for _, segment := range path[:len(path)-1] {
		node = childObject(node, segment)
	}
	leaf := path[len(path)-1]
	if existing, isObject := node[leaf].(object); isObject {
		existing[conflictValueKey] = value
		return
	}
	node[leaf] = value
}

// childObject returns the object under key, creating it. A value already stored under key moves into the
// new object as its conflictValueKey.
func childObject(node object, key string) object {
	existing, present := node[key]
	if child, isObject := existing.(object); isObject {
		return child
	}
	child := object{}
	if present {
		child[conflictValueKey] = existing
	}
	node[key] = child
	return child
}

// plainMaps converts every object into a map[string]any, the type FieldsMap documents.
func plainMaps(node object) map[string]any {
	result := make(map[string]any, len(node))
	for key, value := range node {
		if child, isObject := value.(object); isObject {
			result[key] = plainMaps(child)
			continue
		}
		result[key] = value
	}
	return result
}
