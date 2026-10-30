package wideevent

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNestFields(t *testing.T) {
	testCases := []struct {
		name   string
		fields []Field
		want   map[string]any
	}{
		{
			name:   "flat key",
			fields: []Field{{"status", 200}},
			want:   map[string]any{"status": 200},
		},
		{
			name:   "keys sharing a prefix",
			fields: []Field{{"db.operation", "update"}, {"db.rows", 3}, {"name", "job"}},
			want:   map[string]any{"db": map[string]any{"operation": "update", "rows": 3}, "name": "job"},
		},
		{
			name:   "four levels",
			fields: []Field{{"a.b.c.d", 1}},
			want:   map[string]any{"a": map[string]any{"b": map[string]any{"c": map[string]any{"d": 1}}}},
		},
		{
			name:   "value before a longer key keeps both",
			fields: []Field{{"db", "pg"}, {"db.operation", "update"}},
			want:   map[string]any{"db": map[string]any{"_value": "pg", "operation": "update"}},
		},
		{
			name:   "value after a longer key keeps both",
			fields: []Field{{"db.operation", "update"}, {"db", "pg"}},
			want:   map[string]any{"db": map[string]any{"_value": "pg", "operation": "update"}},
		},
		{
			name:   "conflict in the middle of a path",
			fields: []Field{{"a.b", "x"}, {"a.b.c", "y"}},
			want:   map[string]any{"a": map[string]any{"b": map[string]any{"_value": "x", "c": "y"}}},
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			require.Equal(t, testCase.want, nestFields(testCase.fields))
		})
	}
}

func TestNestFieldsLeavesAMapValueUntouched(t *testing.T) {
	callerMap := map[string]any{"kept": true}

	nested := nestFields([]Field{{"labels", callerMap}, {"labels.added", "x"}})

	require.Equal(t, map[string]any{"kept": true}, callerMap)
	require.Equal(t, map[string]any{"_value": callerMap, "added": "x"}, nested["labels"])
}
