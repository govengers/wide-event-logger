package postgresadapter

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWithoutNullEscapesReplacesOnlyRealEscapes(t *testing.T) {
	for input, expected := range map[string]string{
		`{"a":"x"}`:             `{"a":"x"}`,
		`{"a":"\u0000"}`:        `{"a":"�"}`,
		`{"a":"\\u0000"}`:       `{"a":"\\u0000"}`, // a backslash, then the text u0000
		`{"a":"\\\u0000"}`:      `{"a":"\\�"}`,
		`{"\u0000":"\"\u0000"}`: `{"�":"\"�"}`,
	} {
		output := withoutNullEscapes([]byte(input))
		require.Equal(t, expected, string(output), input)
		require.True(t, json.Valid(output))
	}
}

func TestColumnTextCutsOnACharacterBoundary(t *testing.T) {
	text := strings.Repeat("a", maximumColumnBytes-1) + "ü"
	cut := columnText(text)
	require.Len(t, cut, maximumColumnBytes-1, "the two-byte character does not fit")
}
