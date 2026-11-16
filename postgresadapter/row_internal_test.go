package postgresadapter

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStorableJSONReplacesWhatJSONBRefuses(t *testing.T) {
	for input, expected := range map[string]string{
		`{"a":"x"}`:                  `{"a":"x"}`,
		`{"a":"\u0000"}`:             `{"a":"�"}`,
		`{"a":"\\u0000"}`:            `{"a":"\\u0000"}`, // a backslash, then the text u0000
		`{"a":"\\\u0000"}`:           `{"a":"\\�"}`,
		`{"\u0000":"\"\u0000"}`:      `{"�":"\"�"}`,
		`{"a":"\ud800"}`:             `{"a":"�"}`,
		`{"a":"\uDC00x"}`:            `{"a":"�x"}`,
		`{"a":"\ud800\u0041"}`:       `{"a":"�\u0041"}`,
		`{"a":"\ud800auDC00"}`:       `{"a":"�auDC00"}`,            // text after a high surrogate is not its pair
		`{"a":"\ud83d\ude00\u00e9"}`: `{"a":"\ud83d\ude00\u00e9"}`, // a valid pair and a plain escape stay
		"{\"a\":\"\xff\"}":           `{"a":"�"}`,
	} {
		output := storableJSON([]byte(input))
		require.Equal(t, expected, string(output), input)
		require.True(t, json.Valid(output))
	}
}

func TestColumnTextCutsOnACharacterBoundary(t *testing.T) {
	text := strings.Repeat("a", maximumColumnBytes-1) + "ü"
	cut := columnText(text)
	require.Len(t, cut, maximumColumnBytes-1, "the two-byte character does not fit")
}
