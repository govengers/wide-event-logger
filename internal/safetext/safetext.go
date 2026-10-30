// Package safetext calls the String and Error methods of values the library does not own without
// letting them crash the caller.
package safetext

import "fmt"

// Of returns call(), typically a value's Error or String method, or "!panicked <type of value>" when it
// panics, for example on a nil receiver. Unlike fmt it never formats the panic value, which may be cyclic.
func Of(value any, call func() string) (text string) {
	defer func() {
		if recover() != nil {
			text = fmt.Sprintf("!panicked %T", value)
		}
	}()
	return call()
}
