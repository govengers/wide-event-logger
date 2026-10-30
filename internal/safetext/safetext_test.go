package safetext

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

// cyclicPanicError panics in Error with a map that contains itself.
type cyclicPanicError struct{}

func (cyclicPanicError) Error() string {
	cycle := map[string]any{}
	cycle["self"] = cycle
	panic(cycle)
}

// nilReceiverError dereferences its receiver.
type nilReceiverError struct{ message string }

func (failure *nilReceiverError) Error() string { return failure.message }

func TestOf(t *testing.T) {
	plain := errors.New("boom")
	require.Equal(t, "boom", Of(plain, plain.Error))

	var cyclic error = cyclicPanicError{}
	require.Equal(t, "!panicked safetext.cyclicPanicError", Of(cyclic, cyclic.Error))

	var typedNil error = (*nilReceiverError)(nil)
	require.Equal(t, "!panicked *safetext.nilReceiverError", Of(typedNil, typedNil.Error))
}
