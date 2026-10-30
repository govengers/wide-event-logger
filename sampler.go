package wideevent

import (
	"math/rand/v2"
	"sync/atomic"
)

// Sampler decides whether a finished event is emitted. Use it through SampledEmitter.
type Sampler interface {
	ShouldSample(we *Event) bool
}

// SamplerFunc adapts a function to the Sampler interface.
type SamplerFunc func(we *Event) bool

// ShouldSample calls shouldSample(we).
func (shouldSample SamplerFunc) ShouldSample(we *Event) bool { return shouldSample(we) }

// AlwaysSample keeps every event.
func AlwaysSample() Sampler {
	return SamplerFunc(func(*Event) bool { return true })
}

// NeverSample drops every event.
func NeverSample() Sampler {
	return SamplerFunc(func(*Event) bool { return false })
}

// AlwaysOnError keeps the events for which HasError is true.
func AlwaysOnError() Sampler {
	return SamplerFunc(func(we *Event) bool {
		return we.HasError()
	})
}

// AlwaysOnStatus keeps the events whose response status is one of codes.
func AlwaysOnStatus(codes ...int) Sampler {
	selected := make(map[int]struct{}, len(codes))
	for _, code := range codes {
		selected[code] = struct{}{}
	}
	return SamplerFunc(func(we *Event) bool {
		_, ok := selected[we.StatusCode()]
		return ok
	})
}

// Rate keeps every Nth event, counted across all events it sees (not per second). Rate(1) keeps all,
// zero or less keeps none. It is safe for concurrent use.
func Rate(every int) Sampler {
	if every <= 0 {
		return NeverSample()
	}
	if every == 1 {
		return AlwaysSample()
	}
	var counter atomic.Int64
	return SamplerFunc(func(*Event) bool {
		return counter.Add(1)%int64(every) == 0
	})
}

// Probability keeps each event with the given probability between 0 and 1.
func Probability(probability float64) Sampler {
	if probability <= 0 {
		return NeverSample()
	}
	if probability >= 1 {
		return AlwaysSample()
	}
	return SamplerFunc(func(*Event) bool {
		return rand.Float64() < probability //nolint:gosec // G404: sampling needs no cryptographic randomness
	})
}

// CompositeSampler keeps an event when any of samplers keeps it.
func CompositeSampler(samplers ...Sampler) Sampler {
	return SamplerFunc(func(we *Event) bool {
		for _, sampler := range samplers {
			if sampler.ShouldSample(we) {
				return true
			}
		}
		return false
	})
}
