package wideevent

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSamplers(t *testing.T) {
	failure := Begin(nil, "job").Failure(nil)
	success := Begin(nil, "job").Success().Int(KeyResponseStatus, 200)
	unavailable := Begin(nil, "job").Int(KeyResponseStatus, 503)

	testCases := []struct {
		name    string
		sampler Sampler
		event   *Event
		keep    bool
	}{
		{"always", AlwaysSample(), success, true},
		{"never", NeverSample(), failure, false},
		{"on error keeps a failure", AlwaysOnError(), failure, true},
		{"on error keeps a 5xx", AlwaysOnError(), unavailable, true},
		{"on error drops a success", AlwaysOnError(), success, false},
		{"on status keeps a listed status", AlwaysOnStatus(500, 503), unavailable, true},
		{"on status drops another status", AlwaysOnStatus(500, 503), success, false},
		{"rate zero", Rate(0), success, false},
		{"rate one", Rate(1), success, true},
		{"probability zero", Probability(0), success, false},
		{"probability one", Probability(1), success, true},
		{"composite keeps when any keeps", CompositeSampler(NeverSample(), AlwaysOnError()), failure, true},
		{"composite drops when none keeps", CompositeSampler(NeverSample(), AlwaysOnError()), success, false},
		{"composite of none drops", CompositeSampler(), success, false},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			require.Equal(t, testCase.keep, testCase.sampler.ShouldSample(testCase.event))
		})
	}
}

func TestRateKeepsEveryNthEvent(t *testing.T) {
	sampler := Rate(3)
	kept := 0
	for range 9 {
		if sampler.ShouldSample(nil) {
			kept++
		}
	}
	require.Equal(t, 3, kept)
}

func TestProbabilityKeepsAboutTheGivenShare(t *testing.T) {
	sampler := Probability(0.5)
	kept := 0
	for range 10000 {
		if sampler.ShouldSample(nil) {
			kept++
		}
	}
	require.InDelta(t, 5000, kept, 500)
}
