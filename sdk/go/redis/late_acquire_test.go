package redis

import (
	"errors"
	"testing"

	dilock "example.invalid/dilock/core"
)

func TestLateAcquireFailureRequiresAcknowledgedCleanupForWaitTimeout(t *testing.T) {
	cleanupFailure := errors.New("cleanup acknowledgement unavailable")
	for _, test := range []struct {
		name    string
		outcome dilock.ReleaseOutcome
		cause   error
		want    error
	}{
		{name: "released owner", outcome: dilock.ReleasedOwner, want: dilock.ErrWaitTimeout},
		{name: "already not owned", outcome: dilock.AlreadyNotOwned, want: dilock.ErrWaitTimeout},
		{name: "unknown acknowledgement", outcome: dilock.ReleaseUnknown, want: dilock.ErrAcquisitionUnknown},
		{name: "cleanup context expired", outcome: dilock.ReleaseUnknown, cause: cleanupFailure, want: dilock.ErrAcquisitionUnknown},
		{name: "cleanup returned error", outcome: dilock.ReleasedOwner, cause: cleanupFailure, want: dilock.ErrAcquisitionUnknown},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := lateAcquireFailure(test.outcome, test.cause)
			if !errors.Is(got, test.want) {
				t.Fatalf("lateAcquireFailure() = %v; want errors.Is(_, %v)", got, test.want)
			}
			if test.cause != nil && !errors.Is(got, test.cause) {
				t.Fatalf("lateAcquireFailure() = %v; want to preserve cleanup cause %v", got, test.cause)
			}
		})
	}
}
