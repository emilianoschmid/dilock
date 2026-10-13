package consumer

import (
	"testing"

	dilock "example.invalid/dilock/core"
)

func TestCoreConsumer(t *testing.T) {
	if _, err := dilock.Identity("orders", "invoice/42"); err != nil {
		t.Fatal(err)
	}
	if dilock.DefaultOptions().LeaseDuration <= 0 {
		t.Fatal("default lease duration must be positive")
	}
}
