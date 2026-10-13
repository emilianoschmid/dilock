package consumer

import (
	"testing"

	dilock "example.invalid/dilock/core"
	"example.invalid/dilock/redis"
)

func TestRedisConsumerSurface(t *testing.T) {
	if _, err := dilock.Identity("orders", "invoice/42"); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := dilock.DefaultOptions().Normalized(); err != nil {
		t.Fatal(err)
	}
	options := redis.ProviderOptions{Namespace: "orders", Epoch: "candidate"}
	if options.Namespace == "" {
		t.Fatal("provider options were not retained")
	}
}
